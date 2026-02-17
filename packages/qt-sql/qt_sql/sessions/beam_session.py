"""Beam optimization session — single mode: BEAM (probes + compiler).

BEAM pipeline:
1. Analyst (R1) → 8-16 independent transform probes
2. Workers (qwen, parallel) → PatchPlan JSON per probe
3. Validate (structural + equivalence + benchmark)
4. R1 Compiler shot 1 always; shot 2 only on retryable syntax/error paths
"""

from __future__ import annotations

import copy
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Callable, Tuple

from .base_session import OptimizationSession
from ..schemas import SessionResult

if TYPE_CHECKING:
    from ..pipeline import Pipeline

logger = logging.getLogger(__name__)

# LLM call timeout (seconds)
LLM_TIMEOUT_SECONDS = 600


# ── Per-Iteration Data Classes ──────────────────────────────────────────────


@dataclass
class AppliedPatch:
    """Result from applying a single patch plan to IR."""

    patch_id: str
    family: str
    transform: str
    relevance_score: float
    output_sql: Optional[str] = None
    apply_error: Optional[str] = None
    semantic_passed: bool = False
    speedup: Optional[float] = None
    status: str = "PENDING"
    explain_text: Optional[str] = None
    original_ms: Optional[float] = None
    patch_ms: Optional[float] = None
    raw_plan: Optional[dict] = None  # preserve original LLM JSON for retry/snipe
    worker_prompt: Optional[str] = None   # raw prompt sent to worker LLM
    worker_response: Optional[str] = None  # raw response from worker LLM
    worker_role: Optional[str] = None      # W1/W2/W3/W4
    description: Optional[str] = None      # analyst-provided probe target/intent


@dataclass
class PatchIterationResult:
    """Complete result from one iteration of the session."""

    iteration: int
    prompt: str
    response: str
    n_api_calls: int
    patches: List[AppliedPatch] = field(default_factory=list)
    race_result: Optional[Any] = None
    explains: Dict[str, str] = field(default_factory=dict)
    best_speedup: float = 0.0
    best_patch_id: Optional[str] = None
    best_sql: Optional[str] = None


# ── Session Class ───────────────────────────────────────────────────────────


class BeamSession(OptimizationSession):
    """Patch optimization session supporting BEAM mode."""

    # Pricing is configurable via QT_LLM_PRICING_OVERRIDES_JSON.
    # Values are USD per 1M tokens.
    _DEFAULT_PRICING_PER_1M: Dict[str, Dict[str, float]] = {
        # OpenRouter DeepSeek R1 (documented in repo notes)
        "deepseek/deepseek-r1": {"input": 0.55, "output": 2.19},
        "deepseek-reasoner": {"input": 0.55, "output": 2.19},
        "deepseek/deepseek-r1-0528": {"input": 0.55, "output": 2.19},
        # OpenRouter DeepSeek V3.2 family
        "deepseek/deepseek-v3.2": {"input": 0.26, "output": 0.38},
        "deepseek/deepseek-v3.2-exp": {"input": 0.27, "output": 0.41},
        "deepseek/deepseek-v3.2-speciale": {"input": 0.27, "output": 0.41},
        # Common OpenAI reference models
        "gpt-4o": {"input": 2.50, "output": 10.00},
        "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._llm_calls_lock = threading.Lock()
        self._llm_call_seq = 0
        self._api_call_costs: List[Dict[str, Any]] = []
        self._beam_cost_usd = 0.0
        self._beam_cost_priced_calls = 0
        self._beam_cost_unpriced_calls = 0
        self._beam_token_totals: Dict[str, int] = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "prompt_cache_hit_tokens": 0,
            "prompt_cache_miss_tokens": 0,
            "cached_tokens": 0,
            "reasoning_tokens": 0,
        }
        self._session_dir: Optional[Path] = None
        self._pricing_per_1m = dict(self._DEFAULT_PRICING_PER_1M)
        self._load_pricing_overrides()

    def _load_pricing_overrides(self) -> None:
        """Load optional pricing overrides from environment JSON."""
        raw = os.environ.get("QT_LLM_PRICING_OVERRIDES_JSON", "").strip()
        if not raw:
            return
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise ValueError("expected top-level object")
            for model_name, spec in parsed.items():
                if not isinstance(model_name, str) or not isinstance(spec, dict):
                    continue
                in_rate = spec.get("input")
                out_rate = spec.get("output")
                if isinstance(in_rate, (int, float)) and isinstance(out_rate, (int, float)):
                    self._pricing_per_1m[model_name.lower()] = {
                        "input": float(in_rate),
                        "output": float(out_rate),
                    }
        except Exception as e:
            logger.warning(
                f"[{self.query_id}] Invalid QT_LLM_PRICING_OVERRIDES_JSON: {e}"
            )

    def _reset_cost_tracking(self, session_dir: Path) -> None:
        with self._llm_calls_lock:
            self._session_dir = session_dir
            self._llm_call_seq = 0
            self._api_call_costs = []
            self._beam_cost_usd = 0.0
            self._beam_cost_priced_calls = 0
            self._beam_cost_unpriced_calls = 0
            for k in self._beam_token_totals:
                self._beam_token_totals[k] = 0
        self._write_cost_summary()

    @staticmethod
    def _int_usage(value: Any) -> int:
        try:
            return int(value or 0)
        except Exception:
            return 0

    @staticmethod
    def _normalize_model_key(model: str) -> str:
        key = (model or "").strip().lower()
        if ":" in key:
            # OpenRouter suffixes like ":free"
            key = key.split(":", 1)[0]
        return key

    def _resolve_pricing(self, model: str) -> Optional[Tuple[float, float]]:
        key = self._normalize_model_key(model)
        if not key:
            return None
        spec = self._pricing_per_1m.get(key)
        if spec:
            return spec["input"], spec["output"]
        # Try suffix match for vendor/model aliases.
        for known, rates in self._pricing_per_1m.items():
            if key.endswith(known):
                return rates["input"], rates["output"]
        return None

    def _estimate_call_cost_usd(
        self,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> Optional[float]:
        rates = self._resolve_pricing(model)
        if not rates:
            return None
        input_per_1m, output_per_1m = rates
        cost = (prompt_tokens * input_per_1m + completion_tokens * output_per_1m) / 1_000_000.0
        return round(cost, 8)

    def _write_json_atomic(self, path: Path, payload: Dict[str, Any]) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(path)

    def _cost_summary_payload(self) -> Dict[str, Any]:
        with self._llm_calls_lock:
            return {
                "query_id": self.query_id,
                "beam_cost_usd": round(self._beam_cost_usd, 8),
                "beam_cost_priced_calls": self._beam_cost_priced_calls,
                "beam_cost_unpriced_calls": self._beam_cost_unpriced_calls,
                "n_api_calls": len(self._api_call_costs),
                "beam_token_totals": dict(self._beam_token_totals),
                "models_seen": sorted(
                    {
                        str(c.get("model", ""))
                        for c in self._api_call_costs
                        if c.get("model")
                    }
                ),
            }

    def _write_cost_summary(self) -> None:
        session_dir = self._session_dir
        if not session_dir:
            return
        try:
            self._write_json_atomic(
                session_dir / "llm_cost_summary.json",
                self._cost_summary_payload(),
            )
        except Exception as e:
            logger.warning(f"[{self.query_id}] Failed to write llm_cost_summary.json: {e}")

    def _record_api_call_cost(self, call_record: Dict[str, Any]) -> None:
        with self._llm_calls_lock:
            self._api_call_costs.append(call_record)
            self._beam_token_totals["prompt_tokens"] += self._int_usage(
                call_record.get("prompt_tokens")
            )
            self._beam_token_totals["completion_tokens"] += self._int_usage(
                call_record.get("completion_tokens")
            )
            self._beam_token_totals["total_tokens"] += self._int_usage(
                call_record.get("total_tokens")
            )
            self._beam_token_totals["prompt_cache_hit_tokens"] += self._int_usage(
                call_record.get("prompt_cache_hit_tokens")
            )
            self._beam_token_totals["prompt_cache_miss_tokens"] += self._int_usage(
                call_record.get("prompt_cache_miss_tokens")
            )
            self._beam_token_totals["cached_tokens"] += self._int_usage(
                call_record.get("cached_tokens")
            )
            self._beam_token_totals["reasoning_tokens"] += self._int_usage(
                call_record.get("reasoning_tokens")
            )
            estimated_cost = call_record.get("estimated_cost_usd")
            if isinstance(estimated_cost, (int, float)):
                self._beam_cost_priced_calls += 1
                self._beam_cost_usd += float(estimated_cost)
            else:
                self._beam_cost_unpriced_calls += 1
            session_dir = self._session_dir

            if session_dir:
                try:
                    with (session_dir / "llm_calls.jsonl").open(
                        "a", encoding="utf-8"
                    ) as f:
                        f.write(json.dumps(call_record, default=str) + "\n")
                except Exception as e:
                    logger.warning(f"[{self.query_id}] Failed to append llm_calls.jsonl: {e}")

        self._write_cost_summary()

    def _session_cost_fields(self) -> Dict[str, Any]:
        with self._llm_calls_lock:
            return {
                "beam_cost_usd": round(self._beam_cost_usd, 8),
                "beam_cost_priced_calls": self._beam_cost_priced_calls,
                "beam_cost_unpriced_calls": self._beam_cost_unpriced_calls,
                "beam_token_totals": dict(self._beam_token_totals),
                "api_call_costs": list(self._api_call_costs),
            }

    def run(self) -> SessionResult:
        """Execute BEAM optimization loop (single mode)."""
        logger.info(f"[{self.query_id}] BEAM MODE: BEAM")
        return self._run_beam()

    def run_editor_strike(self, transform_id: str = "") -> SessionResult:
        """Single-call strike path for editor mode.

        Strike mode is intentionally lightweight:
        - no analyst phase
        - no probe fan-out
        - no compiler rounds
        - one worker LLM call scoped to one transform id
        """
        from ..ir import build_script_ir, render_ir_node_map, Dialect
        from ..patches.beam_wide_prompts import build_beam_editor_strike_prompt

        strike_transform = (transform_id or "").strip() or "auto"
        logger.info(
            f"[{self.query_id}] STRIKE MODE: transform={strike_transform}"
        )

        db_path = (
            self.pipeline.config.benchmark_dsn
            or self.pipeline.config.db_path_or_dsn
        )
        dialect_upper = self.dialect.upper()
        dialect_enum = (
            Dialect[dialect_upper]
            if dialect_upper in Dialect.__members__
            else Dialect.POSTGRES
        )
        transform_family = self._lookup_transform_family(strike_transform)
        session_dir = self._create_session_dir()
        script_ir = build_script_ir(self.original_sql, dialect_enum)
        ir_node_map = render_ir_node_map(script_ir)
        beam_edit_mode = self._beam_edit_mode()
        dag_mode = beam_edit_mode == "dag"
        base_dag = self._build_base_dag(self.original_sql) if dag_mode else None
        base_dag_prompt = self._render_dag_for_prompt(base_dag) if base_dag else ""
        beam_provider_override, beam_model_override = self._beam_llm_override()
        logger.info(f"[{self.query_id}] BEAM edit mode: {beam_edit_mode}")

        if self.on_phase_change:
            self.on_phase_change(phase="strike_prepare", iteration=0)

        explain_result = self._get_original_explain_cached(db_path)
        original_explain = self._render_explain_compact(
            explain_result, self.dialect
        )
        schema_context = self._build_schema_context(db_path)

        strike_prompt = build_beam_editor_strike_prompt(
            query_id=self.query_id,
            original_sql=self.original_sql,
            explain_text=original_explain,
            ir_node_map=ir_node_map,
            transform_id=strike_transform,
            dialect=self.dialect,
            schema_context=schema_context,
        )
        if dag_mode:
            strike_prompt = (
                strike_prompt
                + "\n\n"
                + self._worker_dag_mode_suffix(base_dag_prompt)
            )
        self._save_to_disk(session_dir, 0, "strike_prompt", strike_prompt)

        beam_provider_override, beam_model_override = self._beam_llm_override()
        worker_call_fn = self._make_llm_call_fn(
            provider_spec=beam_provider_override,
            model_spec=beam_model_override,
        )

        if self.on_phase_change:
            self.on_phase_change(phase="strike_worker", iteration=0)
        strike_response = worker_call_fn(strike_prompt)
        self._save_to_disk(session_dir, 0, "strike_response", strike_response)

        output_sql = self._apply_worker_response_compat(
            strike_response,
            script_ir,
            dialect_enum,
            dag_base=base_dag,
        )
        patch = AppliedPatch(
            patch_id="strike_01",
            family=transform_family,
            transform=strike_transform,
            relevance_score=1.0,
            output_sql=output_sql,
            status="applied" if output_sql else "FAIL",
            worker_prompt=strike_prompt,
            worker_response=strike_response,
            description=f"Editor strike for transform={strike_transform}",
        )
        if not output_sql:
            patch.apply_error = "Failed to parse/apply strike response"

        if output_sql:
            if self.on_phase_change:
                self.on_phase_change(phase="benchmark", iteration=0)
            self._validate_and_benchmark_patches(
                [patch], db_path, session_dir, 0
            )

        explains = (
            {patch.patch_id: patch.explain_text}
            if patch.explain_text
            else {}
        )
        best_speedup = 0.0
        best_sql = self.original_sql
        best_transforms: List[str] = []
        best_status = "NEUTRAL"

        if patch.semantic_passed and patch.speedup is not None:
            best_speedup = patch.speedup
            best_sql = patch.output_sql or self.original_sql
            best_transforms = [patch.transform]
            best_status = self._classify_speedup(patch.speedup)
        elif patch.status in ("FAIL", "ERROR"):
            best_status = patch.status

        iter_result = PatchIterationResult(
            iteration=0,
            prompt=strike_prompt,
            response=strike_response,
            n_api_calls=1,
            patches=[patch],
            explains=explains,
            best_speedup=best_speedup,
            best_patch_id=patch.patch_id if best_transforms else None,
            best_sql=best_sql if best_transforms else None,
        )

        self._save_to_disk(
            session_dir,
            0,
            "result",
            json.dumps(self._serialize_iteration(iter_result), indent=2, default=str),
        )

        return SessionResult(
            query_id=self.query_id,
            mode="strike",
            best_speedup=best_speedup,
            best_sql=best_sql,
            original_sql=self.original_sql,
            best_transforms=best_transforms,
            status=best_status,
            iterations=[self._serialize_iteration(iter_result)],
            n_iterations=1,
            n_api_calls=1,
            **self._session_cost_fields(),
        )

    @staticmethod
    def _render_explain_compact(explain_result: Optional[dict], dialect: str = "duckdb") -> str:
        """Render EXPLAIN result as compact operator tree.

        Uses structured plan_json when available (DuckDB: ~40 lines vs ~230 box-drawing).
        Falls back to plan_text if no JSON plan.

        IMPORTANT: This is the ONLY entry point for EXPLAIN into prompts.
        The compact format is what analysts, workers, and compilers all see.
        Raw box-drawing must NEVER leak through.
        """
        if not explain_result:
            return "(EXPLAIN unavailable)"

        from ..prompts.analyst_briefing import format_duckdb_explain_tree

        plan_json = explain_result.get("plan_json")
        plan_text = explain_result.get("plan_text", "")

        if dialect.lower() == "duckdb":
            if plan_json and isinstance(plan_json, dict) and plan_json.get("children"):
                import json as _json
                rendered = format_duckdb_explain_tree(_json.dumps(plan_json))
                if rendered:
                    return rendered
            # plan_text may be JSON string or box-drawing.
            # format_duckdb_explain_tree handles JSON; box-drawing falls through.
            if plan_text:
                rendered = format_duckdb_explain_tree(plan_text)
                # Guard: if parser returned box-drawing unchanged, reject it
                if "┌" not in rendered and "└" not in rendered:
                    return rendered
                # Box-drawing leaked — return a warning instead of 300 lines of noise
                return "(EXPLAIN: compact rendering unavailable — re-run with JSON EXPLAIN)"

        # PG / Snowflake fallback
        if plan_text:
            return plan_text
        return "(EXPLAIN unavailable)"

    def _get_original_explain_cached(self, db_path: str) -> Optional[dict]:
        """Load original-query EXPLAIN from cache only in optimization paths."""
        get_explain = getattr(self.pipeline, "_get_explain", None)
        if callable(get_explain):
            try:
                return get_explain(
                    self.query_id,
                    self.original_sql,
                    collect_if_missing=False,
                )
            except TypeError:
                # Compatibility for older helper signatures.
                return get_explain(self.query_id, self.original_sql)
            except Exception as e:
                logger.warning(
                    f"[{self.query_id}] Failed to load cached EXPLAIN: {e}"
                )
                return None

        # Test-stub fallback: use direct explain when Pipeline helper is unavailable.
        from ..execution.database_utils import run_explain_analyze
        return run_explain_analyze(db_path, self.original_sql)

    def _beam_edit_mode(self) -> str:
        """Return BEAM edit representation mode."""
        mode = str(
            getattr(self.pipeline.config, "beam_edit_mode", "patchplan")
            or "patchplan"
        ).strip().lower()
        return "dag" if mode == "dag" else "patchplan"

    @staticmethod
    def _extract_json_value(text: str) -> Optional[Any]:
        """Extract top-level JSON value (object or array) from model output."""
        raw = (text or "").strip()
        if not raw:
            return None

        # Remove fenced wrappers if present.
        if raw.startswith("```"):
            lines = raw.splitlines()
            if lines:
                lines = lines[1:]
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            raw = "\n".join(lines).strip()

        try:
            return json.loads(raw)
        except Exception:
            pass

        # Fallback: find first balanced JSON object/array.
        for open_ch, close_ch in (("{", "}"), ("[", "]")):
            start = raw.find(open_ch)
            if start < 0:
                continue
            depth = 0
            in_str = False
            esc = False
            for idx in range(start, len(raw)):
                ch = raw[idx]
                if in_str:
                    if esc:
                        esc = False
                    elif ch == "\\":
                        esc = True
                    elif ch == '"':
                        in_str = False
                    continue
                if ch == '"':
                    in_str = True
                    continue
                if ch == open_ch:
                    depth += 1
                elif ch == close_ch:
                    depth -= 1
                    if depth == 0:
                        candidate = raw[start : idx + 1]
                        try:
                            return json.loads(candidate)
                        except Exception:
                            break
        return None

    def _extract_dag_candidates(self, response: str) -> List[Dict[str, Any]]:
        """Parse DAG plan candidates from worker/compiler output."""
        payload = self._extract_json_value(response)
        if payload is None:
            return []

        items: List[Any]
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            plans = payload.get("plans")
            if isinstance(plans, list):
                items = plans
            else:
                items = [payload]
        else:
            return []

        candidates: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if isinstance(item.get("dag"), dict):
                candidates.append(item)
                continue
            if isinstance(item.get("nodes"), list) or isinstance(item.get("nodes"), dict):
                # Allow bare DAG object (without enclosing "dag").
                candidates.append(
                    {
                        "plan_id": item.get("plan_id"),
                        "family": item.get("family"),
                        "transform": item.get("transform"),
                        "dag": item,
                    }
                )
        return candidates

    def _extract_dag_outputs(self, sql_text: str) -> List[str]:
        """Extract projected output column names from a SQL fragment."""
        try:
            import sqlglot
            from sqlglot import exp
        except Exception:
            return []
        try:
            ast = sqlglot.parse_one(sql_text, dialect=self.dialect)
            selects = []
            if hasattr(ast, "selects") and ast.selects:
                selects = list(ast.selects)
            elif isinstance(ast, exp.Select):
                selects = list(ast.expressions)
            out: List[str] = []
            for e in selects:
                alias = getattr(e, "alias_or_name", None) or getattr(e, "output_name", None)
                if alias:
                    out.append(str(alias))
                else:
                    out.append((e.sql(dialect=self.dialect) or "").strip())
            return out
        except Exception:
            return []

    def _extract_dag_deps(self, sql_text: str, known_nodes: List[str]) -> List[str]:
        """Extract dependencies on known DAG node ids from table references."""
        if not known_nodes:
            return []
        known = {str(x).lower(): str(x) for x in known_nodes}
        deps: List[str] = []
        try:
            import sqlglot
            from sqlglot import exp
            ast = sqlglot.parse_one(sql_text, dialect=self.dialect)
            for table in ast.find_all(exp.Table):
                name = str(table.name or "").strip()
                if not name:
                    continue
                key = name.lower()
                if key in known and known[key] not in deps:
                    deps.append(known[key])
        except Exception:
            return []
        return deps

    def _build_base_dag(self, sql_text: str) -> Dict[str, Any]:
        """Build a lightweight DAG from query CTEs + final select."""
        base = {
            "order": ["final_select"],
            "final_node_id": "final_select",
            "nodes": [
                {
                    "node_id": "final_select",
                    "deps": [],
                    "grain": [],
                    "keys": {},
                    "outputs": self._extract_dag_outputs(sql_text),
                    "sql": sql_text.strip().rstrip(";"),
                }
            ],
        }
        try:
            import sqlglot
            ast = sqlglot.parse_one(sql_text, dialect=self.dialect)
        except Exception:
            return base

        with_expr = ast.args.get("with")
        if with_expr is None or not getattr(with_expr, "expressions", None):
            return base

        ctes = list(with_expr.expressions or [])
        cte_ids: List[str] = []
        for idx, cte in enumerate(ctes, start=1):
            name = str(cte.alias_or_name or f"cte_{idx}")
            if name in cte_ids:
                name = f"{name}_{idx}"
            cte_ids.append(name)

        nodes: List[Dict[str, Any]] = []
        for idx, cte in enumerate(ctes):
            node_id = cte_ids[idx]
            cte_sql = (cte.this.sql(dialect=self.dialect) or "").strip().rstrip(";")
            nodes.append(
                {
                    "node_id": node_id,
                    "deps": self._extract_dag_deps(cte_sql, cte_ids),
                    "grain": [],
                    "keys": {},
                    "outputs": self._extract_dag_outputs(cte_sql),
                    "sql": cte_sql,
                }
            )

        final_ast = ast.copy()
        final_ast.set("with", None)
        final_sql = (final_ast.sql(dialect=self.dialect) or sql_text).strip().rstrip(";")
        nodes.append(
            {
                "node_id": "final_select",
                "deps": self._extract_dag_deps(final_sql, cte_ids),
                "grain": [],
                "keys": {},
                "outputs": self._extract_dag_outputs(final_sql),
                "sql": final_sql,
            }
        )
        return {
            "order": cte_ids + ["final_select"],
            "final_node_id": "final_select",
            "nodes": nodes,
        }

    def _render_dag_for_prompt(self, dag: Dict[str, Any]) -> str:
        """Render compact DAG spec for prompt context."""
        lines = [
            "## Base DAG Spec",
            "Use this as the authoritative node graph for rewrite proposals.",
            "",
        ]
        for node in dag.get("nodes", []):
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("node_id", "")).strip()
            deps = node.get("deps") or []
            outs = node.get("outputs") or []
            lines.append(f"node: {node_id}")
            lines.append(f"  deps: {deps}")
            lines.append(f"  outputs: {outs}")
            lines.append("  sql: OMITTED")
            lines.append("")
        lines.append(f"order: {dag.get('order', [])}")
        lines.append(f"final_node_id: {dag.get('final_node_id', 'final_select')}")
        return "\n".join(lines)

    @staticmethod
    def _worker_dag_mode_suffix(base_dag_spec: str) -> str:
        """Worker DAG-mode runtime contract (takes precedence over template text)."""
        return (
            "## Runtime Override: DAG Mode (Takes Precedence)\n"
            "Ignore PatchPlan output requirements above.\n"
            "Output ONE JSON object with a `dag` payload.\n"
            "Worker constraints:\n"
            "- exactly ONE changed node\n"
            "- changed node must include full executable SQL in `sql`\n"
            "- unchanged nodes should omit `sql`\n"
            "- first character must be `{` (no prose/markdown)\n\n"
            "Expected shape:\n"
            "{\n"
            "  \"probe_id\": \"pNN\",\n"
            "  \"transform_id\": \"...\",\n"
            "  \"family\": \"A|B|C|D|E|F\",\n"
            "  \"hypothesis\": \"...\",\n"
            "  \"dag\": {\n"
            "    \"order\": [\"...\"],\n"
            "    \"final_node_id\": \"final_select\",\n"
            "    \"nodes\": [\n"
            "      {\"node_id\": \"...\", \"deps\": [\"...\"], \"outputs\": [\"...\"], \"changed\": true, \"sql\": \"SELECT ...\"}\n"
            "    ]\n"
            "  }\n"
            "}\n\n"
            f"{base_dag_spec}"
        )

    @staticmethod
    def _worker_lane_suffix(lane: str) -> str:
        """Lane-specific behavior contract appended to worker prompts."""
        lane_norm = str(lane or "").strip().lower()
        if lane_norm == "reasoner":
            return (
                "## Runtime Override: Reasoner Lane\n"
                "You are the high-reasoning lane.\n"
                "- You may combine families or rewrite shapes when evidence supports it.\n"
                "- You are not constrained to a single-family tactic.\n"
                "- Preserve semantics and hard bans.\n"
            )
        return (
            "## Runtime Override: Qwen Lane\n"
            "You are the wide exploration lane.\n"
            "- Stay within ONE family strategy: the assigned `family` and `transform_id`.\n"
            "- Do not combine multiple families in one rewrite.\n"
            "- Preserve semantics and hard bans.\n"
        )

    @staticmethod
    def _compiler_dag_mode_suffix(base_dag_spec: str) -> str:
        """Compiler DAG-mode runtime contract (takes precedence over template text)."""
        return (
            "## Runtime Override: DAG Mode (Takes Precedence)\n"
            "Ignore PatchPlan output requirements above.\n"
            "Compiler may output ONE or TWO attempts.\n"
            "No constraint on number of changed nodes.\n"
            "Output must be JSON object or JSON array (length 1-2), no prose/markdown.\n"
            "Each attempt should include `plan_id` and `dag`; include full SQL for changed nodes.\n\n"
            "Accepted example:\n"
            "[\n"
            "  {\"plan_id\": \"snipe_p1\", \"hypothesis\": \"...\", \"dag\": {\"order\": [\"...\"], \"nodes\": [{\"node_id\":\"...\",\"changed\":true,\"sql\":\"SELECT ...\"}]}}\n"
            "]\n\n"
            f"{base_dag_spec}"
        )

    @staticmethod
    def _append_dag_shot_results(base_prompt: str, patches: List[AppliedPatch]) -> str:
        """Append shot results with DAG-mode compiler instructions."""
        lines = [base_prompt, "", "## Shot 1 Results", ""]
        lines.append("| # | Transform | Speedup | Status | Error |")
        lines.append("|---|-----------|---------|--------|-------|")
        for p in patches:
            speedup = f"{p.speedup:.2f}x" if p.speedup is not None else "-"
            err = (p.apply_error or "").replace("\n", " ")
            lines.append(
                f"| {p.patch_id} | {p.transform} | {speedup} | {p.status} | {err} |"
            )
        lines.extend(
            [
                "",
                "## Shot 2 — DAG Mode",
                "Return one or two DAG attempts as JSON (object or array).",
                "No changed-node-count constraints.",
                "Preserve semantics and literals.",
            ]
        )
        return "\n".join(lines)

    def _compile_dag_candidate_sql(
        self,
        candidate: Dict[str, Any],
        base_dag: Dict[str, Any],
        *,
        strict_single_change: bool,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Compile a DAG candidate into executable SQL."""
        dag = candidate.get("dag")
        if not isinstance(dag, dict):
            return None, "Missing `dag` object"

        base_nodes = {}
        for node in base_dag.get("nodes", []):
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("node_id", "")).strip()
            sql = str(node.get("sql", "") or "").strip()
            if node_id and sql:
                base_nodes[node_id] = sql.rstrip(";")

        order = dag.get("order")
        if not isinstance(order, list) or not order:
            order = list(base_dag.get("order") or [])
        order = [str(x).strip() for x in order if str(x).strip()]
        if not order:
            return None, "DAG order is empty"

        final_node_id = str(
            dag.get("final_node_id")
            or base_dag.get("final_node_id")
            or order[-1]
        ).strip()
        if not final_node_id:
            return None, "Missing final_node_id"

        node_items = dag.get("nodes")
        if isinstance(node_items, dict):
            node_items = list(node_items.values())
        if not isinstance(node_items, list):
            node_items = []

        changed_count = 0
        for item in node_items:
            if not isinstance(item, dict):
                continue
            node_id = str(item.get("node_id") or item.get("id") or "").strip()
            if not node_id:
                continue
            sql_text = item.get("sql")
            changed = bool(item.get("changed"))
            if isinstance(sql_text, str) and sql_text.strip():
                base_nodes[node_id] = sql_text.strip().rstrip(";")
                changed = True
            if changed:
                changed_count += 1

        if strict_single_change and changed_count != 1:
            return None, f"Worker DAG must change exactly one node (got {changed_count})"

        for node_id in order:
            if node_id not in base_nodes:
                return None, f"Missing SQL for DAG node `{node_id}`"
        if final_node_id not in base_nodes:
            return None, f"Missing SQL for final node `{final_node_id}`"

        final_sql = base_nodes[final_node_id].strip().rstrip(";")
        cte_ids = [nid for nid in order if nid != final_node_id]
        if not cte_ids:
            return final_sql + ";", None

        with_parts = [f"{nid} AS ({base_nodes[nid].strip().rstrip(';')})" for nid in cte_ids]
        if final_sql.lower().startswith("with "):
            # Final node already carries a complete WITH query.
            return final_sql + ";", None
        return f"WITH {', '.join(with_parts)} {final_sql};", None

    def _apply_dag_worker_response(
        self,
        response: str,
        base_dag: Dict[str, Any],
    ) -> Optional[str]:
        """Apply worker DAG response and return SQL if valid."""
        candidates = self._extract_dag_candidates(response)
        if not candidates:
            return None
        sql, _err = self._compile_dag_candidate_sql(
            candidates[0], base_dag, strict_single_change=True
        )
        return sql

    def _apply_dag_compiler_response(
        self,
        response: str,
        base_dag: Dict[str, Any],
        *,
        prefix: str,
    ) -> List[AppliedPatch]:
        """Apply compiler DAG response (1-2 attempts) into AppliedPatch objects."""
        candidates = self._extract_dag_candidates(response)
        if not candidates:
            return []

        patches: List[AppliedPatch] = []
        for idx, candidate in enumerate(candidates[:2], start=1):
            plan_id = str(candidate.get("plan_id") or f"{prefix}_{idx}")
            family = str(candidate.get("family") or "?")
            transform = str(candidate.get("transform") or "dag_rewrite")
            sql, err = self._compile_dag_candidate_sql(
                candidate, base_dag, strict_single_change=False
            )
            patch = AppliedPatch(
                patch_id=plan_id,
                family=family,
                transform=transform,
                relevance_score=1.0,
                output_sql=sql,
                status="applied" if sql else "FAIL",
                raw_plan=candidate,
            )
            if not sql:
                patch.apply_error = err or "Failed to parse/apply DAG plan"
            patches.append(patch)
        return patches

    def _apply_patchplan_array(
        self,
        response: str,
        script_ir,
        dialect_enum,
        prefix: str = "r",
    ) -> List[AppliedPatch]:
        """Parse a JSON array of PatchPlans from R1 response and apply each to IR.

        Args:
            response: Raw R1 response containing a JSON array of PatchPlan objects.
            script_ir: The script IR to apply patches to (deep-copied per patch).
            dialect_enum: Dialect enum for IR rendering.
            prefix: Prefix for patch IDs (e.g., "r1", "r2").

        Returns:
            List of AppliedPatch objects (may include failures).
        """
        import copy as _copy
        from ..patches.beam_patch_validator import _extract_json_array
        from ..ir import dict_to_plan, apply_patch_plan

        def _is_patchplan_obj(obj: Any) -> bool:
            return isinstance(obj, dict) and isinstance(obj.get("steps"), list)

        plans_data = _extract_json_array(response)
        if plans_data:
            # Keep only patch-plan-like dicts with steps[].
            plans_data = [p for p in plans_data if _is_patchplan_obj(p)]

        if not plans_data:
            logger.warning(f"[{self.query_id}] No PatchPlan JSON found in R1 response")
            return []

        patches = []
        for i, plan_data in enumerate(plans_data[:4]):
            if not isinstance(plan_data, dict):
                continue

            patch_id = plan_data.get("plan_id", f"{prefix}_{i+1}")
            family = plan_data.get("family", "?")
            transform = plan_data.get("transform", "unknown")

            patch = AppliedPatch(
                patch_id=patch_id,
                family=family,
                transform=transform,
                relevance_score=1.0,
            )

            # Ensure dialect is set
            if "dialect" not in plan_data:
                plan_data["dialect"] = self.dialect

            # Compatibility normalization: some responses still emit payload.sql
            # for fragment-based ops. Canonical key is payload.sql_fragment.
            for step in plan_data.get("steps", []):
                if not isinstance(step, dict):
                    continue
                payload = step.get("payload")
                if not isinstance(payload, dict):
                    continue
                if "sql_fragment" in payload:
                    continue
                raw_sql = payload.get("sql")
                if not isinstance(raw_sql, str):
                    continue
                if step.get("op") in {
                    "replace_body",
                    "replace_select",
                    "replace_block_with_cte_pair",
                }:
                    payload["sql_fragment"] = raw_sql

            try:
                ir_copy = _copy.deepcopy(script_ir)
                plan = dict_to_plan(plan_data)
                result = apply_patch_plan(ir_copy, plan)

                if result.success and result.output_sql:
                    patch.output_sql = result.output_sql
                    patch.raw_plan = plan_data
                else:
                    error_msg = "; ".join(result.errors) if result.errors else "Unknown apply error"
                    patch.apply_error = error_msg
                    patch.status = "FAIL"
            except Exception as e:
                patch.apply_error = str(e)
                patch.status = "FAIL"

            patches.append(patch)

        return patches

    def _is_compiler_tier0_shape_failure(
        self,
        response: str,
        *,
        dag_mode: bool = False,
    ) -> bool:
        """True when compiler output violates expected top-level shape."""
        if dag_mode:
            return len(self._extract_dag_candidates(response)) == 0

        from ..patches.beam_patch_validator import _extract_json_array

        def _is_patchplan_obj(obj: Any) -> bool:
            return isinstance(obj, dict) and isinstance(obj.get("steps"), list)

        plans_data = _extract_json_array(response)
        if not isinstance(plans_data, list):
            return True
        if len(plans_data) == 0:
            return True
        if any(not _is_patchplan_obj(item) for item in plans_data):
            return True
        return False

    def _build_compiler_tier0_retry_prompt(
        self,
        base_prompt: str,
        *,
        dag_mode: bool = False,
    ) -> str:
        """Append hard shape feedback for one compiler retry."""
        if dag_mode:
            return (
                base_prompt
                + "\n\n## RETRY — Output Shape Failure (DAG Mode)\n"
                + "Your previous output was not parseable as DAG JSON.\n"
                + "Return JSON object or JSON array (length 1-2) only.\n"
                + "Each attempt must include a `dag` object with `nodes` and `order`.\n"
                + "No markdown/prose."
            )
        return (
            base_prompt
            + "\n\n## RETRY — Tier-0 Output Contract Failure\n"
            + "Your previous output had invalid JSON shape for PatchPlans.\n"
            + "Return valid JSON only.\n"
            + "Contract:\n"
            + "- first character must be [\n"
            + "- no leading whitespace/newlines\n"
            + "- top-level value must be an array of exactly 2 objects\n"
            + "- no markdown fences, no prose, no commentary\n"
            + "- based_on must be a string (not an array)\n"
            + "- never emit payload.sql; use payload.sql_fragment\n"
        )

    def _is_compiler_retry_error_patch(self, patch: AppliedPatch) -> bool:
        """True when a patch indicates retry-worthy syntax/parse/error failures."""
        status = str(patch.status or "").upper()
        err = str(patch.apply_error or "").lower()

        if status == "ERROR":
            return True
        if status == "FAIL" and not (patch.output_sql or "").strip():
            # Parse/apply failure before SQL is even runnable.
            return True

        retry_markers = (
            "tier-1:",
            "syntax",
            "parse",
            "json",
            "structural",
            "failed to parse/apply",
        )
        return any(marker in err for marker in retry_markers)

    def _categorize_probe_failure(self, patch: AppliedPatch) -> str:
        """Classify probe outcome for compiler evidence synthesis."""
        status = str(patch.status or "").upper()
        err = str(patch.apply_error or "").lower()

        if status in {"WIN", "IMPROVED", "NEUTRAL"}:
            return "none"
        if status == "REGRESSION":
            return "regression"
        if "timeout" in err:
            return "timeout"

        if any(
            marker in err
            for marker in (
                "row count:",
                "checksum mismatch",
                "synthetic semantic mismatch",
                "equivalence check unavailable",
            )
        ):
            return "equivalence_fail"

        if any(
            marker in err
            for marker in (
                "tier-1:",
                "syntax",
                "parse",
                "json",
                "failed to parse/apply",
                "execution:",
            )
        ):
            return "syntax_error"

        if "semantic" in err:
            return "semantic_violation"

        return "semantic_violation"

    def _validate_and_benchmark_patches(
        self,
        patches: List[AppliedPatch],
        db_path: str,
        session_dir: Path,
        shot: int,
    ) -> None:
        """Validate (structural + synthetic semantics + equivalence) and benchmark patches in-place.

        Modifies patches in-place: sets semantic_passed, speedup, status, explain_text.
        """
        from ..validation.mini_validator import MiniValidator
        from ..execution.factory import create_executor_from_dsn
        from ..validation.equivalence_checker import EquivalenceChecker
        from ..execution.database_utils import run_explain_analyze

        applied = [p for p in patches if p.output_sql]
        if not applied:
            return

        # ── Tier-1 structural check ───────────────────────────────────
        tier1 = MiniValidator(db_path=db_path, dialect=self.dialect, sample_pct=0)
        for p in applied:
            t1 = tier1._tier1_structural(self.original_sql, p.output_sql)
            if not t1.get("passed", True):
                errors = t1.get("errors", ["Structural check failed"])
                p.semantic_passed = False
                p.status = "FAIL"
                p.apply_error = f"Tier-1: {'; '.join(errors)}"
            else:
                p.semantic_passed = True

        # ── Synthetic semantic gate (optional) ────────────────────────
        synth_passed = [p for p in applied if p.semantic_passed]
        if self.pipeline.config.semantic_validation_enabled:
            from ..validation.synthetic_validator import SyntheticValidator

            if synth_passed:
                try:
                    synth = SyntheticValidator(reference_db=db_path, dialect=self.dialect)
                except Exception as e:
                    logger.warning(f"[{self.query_id}] Synthetic validator init failed: {e}")
                    for p in synth_passed:
                        p.semantic_passed = False
                        p.status = "ERROR"
                        p.apply_error = f"Synthetic semantic gate unavailable: {e}"
                    synth_passed = []

            for p in synth_passed:
                try:
                    synth_result = synth.validate_sql_pair(
                        original_sql=self.original_sql,
                        optimized_sql=p.output_sql or "",
                        target_rows=100,
                    )
                except Exception as e:
                    p.semantic_passed = False
                    p.status = "ERROR"
                    p.apply_error = f"Synthetic semantic check failed: {e}"
                    continue

                if not synth_result.get("match", False):
                    p.semantic_passed = False
                    p.status = "FAIL"
                    reason = synth_result.get("reason") or "Synthetic semantic mismatch"
                    p.apply_error = f"Synthetic semantic mismatch: {reason}"
        else:
            logger.info(f"[{self.query_id}] Synthetic validation disabled; using structural + equivalence gates")

        # ── Full-dataset equivalence (after synthetic gate) ───────────
        equiv_passed = [p for p in synth_passed if p.semantic_passed]
        if equiv_passed:
            checker = EquivalenceChecker()
            try:
                with create_executor_from_dsn(db_path) as executor:
                    orig_result = executor.execute(self.original_sql)
                    orig_rows = orig_result if isinstance(orig_result, list) else []
                    orig_count = len(orig_rows)
                    orig_checksum = None
                    if orig_rows:
                        try:
                            orig_checksum = checker.compute_checksum(orig_rows)
                        except Exception:
                            pass

                    for p in equiv_passed:
                        try:
                            patch_result = executor.execute(p.output_sql)
                            patch_rows = patch_result if isinstance(patch_result, list) else []
                            patch_count = len(patch_rows)

                            if patch_count != orig_count:
                                p.semantic_passed = False
                                p.status = "FAIL"
                                p.apply_error = f"Row count: orig={orig_count}, patch={patch_count}"
                            elif orig_checksum and patch_rows:
                                try:
                                    pc = checker.compute_checksum(patch_rows)
                                    if pc != orig_checksum:
                                        p.semantic_passed = False
                                        p.status = "FAIL"
                                        p.apply_error = "Checksum mismatch"
                                except Exception:
                                    pass
                        except Exception as e:
                            p.semantic_passed = False
                            p.status = "ERROR"
                            p.apply_error = f"Execution: {e}"
            except Exception as e:
                logger.warning(f"[{self.query_id}] Equiv check failed: {e}")
                # Do not allow structurally-valid patches to pass without
                # full-dataset equivalence confirmation.
                for p in equiv_passed:
                    p.semantic_passed = False
                    p.status = "ERROR"
                    p.apply_error = f"Equivalence check unavailable: {e}"

        # ── Benchmark ─────────────────────────────────────────────────
        sem_passed = [p for p in applied if p.semantic_passed]
        if sem_passed:
            from contextlib import nullcontext
            bench_ctx = self.benchmark_lock if self.benchmark_lock else nullcontext()
            with bench_ctx:
                self._sequential_benchmark(sem_passed, db_path)

        # ── EXPLAIN collection ────────────────────────────────────────
        for p in sem_passed:
            if p.output_sql:
                try:
                    exp_data = run_explain_analyze(db_path, p.output_sql)
                    p.explain_text = self._render_explain_compact(exp_data, self.dialect)
                except Exception as e:
                    logger.warning(f"EXPLAIN failed for {p.patch_id}: {e}")

    # ── BEAM Mode ─────────────────────────────────────────────────────────

    def _run_beam(self, baseline_ms: Optional[float] = None) -> SessionResult:
        """BEAM mode: mixed worker lanes + compiler with retry-gated shot2.

        Pipeline:
        1. Analyst (R1) → 8-16 independent transform probes
        2. Workers (reasoner lane + qwen lane, parallel) → execute probes
        3. Validate + benchmark all probes
        4. R1 Compiler shot1, then shot2 only for retryable syntax/error failures
        """
        from ..ir import build_script_ir, render_ir_node_map, Dialect
        from ..patches.beam_prompt_builder import (
            load_gold_examples,
            build_beam_compiler_prompt,
            append_shot_results,
            _load_engine_intelligence,
        )
        from ..patches.beam_wide_prompts import (
            build_beam_analyst_prompt,
            build_beam_worker_prompt,
            build_beam_worker_retry_prompt,
            parse_analyst_response,
            _load_gold_example_for_family,
            _load_gold_example_by_id,
        )

        target_speedup = self.target_speedup or getattr(
            self.pipeline.config, "target_speedup", 10.0
        )
        max_probes = getattr(self.pipeline.config, "wide_max_probes", 16)

        logger.info(
            f"[{self.query_id}] BeamSession BEAM: "
            f"max {max_probes} probes, target {target_speedup:.1f}x"
        )

        # ── Setup ──────────────────────────────────────────────────────
        db_path = (
            self.pipeline.config.benchmark_dsn
            or self.pipeline.config.db_path_or_dsn
        )
        dialect_upper = self.dialect.upper()
        dialect_enum = (
            Dialect[dialect_upper]
            if dialect_upper in Dialect.__members__
            else Dialect.POSTGRES
        )

        session_dir = self._create_session_dir()
        script_ir = build_script_ir(self.original_sql, dialect_enum)
        ir_node_map = render_ir_node_map(script_ir)
        beam_edit_mode = self._beam_edit_mode()
        dag_mode = beam_edit_mode == "dag"
        base_dag = self._build_base_dag(self.original_sql) if dag_mode else None
        base_dag_prompt = self._render_dag_for_prompt(base_dag) if base_dag else ""
        beam_provider_override, beam_model_override = self._beam_llm_override()
        logger.info(f"[{self.query_id}] BEAM edit mode: {beam_edit_mode}")

        explain_result = self._get_original_explain_cached(db_path)
        original_explain = self._render_explain_compact(
            explain_result, self.dialect
        )
        baseline_ms = None
        if isinstance(explain_result, dict):
            ems = explain_result.get("execution_time_ms")
            if isinstance(ems, (int, float)) and ems > 0:
                baseline_ms = float(ems)
        importance_stars = self._compute_importance_stars(baseline_ms)
        logger.info(
            f"[{self.query_id}] Runtime importance: {'*' * importance_stars} "
            f"(baseline={baseline_ms if baseline_ms is not None else 'unknown'}ms)"
        )
        schema_context = self._build_schema_context(db_path)
        if schema_context:
            logger.info(f"[{self.query_id}] Schema context attached to beam prompts")
        engine_knowledge = _load_engine_intelligence(self.dialect) or ""
        if engine_knowledge:
            logger.info(f"[{self.query_id}] Engine knowledge attached to beam prompts")

        gold_examples = load_gold_examples(self.dialect)
        total_api_calls = 0

        # ── Intelligence Brief ────────────────────────────────────────
        intelligence_brief = ""
        try:
            from ..detection import detect_transforms, load_transforms
            from ..patches.pathology_classifier import build_intelligence_brief

            transforms_catalog = load_transforms()
            detected = detect_transforms(
                self.original_sql, transforms_catalog,
                dialect=self.dialect,
            )
            classification = self._load_cached_classification(self.query_id)
            intelligence_brief = build_intelligence_brief(
                detected,
                classification,
                runtime_dialect=self.dialect,
            )
        except Exception as e:
            logger.warning(f"[{self.query_id}] Intelligence brief failed: {e}")

        # ── Phase 1: Analyst → 8-16 probes ─────────────────────────
        if self.on_phase_change:
            self.on_phase_change(phase="analyst", iteration=0)

        analyst_call_fn = self._make_llm_call_fn(
            provider_spec=beam_provider_override,
            model_spec=beam_model_override,
        )

        analyst_prompt_base = build_beam_analyst_prompt(
            query_id=self.query_id,
            original_sql=self.original_sql,
            explain_text=original_explain,
            ir_node_map=ir_node_map,
            gold_examples=gold_examples,
            dialect=self.dialect,
            intelligence_brief=intelligence_brief,
            importance_stars=importance_stars,
            schema_context=schema_context,
            engine_knowledge=engine_knowledge,
        )

        max_analyst_attempts = max(
            1, int(getattr(self.pipeline.config, "analyst_max_attempts", 2) or 1)
        )
        analyst_prompt = analyst_prompt_base
        analyst_response = ""
        scout_result = None
        for attempt in range(1, max_analyst_attempts + 1):
            if attempt > 1:
                analyst_prompt = (
                    analyst_prompt_base
                    + "\n\n## Retry Requirements\n"
                    + "Your prior response was not parseable. Return ONLY valid JSON "
                    + "with keys: dispatch, probes, dropped. No markdown fences."
                )
            analyst_response = analyst_call_fn(analyst_prompt)
            total_api_calls += 1
            prompt_label = (
                "analyst_prompt"
                if attempt == 1
                else f"analyst_prompt_retry{attempt - 1}"
            )
            response_label = (
                "analyst_response"
                if attempt == 1
                else f"analyst_response_retry{attempt - 1}"
            )
            self._save_to_disk(session_dir, 0, prompt_label, analyst_prompt)
            self._save_to_disk(session_dir, 0, response_label, analyst_response)
            scout_result = parse_analyst_response(analyst_response)
            if scout_result and scout_result.probes:
                break
            logger.warning(
                f"[{self.query_id}] Analyst parse failed (attempt "
                f"{attempt}/{max_analyst_attempts})"
            )

        if not scout_result or not scout_result.probes:
            logger.warning(f"[{self.query_id}] Analyst returned no probes")
            return SessionResult(
                query_id=self.query_id,
                mode="beam",
                best_speedup=0.0,
                best_sql=self.original_sql,
                original_sql=self.original_sql,
                status="ERROR",
                n_api_calls=total_api_calls,
                **self._session_cost_fields(),
            )

        probes = sorted(
            scout_result.probes,
            key=lambda p: (
                p.phase if p.phase is not None else 99,
                -(p.confidence or 0.0),
            ),
        )[:max_probes]
        logger.info(
            f"[{self.query_id}] Analyst: {len(probes)} probes designed"
        )

        # ── Phase 2: Workers (parallel) ────────────────────────────────
        if self.on_phase_change:
            self.on_phase_change(phase="workers", iteration=0)

        qwen_provider_override, qwen_model_override = self._beam_qwen_llm_override(
            beam_provider_override or getattr(self.pipeline, "provider", None),
            beam_model_override or getattr(self.pipeline, "model", None),
        )
        reasoner_provider_override, reasoner_model_override = (
            self._beam_reasoner_llm_override()
        )

        patches: List[AppliedPatch] = []
        worker_call_fn_by_patch_id: Dict[str, Callable[[str], str]] = {}
        worker_parallelism = max(
            1,
            int(getattr(self.pipeline.config, "wide_worker_parallelism", 8) or 8),
        )
        reasoner_slots = max(
            0, int(getattr(self.pipeline.config, "beam_reasoner_workers", 0) or 0)
        )
        qwen_slots = max(
            0,
            int(
                getattr(
                    self.pipeline.config,
                    "beam_qwen_workers",
                    worker_parallelism,
                )
                or 0
            ),
        )
        if reasoner_slots > 0 and (
            not reasoner_provider_override or not reasoner_model_override
        ):
            raise RuntimeError(
                f"[{self.query_id}] beam_reasoner_workers={reasoner_slots} "
                "requires beam_reasoner_provider and beam_reasoner_model"
            )
        if qwen_slots > 0 and (
            not qwen_provider_override or not qwen_model_override
        ):
            raise RuntimeError(
                f"[{self.query_id}] beam_qwen_workers={qwen_slots} "
                "requires qwen lane provider/model (beam_qwen_* or beam_llm_*)"
            )

        reasoner_selected: List[Any] = []
        if reasoner_slots > 0:
            ranked = sorted(
                probes,
                key=self._probe_hardness_score,
                reverse=True,
            )
            reasoner_selected = ranked[: min(reasoner_slots, len(ranked))]

        reasoner_probe_ids = {
            str(getattr(p, "probe_id", "")) for p in reasoner_selected
        }
        remaining_for_qwen = [
            p for p in probes if str(getattr(p, "probe_id", "")) not in reasoner_probe_ids
        ]
        qwen_selected = remaining_for_qwen[: min(qwen_slots, len(remaining_for_qwen))]

        qwen_worker_call_fn: Optional[Callable[[str], str]] = None
        if qwen_selected:
            qwen_worker_call_fn = self._make_llm_call_fn(
                provider_spec=qwen_provider_override,
                model_spec=qwen_model_override,
            )
        reasoner_worker_call_fn: Optional[Callable[[str], str]] = None
        if reasoner_selected:
            reasoner_worker_call_fn = self._make_llm_call_fn(
                provider_spec=reasoner_provider_override,
                model_spec=reasoner_model_override,
            )

        lane_assignments: List[Tuple[Any, str, Callable[[str], str], str]] = []
        for probe in reasoner_selected:
            lane_assignments.append(
                (
                    probe,
                    "reasoner",
                    reasoner_worker_call_fn,  # type: ignore[arg-type]
                    str(reasoner_model_override or ""),
                )
            )
        for probe in qwen_selected:
            lane_assignments.append(
                (
                    probe,
                    "qwen",
                    qwen_worker_call_fn,  # type: ignore[arg-type]
                    str(qwen_model_override or ""),
                )
            )

        if not lane_assignments:
            raise RuntimeError(
                f"[{self.query_id}] No worker lanes scheduled. "
                "Set beam_qwen_workers and/or beam_reasoner_workers > 0."
            )

        n_workers = min(len(lane_assignments), worker_parallelism)
        logger.info(
            f"[{self.query_id}] Worker lanes: reasoner={len(reasoner_selected)}, "
            f"qwen={len(qwen_selected)}, parallelism={n_workers} "
            f"(cap={worker_parallelism}, probes={len(probes)})"
        )

        with ThreadPoolExecutor(max_workers=max(1, n_workers)) as pool:
            futures = {}
            for probe, lane, lane_call_fn, lane_model in lane_assignments:
                gold_ex = None
                for ex_id in probe.recommended_examples:
                    gold_ex = _load_gold_example_by_id(ex_id, self.dialect)
                    if gold_ex:
                        break
                if not gold_ex and probe.gold_example_id:
                    gold_ex = _load_gold_example_by_id(
                        probe.gold_example_id, self.dialect
                    )
                if not gold_ex:
                    gold_ex = _load_gold_example_for_family(
                        probe.family, self.dialect
                    )
                gold_patch_plan = gold_ex.get("patch_plan") if gold_ex else None

                worker_prompt = build_beam_worker_prompt(
                    original_sql=self.original_sql,
                    ir_node_map=ir_node_map,
                    hypothesis=scout_result.hypothesis,
                    probe=probe,
                    gold_patch_plan=gold_patch_plan,
                    dialect=self.dialect,
                    schema_context=schema_context,
                    equivalence_tier=scout_result.equivalence_tier,
                    reasoning_trace=scout_result.reasoning_trace,
                    engine_knowledge=engine_knowledge,
                    do_not_do=scout_result.do_not_do,
                )
                worker_prompt = (
                    worker_prompt
                    + "\n\n"
                    + self._worker_lane_suffix(lane)
                )
                if dag_mode:
                    worker_prompt = (
                        worker_prompt
                        + "\n\n"
                        + self._worker_dag_mode_suffix(base_dag_prompt)
                    )

                probe_id = str(getattr(probe, "probe_id", ""))
                if probe_id:
                    worker_call_fn_by_patch_id[probe_id] = lane_call_fn
                future = pool.submit(lane_call_fn, worker_prompt)
                futures[future] = (probe, worker_prompt, lane, lane_model)

            for future in as_completed(futures):
                probe, w_prompt, lane, lane_model = futures[future]
                total_api_calls += 1
                try:
                    response = future.result()
                    self._save_to_disk(
                        session_dir, 0,
                        f"worker_{probe.probe_id}_response", response,
                    )

                    output_sql = self._apply_worker_response_compat(
                        response,
                        script_ir,
                        dialect_enum,
                        dag_base=base_dag,
                    )

                    patch = AppliedPatch(
                        patch_id=probe.probe_id,
                        family=probe.family,
                        transform=probe.transform_id,
                        relevance_score=probe.confidence,
                        output_sql=output_sql,
                        status="applied" if output_sql else "FAIL",
                        worker_prompt=w_prompt,
                        worker_response=response,
                        worker_role=f"{lane}:{lane_model}",
                        description=f"[{lane}:{lane_model}] {probe.target}",
                    )
                    if not output_sql:
                        patch.apply_error = "Failed to parse/apply worker output"
                    patches.append(patch)

                    logger.info(
                        f"[{self.query_id}] Worker {probe.probe_id} "
                        f"({lane}/{lane_model}, {probe.transform_id}): "
                        f"{'OK' if output_sql else 'FAIL'}"
                    )
                except Exception as e:
                    logger.warning(
                        f"[{self.query_id}] Worker {probe.probe_id} "
                        f"({lane}/{lane_model}) error: {e}"
                    )
                    patches.append(AppliedPatch(
                        patch_id=probe.probe_id,
                        family=probe.family,
                        transform=probe.transform_id,
                        relevance_score=probe.confidence,
                        apply_error=str(e),
                        status="ERROR",
                        worker_role=f"{lane}:{lane_model}",
                        description=f"[{lane}:{lane_model}] {probe.target}",
                    ))

        applied = [p for p in patches if p.output_sql]
        logger.info(
            f"[{self.query_id}] Workers: {len(applied)}/{len(patches)} "
            f"produced SQL"
        )

        if not applied:
            return SessionResult(
                query_id=self.query_id,
                mode="beam",
                best_speedup=0.0,
                best_sql=self.original_sql,
                original_sql=self.original_sql,
                status="NEUTRAL",
                n_api_calls=total_api_calls,
                **self._session_cost_fields(),
            )

        # ── Dedup identical SQL ────────────────────────────────────────
        seen_sql: Dict[str, str] = {}
        deduped = []
        for p in applied:
            norm = " ".join(p.output_sql.split())
            if norm in seen_sql:
                logger.info(
                    f"[{self.query_id}] Dedup: {p.patch_id} = {seen_sql[norm]}"
                )
                p.status = "DEDUP"
            else:
                seen_sql[norm] = p.patch_id
                deduped.append(p)
        applied = deduped

        # ── Phase 3: Validate + benchmark probes ──────────────────────
        if self.on_phase_change:
            self.on_phase_change(phase="benchmark", iteration=0)

        self._validate_and_benchmark_patches(applied, db_path, session_dir, 0)
        retry_default_fn = qwen_worker_call_fn or reasoner_worker_call_fn
        retry_calls = self._retry_tier1_worker_failures(
            patches=applied,
            worker_call_fn=retry_default_fn,  # type: ignore[arg-type]
            build_retry_prompt_fn=build_beam_worker_retry_prompt,
            script_ir=script_ir,
            dialect_enum=dialect_enum,
            db_path=db_path,
            session_dir=session_dir,
            shot=0,
            dag_base=base_dag,
            worker_call_fn_by_patch_id=worker_call_fn_by_patch_id,
        )
        total_api_calls += retry_calls

        sem_passed = [p for p in applied if p.semantic_passed]
        logger.info(
            f"[{self.query_id}] Probe validation: "
            f"{len(sem_passed)}/{len(applied)} passed"
        )

        # ── Phase 4: R1 Compiler (shot2 only on retryable syntax/error) ─
        compiler_patches: List[AppliedPatch] = []
        winners = [p for p in sem_passed if p.speedup and p.speedup >= 1.05]
        retry_error_candidates = [
            p for p in patches if self._is_compiler_retry_error_patch(p)
        ]
        has_retry_errors = bool(retry_error_candidates)
        snipe_rounds = int(getattr(self.pipeline.config, "snipe_rounds", 2) or 0)

        should_snipe = snipe_rounds > 0 and (bool(sem_passed) or has_retry_errors)
        if should_snipe:
            if self.on_phase_change:
                self.on_phase_change(phase="snipe", iteration=0)

            logger.info(
                f"[{self.query_id}] Phase 4: R1 compiler "
                f"(winners={len(winners)}, sem_passed={len(sem_passed)}, "
                f"retryable_errors={len(retry_error_candidates)})"
            )

            strike_results = [
                {
                    "probe_id": p.patch_id,
                    "transform_id": p.transform,
                    "family": p.family,
                    "status": p.status,
                    "failure_category": self._categorize_probe_failure(p),
                    "speedup": p.speedup,
                    "error": p.apply_error,
                    "explain_text": p.explain_text,
                    "sql": p.output_sql,
                    "description": p.description or "",
                }
                for p in patches
            ]

            # Shot 1: BDA + intelligence → 2 PatchPlans
            compiler_shot1_prompt = build_beam_compiler_prompt(
                query_id=self.query_id,
                original_sql=self.original_sql,
                explain_text=original_explain,
                ir_node_map=ir_node_map,
                all_5_examples=gold_examples,
                dialect=self.dialect,
                intelligence_brief=intelligence_brief,
                strike_results=strike_results,
                importance_stars=importance_stars,
                schema_context=schema_context,
                engine_knowledge=engine_knowledge,
                dispatch_hypothesis=scout_result.hypothesis,
                dispatch_reasoning_trace=scout_result.reasoning_trace,
                equivalence_tier=scout_result.equivalence_tier,
            )
            if dag_mode:
                compiler_shot1_prompt = (
                    compiler_shot1_prompt
                    + "\n\n"
                    + self._compiler_dag_mode_suffix(base_dag_prompt)
                )

            self._save_to_disk(
                session_dir, 0, "compiler_shot1_prompt", compiler_shot1_prompt
            )
            compiler_shot1_response = analyst_call_fn(compiler_shot1_prompt)
            total_api_calls += 1
            self._save_to_disk(
                session_dir, 0, "compiler_shot1_response", compiler_shot1_response
            )

            if dag_mode:
                shot1_compiler = self._apply_dag_compiler_response(
                    compiler_shot1_response, base_dag or {}, prefix="s1"
                )
            else:
                shot1_compiler = self._apply_patchplan_array(
                    compiler_shot1_response, script_ir, dialect_enum, prefix="s1"
                )
            if not shot1_compiler and self._is_compiler_tier0_shape_failure(
                compiler_shot1_response,
                dag_mode=dag_mode,
            ):
                logger.warning(
                    f"[{self.query_id}] Compiler shot 1 Tier-0 shape failure, retrying once"
                )
                compiler_shot1_retry_prompt = self._build_compiler_tier0_retry_prompt(
                    compiler_shot1_prompt,
                    dag_mode=dag_mode,
                )
                self._save_to_disk(
                    session_dir, 0, "compiler_shot1_retry_prompt", compiler_shot1_retry_prompt
                )
                compiler_shot1_retry_response = analyst_call_fn(compiler_shot1_retry_prompt)
                total_api_calls += 1
                self._save_to_disk(
                    session_dir, 0, "compiler_shot1_retry_response", compiler_shot1_retry_response
                )
                if dag_mode:
                    shot1_compiler = self._apply_dag_compiler_response(
                        compiler_shot1_retry_response, base_dag or {}, prefix="s1r"
                    )
                else:
                    shot1_compiler = self._apply_patchplan_array(
                        compiler_shot1_retry_response, script_ir, dialect_enum, prefix="s1r"
                    )
            logger.info(
                f"[{self.query_id}] Compiler shot 1: {len(shot1_compiler)} patches"
            )

            self._validate_and_benchmark_patches(
                shot1_compiler, db_path, session_dir, 1
            )
            compiler_patches.extend(shot1_compiler)

            # Shot 2 is reserved for retry paths (syntax/parse/error), not for speed-only cases.
            shot1_retry_errors = [
                p for p in shot1_compiler if self._is_compiler_retry_error_patch(p)
            ]
            should_run_shot2 = snipe_rounds > 1 and (
                has_retry_errors or bool(shot1_retry_errors)
            )
            if should_run_shot2:
                if dag_mode:
                    compiler_shot2_prompt = self._append_dag_shot_results(
                        base_prompt=compiler_shot1_prompt,
                        patches=shot1_compiler,
                    )
                else:
                    compiler_shot2_prompt = append_shot_results(
                        base_prompt=compiler_shot1_prompt,
                        patches=shot1_compiler,
                        explains={
                            p.patch_id: p.explain_text or ""
                            for p in shot1_compiler
                        },
                    )

                self._save_to_disk(
                    session_dir, 0, "compiler_shot2_prompt", compiler_shot2_prompt
                )
                compiler_shot2_response = analyst_call_fn(compiler_shot2_prompt)
                total_api_calls += 1
                self._save_to_disk(
                    session_dir, 0, "compiler_shot2_response", compiler_shot2_response
                )

                if dag_mode:
                    shot2_compiler = self._apply_dag_compiler_response(
                        compiler_shot2_response, base_dag or {}, prefix="s2"
                    )
                else:
                    shot2_compiler = self._apply_patchplan_array(
                        compiler_shot2_response, script_ir, dialect_enum, prefix="s2"
                    )
                if not shot2_compiler and self._is_compiler_tier0_shape_failure(
                    compiler_shot2_response,
                    dag_mode=dag_mode,
                ):
                    logger.warning(
                        f"[{self.query_id}] Compiler shot 2 Tier-0 shape failure, retrying once"
                    )
                    compiler_shot2_retry_prompt = self._build_compiler_tier0_retry_prompt(
                        compiler_shot2_prompt,
                        dag_mode=dag_mode,
                    )
                    self._save_to_disk(
                        session_dir, 0, "compiler_shot2_retry_prompt", compiler_shot2_retry_prompt
                    )
                    compiler_shot2_retry_response = analyst_call_fn(compiler_shot2_retry_prompt)
                    total_api_calls += 1
                    self._save_to_disk(
                        session_dir, 0, "compiler_shot2_retry_response", compiler_shot2_retry_response
                    )
                    if dag_mode:
                        shot2_compiler = self._apply_dag_compiler_response(
                            compiler_shot2_retry_response, base_dag or {}, prefix="s2r"
                        )
                    else:
                        shot2_compiler = self._apply_patchplan_array(
                            compiler_shot2_retry_response, script_ir, dialect_enum, prefix="s2r"
                        )
                logger.info(
                    f"[{self.query_id}] Compiler shot 2 (retry path): {len(shot2_compiler)} patches"
                )

                self._validate_and_benchmark_patches(
                    shot2_compiler, db_path, session_dir, 2
                )
                compiler_patches.extend(shot2_compiler)
            else:
                logger.info(
                    f"[{self.query_id}] Compiler shot 2 skipped "
                    f"(no retryable syntax/error failures)"
                )

        # ── Collect all results ────────────────────────────────────────
        all_final = list(sem_passed) + [
            sp for sp in compiler_patches if sp.semantic_passed
        ]

        all_patches_full = patches + compiler_patches
        iter_explains: Dict[str, str] = {
            p.patch_id: p.explain_text
            for p in all_patches_full
            if p.explain_text
        }
        iter_result = PatchIterationResult(
            iteration=0,
            prompt=analyst_prompt,
            response=analyst_response,
            n_api_calls=total_api_calls,
            patches=all_patches_full,
            explains=iter_explains,
        )

        # Find best across probes + compiler
        best_speedup = 0.0
        best_sql = self.original_sql
        best_transforms: List[str] = []
        best_status = "NEUTRAL"

        candidates = [
            p for p in all_final
            if p.speedup is not None and p.speedup >= 1.0
        ]
        if candidates:
            best_patch = max(candidates, key=lambda p: p.speedup)
            best_speedup = best_patch.speedup
            best_sql = best_patch.output_sql or self.original_sql
            best_transforms = [best_patch.transform]
            best_status = self._classify_speedup(best_speedup)

            iter_result.best_speedup = best_speedup
            iter_result.best_patch_id = best_patch.patch_id
            iter_result.best_sql = best_sql

        logger.info(
            f"[{self.query_id}] BEAM result: {best_speedup:.2f}x "
            f"({best_status})"
        )

        self._save_to_disk(
            session_dir, 0, "result",
            json.dumps(
                self._serialize_iteration(iter_result),
                indent=2, default=str,
            ),
        )

        return SessionResult(
            query_id=self.query_id,
            mode="beam",
            best_speedup=best_speedup,
            best_sql=best_sql,
            original_sql=self.original_sql,
            best_transforms=best_transforms,
            status=best_status,
            iterations=[self._serialize_iteration(iter_result)],
            n_iterations=1,
            n_api_calls=total_api_calls,
            **self._session_cost_fields(),
        )

    def _retry_tier1_worker_failures(
        self,
        *,
        patches: List[AppliedPatch],
        worker_call_fn: Callable[[str], str],
        build_retry_prompt_fn: Callable[..., str],
        script_ir,
        dialect_enum,
        db_path: str,
        session_dir: Path,
        shot: int,
        dag_base: Optional[Dict[str, Any]] = None,
        worker_call_fn_by_patch_id: Optional[Dict[str, Callable[[str], str]]] = None,
    ) -> int:
        """Retry workers once with structured gate feedback for Tier-1 failures."""
        max_retry_attempts = max(
            0,
            int(
                getattr(
                    self.pipeline.config,
                    "worker_retry_on_tier1_max_attempts",
                    1,
                )
                or 0
            ),
        )
        if max_retry_attempts <= 0:
            return 0

        retryable = [
            p
            for p in patches
            if p.output_sql
            and p.apply_error
            and str(p.apply_error).startswith("Tier-1:")
        ]
        if not retryable:
            return 0

        logger.info(
            f"[{self.query_id}] Retrying {len(retryable)} Tier-1 worker failures"
        )

        retry_calls = 0
        retry_candidates: List[AppliedPatch] = []
        max_workers = min(4, len(retryable))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for patch in retryable:
                retry_prompt = build_retry_prompt_fn(
                    patch.worker_prompt or "",
                    probe_id=patch.patch_id,
                    transform_id=patch.transform,
                    gate_name="tier1_structural",
                    gate_error=patch.apply_error or "Tier-1 structural failure",
                    failed_sql=patch.output_sql or "",
                    previous_response=patch.worker_response or "",
                )
                self._save_to_disk(
                    session_dir,
                    shot,
                    f"worker_{patch.patch_id}_retry_prompt",
                    retry_prompt,
                )
                lane_call_fn = (
                    (worker_call_fn_by_patch_id or {}).get(patch.patch_id)
                    or worker_call_fn
                )
                future = pool.submit(lane_call_fn, retry_prompt)
                futures[future] = (patch, retry_prompt)

            for future in as_completed(futures):
                patch, retry_prompt = futures[future]
                retry_calls += 1
                try:
                    retry_response = future.result()
                except Exception as e:
                    patch.status = "ERROR"
                    patch.semantic_passed = False
                    patch.apply_error = f"Worker retry failed: {e}"
                    continue

                self._save_to_disk(
                    session_dir,
                    shot,
                    f"worker_{patch.patch_id}_retry_response",
                    retry_response,
                )
                output_sql = self._apply_worker_response_compat(
                    retry_response,
                    script_ir,
                    dialect_enum,
                    dag_base=dag_base,
                )
                if not output_sql:
                    patch.status = "FAIL"
                    patch.semantic_passed = False
                    patch.apply_error = "Retry failed: Failed to parse/apply worker output"
                    patch.worker_prompt = retry_prompt
                    patch.worker_response = retry_response
                    continue

                patch.output_sql = output_sql
                patch.worker_prompt = retry_prompt
                patch.worker_response = retry_response
                patch.status = "applied"
                patch.semantic_passed = False
                patch.speedup = None
                patch.explain_text = None
                patch.original_ms = None
                patch.patch_ms = None
                patch.apply_error = None
                retry_candidates.append(patch)

        if retry_candidates:
            self._validate_and_benchmark_patches(
                retry_candidates,
                db_path=db_path,
                session_dir=session_dir,
                shot=shot,
            )

        return retry_calls

    def _apply_wide_worker_response(
        self,
        response: str,
        script_ir,
        dialect_enum,
        *,
        dag_base: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Parse a wide worker's PatchPlan JSON and apply to IR → SQL.

        Wide workers output PatchPlan JSON. We parse it, apply to a copy
        of the script IR, and render to SQL. Falls back to treating the
        response as raw SQL if JSON parsing fails.

        Returns:
            Output SQL string, or None if both approaches fail.
        """
        import copy as _copy
        import json as _json
        import re as _re
        from ..ir import dict_to_plan, apply_patch_plan

        def _extract_json_object(text: str) -> Optional[dict]:
            """Extract one JSON object from raw worker text."""
            t = text.strip()
            if t.startswith("{"):
                try:
                    obj = _json.loads(t)
                    return obj if isinstance(obj, dict) else None
                except Exception:
                    pass

            m = _re.search(r"```(?:json)?\s*(\{.*?\})\s*```", t, _re.DOTALL)
            if m:
                try:
                    obj = _json.loads(m.group(1))
                    return obj if isinstance(obj, dict) else None
                except Exception:
                    pass

            start = t.find("{")
            if start >= 0:
                depth = 0
                for i in range(start, len(t)):
                    if t[i] == "{":
                        depth += 1
                    elif t[i] == "}":
                        depth -= 1
                        if depth == 0:
                            try:
                                obj = _json.loads(t[start:i + 1])
                                return obj if isinstance(obj, dict) else None
                            except Exception:
                                break
            return None

        # DAG mode: parse DAG candidate first.
        if dag_base is not None:
            sql = self._apply_dag_worker_response(response, dag_base)
            if sql and sql.strip():
                return sql.strip()
            return None

        # PatchPlan mode: try JSON PatchPlan first
        try:
            plan_data = _extract_json_object(response)
            if plan_data and isinstance(plan_data, dict) and "steps" in plan_data:
                # Some workers emit a root-level target (e.g., by_node_id)
                # instead of per-step targets. Normalize that shape.
                root_target = {}
                for k in ("by_node_id", "by_label", "by_anchor_hash", "by_path"):
                    v = plan_data.get(k)
                    if v is not None:
                        root_target[k] = v
                if root_target:
                    for step in plan_data.get("steps", []):
                        if isinstance(step, dict) and not step.get("target"):
                            step["target"] = dict(root_target)

                if not plan_data.get("plan_id"):
                    plan_data["plan_id"] = "beam_worker_plan"

                plan = dict_to_plan(plan_data)
                patched_ir = _copy.deepcopy(script_ir)
                result = apply_patch_plan(patched_ir, plan)
                sql = result.output_sql if result and result.success else None
                if sql and sql.strip():
                    return sql.strip()
        except Exception as e:
            logger.debug(
                f"[{self.query_id}] PatchPlan apply failed: {e}"
            )

        # Fallback: treat response as raw SQL
        # Strip markdown fences if present
        text = response.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first and last fence lines
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        # Don't treat JSON objects/arrays as SQL fallback.
        if text.startswith("{") or text.startswith("["):
            return None

        # Basic validation: must contain SELECT
        if "SELECT" in text.upper() and len(text) > 20:
            return text

        return None

    def _apply_worker_response_compat(
        self,
        response: str,
        script_ir,
        dialect_enum,
        dag_base: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Call worker apply path with backwards-compatible signature handling."""
        try:
            return self._apply_wide_worker_response(
                response,
                script_ir,
                dialect_enum,
                dag_base=dag_base,
            )
        except TypeError:
            # Test monkeypatches may still use the legacy 3-arg signature.
            return self._apply_wide_worker_response(
                response,
                script_ir,
                dialect_enum,
            )

    # ── Internal Methods ────────────────────────────────────────────────────

    def _beam_llm_override(self) -> Tuple[Optional[str], Optional[str]]:
        """Return BEAM-only provider/model overrides from config/env."""
        cfg = getattr(self.pipeline, "config", None)
        provider = ""
        model = ""
        if cfg is not None:
            provider = str(getattr(cfg, "beam_llm_provider", "") or "").strip()
            model = str(getattr(cfg, "beam_llm_model", "") or "").strip()

        # Env wins over benchmark config when provided.
        env_provider = str(os.environ.get("QT_BEAM_LLM_PROVIDER", "") or "").strip()
        env_model = str(os.environ.get("QT_BEAM_LLM_MODEL", "") or "").strip()
        if env_provider:
            provider = env_provider
        if env_model:
            model = env_model

        return (provider or None, model or None)

    def _beam_qwen_llm_override(
        self,
        default_provider: Optional[str],
        default_model: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        """Return qwen-lane provider/model overrides from config/env.

        Falls back to BEAM default provider/model because qwen lane is the
        baseline worker lane in mixed execution.
        """
        cfg = getattr(self.pipeline, "config", None)
        provider = str(default_provider or "").strip()
        model = str(default_model or "").strip()
        if cfg is not None:
            provider_cfg = str(
                getattr(cfg, "beam_qwen_provider", "") or ""
            ).strip()
            model_cfg = str(
                getattr(cfg, "beam_qwen_model", "") or ""
            ).strip()
            if provider_cfg:
                provider = provider_cfg
            if model_cfg:
                model = model_cfg

        env_provider = str(os.environ.get("QT_BEAM_QWEN_PROVIDER", "") or "").strip()
        env_model = str(os.environ.get("QT_BEAM_QWEN_MODEL", "") or "").strip()
        if env_provider:
            provider = env_provider
        if env_model:
            model = env_model

        return (provider or None, model or None)

    def _beam_reasoner_llm_override(self) -> Tuple[Optional[str], Optional[str]]:
        """Return reasoner-lane provider/model overrides from config/env."""
        cfg = getattr(self.pipeline, "config", None)
        provider = ""
        model = ""
        if cfg is not None:
            provider = str(
                getattr(cfg, "beam_reasoner_provider", "") or ""
            ).strip()
            model = str(
                getattr(cfg, "beam_reasoner_model", "") or ""
            ).strip()

        env_provider = str(
            os.environ.get("QT_BEAM_REASONER_PROVIDER", "") or ""
        ).strip()
        env_model = str(
            os.environ.get("QT_BEAM_REASONER_MODEL", "") or ""
        ).strip()
        if env_provider:
            provider = env_provider
        if env_model:
            model = env_model

        return (provider or None, model or None)

    @staticmethod
    def _probe_hardness_score(probe: Any) -> Tuple[int, float]:
        """Heuristic score for assigning hardest probes to reasoner lane."""
        family = str(getattr(probe, "family", "") or "").upper()
        transform = str(getattr(probe, "transform_id", "") or "").lower()
        confidence = float(getattr(probe, "confidence", 0.0) or 0.0)
        decorrelation_hit = (
            family == "B"
            or "decorrelate" in transform
            or "de-correlate" in transform
        )
        return (1 if decorrelation_hit else 0, confidence)

    def _make_llm_call_fn(
        self,
        provider_spec: Optional[str] = None,
        model_spec: Optional[str] = None,
    ) -> callable:
        """Create an LLM call function for a specific model.

        Args:
            provider_spec: Optional per-call provider override.
            model_spec: Optional per-call model override.

        Returns:
            Callable that takes prompt string, returns response string.
        """
        from ..generate import CandidateGenerator

        effective_provider = str(
            provider_spec or getattr(self.pipeline, "provider", "") or ""
        ).strip()
        effective_model = str(
            model_spec or getattr(self.pipeline, "model", "") or ""
        ).strip()

        if not effective_provider:
            raise RuntimeError(
                f"[{self.query_id}] Missing global LLM provider. "
                "Set QT_LLM_PROVIDER explicitly."
            )
        if not effective_model:
            raise RuntimeError(
                f"[{self.query_id}] Missing global LLM model. "
                "Set QT_LLM_MODEL explicitly."
            )

        logger.info(
            f"[{self.query_id}] LLM call fn: "
            f"provider={effective_provider}, model={effective_model}"
        )

        def call_fn(prompt: str) -> str:
            with self._llm_calls_lock:
                self._llm_call_seq += 1
                call_id = self._llm_call_seq

            generator = CandidateGenerator(
                provider=effective_provider,
                model=effective_model,
                analyze_fn=self.pipeline.analyze_fn,
            )
            logger.info(
                f"[{self.query_id}] LLM call → {effective_model} "
                f"({len(prompt)} chars prompt)"
            )
            t0 = time.time()
            result = self._call_llm_with_timeout(generator, prompt)
            elapsed = time.time() - t0

            usage = {}
            client = getattr(generator, "_llm_client", None)
            if client is not None:
                raw_usage = getattr(client, "last_usage", {})
                if isinstance(raw_usage, dict):
                    usage = dict(raw_usage)

            prompt_tokens = self._int_usage(usage.get("prompt_tokens"))
            completion_tokens = self._int_usage(usage.get("completion_tokens"))
            total_tokens = self._int_usage(usage.get("total_tokens"))
            if total_tokens <= 0:
                total_tokens = prompt_tokens + completion_tokens
            prompt_cache_hit_tokens = self._int_usage(
                usage.get("prompt_cache_hit_tokens")
            )
            prompt_cache_miss_tokens = self._int_usage(
                usage.get("prompt_cache_miss_tokens")
            )
            cached_tokens = self._int_usage(usage.get("cached_tokens"))
            reasoning_tokens = self._int_usage(
                usage.get("reasoning_tokens") or usage.get("reasoningTokens")
            )

            explicit_cost = usage.get("cost_usd")
            if explicit_cost is None:
                explicit_cost = usage.get("estimated_cost_usd")
            if explicit_cost is None:
                explicit_cost = usage.get("cost")
            if isinstance(explicit_cost, str):
                try:
                    explicit_cost = float(explicit_cost.strip())
                except Exception:
                    explicit_cost = None
            has_usage_tokens = any(
                v > 0
                for v in (
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    prompt_cache_hit_tokens,
                    prompt_cache_miss_tokens,
                    cached_tokens,
                    reasoning_tokens,
                )
            )
            estimated_cost = None
            if isinstance(explicit_cost, (int, float)):
                estimated_cost = float(explicit_cost)
            elif has_usage_tokens:
                estimated_cost = self._estimate_call_cost_usd(
                    model=str(effective_model or ""),
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                )

            call_record = {
                "call_id": call_id,
                "timestamp": datetime.now().isoformat(),
                "provider": effective_provider,
                "model": effective_model,
                "prompt_chars": len(prompt),
                "response_chars": len(result or ""),
                "elapsed_seconds": round(elapsed, 3),
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "prompt_cache_hit_tokens": prompt_cache_hit_tokens,
                "prompt_cache_miss_tokens": prompt_cache_miss_tokens,
                "cached_tokens": cached_tokens,
                "reasoning_tokens": reasoning_tokens,
                "estimated_cost_usd": estimated_cost,
                "usage_raw": usage,
            }
            self._record_api_call_cost(call_record)

            cost_msg = (
                f"${estimated_cost:.6f}"
                if isinstance(estimated_cost, (int, float))
                else "unpriced"
            )
            logger.info(
                f"[{self.query_id}] LLM done ← {effective_model} "
                f"({len(result)} chars response, {elapsed:.1f}s, "
                f"{total_tokens} tok, {cost_msg})"
            )
            return result

        return call_fn

    def _load_cached_classification(self, query_id: str):
        """Load pre-computed classification from benchmark_dir/classifications.json.

        Returns ClassificationResult or None if not available.
        """
        try:
            classifications_path = (
                self.pipeline.benchmark_dir / "classifications.json"
            )
            if not classifications_path.exists():
                return None

            import json
            data = json.loads(classifications_path.read_text())
            entry = data.get(query_id)
            if not entry:
                return None

            from ..patches.pathology_classifier import (
                ClassificationResult,
                PathologyMatch,
            )

            matches = [
                PathologyMatch(
                    pathology_id=m["pathology_id"],
                    name=m.get("name", ""),
                    confidence=m.get("confidence", 0.0),
                    evidence=m.get("evidence", ""),
                    recommended_transform=m.get("transform", ""),
                )
                for m in entry.get("llm_matches", [])
            ]

            return ClassificationResult(
                query_id=query_id,
                matches=matches,
                reasoning=entry.get("reasoning", ""),
            )
        except Exception as e:
            logger.debug(f"[{query_id}] No cached classification: {e}")
            return None

    def _load_workload_baselines(self) -> Dict[str, float]:
        """Load cached baseline runtimes from benchmark explains."""
        baselines: Dict[str, float] = {}
        explains_dir = self.pipeline.benchmark_dir / "explains"
        if not explains_dir.exists():
            return baselines

        candidate_files: List[Path] = sorted(explains_dir.glob("*.json"))
        for legacy_dir in ("sf10", "sf5"):
            subdir = explains_dir / legacy_dir
            if subdir.exists():
                candidate_files.extend(sorted(subdir.glob("*.json")))

        for path in candidate_files:
            qid = path.stem
            if qid in baselines:
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                ms = data.get("execution_time_ms")
                if isinstance(ms, (int, float)) and ms > 0:
                    baselines[qid] = float(ms)
            except Exception:
                continue
        return baselines

    def _lookup_transform_family(self, transform_id: str) -> str:
        """Return transform family id (A/B/C/...) when known."""
        try:
            from ..detection import load_transforms

            for transform in load_transforms():
                if (
                    isinstance(transform, dict)
                    and str(transform.get("id", "")) == transform_id
                ):
                    return str(transform.get("family", "?"))
        except Exception:
            pass
        return "?"

    def _compute_importance_stars(self, current_baseline_ms: Optional[float]) -> int:
        """Assign importance stars from workload distribution (80/10/10)."""
        try:
            from ..patches.beam_router import assign_importance_stars

            baselines = self._load_workload_baselines()
            if (
                self.query_id not in baselines
                and isinstance(current_baseline_ms, (int, float))
                and current_baseline_ms > 0
            ):
                baselines[self.query_id] = float(current_baseline_ms)
            if not baselines:
                return 2
            stars_map = assign_importance_stars(
                baselines,
                high_workload_pct=80.0,
                medium_workload_pct=10.0,
            )
            return int(stars_map.get(self.query_id, 2))
        except Exception as e:
            logger.debug(f"[{self.query_id}] Importance score failed: {e}")
            return 2

    def _build_schema_context(self, db_path: str) -> str:
        """Build compact schema/index/stats context for prompt grounding."""
        from ..execution.database_utils import fetch_schema_with_stats

        try:
            schema = fetch_schema_with_stats(database_path=db_path, sql=self.original_sql)
        except Exception as e:
            logger.debug(f"[{self.query_id}] Schema context fetch failed: {e}")
            return ""

        if not isinstance(schema, dict):
            return ""
        tables = schema.get("tables")
        if not isinstance(tables, list) or not tables:
            return ""

        def _fmt_index(idx: Any) -> str:
            if isinstance(idx, str):
                return idx
            if isinstance(idx, dict):
                for k in ("name", "indexname", "index_name"):
                    v = idx.get(k)
                    if v:
                        return str(v)
                cols = idx.get("columns")
                if isinstance(cols, list) and cols:
                    return "idx(" + ", ".join(str(c) for c in cols[:4]) + ")"
            return str(idx)

        lines = [
            f"- source: {schema.get('source', self.dialect)}",
            f"- referenced_tables: {len(tables)}",
            "",
            "| Table | Rows(est) | PK | Indexes |",
            "|-------|-----------|----|---------|",
        ]
        for table in tables:
            name = str(table.get("name") or table.get("table_name") or "?")
            row_count = table.get("row_count")
            rows = str(int(row_count)) if isinstance(row_count, (int, float)) else "?"
            pk = table.get("primary_key") or []
            pk_str = ", ".join(str(c) for c in pk) if isinstance(pk, list) and pk else "-"
            indexes = table.get("indexes") or []
            if isinstance(indexes, list) and indexes:
                idx_str = ", ".join(_fmt_index(i) for i in indexes[:6])
            else:
                idx_str = "-"
            lines.append(f"| {name} | {rows} | {pk_str} | {idx_str} |")

        lines.extend(
            [
                "",
                "### Column Signatures",
                "| Table | Column | Type | Nullable | Key Hint |",
                "|-------|--------|------|----------|----------|",
            ]
        )
        for table in tables:
            table_name = str(table.get("name") or table.get("table_name") or "?")
            pk = table.get("primary_key") or []
            pk_set = {
                str(col).lower()
                for col in pk
                if isinstance(col, (str, int, float))
            }
            columns = table.get("columns") or []
            if not isinstance(columns, list):
                continue
            for col in columns[:24]:
                if isinstance(col, dict):
                    col_name = str(
                        col.get("name")
                        or col.get("column_name")
                        or col.get("field")
                        or "?"
                    )
                    col_type = str(col.get("type") or col.get("data_type") or "?")
                    nullable_raw = col.get("nullable")
                    if nullable_raw is None:
                        nullable_raw = col.get("is_nullable")
                    if isinstance(nullable_raw, bool):
                        nullable = "YES" if nullable_raw else "NO"
                    elif isinstance(nullable_raw, str):
                        nullable = nullable_raw.upper()
                    else:
                        nullable = "?"
                else:
                    col_name = str(col)
                    col_type = "?"
                    nullable = "?"
                key_hint = "PK" if col_name.lower() in pk_set else "-"
                lines.append(
                    f"| {table_name} | {col_name} | {col_type} | {nullable} | {key_hint} |"
                )

        return "\n".join(lines)

    def _create_session_dir(self) -> Path:
        """Create a session directory for disk persistence."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        session_dir = (
            self.pipeline.benchmark_dir
            / "beam_sessions"
            / f"{self.query_id}_{timestamp}"
        )
        session_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"[{self.query_id}] Session dir: {session_dir}")

        # Write session metadata
        metadata = {
            "query_id": self.query_id,
            "timestamp": datetime.now().isoformat(),
            "engine": self.pipeline.config.engine,
            "benchmark": self.pipeline.config.benchmark,
            "db_path": self.pipeline.config.db_path_or_dsn,
            "scale_factor": self.pipeline.config.scale_factor,
            "max_iterations": self.max_iterations,
            "target_speedup": self.target_speedup,
            "llm_provider": self.pipeline.provider or "?",
            "llm_model": self.pipeline.model or "?",
            "beam_llm_provider": getattr(self.pipeline.config, "beam_llm_provider", "") or "",
            "beam_llm_model": getattr(self.pipeline.config, "beam_llm_model", "") or "",
            "beam_qwen_workers": int(getattr(self.pipeline.config, "beam_qwen_workers", 8) or 0),
            "beam_reasoner_workers": int(getattr(self.pipeline.config, "beam_reasoner_workers", 0) or 0),
            "beam_qwen_provider": getattr(self.pipeline.config, "beam_qwen_provider", "") or "",
            "beam_qwen_model": getattr(self.pipeline.config, "beam_qwen_model", "") or "",
            "beam_reasoner_provider": getattr(self.pipeline.config, "beam_reasoner_provider", "") or "",
            "beam_reasoner_model": getattr(self.pipeline.config, "beam_reasoner_model", "") or "",
            "beam_edit_mode": getattr(self.pipeline.config, "beam_edit_mode", "patchplan"),
            "provider": self.pipeline.provider or "?",
            "semantic_validation_enabled": self.pipeline.config.semantic_validation_enabled,
            "semantic_sample_pct": self.pipeline.config.semantic_sample_pct,
            "validation_method": self.pipeline.config.validation_method,
        }
        (session_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2)
        )
        self._reset_cost_tracking(session_dir)
        return session_dir

    def _save_to_disk(self, session_dir: Path, iteration: int, label: str, content: str) -> None:
        """Save content to disk for debugging/audit."""
        filename = f"iter{iteration}_{label}.txt"
        filepath = session_dir / filename
        try:
            filepath.write_text(content, encoding="utf-8")
            logger.debug(f"Saved {filepath} ({len(content)} chars)")
        except Exception as e:
            logger.warning(f"Failed to save {filepath}: {e}")

    def _call_llm_with_timeout(self, generator, prompt: str) -> str:
        """Call LLM with timeout protection."""
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(generator._analyze, prompt)
            try:
                return future.result(timeout=LLM_TIMEOUT_SECONDS)
            except TimeoutError:
                logger.error(
                    f"[{self.query_id}] LLM call timed out after {LLM_TIMEOUT_SECONDS}s"
                )
                return '[]'  # empty JSON array — will trigger retry or skip
            except Exception as e:
                logger.error(f"[{self.query_id}] LLM call failed: {e}")
                return '[]'

    def _sequential_benchmark(
        self, patches: List[AppliedPatch], db_path: str
    ) -> None:
        """Sequential benchmark (3-run: warmup + average of 2 measured runs).

        Correctness gate: row count + checksum must match original.
        """
        from ..execution.factory import create_executor_from_dsn
        from ..validate import _timed_runs_pg
        from ..validation.equivalence_checker import EquivalenceChecker

        BENCH_RUNS = 3
        checker = EquivalenceChecker()

        logger.info(
            f"[{self.query_id}] Sequential benchmark: "
            f"{len(patches)} patches, {BENCH_RUNS}x (warmup + avg last 2)"
        )

        try:
            with create_executor_from_dsn(db_path) as executor:
                # Measure original and capture rows for correctness.
                logger.info(
                    f"[{self.query_id}] Baseline: {BENCH_RUNS}x..."
                )
                orig_ms, orig_rows, orig_times = _timed_runs_pg(
                    executor, self.original_sql, runs=BENCH_RUNS,
                    capture_rows=True,
                )
                orig_count = len(orig_rows) if orig_rows else 0
                orig_checksum = None
                if orig_rows:
                    try:
                        orig_checksum = checker.compute_checksum(orig_rows)
                    except Exception:
                        pass
                logger.info(
                    f"[{self.query_id}] Baseline: {orig_ms:.1f}ms "
                    f"({orig_count} rows, checksum={orig_checksum}) "
                    f"[{', '.join(f'{t:.0f}' for t in orig_times)}]"
                )

                for idx, p in enumerate(patches):
                    logger.info(
                        f"[{self.query_id}] Benchmark {idx + 1}/{len(patches)}: "
                        f"{p.patch_id} ({p.family}/{p.transform})"
                    )
                    try:
                        patch_ms, patch_rows, patch_times = _timed_runs_pg(
                            executor, p.output_sql, runs=BENCH_RUNS,
                            capture_rows=True,
                        )
                        patch_count = len(patch_rows) if patch_rows else 0

                        # ── Correctness gate: row count + checksum ──
                        if patch_count != orig_count:
                            p.speedup = 0.0
                            p.status = "FAIL"
                            p.apply_error = (
                                f"Row count mismatch: original={orig_count}, "
                                f"patch={patch_count}"
                            )
                            logger.warning(
                                f"[{self.query_id}]   FAIL: {p.patch_id}: "
                                f"{p.apply_error}"
                            )
                            continue

                        if orig_checksum and patch_rows:
                            try:
                                patch_checksum = checker.compute_checksum(patch_rows)
                                if patch_checksum != orig_checksum:
                                    p.speedup = 0.0
                                    p.status = "FAIL"
                                    p.apply_error = (
                                        f"Checksum mismatch: original={orig_checksum}, "
                                        f"patch={patch_checksum}"
                                    )
                                    logger.warning(
                                        f"[{self.query_id}]   FAIL: {p.patch_id}: "
                                        f"{p.apply_error}"
                                    )
                                    continue
                            except Exception:
                                pass  # checksum compute failed — don't block

                        p.original_ms = orig_ms
                        p.patch_ms = patch_ms
                        p.speedup = orig_ms / patch_ms if patch_ms > 0 else 1.0
                        p.status = self._classify_speedup(p.speedup)

                        logger.info(
                            f"[{self.query_id}]   result: orig={orig_ms:.1f}ms, "
                            f"patch={patch_ms:.1f}ms, speedup={p.speedup:.2f}x "
                            f"({p.status}, {patch_count} rows) "
                            f"[{', '.join(f'{t:.0f}' for t in patch_times)}]"
                        )
                    except Exception as e:
                        p.speedup = 0.0
                        p.status = "ERROR"
                        p.apply_error = str(e)
                        logger.warning(
                            f"[{self.query_id}]   ERROR: {p.patch_id}: {e}"
                        )
        except Exception as e:
            logger.warning(f"Sequential benchmark failed: {e}")

    def _serialize_iteration(self, it: PatchIterationResult) -> dict:
        """Serialize iteration result for SessionResult.iterations."""
        cost_fields = self._session_cost_fields()
        return {
            "iteration": it.iteration,
            "n_api_calls": it.n_api_calls,
            "beam_cost_usd": cost_fields.get("beam_cost_usd", 0.0),
            "beam_cost_priced_calls": cost_fields.get("beam_cost_priced_calls", 0),
            "beam_cost_unpriced_calls": cost_fields.get("beam_cost_unpriced_calls", 0),
            "beam_token_totals": cost_fields.get("beam_token_totals", {}),
            "best_speedup": round(it.best_speedup, 2),
            "best_patch_id": it.best_patch_id,
            "best_sql": it.best_sql,
            "patches": [
                {
                    "patch_id": p.patch_id,
                    "family": p.family,
                    "transform": p.transform,
                    "relevance_score": p.relevance_score,
                    "status": p.status,
                    "speedup": round(p.speedup, 2) if p.speedup is not None else None,
                    "semantic_passed": p.semantic_passed,
                    "error": p.apply_error,
                    "original_ms": round(p.original_ms, 1) if p.original_ms is not None else None,
                    "patch_ms": round(p.patch_ms, 1) if p.patch_ms is not None else None,
                    "output_sql": p.output_sql,
                    "has_explain": bool(p.explain_text),
                    "description": p.description,
                }
                for p in it.patches
            ],
            "explains": {pid: text[:500] for pid, text in it.explains.items()} if it.explains else {},
        }
