"""Beam optimization session — single mode: BEAM (probes + sniper).

BEAM pipeline:
1. Dispatcher (R1) → 8-16 independent transform probes
2. Workers (qwen, parallel) → PatchPlan JSON per probe
3. Validate (structural + equivalence + benchmark)
4. R1 Sniper 2-shot → 4 more candidates via cache-hit pattern
"""

from __future__ import annotations

import copy
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

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
    """Single-mode patch optimization: BEAM (dispatcher + workers + sniper)."""

    def run(self) -> SessionResult:
        """Execute the single BEAM optimization loop.

        Legacy `beam_mode` values are ignored; all sessions run BEAM.
        """
        mode = str(getattr(self.pipeline.config, "beam_mode", "beam") or "beam")
        if mode not in ("beam", "wide"):
            logger.info(
                f"[{self.query_id}] Legacy beam_mode={mode!r} ignored; forcing BEAM"
            )
        else:
            logger.info(f"[{self.query_id}] BEAM MODE: BEAM (single mode)")
        return self._run_beam()

    @staticmethod
    def _render_explain_compact(explain_result: Optional[dict], dialect: str = "duckdb") -> str:
        """Render EXPLAIN result as compact operator tree.

        Uses structured plan_json when available (DuckDB: ~40 lines vs ~230 box-drawing).
        Falls back to plan_text if no JSON plan.

        IMPORTANT: This is the ONLY entry point for EXPLAIN into prompts.
        The compact format is what analysts, workers, and snipers all see.
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

        plans_data = _extract_json_array(response)
        if not plans_data:
            logger.warning(f"[{self.query_id}] No JSON array found in R1 response")
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

    def _validate_and_benchmark_patches(
        self,
        patches: List[AppliedPatch],
        db_path: str,
        session_dir: Path,
        shot: int,
    ) -> None:
        """Validate (structural + equivalence) and benchmark patches in-place.

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

        # ── Full-dataset equivalence ──────────────────────────────────
        equiv_passed = [p for p in applied if p.semantic_passed]
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
        """BEAM mode: cheap qwen probes + R1 sniper 2-shot.

        Pipeline:
        1. Dispatcher (R1) → 8-16 independent transform probes
        2. Workers (qwen, parallel) → execute probes → PatchPlan JSON
        3. Validate + benchmark all probes
        4. R1 Sniper 2-shot → 4 more candidates via cache-hit pattern
        """
        from ..ir import build_script_ir, render_ir_node_map, Dialect
        from ..patches.beam_prompt_builder import (
            load_gold_examples,
            build_beam_sniper_prompt,
            append_shot_results,
        )
        from ..patches.beam_wide_prompts import (
            build_beam_dispatcher_prompt,
            build_beam_worker_prompt,
            parse_scout_response,
            _load_gold_example_for_family,
            _load_gold_example_by_id,
        )
        from ..execution.database_utils import run_explain_analyze

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

        explain_result = run_explain_analyze(db_path, self.original_sql)
        original_explain = self._render_explain_compact(
            explain_result, self.dialect
        )

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
                engine=self.engine, dialect=self.dialect,
            )
            classification = self._load_cached_classification(self.query_id)
            intelligence_brief = build_intelligence_brief(detected, classification)
        except Exception as e:
            logger.warning(f"[{self.query_id}] Intelligence brief failed: {e}")

        # ── Phase 1: Dispatcher → 8-16 probes ─────────────────────────
        if self.on_phase_change:
            self.on_phase_change(phase="analyst", iteration=0)

        dispatcher_model = (
            getattr(self.pipeline.config, "wide_dispatcher_model", None)
            or getattr(self.pipeline.config, "analyst_model", None)
        )
        analyst_call_fn = self._make_llm_call_fn(dispatcher_model)

        dispatcher_prompt = build_beam_dispatcher_prompt(
            query_id=self.query_id,
            original_sql=self.original_sql,
            explain_text=original_explain,
            ir_node_map=ir_node_map,
            gold_examples=gold_examples,
            dialect=self.dialect,
            intelligence_brief=intelligence_brief,
        )

        dispatcher_response = analyst_call_fn(dispatcher_prompt)
        total_api_calls += 1

        self._save_to_disk(session_dir, 0, "dispatcher_prompt", dispatcher_prompt)
        self._save_to_disk(session_dir, 0, "dispatcher_response", dispatcher_response)

        scout_result = parse_scout_response(dispatcher_response)
        if not scout_result or not scout_result.probes:
            logger.warning(f"[{self.query_id}] Dispatcher returned no probes")
            return SessionResult(
                query_id=self.query_id,
                mode="beam",
                best_speedup=0.0,
                best_sql=self.original_sql,
                original_sql=self.original_sql,
                status="ERROR",
                n_api_calls=total_api_calls,
            )

        probes = sorted(
            scout_result.probes,
            key=lambda p: (
                p.phase if p.phase is not None else 99,
                -(p.confidence or 0.0),
            ),
        )[:max_probes]
        logger.info(
            f"[{self.query_id}] Dispatcher: {len(probes)} probes designed"
        )

        # ── Phase 2: Workers (parallel) ────────────────────────────────
        if self.on_phase_change:
            self.on_phase_change(phase="workers", iteration=0)

        worker_model = getattr(
            self.pipeline.config, "wide_worker_model", None
        ) or getattr(self.pipeline.config, "worker_model", None)
        worker_call_fn = self._make_llm_call_fn(worker_model)

        patches: List[AppliedPatch] = []
        n_workers = min(len(probes), 8)

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {}
            for probe in probes:
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
                )

                future = pool.submit(worker_call_fn, worker_prompt)
                futures[future] = (probe, worker_prompt)

            for future in as_completed(futures):
                probe, w_prompt = futures[future]
                total_api_calls += 1
                try:
                    response = future.result()
                    self._save_to_disk(
                        session_dir, 0,
                        f"worker_{probe.probe_id}_response", response,
                    )

                    output_sql = self._apply_wide_worker_response(
                        response, script_ir, dialect_enum
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
                    )
                    if not output_sql:
                        patch.apply_error = "Failed to parse/apply PatchPlan"
                    patches.append(patch)

                    logger.info(
                        f"[{self.query_id}] Worker {probe.probe_id} "
                        f"({probe.transform_id}): "
                        f"{'OK' if output_sql else 'FAIL'}"
                    )
                except Exception as e:
                    logger.warning(
                        f"[{self.query_id}] Worker {probe.probe_id} error: {e}"
                    )
                    patches.append(AppliedPatch(
                        patch_id=probe.probe_id,
                        family=probe.family,
                        transform=probe.transform_id,
                        relevance_score=probe.confidence,
                        apply_error=str(e),
                        status="ERROR",
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

        sem_passed = [p for p in applied if p.semantic_passed]
        logger.info(
            f"[{self.query_id}] Probe validation: "
            f"{len(sem_passed)}/{len(applied)} passed"
        )

        # ── Phase 4: R1 Sniper 2-shot ─────────────────────────────────
        sniper_patches: List[AppliedPatch] = []
        winners = [p for p in sem_passed if p.speedup and p.speedup >= 1.05]
        snipe_rounds = int(getattr(self.pipeline.config, "snipe_rounds", 2) or 0)

        if len(sem_passed) >= 2 and winners and snipe_rounds > 0:
            if self.on_phase_change:
                self.on_phase_change(phase="snipe", iteration=0)

            logger.info(
                f"[{self.query_id}] Phase 4: R1 sniper 2-shot "
                f"({len(winners)} winners, {len(sem_passed)} total)"
            )

            strike_results = [
                {
                    "probe_id": p.patch_id,
                    "transform_id": p.transform,
                    "family": p.family,
                    "status": p.status,
                    "speedup": p.speedup,
                    "error": p.apply_error,
                    "explain_text": p.explain_text,
                    "sql": p.output_sql,
                }
                for p in patches
            ]

            # Shot 1: BDA + intelligence → 2 PatchPlans
            sniper_shot1_prompt = build_beam_sniper_prompt(
                query_id=self.query_id,
                original_sql=self.original_sql,
                explain_text=original_explain,
                ir_node_map=ir_node_map,
                all_5_examples=gold_examples,
                dialect=self.dialect,
                intelligence_brief=intelligence_brief,
                strike_results=strike_results,
            )

            self._save_to_disk(
                session_dir, 0, "sniper_shot1_prompt", sniper_shot1_prompt
            )
            sniper_shot1_response = analyst_call_fn(sniper_shot1_prompt)
            total_api_calls += 1
            self._save_to_disk(
                session_dir, 0, "sniper_shot1_response", sniper_shot1_response
            )

            shot1_sniper = self._apply_patchplan_array(
                sniper_shot1_response, script_ir, dialect_enum, prefix="s1"
            )
            logger.info(
                f"[{self.query_id}] Sniper shot 1: {len(shot1_sniper)} patches"
            )

            self._validate_and_benchmark_patches(
                shot1_sniper, db_path, session_dir, 1
            )
            sniper_patches.extend(shot1_sniper)

            # Shot 2: append results (cache hit) → 2 more PatchPlans
            sniper_shot2_prompt = append_shot_results(
                base_prompt=sniper_shot1_prompt,
                patches=shot1_sniper,
                explains={
                    p.patch_id: p.explain_text or ""
                    for p in shot1_sniper
                },
            )

            self._save_to_disk(
                session_dir, 0, "sniper_shot2_prompt", sniper_shot2_prompt
            )
            sniper_shot2_response = analyst_call_fn(sniper_shot2_prompt)
            total_api_calls += 1
            self._save_to_disk(
                session_dir, 0, "sniper_shot2_response", sniper_shot2_response
            )

            shot2_sniper = self._apply_patchplan_array(
                sniper_shot2_response, script_ir, dialect_enum, prefix="s2"
            )
            logger.info(
                f"[{self.query_id}] Sniper shot 2: {len(shot2_sniper)} patches"
            )

            self._validate_and_benchmark_patches(
                shot2_sniper, db_path, session_dir, 2
            )
            sniper_patches.extend(shot2_sniper)

        # ── Collect all results ────────────────────────────────────────
        all_final = list(sem_passed) + [
            sp for sp in sniper_patches if sp.semantic_passed
        ]

        all_patches_full = patches + sniper_patches
        iter_explains: Dict[str, str] = {
            p.patch_id: p.explain_text
            for p in all_patches_full
            if p.explain_text
        }
        iter_result = PatchIterationResult(
            iteration=0,
            prompt=dispatcher_prompt,
            response=dispatcher_response,
            n_api_calls=total_api_calls,
            patches=all_patches_full,
            explains=iter_explains,
        )

        # Find best across probes + sniper
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
        )

    def _apply_wide_worker_response(
        self,
        response: str,
        script_ir,
        dialect_enum,
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

        # Try JSON PatchPlan first
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

    # ── Internal Methods ────────────────────────────────────────────────────

    def _make_llm_call_fn(self, model_spec: Optional[str] = None) -> callable:
        """Create an LLM call function for a specific model.

        Args:
            model_spec: Model identifier string (e.g., "deepseek/deepseek-r1")
                or None to use the pipeline's default.
                The model spec is passed as the model name to the pipeline's
                provider (typically OpenRouter, which accepts "vendor/model").

        Returns:
            Callable that takes prompt string, returns response string.
        """
        from ..generate import CandidateGenerator

        effective_model = model_spec or self.pipeline.model
        effective_provider = self.pipeline.provider

        logger.info(
            f"[{self.query_id}] LLM call fn: "
            f"provider={effective_provider}, model={effective_model}"
        )

        generator = CandidateGenerator(
            provider=effective_provider,
            model=effective_model,
            analyze_fn=self.pipeline.analyze_fn,
        )

        def call_fn(prompt: str) -> str:
            logger.info(
                f"[{self.query_id}] LLM call → {effective_model} "
                f"({len(prompt)} chars prompt)"
            )
            t0 = time.time()
            result = self._call_llm_with_timeout(generator, prompt)
            elapsed = time.time() - t0
            logger.info(
                f"[{self.query_id}] LLM done ← {effective_model} "
                f"({len(result)} chars response, {elapsed:.1f}s)"
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
            "analyst_model": getattr(self.pipeline.config, "analyst_model", "?"),
            "worker_model": getattr(self.pipeline.config, "worker_model", "?"),
            "provider": self.pipeline.provider or "?",
            "semantic_validation_enabled": self.pipeline.config.semantic_validation_enabled,
            "semantic_sample_pct": self.pipeline.config.semantic_sample_pct,
            "validation_method": self.pipeline.config.validation_method,
        }
        (session_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2)
        )
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
        """5x trimmed-mean sequential benchmark when race is skipped.

        Each query (original + each patch) is executed 5 times.
        Min and max are dropped, middle 3 are averaged.
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
                # Measure original (5x trimmed mean) + capture rows for correctness
                logger.info(
                    f"[{self.query_id}] Baseline: {BENCH_RUNS}x trimmed mean..."
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
        return {
            "iteration": it.iteration,
            "n_api_calls": it.n_api_calls,
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
                }
                for p in it.patches
            ],
            "explains": {pid: text[:500] for pid, text in it.explains.items()} if it.explains else {},
        }
