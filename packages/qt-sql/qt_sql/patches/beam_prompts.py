"""Beam prompt builders — analyst + worker probes for BEAM mode.

Pipeline: analyst (8-16 probes) → workers → compiler stage (beam_prompt_builder).

Functions:
    build_beam_analyst_prompt()    — analyst: hypothesis + 8-16 probes
    build_beam_worker_prompt()     — worker: one transform, DAG candidate out
    parse_analyst_response()       — parse analyst JSON response
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from qt_sql.knowledge.normalization import normalize_dialect

logger = logging.getLogger(__name__)

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
PROMPT_TEMPLATES_DIR = (
    Path(__file__).resolve().parent.parent / "prompts" / "templates" / "V3"
)
IR_SCHEMA_REFERENCE = """## IR Node Map Reference
- `S0` = top-level SELECT statement for this query mission.
- Anchor hashes are parser-generated and formatting-stable; copy verbatim.
- Use node ids/anchor hashes only as structural locators for downstream edits.
- If a locator is ambiguous, prefer safer coarse targeting and preserve semantics."""


@lru_cache(maxsize=8)
def _load_prompt_template(filename: str) -> str:
    """Load a prompt template from prompts/templates/V3."""
    path = PROMPT_TEMPLATES_DIR / filename
    if not path.exists():
        logger.warning("Prompt template missing: %s", path)
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception as e:
        logger.warning("Failed loading prompt template %s: %s", path, e)
        return ""


@lru_cache(maxsize=1)
def _load_transform_index() -> Dict[str, Dict[str, Any]]:
    """Load transform catalog indexed by transform id."""
    from ..detection import load_transforms

    transforms = load_transforms()
    by_id: Dict[str, Dict[str, Any]] = {}
    for t in transforms:
        if isinstance(t, dict) and t.get("id"):
            by_id[str(t["id"])] = t
    return by_id


def _build_transform_recipe_section(transform_id: str) -> str:
    """Build a compact, transform-specific execution recipe for workers."""
    by_id = _load_transform_index()
    t = by_id.get(transform_id)
    if not t:
        return "### Transform Recipe\nNo transform recipe found; follow probe assignment strictly."

    lines = [
        "### Transform Recipe",
        f"- `transform_id`: `{transform_id}`",
        f"- `family`: `{t.get('family', '?')}`",
    ]
    principle = t.get("principle")
    if principle:
        lines.append(f"- `principle`: {principle}")

    pre = t.get("precondition_features") or []
    if pre:
        lines.append(
            "- `expected_features`: "
            + ", ".join(f"`{str(x)}`" for x in pre[:8])
        )

    contra = t.get("contraindications") or []
    if contra:
        caution_bits = []
        for c in contra[:4]:
            if isinstance(c, dict):
                cid = c.get("id", "unknown")
                instr = c.get("instruction", "")
                caution_bits.append(f"{cid}: {instr}".strip(": "))
            else:
                caution_bits.append(str(c))
        lines.append("- `contraindications`: " + " | ".join(caution_bits))

    confirm = t.get("confirm_with_explain")
    if confirm is True:
        lines.append("- `confirm_with_explain`: true")

    return "\n".join(lines)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ProbeSpec:
    """A single transform probe to fire."""
    probe_id: str
    transform_id: str
    family: str
    target: str           # one-sentence WHERE to apply
    confidence: float
    gold_example_id: Optional[str] = None
    recommended_examples: List[str] = field(default_factory=list)
    node_contract: Optional[Dict[str, Any]] = None
    gates_checked: List[str] = field(default_factory=list)
    expected_explain_delta: str = ""
    recommended_patch_ops: List[str] = field(default_factory=list)
    phase: Optional[int] = None
    exploration: bool = False
    exploration_hypothesis: str = ""


@dataclass
class ScoutResult:
    """Output of the scout analyst."""
    hypothesis: str       # compressed bottleneck reasoning
    probes: List[ProbeSpec]
    dropped: List[Dict[str, str]]  # [{transform_id, reason}]
    equivalence_tier: str = ""
    reasoning_trace: List[str] = field(default_factory=list)
    do_not_do: List[str] = field(default_factory=list)


def _extract_cte_names(sql: str) -> List[str]:
    """Extract top-level CTE names from SQL for collision-avoidance hints."""
    if not sql:
        return []
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
        if not parsed:
            return []
        names: List[str] = []
        for cte in parsed.find_all(exp.CTE):
            alias = cte.alias_or_name
            if alias:
                names.append(str(alias))
        # Preserve order, deduplicate.
        return list(dict.fromkeys(names))
    except Exception:
        pass

    # Regex fallback for common WITH cte_name AS (...) patterns.
    import re
    ctes = re.findall(r"(?i)\bwith\s+(?:recursive\s+)?([a-z_][a-z0-9_]*)\s+as\b", sql)
    return list(dict.fromkeys(ctes))


# ── Gold Example Loader ───────────────────────────────────────────────────────

def _load_gold_example_for_family(
    family: str,
    dialect: str,
) -> Optional[Dict[str, Any]]:
    """Load the best gold example for a family+dialect.

    Returns the first matching example file, or None.
    """
    dialect_dir = dialect.replace("postgresql", "postgres")
    example_dir = EXAMPLES_DIR / dialect_dir

    if not example_dir.exists():
        return None

    for p in sorted(example_dir.glob("*.json")):
        try:
            with open(p) as f:
                ex = json.load(f)
            if ex.get("family", "") == family:
                return ex
        except Exception:
            continue

    return None


def _load_gold_example_by_id(
    example_id: str,
    dialect: str,
) -> Optional[Dict[str, Any]]:
    """Load a specific gold example by its ID."""
    dialect_dir = dialect.replace("postgresql", "postgres")
    example_dir = EXAMPLES_DIR / dialect_dir

    if not example_dir.exists():
        return None

    target_path = example_dir / f"{example_id}.json"
    if target_path.exists():
        try:
            with open(target_path) as f:
                return json.load(f)
        except Exception:
            return None

    # Fallback: scan all examples
    for p in sorted(example_dir.glob("*.json")):
        try:
            with open(p) as f:
                ex = json.load(f)
            if ex.get("id") == example_id:
                return ex
        except Exception:
            continue

    return None


def _format_gold_dag_hint(gold_dag_example: Dict[str, Any]) -> List[str]:
    """Compact gold DAG hint (avoid large JSON blocks in worker prompt)."""
    lines: List[str] = []
    if not isinstance(gold_dag_example, dict):
        return lines

    dag = gold_dag_example.get("dag")
    if not isinstance(dag, dict):
        # Allow bare DAG payloads.
        if isinstance(gold_dag_example.get("nodes"), list):
            dag = gold_dag_example
        else:
            return lines

    nodes = dag.get("nodes") or []
    if not isinstance(nodes, list) or not nodes:
        return lines

    node_ids = [
        str(n.get("node_id", "")).strip()
        for n in nodes
        if isinstance(n, dict) and str(n.get("node_id", "")).strip()
    ]
    changed_nodes = [
        str(n.get("node_id", "")).strip()
        for n in nodes
        if isinstance(n, dict)
        and n.get("changed") is True
        and str(n.get("node_id", "")).strip()
    ]
    final_node = str(dag.get("final_node_id", "")).strip() or "final_select"

    lines.extend([
        "### Gold DAG Pattern Reference",
        f"- `plan_id`: `{gold_dag_example.get('plan_id', 'gold_dag')}`",
        f"- `final_node_id`: `{final_node}`",
    ])
    if node_ids:
        lines.append("- `order`: " + ", ".join(f"`{n}`" for n in (dag.get("order") or node_ids)[:10]))
    if changed_nodes:
        lines.append(
            "- `changed_nodes`: " + ", ".join(f"`{n}`" for n in changed_nodes[:6])
        )
    lines.append("- Reuse DAG shape and invariants, not literal table/column names.")
    lines.append("")
    return lines


def _format_qerror_section(
    qerror_analysis: Optional[Any],
    *,
    heading: str = "## Estimation Errors (Q-Error)",
) -> str:
    """Render Q-Error routing guidance when analysis is available."""
    if qerror_analysis is None:
        return ""
    try:
        from ..qerror import format_qerror_for_prompt

        qerror_text = format_qerror_for_prompt(qerror_analysis)
    except Exception as e:
        logger.debug("Q-Error formatting unavailable: %s", e)
        return ""

    if not qerror_text:
        return ""
    return f"{heading}\n{qerror_text}"


def _format_iteration_history_section(
    iteration_history: Optional[Dict[str, Any]],
) -> str:
    """Compact prior-attempt summary for analyst guidance."""
    if not isinstance(iteration_history, dict):
        return ""
    attempts = iteration_history.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        return ""

    lines = [
        "## Previous Optimization Attempts",
        "Do not repeat failed strategies. Prefer ideas that improved speedup.",
    ]
    for i, attempt in enumerate(attempts[:8], 1):
        if not isinstance(attempt, dict):
            continue
        status = str(attempt.get("status", "unknown")).upper()
        speedup_raw = attempt.get("speedup")
        speedup_text = (
            f"{float(speedup_raw):.2f}x"
            if isinstance(speedup_raw, (int, float))
            else str(speedup_raw or "n/a")
        )
        transforms = (
            attempt.get("transforms")
            or attempt.get("transforms_applied")
            or []
        )
        if isinstance(transforms, list) and transforms:
            transforms_text = ", ".join(f"`{str(t)}`" for t in transforms[:4])
        else:
            transforms_text = "(none)"
        lines.append(
            f"- attempt_{i}: status={status}, speedup={speedup_text}, "
            f"transforms={transforms_text}"
        )
    return "\n".join(lines)


def _format_gold_overview_section(
    gold_examples: Optional[Dict[str, Dict[str, Any]]],
) -> str:
    """Compact gold example coverage summary for analyst dispatch."""
    if not isinstance(gold_examples, dict) or not gold_examples:
        return ""

    lines = [
        "## Gold DAG Pattern Cards",
        "Use these as pattern priors; adapt shape, not literal table names.",
    ]
    for family in sorted(gold_examples.keys()):
        ex = gold_examples.get(family) or {}
        if not isinstance(ex, dict):
            continue
        ex_id = str(ex.get("id") or "unknown")
        speedup = str(ex.get("verified_speedup") or "n/a")
        dag_example = ex.get("dag_example") or ex.get("dag")
        dag_payload = None
        if isinstance(dag_example, dict):
            if isinstance(dag_example.get("dag"), dict):
                dag_payload = dag_example.get("dag")
            elif isinstance(dag_example.get("nodes"), list):
                dag_payload = dag_example
        changed_nodes: List[str] = []
        if isinstance(dag_payload, dict):
            nodes = dag_payload.get("nodes") or []
            if isinstance(nodes, list):
                changed_nodes = [
                    str(n.get("node_id", "")).strip()
                    for n in nodes
                    if isinstance(n, dict)
                    and n.get("changed") is True
                    and str(n.get("node_id", "")).strip()
                ]
        if changed_nodes:
            changed_text = ", ".join(f"`{n}`" for n in changed_nodes[:4])
        else:
            changed_text = "(unknown)"
        lines.append(
            f"- family {family}: `{ex_id}` ({speedup}), changed_nodes={changed_text}"
        )
    return "\n".join(lines)


# ── Beam Analyst Prompt ───────────────────────────────────────────────────────

def build_beam_analyst_prompt(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    current_dag_map: str = "",
    gold_examples: Optional[Dict[str, Dict[str, Any]]] = None,
    dialect: str = "postgres",
    intelligence_brief: str = "",
    importance_stars: int = 2,
    budget_hint: str = "",
    schema_context: str = "",
    engine_knowledge: str = "",
    qerror_analysis: Optional[Any] = None,
    iteration_history: Optional[Dict[str, Any]] = None,
) -> str:
    """Build analyst prompt from beam_analyst_v3 template + dynamic tail."""
    template = _load_prompt_template("beam_analyst_v3.txt")
    stars = max(1, min(3, int(importance_stars or 1)))
    star_label = "*" * stars
    _ = ir_node_map  # Backward-compatible arg; DAG map is the structural source.

    dynamic_sections = [
        f"## Query ID\n{query_id}",
        (
            "## Runtime Dialect Contract\n"
            f"- target_dialect: {dialect}\n"
            "- runtime_dialect_is_source_of_truth: true\n"
            "- if static examples conflict, follow runtime dialect behavior"
        ),
        (
            "## Query Importance\n"
            f"- importance_stars: {stars}\n"
            f"- importance_label: {star_label}\n"
            f"- budget_hint: {budget_hint or 'n/a'}"
        ),
        f"## Original SQL\n```sql\n{original_sql}\n```",
        f"## Execution Plan\n```\n{explain_text}\n```",
        f"## Current DAG Node Map\n```\n{current_dag_map or '(not provided)'}\n```",
        _build_transform_catalog_section(dialect),
    ]
    qerror_section = _format_qerror_section(qerror_analysis)
    if qerror_section:
        dynamic_sections.append(qerror_section)
    history_section = _format_iteration_history_section(iteration_history)
    if history_section:
        dynamic_sections.append(history_section)
    gold_overview = _format_gold_overview_section(gold_examples)
    if gold_overview:
        dynamic_sections.append(gold_overview)
    if schema_context:
        dynamic_sections.append(f"## Schema / Index / Stats Context\n{schema_context}")
    if engine_knowledge:
        dynamic_sections.append(f"## Engine-Specific Knowledge\n{engine_knowledge}")
    if intelligence_brief:
        dynamic_sections.append(
            "## Additional Intelligence\n"
            f"{intelligence_brief}"
        )

    if template:
        return f"{template}\n\n" + "\n\n".join(dynamic_sections)

    # Minimal fallback if template file is unavailable.
    return "\n\n".join(
        [
            "## Role",
            "You are a Senior SQL Optimization Strategist. Return JSON with dispatch+hypothesis+probes+dropped.",
            "## Cache Boundary",
            "Everything below is query-specific input.",
        ]
        + dynamic_sections
    )

# ── Beam Worker Prompt (qwen) ─────────────────────────────────────────────────

def build_beam_worker_prompt(
    original_sql: str,
    ir_node_map: str,
    hypothesis: str,
    probe: ProbeSpec,
    current_dag_map: str = "",
    gold_dag_example: Optional[Dict[str, Any]] = None,
    explain_text: str = "",
    dialect: str = "postgres",
    schema_context: str = "",
    equivalence_tier: str = "",
    reasoning_trace: Optional[List[str]] = None,
    qerror_analysis: Optional[Any] = None,
    engine_knowledge: str = "",
    do_not_do: Optional[List[str]] = None,
    worker_lane: str = "qwen",
) -> str:
    """Build worker prompt from lane template + dynamic tail."""
    lane = str(worker_lane or "qwen").strip().lower()
    template_name = (
        "beam_reasoning_worker_v1.txt"
        if lane == "reasoner"
        else "beam_worker_v3.txt"
    )
    template = _load_prompt_template(template_name)
    lines = [
        f"## Shared Analyst Hypothesis\n{hypothesis or '(none)'}",
        (
            "## Runtime Dialect Contract\n"
            f"- target_dialect: {dialect}\n"
            "- runtime_dialect_is_source_of_truth: true\n"
            "- if static examples conflict, follow runtime dialect behavior"
        ),
        "## Probe Assignment",
        f"- transform_id: {probe.transform_id}",
        f"- family: {probe.family}",
        f"- target: {probe.target}",
        f"- phase: {probe.phase if probe.phase is not None else '?'}",
        f"- exploration: {'yes' if probe.exploration else 'no'}",
        f"- worker_lane: {lane}",
        f"- dialect: {dialect}",
    ]
    if probe.recommended_examples:
        lines.append(
            "- recommended_examples: "
            + ", ".join(f"`{e}`" for e in probe.recommended_examples[:6])
        )
    if probe.recommended_patch_ops:
        lines.append(
            "- recommended_patch_ops: "
            + ", ".join(f"`{op}`" for op in probe.recommended_patch_ops[:8])
        )
    if probe.expected_explain_delta:
        lines.append(f"- expected_explain_delta: {probe.expected_explain_delta}")
    if equivalence_tier:
        lines.append(f"- equivalence_tier: {equivalence_tier}")
    if probe.exploration and probe.exploration_hypothesis:
        lines.append(f"- exploration_hypothesis: {probe.exploration_hypothesis}")
    existing_ctes = _extract_cte_names(original_sql)
    lines.append(
        "- existing_ctes: "
        + (", ".join(f"`{name}`" for name in existing_ctes[:24]) if existing_ctes else "(none)")
    )

    lines.append("")
    lines.append("### Gates Checked")
    if probe.gates_checked:
        lines.append("; ".join(probe.gates_checked[:8]))
    else:
        lines.append("not provided")
    lines.append("")
    lines.append("### Analyst Do-Not-Do")
    if do_not_do:
        for item in do_not_do[:8]:
            lines.append(f"- {item}")
    else:
        lines.append("not provided")
    lines.append("")

    lines.extend(["### Node Contract\n"])
    if probe.node_contract:
        lines.extend([
            "```json",
            json.dumps(probe.node_contract, indent=2),
            "```",
            "",
        ])
    else:
        lines.extend([
            "not provided",
            "",
        ])
    lines.extend(
        [
            "### Original SQL\n",
            f"```sql\n{original_sql}\n```",
            "",
        ]
    )
    if explain_text:
        lines.extend(
            [
                "### Execution Plan Snippet\n",
                f"```\n{explain_text}\n```",
                "",
            ]
        )
    qerror_section = _format_qerror_section(
        qerror_analysis,
        heading="### Estimation Errors (Q-Error)",
    )
    if qerror_section:
        lines.extend(
            [
                qerror_section,
                "",
            ]
        )
    lines.extend(
        [
            "### Current IR Node Map\n",
            f"```\n{ir_node_map}\n```",
            "",
            "### Current DAG Node Map\n",
            f"```\n{current_dag_map or '(not provided)'}\n```",
            "",
        ]
    )
    if reasoning_trace:
        lines.append("### Analyst Reasoning Trace")
        for item in reasoning_trace[:4]:
            lines.append(f"- {item}")
        lines.append("")
    if schema_context:
        lines.append("### Schema / Index / Stats Context")
        lines.append(schema_context)
        lines.append("")
    if engine_knowledge:
        lines.append("### Engine-Specific Knowledge")
        lines.append(engine_knowledge)
        lines.append("")
    lines.append(_build_transform_recipe_section(probe.transform_id))
    lines.append("")

    # Gold DAG pattern (compact)
    if gold_dag_example:
        lines.extend(_format_gold_dag_hint(gold_dag_example))

    dynamic = "\n".join(lines)
    if template:
        return f"{template}\n\n{dynamic}"

    return "\n".join(
        [
            "## Role",
            "You are a Senior SQL Rewrite Engineer. Output ONLY one DAG JSON object.",
            "First character must be `{` with no leading whitespace.",
            "## Cache Boundary",
            "Everything below is probe-specific input.",
            dynamic,
        ]
    )


def build_beam_editor_strike_prompt(
    *,
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    transform_id: str,
    dialect: str = "postgres",
    schema_context: str = "",
) -> str:
    """Build dedicated single-call strike prompt for editor mode.

    This path intentionally avoids analyst overhead and injects one
    explicit transform target for the worker model.
    """
    template = _load_prompt_template("beam_strike_worker_v1.txt")
    tid = (transform_id or "").strip()
    if not tid:
        tid = "auto"

    dynamic_sections: List[str] = [
        f"## Query ID\n{query_id}",
        "## Strike Assignment",
        f"- mode: editor_strike",
        f"- transform_id: {tid}",
        f"- dialect: {dialect}",
        "- objective: one candidate rewrite using the selected transform",
        f"## Original SQL\n```sql\n{original_sql}\n```",
        f"## Execution Plan\n```\n{explain_text}\n```",
        IR_SCHEMA_REFERENCE,
        f"## IR Structure + Anchor Hashes\n```\n{ir_node_map}\n```",
        _build_transform_recipe_section(tid),
    ]

    if schema_context:
        dynamic_sections.append(f"## Schema / Index / Stats Context\n{schema_context}")

    dynamic = "\n\n".join(dynamic_sections)
    if template:
        return f"{template}\n\n{dynamic}"

    return "\n\n".join(
        [
            "## Role",
            "You are an editor strike worker. Return ONLY one strict JSON object.",
            "First character must be `{` with no leading whitespace.",
            "## Cache Boundary",
            "Everything below is strike-specific input.",
            dynamic,
        ]
    )


def build_beam_worker_retry_prompt(
    worker_prompt: str,
    *,
    probe_id: str,
    transform_id: str,
    gate_name: str,
    gate_error: str,
    failed_sql: str = "",
    previous_response: str = "",
    output_mode: str = "dag",
) -> str:
    """Append structured gate-failure feedback for one worker retry."""
    mode = str(output_mode or "dag").strip().lower()
    dag_mode = mode != "patchplan"
    parts = [
        worker_prompt,
        "",
        "## RETRY — Gate failure feedback (attempt 2/2)",
        (
            "Your previous rewrite failed validation. Return a corrected DAG JSON object only."
            if dag_mode
            else "Your previous patch failed validation. Return a corrected PatchPlan JSON only."
        ),
        "First character must be `{` and output must contain no markdown/prose.",
        "",
        "### Failure Object",
        "```json",
        json.dumps(
            {
                "probe_id": probe_id,
                "transform_id": transform_id,
                "gate": gate_name,
                "status": "FAIL",
                "error": gate_error,
            },
            indent=2,
        ),
        "```",
    ]
    if failed_sql:
        parts.extend(
            [
                "",
                "### Failed SQL (from attempt 1)",
                "```sql",
                failed_sql,
                "```",
            ]
        )
    if previous_response:
        parts.extend(
            [
                "",
                "### Previous Worker Output (attempt 1)",
                "```",
                previous_response,
                "```",
            ]
        )
    parts.extend(
        [
            "",
            "Fix only what caused the gate failure while preserving transform intent and semantics.",
            (
                "Output ONLY valid DAG JSON."
                if dag_mode
                else "Output ONLY valid PatchPlan JSON."
            ),
            (
                "Do not emit PatchPlan `steps`/`payload` fields in DAG mode."
                if dag_mode
                else "Never emit payload.sql; use payload.sql_fragment where SQL fragments are required."
            ),
        ]
    )
    return "\n".join(parts)

# ── Transform Catalog (all transforms with match annotations) ────────────────

def _build_transform_catalog_section(dialect: str) -> str:
    """Build full transform catalog (no pre-filter/radar)."""
    from ..detection import load_transforms

    transforms = load_transforms()
    dialect_norm = normalize_dialect(dialect)
    catalog = [
        t for t in transforms
        if isinstance(t, dict)
        and t.get("id")
    ]
    catalog.sort(key=lambda t: (str(t.get("family", "?")), str(t.get("id"))))

    lines = [
        "## Transform Catalog (full list; not pre-filtered)",
        "",
        f"- runtime_dialect: `{dialect_norm}`",
        "- selection_policy: prioritize native/universal transforms first.",
        "- portability_policy: non-native transforms may be used as exploration probes "
        "when runtime syntax/semantics remain valid and engine knowledge does not contraindicate.",
        "",
    ]
    for t in catalog:
        tid = t.get("id", "?")
        fam = t.get("family", "?")
        gap = t.get("gap", "-")
        principle = t.get("principle") or t.get("description") or "-"
        engines = t.get("engines") or []
        engines_list = ", ".join(sorted(str(e) for e in engines)) if engines else "all"
        is_native = not engines or dialect_norm in engines or "all" in engines
        support = "native_or_universal" if is_native else "portability_candidate"
        lines.append(
            f"- `{tid}` (Family {fam}, gap `{gap}`, support `{support}`, "
            f"engines `{engines_list}`): {principle}"
        )
    lines.append("")
    return "\n".join(lines)


# ── Parse Reasoner Response ──────────────────────────────────────────────────

def parse_analyst_response(response: str) -> Optional[ScoutResult]:
    """Parse the analyst JSON response into a ScoutResult.

    Args:
        response: Raw LLM response text.

    Returns:
        ScoutResult or None if parsing fails.
    """
    import re

    # Extract JSON block
    json_match = re.search(r'```json\s*\n?(.*?)\n?```', response, re.DOTALL)
    if json_match:
        json_text = json_match.group(1).strip()
    else:
        # Try raw JSON
        json_match = re.search(r'\{[\s\S]*\}', response)
        if json_match:
            json_text = json_match.group(0).strip()
        else:
            logger.warning("No JSON found in analyst response")
            return None

    try:
        data = json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse analyst JSON: {e}")
        return None

    dispatch = data.get("dispatch") if isinstance(data.get("dispatch"), dict) else {}
    explain_analysis = data.get("explain_analysis") or {}
    hypothesis = (
        data.get("hypothesis", "")
        or dispatch.get("hypothesis", "")
        or explain_analysis.get("bottleneck_hypothesis", "")
    )
    probes_raw = data.get("probes", [])
    dropped = data.get("dropped", [])

    probes = []
    for i, p in enumerate(probes_raw):
        rec = p.get("recommended_examples", [])
        if not isinstance(rec, list):
            rec = [str(rec)]
        gates = p.get("gates_checked", [])
        if not isinstance(gates, list):
            gates = [str(gates)]
        rec_ops = p.get("recommended_patch_ops", [])
        if not isinstance(rec_ops, list):
            rec_ops = [str(rec_ops)]
        try:
            confidence = float(p.get("confidence", 0.5))
        except (TypeError, ValueError):
            confidence = 0.5
        probes.append(
            ProbeSpec(
                probe_id=p.get("probe_id", f"p{i+1:02d}"),
                transform_id=p.get("transform_id", "unknown"),
                family=p.get("family", "?"),
                target=p.get("target", ""),
                confidence=confidence,
                gold_example_id=p.get("gold_example_id"),
                recommended_examples=[str(x) for x in rec if x],
                node_contract=p.get("node_contract"),
                gates_checked=[str(x) for x in gates if x],
                expected_explain_delta=str(p.get("expected_explain_delta", "")),
                recommended_patch_ops=[
                    str(x)
                    for x in rec_ops
                    if x
                ],
                phase=(
                    int(p["phase"])
                    if isinstance(p.get("phase"), (int, float, str))
                    and str(p.get("phase")).strip().isdigit()
                    else None
                ),
                exploration=bool(p.get("exploration", False)),
                exploration_hypothesis=str(p.get("exploration_hypothesis", "")),
            )
        )

    probe_count = dispatch.get("probe_count")
    if isinstance(probe_count, int) and probe_count > 0:
        probes = probes[:probe_count]

    dispatch_trace = dispatch.get("reasoning_trace") or []
    if not isinstance(dispatch_trace, list):
        dispatch_trace = [str(dispatch_trace)]
    do_not_do = dispatch.get("do_not_do") or []
    if not isinstance(do_not_do, list):
        do_not_do = [str(do_not_do)]

    return ScoutResult(
        hypothesis=hypothesis,
        probes=probes,
        dropped=dropped,
        equivalence_tier=str(dispatch.get("equivalence_tier", "")),
        reasoning_trace=[str(x) for x in dispatch_trace if x],
        do_not_do=[str(x) for x in do_not_do if x],
    )
