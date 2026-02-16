"""Beam prompt builders — reasoning dispatcher + worker probes for BEAM mode.

Pipeline: R1 dispatcher (8-16 probes) → workers → (sniper handled by beam_prompt_builder).

Functions:
    build_beam_dispatcher_prompt() — R1 analyst: hypothesis + 8-16 probes
    build_beam_worker_prompt()     — qwen worker: one transform, PatchPlan out
    parse_scout_response()         — parse dispatcher JSON response
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from qt_sql.knowledge.normalization import normalize_dialect

logger = logging.getLogger(__name__)

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"


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
    phase: Optional[int] = None
    exploration: bool = False
    exploration_hypothesis: str = ""


@dataclass
class ScoutResult:
    """Output of the scout analyst."""
    hypothesis: str       # compressed bottleneck reasoning
    probes: List[ProbeSpec]
    dropped: List[Dict[str, str]]  # [{transform_id, reason}]


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


def _format_gold_patch_hint(gold_patch_plan: Dict[str, Any]) -> List[str]:
    """Compact gold patch-plan hint (avoid large JSON blocks in worker prompt)."""
    lines: List[str] = []
    if not isinstance(gold_patch_plan, dict):
        return lines

    def _norm(op: str) -> str:
        if op == "replace_block_with_cte_pair":
            return "insert_cte+replace_from"
        return op

    steps = [
        s for s in gold_patch_plan.get("steps", [])
        if isinstance(s, dict)
    ]
    ops = [_norm(s.get("op")) for s in steps if s.get("op")]
    ctes = [
        s.get("payload", {}).get("cte_name")
        for s in steps
        if isinstance(s.get("payload"), dict) and s.get("payload", {}).get("cte_name")
    ]
    if not ops and not ctes:
        return lines

    lines.extend([
        "### Gold Pattern Reference",
        f"- `plan_id`: `{gold_patch_plan.get('plan_id', 'gold')}`",
    ])
    if ops:
        lines.append(f"- `step_ops`: {' -> '.join(ops[:8])}")
    if ctes:
        lines.append(f"- `ctes`: {', '.join(f'`{c}`' for c in ctes[:6])}")
    lines.append("- Reuse pattern shape, not literal table/column names.")
    lines.append("")
    return lines


# ── Beam Dispatcher Prompt (R1) ───────────────────────────────────────────────

def build_beam_dispatcher_prompt(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    gold_examples: Optional[Dict[str, Dict[str, Any]]] = None,
    dialect: str = "postgres",
    intelligence_brief: str = "",
) -> str:
    """Build the beam dispatcher prompt — diagnose bottleneck, design 8-16 probes.

    Shows all transforms from catalog with match/no-match annotations.
    No AST gate — the dispatcher sees everything and decides what to fire.

    Args:
        query_id: Query identifier.
        original_sql: Full original SQL.
        explain_text: EXPLAIN ANALYZE output.
        ir_node_map: IR node map (from render_ir_node_map).
        gold_examples: Dict mapping family ID to gold example JSON.
        dialect: SQL dialect.
        intelligence_brief: Pre-computed detection + classification summary.

    Returns:
        Complete dispatcher prompt.
    """
    from .beam_prompt_builder import (
        _build_prompt_body,
        _build_explain_analysis_procedure_section,
        _build_pathology_routing_section,
        _build_regression_registry_section,
        _build_aggregation_equivalence_rules_section,
    )

    engine_name = dialect.upper().replace('POSTGRES', 'PostgreSQL')
    role_text = (
        f"You are a SQL optimization analyst for {engine_name}. "
        "Diagnose the bottleneck from EXPLAIN and design 8-16 independent transform probes.\n\n"
        "Each probe is executed by one worker and must describe exactly where to apply one transform.\n"
        "Use dialect profile + family cards + transform radar to target known engine gaps."
    )

    static, dynamic, n_families = _build_prompt_body(
        query_id=query_id,
        original_sql=original_sql,
        explain_text=explain_text,
        ir_node_map=ir_node_map,
        all_5_examples=gold_examples or {},
        dialect=dialect,
        role_text=role_text,
        include_patch_plans=False,
        intelligence_brief=intelligence_brief,
        phase_a_items=[
            "Dialect Profile",
            "Optimization Families (with decision gates)",
            "EXPLAIN Analysis Procedure",
            "Pathology Routing + Pruning",
            "Regression Registry",
            "Aggregation Equivalence Rules",
            "Task Contract",
        ],
        phase_b_items=[
            "Query SQL",
            "Execution Plan",
            "IR Structure",
            "Detected Patterns / Transform Radar",
        ],
        extra_static_sections=[
            _build_explain_analysis_procedure_section(),
            _build_pathology_routing_section(),
            _build_regression_registry_section(),
            _build_aggregation_equivalence_rules_section(),
        ],
    )

    # Task section in static prefix (system instructions)
    static.append("""## Your Task

1. Run EXPLAIN procedure -> produce bottleneck hypothesis.
2. Route candidate families, then prune using stop-gates.
3. Check every candidate against regression registry.
4. Design 8-16 probes:
- one probe = one transform
- one probe = one precise target
- include node contract + gates checked
- reserve 1-2 probes for exploration

Output JSON:
```json
{
  "explain_analysis": {
    "cost_spine": "...",
    "bottleneck_hypothesis": "...",
    "scan_count": {"table": 3}
  },
  "hypothesis": "...",
  "probes": [
    {
      "probe_id": "p01",
      "transform_id": "decorrelate",
      "family": "B",
      "target": "...",
      "node_contract": {"from":"...","where":"...","output":["..."]},
      "gates_checked": ["not_simple_exists:PASS"],
      "phase": 2,
      "exploration": false,
      "confidence": 0.91,
      "recommended_examples": ["early_filter_decorrelate"]
    }
  ],
  "dropped": [{"transform_id":"...","family":"...","reason":"gate failed: ..."}]
}
```

Rules:
- rank by phase then expected impact
- phase ordering: row-volume reduction -> redundancy elimination -> topology repair
- use canonical family codes A-F
- include all dropped candidates with explicit gate-failure reason
- exploration probes must include `exploration_hypothesis`""")

    # Cache boundary
    static.append(
        "---\n\n"
        "## Cache Boundary\n"
        "Everything below is query-specific input.\n\n"
        "## Query to Analyze"
    )

    # Transform Catalog (per-query: AST match annotations depend on SQL)
    dynamic.append(_build_transform_catalog_section(original_sql, dialect))

    return "\n\n".join(static + dynamic)


# Keep old names as aliases for backwards compatibility
build_wide_analyst_prompt = build_beam_dispatcher_prompt
build_wide_scout_prompt = build_beam_dispatcher_prompt


# ── Beam Worker Prompt (qwen) ─────────────────────────────────────────────────

def build_beam_worker_prompt(
    original_sql: str,
    ir_node_map: str,
    hypothesis: str,
    probe: ProbeSpec,
    gold_patch_plan: Optional[Dict[str, Any]] = None,
    dialect: str = "postgres",
) -> str:
    """Build a single strike worker prompt for beam wide.

    Same structure as focused worker: IR + PatchPlan JSON output.
    Worker gets the analyst's target description and derives target IR.

    Args:
        original_sql: Full original SQL.
        ir_node_map: Current IR node map (from render_ir_node_map).
        hypothesis: Analyst's bottleneck hypothesis (shared context).
        probe: The specific probe to execute.
        gold_patch_plan: The patch_plan field from the recommended gold example.
        dialect: SQL dialect.

    Returns:
        Strike worker prompt.
    """
    engine_name = dialect.upper().replace('POSTGRES', 'PostgreSQL')

    # ═══ STATIC PREFIX (cached across all probes) ═════════════════
    lines = [
        "## Role\n",
        "Transform a SQL query by applying ONE specific optimization. "
        f"Target engine: {engine_name}.",
        "Output a PatchPlan JSON that transforms the query's IR structure.",
        "",
        "## Prompt Map\n",
        "### Phase A — Cached Instructions",
        "A1. Patch operations and output rules",
        "A2. Verification checklist",
        "A3. Gold pattern reference (if provided)",
        "",
        "### Phase B — Probe-Specific Input",
        "B1. Probe assignment + node contract",
        "B2. Original SQL",
        "B3. Current IR node map",
        "",
        "## Patch Operations\n",
        "| Op | Description | Payload |",
        "|----|-------------|---------|",
        "| insert_cte | Add a new CTE to the WITH clause | cte_name, cte_query_sql |",
        "| replace_from | Replace the FROM clause | from_sql |",
        "| replace_where_predicate | Replace the WHERE clause | expr_sql |",
        "| replace_body | Replace entire query body (SELECT, FROM, WHERE, GROUP BY) | sql_fragment |",
        "| replace_expr_subtree | Replace a specific expression | expr_sql (+ by_anchor_hash) |",
        "| delete_expr_subtree | Remove a specific expression | (target only, no payload) |",
        "",
        "## Instructions\n",
        "1. Read the **Target** description — it tells you WHERE and HOW to apply the transform",
        "2. Respect **Node Contract** as target-state spec (FROM/WHERE/OUTPUT).",
        "3. Design a target IR showing what the optimized query should look like",
        "4. Build patch steps to get from current IR → target IR",
        "5. Adapt the gold pattern shape; never copy literal SQL",
        "6. All SQL in payloads must be complete, executable fragments (no ellipsis)",
        f"7. Use dialect: \"{dialect}\" in the output",
        "8. Target all steps at by_node_id: \"S0\" (the main statement)",
        "",
        "**Semantic guards** — MUST preserve:",
        "- All WHERE/HAVING/ON conditions exactly",
        "- All literal values unchanged (35*0.01 stays as 35*0.01)",
        "- Column names, aliases, ORDER BY, and LIMIT exactly",
        "- Do NOT add new filter conditions",
        "- No orphaned CTEs or duplicated source scans after replacement",
        "",
        "## Verification Checklist",
        "- [ ] every new CTE has a selective WHERE",
        "- [ ] no orphaned CTEs/tables remain",
        "- [ ] EXISTS semantics preserved unless anti-join decorrelation is explicit",
        "- [ ] same-column OR conditions were not split into UNION branches",
        "- [ ] downstream consumers have all required projected columns",
        "",
        "Output ONLY the JSON object (no markdown, no explanation).",
        "",
        "---",
        "",
        "## Cache Boundary",
        "Everything below is probe-specific input.",
        "",
        "## Probe Assignment",
        "",
    ]

    # ═══ DYNAMIC SUFFIX (unique per probe) ════════════════════════
    lines.extend([
        f"**Transform**: {probe.transform_id} (Family {probe.family})",
        f"**Hypothesis**: {hypothesis}",
        f"**Target**: {probe.target}",
        f"**Phase**: {probe.phase if probe.phase is not None else '?'}",
        f"**Exploration**: {'yes' if probe.exploration else 'no'}",
        "",
    ])
    if probe.recommended_examples:
        lines.extend([
            "**Recommended examples**: "
            + ", ".join(f"`{e}`" for e in probe.recommended_examples[:4]),
            "",
        ])
    if probe.exploration and probe.exploration_hypothesis:
        lines.extend([
            f"**Exploration hypothesis**: {probe.exploration_hypothesis}",
            "",
        ])
    lines.append("### Gates Checked")
    if probe.gates_checked:
        lines.append("; ".join(probe.gates_checked[:8]))
    else:
        lines.append("not provided")
    lines.append("")

    lines.extend([
        "### Node Contract\n",
    ])
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
    lines.extend([
        "### Original SQL\n",
        f"```sql\n{original_sql}\n```",
        "",
        "### Current IR Node Map\n",
        f"```\n{ir_node_map}\n```",
        "",
    ])

    # Gold patch plan pattern (compact)
    if gold_patch_plan:
        lines.extend(_format_gold_patch_hint(gold_patch_plan))

    return "\n".join(lines)


# Keep old name as alias
build_wide_strike_prompt = build_beam_worker_prompt


# ── Transform Catalog (all transforms with match annotations) ────────────────

def _build_transform_catalog_section(
    sql: str,
    dialect: str,
) -> str:
    """Build compact transform radar for dispatcher probe planning.

    Args:
        sql: Original SQL for AST detection.
        dialect: SQL dialect.

    Returns:
        Formatted catalog section string.
    """
    from ..detection import detect_transforms, load_transforms

    transforms = load_transforms()
    by_id = {
        t["id"]: t for t in transforms
        if isinstance(t, dict) and t.get("id")
    }

    try:
        matches = detect_transforms(sql, transforms, dialect_filter=dialect, dialect=dialect)
    except Exception as e:
        logger.warning("AST detection failed, showing fallback transform buckets: %s", e)
        matches = []

    lines = ["## Transform Radar", ""]

    if matches:
        strong = [m for m in matches if m.overlap_ratio >= 0.5]
        if not strong:
            strong = matches[:8]
        strong = strong[:12]

        lines.append("### High-Fit Candidates")
        for m in strong:
            t = by_id.get(m.id, {})
            family = t.get("family", "?")
            gap = t.get("gap") or m.gap or "-"
            feats = ", ".join(m.matched_features[:4]) if m.matched_features else "-"
            lines.append(
                f"- `{m.id}` (Family {family}, {m.overlap_ratio:.0%}, gap `{gap}`) matched: {feats}"
            )
        lines.append("")

    dialect_norm = normalize_dialect(dialect)
    eligible = [
        t for t in transforms
        if not t.get("engines") or dialect_norm in (t.get("engines") or [])
    ]
    buckets: Dict[str, List[str]] = {}
    for t in eligible:
        fam = str(t.get("family", "?"))
        buckets.setdefault(fam, []).append(t["id"])

    lines.append("### Reserve Catalog by Family")
    for fam in sorted(buckets):
        ids = sorted(buckets[fam])[:6]
        lines.append(f"- Family {fam}: {', '.join(f'`{tid}`' for tid in ids)}")
    lines.append("")

    return "\n".join(lines)


# ── Parse Dispatcher Response ────────────────────────────────────────────────

def parse_scout_response(response: str) -> Optional[ScoutResult]:
    """Parse the dispatcher's JSON response into a ScoutResult.

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
        json_match = re.search(r'\{[\s\S]*"hypothesis"[\s\S]*\}', response)
        if json_match:
            json_text = json_match.group(0).strip()
        else:
            logger.warning("No JSON found in dispatcher response")
            return None

    try:
        data = json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse dispatcher JSON: {e}")
        return None

    explain_analysis = data.get("explain_analysis") or {}
    hypothesis = data.get("hypothesis", "") or explain_analysis.get(
        "bottleneck_hypothesis", ""
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
        probes.append(ProbeSpec(
            probe_id=p.get("probe_id", f"p{i+1:02d}"),
            transform_id=p.get("transform_id", "unknown"),
            family=p.get("family", "?"),
            target=p.get("target", ""),
            confidence=float(p.get("confidence", 0.5)),
            gold_example_id=p.get("gold_example_id"),
            recommended_examples=[str(x) for x in rec if x],
            node_contract=p.get("node_contract"),
            gates_checked=[str(x) for x in gates if x],
            phase=int(p["phase"]) if isinstance(p.get("phase"), (int, float, str)) and str(p.get("phase")).strip().isdigit() else None,
            exploration=bool(p.get("exploration", False)),
            exploration_hypothesis=str(p.get("exploration_hypothesis", "")),
        ))

    return ScoutResult(
        hypothesis=hypothesis,
        probes=probes,
        dropped=dropped,
    )
