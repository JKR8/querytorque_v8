"""Beam prompt builders — qwen dispatcher + workers for BEAM mode.

Pipeline: R1 dispatcher (8-16 probes) → qwen workers → (sniper handled by beam_prompt_builder).

Functions:
    build_beam_dispatcher_prompt() — R1 analyst: hypothesis + 8-16 probes
    build_beam_worker_prompt()     — qwen worker: one transform, PatchPlan out
    parse_scout_response()         — parse dispatcher JSON response
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    from .beam_prompt_builder import _build_prompt_body

    engine_name = dialect.upper().replace('POSTGRES', 'PostgreSQL')
    role_text = (
        f"You are a SQL optimization analyst for {engine_name}. "
        "Your mission: diagnose a query's bottleneck from the EXPLAIN plan "
        "and design 8-16 independent transform PROBES.\n\n"
        "A team of workers will execute each probe independently — one transform "
        "per worker. Each worker outputs a PatchPlan JSON that transforms the "
        "query's IR structure. You design the strike package, they execute it.\n\n"
        "You have the full engine playbook, gold examples, and the complete "
        "transform catalog below. Use them to design probes that exploit KNOWN "
        "engine weaknesses, not generic rewrites."
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
    )

    # Task section in static prefix (system instructions)
    static.append("""## Your Task

### Step 1: BOTTLENECK HYPOTHESIS (2-3 sentences)
Cite specific EXPLAIN operators, row counts, and cost. This hypothesis
gets passed to every worker as shared context.

### Step 2: DESIGN 8-16 PROBES
Each probe = ONE narrow transform applied to ONE specific part of the query.
Draw from the playbook pathologies, gold examples, AST matches, and your
own EXPLAIN-driven hypotheses.

Each probe's `target` field must tell the worker exactly WHERE and HOW to
apply the transform — the worker uses it to derive a target IR and build
a PatchPlan.

Good probes are specific:
- "Convert the 3 correlated NOT EXISTS subqueries (web_sales, catalog_sales) into MATERIALIZED CTEs with DISTINCT customer_sk, then use LEFT JOIN ... IS NULL anti-pattern"
- "Extract the shared date_dim filter (d_year=2002, d_moy BETWEEN 10 AND 12) into a single CTE, join all 3 fact tables through it"
- "Replace comma join customer,customer_address,customer_demographics with explicit INNER JOIN chain"

Bad probes are vague:
- "Optimize the subqueries" (which ones? how?)
- "Add indexes" (not a SQL rewrite)

### Step 3: OUTPUT

```json
{
  "hypothesis": "The bottleneck is ...",
  "probes": [
    {
      "probe_id": "p01",
      "transform_id": "decorrelate_not_exists_to_cte",
      "family": "B",
      "target": "Convert the NOT EXISTS (web_sales, date_dim) correlated subquery into a MATERIALIZED CTE: SELECT DISTINCT ws_bill_customer_sk FROM web_sales JOIN date_dim ON ws_sold_date_sk = d_date_sk WHERE d_year = 2002 AND d_moy BETWEEN 10 AND 12 AND ws_list_price BETWEEN 80 AND 169. Then replace NOT EXISTS with LEFT JOIN cte ON c_customer_sk = ws_bill_customer_sk WHERE ws_bill_customer_sk IS NULL",
      "confidence": 0.95,
      "recommended_examples": ["early_filter_decorrelate"]
    }
  ],
  "dropped": [
    {"transform_id": "materialize_cte", "reason": "No CTEs in original query to materialize"}
  ]
}
```

Rules:
- Design 8-16 probes (more is better — they're cheap)
- Each probe = ONE transform, not a compound strategy
- Rank by expected impact (highest confidence first)
- `target` is critical: specific enough that a worker can derive a target IR from it
- `recommended_examples`: list gold example IDs the worker should use as patch template
- Include `dropped` list for AST catalog suggestions you chose not to fire
- `transform_id` can be a catalog ID or a descriptive name you invent
- Family codes: A=Early Filter, B=Decorrelate, C=Aggregation, D=Set Ops, E=Materialization, F=Join Transform""")

    # Cache boundary
    static.append("---\n\n## Query to Analyze")

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
    lines = [
        "## Role\n",
        "Transform this SQL query by applying ONE specific optimization. "
        f"Target engine: {dialect.upper().replace('POSTGRES', 'PostgreSQL')}.",
        "Output a PatchPlan JSON that transforms the query's IR structure.",
        "",
        f"**Transform**: {probe.transform_id} (Family {probe.family})",
        f"**Hypothesis**: {hypothesis}",
        f"**Target**: {probe.target}",
        "",
        "## Original SQL\n",
        f"```sql\n{original_sql}\n```",
        "",
        "## Current IR Node Map\n",
        f"```\n{ir_node_map}\n```",
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
    ]

    # Gold patch plan example
    if gold_patch_plan:
        lines.extend([
            "## Gold Patch Example (reference pattern)\n",
            "```json",
            json.dumps(gold_patch_plan, indent=2),
            "```",
            "",
        ])

    lines.extend([
        "## Instructions\n",
        "1. Read the **Target** description above — it tells you WHERE and HOW to apply the transform",
        "2. Design a target IR showing what the optimized query should look like",
        "3. Build patch steps to get from current IR → target IR",
        "4. Adapt the gold example pattern to THIS query's tables, columns, and predicates",
        "5. All SQL in payloads must be complete, executable fragments (no ellipsis)",
        f"6. Use dialect: \"{dialect}\" in the output",
        "7. Target all steps at by_node_id: \"S0\" (the main statement)",
        "",
        "**Semantic guards** — MUST preserve:",
        "- All WHERE/HAVING/ON conditions exactly",
        "- All literal values unchanged (35*0.01 stays as 35*0.01)",
        "- Column names, aliases, ORDER BY, and LIMIT exactly",
        "- Do NOT add new filter conditions",
        "",
        "Output ONLY the JSON object (no markdown, no explanation):",
    ])

    return "\n".join(lines)


# Keep old name as alias
build_wide_strike_prompt = build_beam_worker_prompt


# ── Transform Catalog (all transforms with match annotations) ────────────────

def _build_transform_catalog_section(
    sql: str,
    dialect: str,
) -> str:
    """Build the full transform catalog with match/no-match annotations.

    Loads ALL transforms from the catalog and runs AST detection to
    annotate which ones match this query. No filtering — the dispatcher
    sees everything and decides what to fire.

    Args:
        sql: Original SQL for AST detection.
        dialect: SQL dialect.

    Returns:
        Formatted catalog section string.
    """
    from ..detection import detect_transforms, load_transforms

    transforms = load_transforms()

    # Run detection to get overlap ratios
    engine_key = dialect.replace("postgresql", "postgres") if dialect else None
    if engine_key == "postgres":
        engine_key = "postgresql"

    try:
        matches = detect_transforms(sql, transforms, engine=engine_key, dialect=dialect)
        match_map = {m.id: m for m in matches}
    except Exception as e:
        logger.warning(f"AST detection failed, showing all transforms without annotations: {e}")
        match_map = {}

    # Split into matched and unmatched
    matched = []
    unmatched = []
    for t in transforms:
        tid = t["id"]
        m = match_map.get(tid)
        if m and m.overlap_ratio >= 0.5:
            matched.append((t, m))
        else:
            unmatched.append(t)

    # Sort matched by overlap ratio descending
    matched.sort(key=lambda x: x[1].overlap_ratio, reverse=True)

    lines = ["## Transform Catalog\n"]

    if matched:
        lines.append("### Matched (preconditions overlap with this query):\n")
        for t, m in matched:
            family = t.get("family", "?")
            principle = t.get("principle", "")
            gap_str = f" — Engine gap: {m.gap}" if m.gap else ""
            lines.append(
                f"- **{t['id']}** (Family {family}, {m.overlap_ratio:.0%} overlap): "
                f"{principle}{gap_str}"
            )
        lines.append("")

    if unmatched:
        lines.append("### Available (no precondition match — try if EXPLAIN warrants):\n")
        for t in unmatched:
            family = t.get("family", "?")
            principle = t.get("principle", "")
            lines.append(f"- **{t['id']}** (Family {family}): {principle}")
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

    hypothesis = data.get("hypothesis", "")
    probes_raw = data.get("probes", [])
    dropped = data.get("dropped", [])

    probes = []
    for i, p in enumerate(probes_raw):
        probes.append(ProbeSpec(
            probe_id=p.get("probe_id", f"p{i+1:02d}"),
            transform_id=p.get("transform_id", "unknown"),
            family=p.get("family", "?"),
            target=p.get("target", ""),
            confidence=float(p.get("confidence", 0.5)),
        ))

    return ScoutResult(
        hypothesis=hypothesis,
        probes=probes,
        dropped=dropped,
    )
