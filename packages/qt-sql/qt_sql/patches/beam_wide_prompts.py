"""Beam Wide prompt builders — 8-16 qwen probes + R1 sniper synthesis.

Pipeline: AST front gate → R1 analyst (8-16 probes) → qwen strikes → R1 sniper.

Functions:
    filter_applicable_transforms() — AST front gate, wraps detection.py
    build_wide_analyst_prompt()   — R1 analyst: hypothesis + 8-16 probes
    build_wide_strike_prompt()    — qwen worker: one transform, SQL-in/SQL-out
    build_wide_sniper_prompt()    — R1 synthesizer: combine probe BDA
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

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


@dataclass
class ApplicableTransform:
    """A transform that passed the AST front gate."""
    id: str
    family: str
    principle: str
    gap: Optional[str]
    overlap_ratio: float
    engines: List[str]
    contraindications: List[Dict[str, Any]]
    notes: str = ""


# ── AST Front Gate ────────────────────────────────────────────────────────────

def filter_applicable_transforms(
    sql: str,
    engine: str,
    dialect: str = "postgres",
    min_overlap: float = 0.5,
) -> List[ApplicableTransform]:
    """AST front gate: filter transforms by precondition overlap.

    Deterministic, <50ms, no LLM calls. Kills transforms that can't
    possibly apply to this query's structure.

    Args:
        sql: Original SQL query.
        engine: Target engine ("duckdb", "postgresql", "snowflake").
        dialect: SQL dialect for parsing.
        min_overlap: Minimum precondition overlap ratio to survive.

    Returns:
        List of applicable transforms, sorted by overlap ratio descending.
    """
    from ..detection import detect_transforms, load_transforms

    transforms = load_transforms()

    # Normalize engine name for detection
    engine_key = engine.replace("postgresql", "postgres") if engine else None
    if engine_key == "postgres":
        engine_key = "postgresql"

    matches = detect_transforms(sql, transforms, engine=engine_key, dialect=dialect)

    applicable = []
    for m in matches:
        if m.overlap_ratio < min_overlap:
            continue

        # Check contraindications — if any are active, skip
        # (conservative: skip if any contraindication could apply)
        # For now we include them but pass caution to analyst
        t_data = next((t for t in transforms if t["id"] == m.id), {})

        applicable.append(ApplicableTransform(
            id=m.id,
            family=t_data.get("family", "?"),
            principle=t_data.get("principle", ""),
            gap=m.gap,
            overlap_ratio=m.overlap_ratio,
            engines=m.engines,
            contraindications=m.contraindications,
            notes=t_data.get("notes", ""),
        ))

    return applicable


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


# ── Wide Analyst Prompt (R1) ──────────────────────────────────────────────────

def build_wide_analyst_prompt(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    applicable_transforms: List[ApplicableTransform],
    gold_examples: Optional[Dict[str, Dict[str, Any]]] = None,
    dialect: str = "postgres",
    intelligence_brief: str = "",
) -> str:
    """Build the wide analyst prompt — same intelligence as focused, 8-16 probes out.

    Same sections as focused analyst (query, EXPLAIN, IR, gold examples,
    engine playbook) but different task: design 8-16 independent probes
    instead of 4 deep targets.

    Args:
        query_id: Query identifier.
        original_sql: Full original SQL.
        explain_text: EXPLAIN ANALYZE output.
        ir_node_map: IR node map (from render_ir_node_map).
        applicable_transforms: Transforms that passed the AST front gate.
        gold_examples: Dict mapping family ID to gold example JSON.
        dialect: SQL dialect.
        intelligence_brief: Pre-computed detection + classification summary.

    Returns:
        Complete analyst prompt.
    """
    from .beam_prompt_builder import _build_prompt_body

    engine_name = dialect.upper().replace('POSTGRES', 'PostgreSQL')
    role_text = (
        f"You are a SQL optimization analyst for {engine_name}. "
        "Your mission: diagnose this query's bottleneck from the EXPLAIN plan "
        "and design 8-16 independent transform PROBES.\n\n"
        "A team of workers will execute each probe independently — one transform "
        "per worker. Each worker outputs a PatchPlan JSON that transforms the "
        "query's IR structure. You design the strike package, they execute it.\n\n"
        "You have the full engine playbook, gold examples, and AST-detected "
        "transform matches below. Use them to design probes that exploit KNOWN "
        "engine weaknesses, not generic rewrites."
    )

    sections, n_families = _build_prompt_body(
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

    # ── AST Catalog Matches (additional suggestions) ─────────────────
    if applicable_transforms:
        transform_lines = [
            "## AST-Detected Transform Matches\n",
            "These transforms matched this query's AST structure (precondition "
            "overlap). Use as additional suggestions — you are NOT limited to "
            "this list.\n",
        ]

        for t in applicable_transforms:
            transform_lines.append(
                f"- **{t.id}** (Family {t.family}, {t.overlap_ratio:.0%} overlap): "
                f"{t.principle}"
            )
            if t.gap:
                transform_lines.append(f"  Engine gap: {t.gap}")

        sections.append("\n".join(transform_lines))

    # ── Wide Task: Design 8-16 Probes ────────────────────────────────
    sections.append("""## Your Task

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

    return "\n\n".join(sections)


# Keep old name as alias for backwards compatibility
build_wide_scout_prompt = build_wide_analyst_prompt


# ── Strike Worker Prompt (qwen) ───────────────────────────────────────────────

def build_wide_strike_prompt(
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


# ── Sniper Prompt (R1 synthesizer) ────────────────────────────────────────────

@dataclass
class StrikeBDA:
    """Battle Damage Assessment for a single strike."""
    probe_id: str
    transform_id: str
    family: str
    status: str          # PASS, FAIL, REGRESSION, NEUTRAL, WIN
    speedup: Optional[float]
    error: Optional[str]
    explain_text: Optional[str]
    sql: Optional[str]


def build_wide_sniper_prompt(
    query_id: str,
    original_sql: str,
    original_explain: str,
    hypothesis: str,
    strike_results: List[StrikeBDA],
    dialect: str = "postgres",
) -> str:
    """Build the sniper synthesis prompt for beam wide.

    The sniper sees all probe BDA and combines winning strategies into
    compound rewrites.

    Args:
        query_id: Query identifier.
        original_sql: Full original SQL.
        original_explain: EXPLAIN ANALYZE of original.
        hypothesis: Scout's bottleneck hypothesis.
        strike_results: BDA for all strikes.
        dialect: SQL dialect.

    Returns:
        Sniper synthesis prompt.
    """
    n_total = len(strike_results)
    n_pass = sum(1 for s in strike_results if s.status in ("PASS", "WIN", "IMPROVED"))
    n_win = sum(1 for s in strike_results if s.status == "WIN")

    sections = []

    # ── Role ──────────────────────────────────────────────────────────
    sections.append(
        "## Role\n\n"
        f"You are a strike synthesizer for {dialect.upper().replace('POSTGRES', 'PostgreSQL')}. "
        f"{n_total} transform probes were fired against query {query_id}. "
        f"{n_pass} passed validation, {n_win} showed speedup. "
        "Your job: combine the best insights into 2-3 compound rewrites."
    )

    # ── Original ──────────────────────────────────────────────────────
    sections.append(
        f"## Original SQL\n\n```sql\n{original_sql}\n```"
    )

    sections.append(
        f"## Bottleneck Hypothesis (from scout)\n\n{hypothesis}"
    )

    if original_explain:
        explain_lines = original_explain.strip().split("\n")[:40]
        explain_display = "\n".join(explain_lines)
        sections.append(
            f"## EXPLAIN (Original)\n\n```\n{explain_display}\n```"
        )

    # ── BDA Table ─────────────────────────────────────────────────────
    bda_lines = [
        "## Strike BDA (Battle Damage Assessment)\n",
        "| Probe | Transform | Family | Status | Speedup | Error/Notes |",
        "|-------|-----------|--------|--------|---------|-------------|",
    ]
    for s in strike_results:
        speedup_str = f"{s.speedup:.2f}x" if s.speedup else "-"
        error_str = (s.error or "")[:60]
        bda_lines.append(
            f"| {s.probe_id} | {s.transform_id} | {s.family} | "
            f"{s.status} | {speedup_str} | {error_str} |"
        )
    sections.append("\n".join(bda_lines))

    # ── EXPLAIN Plans for winning strikes ─────────────────────────────
    winning = [s for s in strike_results
               if s.status in ("WIN", "IMPROVED", "PASS")
               and s.speedup and s.speedup >= 1.0
               and s.explain_text]

    if winning:
        explain_sections = ["## EXPLAIN Plans (winning strikes)\n"]
        for s in sorted(winning, key=lambda x: -(x.speedup or 0)):
            explain_lines = s.explain_text.strip().split("\n")[:40]
            explain_display = "\n".join(explain_lines)
            explain_sections.append(
                f"### {s.probe_id}: {s.transform_id} ({s.speedup:.2f}x)\n"
                f"```\n{explain_display}\n```\n"
            )
        sections.append("\n".join(explain_sections))

    # ── SQL of winning strikes (for reference) ────────────────────────
    if winning:
        sql_sections = ["## SQL of Winning Strikes\n"]
        for s in sorted(winning, key=lambda x: -(x.speedup or 0))[:3]:
            if s.sql:
                sql_lines = s.sql.strip().split("\n")[:30]
                sql_display = "\n".join(sql_lines)
                if len(s.sql.strip().split("\n")) > 30:
                    sql_display += "\n..."
                sql_sections.append(
                    f"### {s.probe_id}: {s.transform_id} ({s.speedup:.2f}x)\n"
                    f"```sql\n{sql_display}\n```\n"
                )
        sections.append("\n".join(sql_sections))

    # ── Task ──────────────────────────────────────────────────────────
    sections.append("""## Your Task

The BDA tells you WHAT WORKS on this query. Design 2-3 compound rewrites:

1. **Analyze winning strikes**: Compare their EXPLAINs to original —
   where did row counts drop? Which operators changed? What bottleneck
   remains?

2. **Learn from failures**: Which transforms made things worse or broke
   correctness? What should NOT be combined?

3. **Design compound rewrites**: Stack winning transforms together.
   If probe p01 pushed a filter and p03 decorrelated, try both in one rewrite.

Output format:

```json
[
  {
    "strike_id": "s1",
    "strategy": "B1+A2: decorrelate + push item filter",
    "based_on": ["p01", "p03"],
    "confidence": 0.9,
    "sql": "WITH filtered_items AS (...) SELECT ..."
  }
]
```

Rules:
- Output ONLY the JSON array
- Each rewrite must be complete, executable SQL
- Do NOT change literal values
- Do NOT remove WHERE conditions
- Preserve column names, ordering, and LIMIT""")

    return "\n\n".join(sections)


# ── Parse Scout Response ──────────────────────────────────────────────────────

def parse_scout_response(response: str) -> Optional[ScoutResult]:
    """Parse the scout analyst's JSON response into a ScoutResult.

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
            logger.warning("No JSON found in scout response")
            return None

    try:
        data = json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse scout JSON: {e}")
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


def parse_sniper_response(response: str) -> List[Dict[str, Any]]:
    """Parse the sniper's JSON response into compound rewrite specs.

    Args:
        response: Raw LLM response text.

    Returns:
        List of rewrite dicts with strike_id, strategy, sql, confidence.
    """
    import re

    json_match = re.search(r'```json\s*\n?(.*?)\n?```', response, re.DOTALL)
    if json_match:
        json_text = json_match.group(1).strip()
    else:
        json_match = re.search(r'\[[\s\S]*\]', response)
        if json_match:
            json_text = json_match.group(0).strip()
        else:
            logger.warning("No JSON array found in sniper response")
            return []

    try:
        rewrites = json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse sniper JSON: {e}")
        return []

    if not isinstance(rewrites, list):
        return []

    return rewrites
