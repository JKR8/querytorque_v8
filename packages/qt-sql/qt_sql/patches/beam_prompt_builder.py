"""Build beam patch optimization prompt with all 5 families + gold examples.

The prompt shows all 5 optimization families (A-E) with gold examples,
instructs the LLM to choose the 4 most relevant families for this specific query,
and outputs 4 independent patch plans.
"""

from typing import Optional, Dict, Any, List, Tuple
import json
import logging

from qt_sql.prompter import load_exploit_algorithm, _load_engine_profile

logger = logging.getLogger(__name__)


# ── Family Descriptions ────────────────────────────────────────────────────

FAMILY_DESCRIPTIONS = {
    "A": {
        "name": "Early Filtering",
        "pattern": "Predicate Pushback",
        "description": "Push small filters into CTEs early, reduce row count before expensive operations",
        "speedup_range": "1.3–4.0x",
        "win_rate": "~35% of all wins",
        "use_when": [
            "Late WHERE filters on dimension tables",
            "Cascading CTEs with filters applied downstream",
            "Expensive joins after filters could be pushed earlier"
        ]
    },
    "B": {
        "name": "Decorrelation",
        "pattern": "Sets Over Loops",
        "description": "Convert correlated subqueries to standalone CTEs with GROUP BY, eliminate per-row re-execution",
        "speedup_range": "2.4–2.9x",
        "win_rate": "~15% of all wins",
        "use_when": [
            "Correlated subqueries in WHERE clause",
            "Scalar aggregates computed per outer row",
            "DELIM_SCAN in execution plan (indicates correlation)"
        ]
    },
    "C": {
        "name": "Aggregation Pushdown",
        "pattern": "Minimize Rows Touched",
        "description": "Aggregate before expensive joins when GROUP BY keys ⊇ join keys, reduce intermediate sizes",
        "speedup_range": "1.3–15.3x",
        "win_rate": "~5% of all wins (high variance)",
        "use_when": [
            "GROUP BY happens after large joins",
            "GROUP BY keys are subset of join keys",
            "Intermediate result size >> final result size"
        ]
    },
    "D": {
        "name": "Set Operation Optimization",
        "pattern": "Sets Over Loops",
        "description": "Replace INTERSECT/UNION-based patterns with EXISTS/NOT EXISTS, avoid full materialization",
        "speedup_range": "1.7–2.7x",
        "win_rate": "~8% of all wins",
        "use_when": [
            "INTERSECT patterns between large sets",
            "UNION ALL with duplicate elimination",
            "Set operations materializing full intermediate results"
        ]
    },
    "E": {
        "name": "Materialization / Prefetch",
        "pattern": "Don't Repeat Work",
        "description": "Extract repeated scans or pre-compute intermediate results for reuse across multiple consumers",
        "speedup_range": "1.3–6.2x",
        "win_rate": "~18% of all wins",
        "use_when": [
            "Repeated scans of same table with different filters",
            "Dimension filters applied independently multiple times",
            "CTE referenced multiple times with implicit re-evaluation"
        ]
    },
    "F": {
        "name": "Join Transform",
        "pattern": "Right Shape First",
        "description": "Restructure join topology: convert comma joins to explicit INNER JOIN, optimize join order, eliminate self-joins via single-pass aggregation",
        "speedup_range": "1.8–8.6x",
        "win_rate": "~19% of all wins",
        "use_when": [
            "Comma-separated joins (implicit cross joins) in FROM clause",
            "Self-joins scanning same table multiple times",
            "Dimension-fact join order suboptimal for predicate pushdown"
        ]
    }
}


# ── Build Individual Family Sections ───────────────────────────────────────

def format_family_section(
    family_id: str,
    gold_example: Dict[str, Any],
    include_patch_plans: bool = True,
    analyst_only: bool = False,
) -> str:
    """Format a single family section with description + gold example.

    Args:
        family_id: Family letter (A-F).
        gold_example: Gold example dict with original_sql, optimized_sql, ir_*, patch_plan.
        include_patch_plans: If False, omit the PATCH PLAN JSON block.
        analyst_only: If True, only emit family description + example ID.
            The analyst picks strategies; workers get the full examples.
    """

    desc = FAMILY_DESCRIPTIONS[family_id]

    lines = [
        f"### Family {family_id}: {desc['name']} ({desc['pattern']})",
        f"**Description**: {desc['description']}",
        f"**Speedup Range**: {desc['speedup_range']} ({desc['win_rate']})",
        f"**Use When**:",
    ]

    for i, condition in enumerate(desc['use_when'], 1):
        lines.append(f"  {i}. {condition}")

    # Gold example ID + speedup (always shown)
    example_name = gold_example.get("id", "example")
    speedup = gold_example.get("verified_speedup", "?")

    lines.append("")
    lines.append(f"**Gold Example**: `{example_name}` ({speedup})")

    # Analyst only sees family description + example ID — workers get the SQL
    if analyst_only:
        return "\n".join(lines)

    # Before SQL
    before_sql = gold_example.get("original_sql", "")
    if before_sql:
        lines.append("")
        lines.append("**BEFORE (slow):**")
        lines.append(f"```sql\n{before_sql.strip()}\n```")

    # After SQL
    after_sql = gold_example.get("optimized_sql", "")
    if after_sql:
        lines.append("")
        lines.append("**AFTER (fast):**")
        lines.append(f"```sql\n{after_sql.strip()}\n```")

    # IR node maps (if enriched)
    ir_before = gold_example.get("ir_node_map_before")
    ir_target = gold_example.get("ir_node_map_target")
    if ir_before and ir_target:
        lines.append("")
        lines.append("**IR BEFORE:**")
        lines.append(f"```\n{ir_before}\n```")
        lines.append("")
        lines.append("**IR TARGET:**")
        lines.append(f"```\n{ir_target}\n```")

    # Patch plan (if available and requested)
    if include_patch_plans:
        patch_plan = gold_example.get("patch_plan")
        if patch_plan:
            lines.append("")
            lines.append("**PATCH PLAN:**")
            lines.append("```json")
            lines.append(json.dumps(patch_plan, indent=2))
            lines.append("```")

    return "\n".join(lines)


def format_family_description_only(family_id: str, dialect: str = "") -> str:
    """Format a family section when no gold example exists for this engine.

    Shows the full family description with a caveat that no gold example
    has been observed yet. The analyst can still try the strategy if the
    EXPLAIN plan warrants it.
    """
    desc = FAMILY_DESCRIPTIONS[family_id]
    engine_label = dialect.upper().replace('POSTGRES', 'PostgreSQL') if dialect else "this engine"

    lines = [
        f"### Family {family_id}: {desc['name']} ({desc['pattern']})",
        f"**Description**: {desc['description']}",
        f"**Speedup Range**: {desc['speedup_range']} ({desc['win_rate']})",
        f"**Use When**:",
    ]

    for i, condition in enumerate(desc['use_when'], 1):
        lines.append(f"  {i}. {condition}")

    lines.append("")
    lines.append(f"**Gold Example**: None yet observed on {engine_label}. "
                 "Strategy is valid if EXPLAIN evidence supports it.")

    return "\n".join(lines)


# ── Engine Intelligence Loader ─────────────────────────────────────────────

def _load_engine_intelligence(dialect: str) -> Optional[str]:
    """Load engine playbook for injection into beam prompt.

    Single source of truth: knowledge/{dialect}.md — the distilled playbook
    with pathologies, gates, regression registry, pruning guide.

    Returns formatted prompt section or None if not available.
    """
    algo_text = load_exploit_algorithm(dialect)
    if not algo_text:
        return None

    return f"## Engine Playbook ({dialect.upper()})\n\n{algo_text}"


# ── Shared Prompt Body — Cache-Friendly Order ─────────────────────────────
#
# Structure (for API prefix caching):
#   1st — System Instructions (CACHED): Role, task spec
#   2nd — Database Schema / Context (CACHED): Engine playbook, gold examples
#   3rd — "Query to Analyze" marker (CACHED)
#   4th — Unique query data (CACHE MISS): SQL, EXPLAIN, IR, intelligence
#
# Callers insert their task section into static, add the marker,
# then concatenate: "\n\n".join(static + dynamic).

def _build_prompt_body(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    all_5_examples: Dict[str, Dict[str, Any]],
    dialect: str,
    role_text: str,
    include_patch_plans: bool = True,
    intelligence_brief: str = "",
) -> tuple[List[str], List[str], int]:
    """Build prompt sections in cache-friendly order.

    Returns:
        (static_sections, dynamic_sections, n_families).
        Callers append task section + marker to static, then join all.
    """
    static: List[str] = []
    dynamic: List[str] = []

    # ═══ STATIC PREFIX (identical for all queries on same dialect) ═════

    # ── Role ───────────────────────────────────────────────────────
    static.append(f"## Role\n\n{role_text}")

    # ── Engine Intelligence ────────────────────────────────────────
    engine_intel = _load_engine_intelligence(dialect)
    if engine_intel:
        static.append(engine_intel)

    # ── Families with examples ─────────────────────────────────────
    all_family_ids = ["A", "B", "C", "D", "E", "F"]
    families_with_plans = [f for f in all_family_ids
                           if f in all_5_examples and all_5_examples[f].get("patch_plan")]
    n_families = len(families_with_plans)

    analyst_only = not include_patch_plans
    family_intro = (
        "This is a briefing of what we know, not a set of hard rules. "
        "Use your judgement about what's worth trying based on the EXPLAIN plan.\n\n"
        f"**{n_families} families have proven gold examples** on this engine. "
        f"All 6 families are listed — those without gold examples are still valid "
        "strategies if the EXPLAIN plan warrants them.\n\n"
        "Choose the most relevant families for this query based on:\n"
        "- Query structure (CTEs, subqueries, joins, aggregations, set operations)\n"
        "- Execution plan signals (WHERE placement, repeated scans, correlated subqueries)\n"
        "- Problem signature (cardinality estimation errors, loops vs sets, filter ordering)\n"
    )
    static.append(f"## Optimization Families\n\n{family_intro}\n")

    for family_id in all_family_ids:
        ex = all_5_examples.get(family_id)
        if ex and ex.get("patch_plan"):
            static.append(format_family_section(
                family_id, ex, include_patch_plans, analyst_only=analyst_only,
            ))
            static.append("")
        else:
            static.append(format_family_description_only(family_id, dialect))
            static.append("")

    # ═══ DYNAMIC SUFFIX (unique per query — cache miss) ═══════════════

    # ── Query ──────────────────────────────────────────────────────
    dynamic.append(f"**Dialect**: {dialect.upper()}\n\n```sql\n{original_sql}\n```")

    # ── Execution Plan ─────────────────────────────────────────────
    dynamic.append(f"### Execution Plan\n\n```\n{explain_text}\n```")

    # ── IR Structure ───────────────────────────────────────────────
    dynamic.append(f"""### IR Structure (for patch targeting)

```
{ir_node_map}
```

**Note**: Use `by_node_id` (e.g., "S0") and `by_anchor_hash` (16-char hex) from map above to target patch operations.""")

    # ── Detected Patterns (per-query intelligence) ─────────────────
    if intelligence_brief:
        dynamic.append(f"""### Detected Patterns

{intelligence_brief}

**Instruction**: Prioritize detected patterns above. If a high-confidence
pathology is detected, your primary target SHOULD address it.""")

    return static, dynamic, n_families


# ── Reasoning Prompt Builder (R1 2-shot, outputs 2 PatchPlans) ──────────

def build_reasoning_prompt(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    all_5_examples: Dict[str, Dict[str, Any]],
    dialect: str,
    intelligence_brief: str = "",
) -> str:
    """Build the R1 reasoning prompt — full intelligence, outputs 2 PatchPlans.

    Cache-friendly structure:
      CACHED: Role + playbook + gold examples + task spec + marker
      MISS:   Query SQL + EXPLAIN + IR + intelligence brief

    Used as shot 1 in REASONING mode. The same prompt prefix is reused
    in shot 2 (via append_shot_results) for cache-hit efficiency.
    """
    engine_name = dialect.upper().replace('POSTGRES', 'PostgreSQL')
    role_text = (
        f"You are a SQL optimization specialist for {engine_name}. "
        "Your task is to analyze a query's execution plan, identify the primary "
        "bottleneck, and propose **exactly 2 independent patch plans** that target "
        "different optimization strategies.\n\n"
        "Each patch plan must:\n"
        "- Be atomic (steps applied sequentially: s1 → s2 → s3 → ...)\n"
        "- Transform the original query using patch operations\n"
        "- Preserve semantic equivalence (same rows, columns, ordering)\n"
        "- Follow the patterns shown in reference examples below"
    )

    static, dynamic, n_families = _build_prompt_body(
        query_id, original_sql, explain_text, ir_node_map,
        all_5_examples, dialect, role_text,
        include_patch_plans=True,
        intelligence_brief=intelligence_brief,
    )

    # Task spec in static prefix (system instructions)
    static.append(_build_patchplan_task_section(n_plans=2))

    # Cache boundary
    static.append("---\n\n## Query to Analyze")

    return "\n\n".join(static + dynamic)


# ── Worker Prompt Builder (for qwen code-generation) ──────────────────────

def build_worker_patch_prompt(
    original_sql: str,
    ir_node_map: str,
    target: Dict[str, Any],
    gold_patch_plan: Dict[str, Any],
    dialect: str,
    dialect_constraints: str = "",
    worker_role: Optional[Dict[str, Any]] = None,
) -> str:
    """Build focused worker prompt for converting a target IR into a PatchPlan JSON.

    Args:
        original_sql: Full original SQL
        ir_node_map: Current IR node map (from render_ir_node_map)
        target: AnalystTarget dict (family, target_ir, hypothesis, etc.)
        gold_patch_plan: The patch_plan field from the recommended gold example
        dialect: SQL dialect
        dialect_constraints: Optional dialect-specific rules
        worker_role: Optional worker role dict (key, name, focus, description)

    Returns:
        Worker prompt string (~2K tokens)
    """
    target_ir = target.get("target_ir", "")
    hypothesis = target.get("hypothesis", "")
    family = target.get("family", "?")
    transform = target.get("transform", "unknown")

    lines = [
        "## Role",
        "",
    ]

    # Add worker specialization context if available
    if worker_role:
        role_key = worker_role.get("key", "W?")
        role_name = worker_role.get("name", "Worker")
        role_focus = worker_role.get("focus", "")
        role_desc = worker_role.get("description", "")
        lines.append(
            f"You are **{role_key} \"{role_name}\"** — {role_focus}. "
            f"{role_desc}"
        )
        lines.append("")

    lines.extend([
        "Transform this SQL query from its CURRENT IR structure to a TARGET IR structure "
        "using patch operations. Output a single PatchPlan JSON.",
        "",
        f"**Family**: {family} — {transform}",
        f"**Hypothesis**: {hypothesis}",
        "",
        "## Original SQL",
        "",
        f"```sql\n{original_sql}\n```",
        "",
        "## Current IR Node Map",
        "",
        f"```\n{ir_node_map}\n```",
        "",
        "## Target IR (what the optimized query should look like)",
        "",
        f"```\n{target_ir}\n```",
        "",
        "## Patch Operations",
        "",
        "| Op | Description | Payload |",
        "|----|-------------|---------|",
        "| insert_cte | Add a new CTE to the WITH clause | cte_name, cte_query_sql |",
        "| replace_from | Replace the FROM clause | from_sql |",
        "| replace_where_predicate | Replace the WHERE clause | expr_sql |",
        "| replace_body | Replace entire query body (SELECT, FROM, WHERE, GROUP BY) | sql_fragment |",
        "| replace_expr_subtree | Replace a specific expression | expr_sql (+ by_anchor_hash) |",
        "| delete_expr_subtree | Remove a specific expression | (target only, no payload) |",
        "",
        "## Gold Patch Example (reference pattern)",
        "",
        "```json",
        json.dumps(gold_patch_plan, indent=2),
        "```",
        "",
    ])

    if dialect_constraints:
        lines.extend([
            f"## Dialect Constraints ({dialect.upper()})",
            "",
            dialect_constraints,
            "",
        ])

    lines.extend([
        "## Instructions",
        "",
        "Adapt the gold example pattern to match the ORIGINAL SQL above.",
        "Use the TARGET IR as your structural guide — create CTEs matching the target's CTE names "
        "and structure.",
        f"Preferred approach: insert_cte (x2-3) + replace_from or replace_body.",
        "All SQL in payloads must be complete, executable fragments (no ellipsis).",
        f"Use dialect: \"{dialect}\" in the output.",
        "Target all steps at by_node_id: \"S0\" (the main statement).",
        "",
        "Output ONLY the JSON object (no markdown, no explanation):",
    ])

    return "\n".join(lines)


def build_worker_retry_prompt(worker_prompt: str, error_msg: str) -> str:
    """Append error context to a worker prompt for retry."""
    return (
        worker_prompt
        + f"\n\n## RETRY — Previous patch failed:\n{error_msg}\n\n"
        "Fix the error. Output ONLY the corrected JSON."
    )


def build_worker_semantic_retry_prompt(
    worker_prompt: str,
    sem_result,
    original_sql: str,
    rewrite_sql: str,
) -> str:
    """Append semantic validation errors to a worker prompt for retry.

    Args:
        worker_prompt: Original worker prompt.
        sem_result: SemanticValidationResult with diagnostic fields.
        original_sql: Original query SQL.
        rewrite_sql: The rewritten SQL that failed semantic validation.

    Returns:
        Prompt with semantic error context appended.
    """
    from ..validation.sql_differ import SQLDiffer

    lines = [
        worker_prompt,
        "",
        "## RETRY — Semantic validation FAILED",
        "",
        f"The patch produced SQL that returns DIFFERENT results from the original.",
        "",
    ]

    # Error summary
    if sem_result.errors:
        lines.append("**Errors:**")
        for err in sem_result.errors:
            lines.append(f"- {err}")
        lines.append("")

    # Row count diff
    if sem_result.row_count_diff:
        rcd = sem_result.row_count_diff
        lines.append(
            f"**Row count mismatch** (on {rcd.sample_pct}% sample): "
            f"original={rcd.original_count}, rewrite={rcd.rewrite_count} "
            f"(diff={rcd.diff:+d})"
        )
        lines.append("")

    # Value diffs
    if sem_result.value_diffs:
        formatted = SQLDiffer.format_value_diffs(sem_result.value_diffs)
        if formatted:
            lines.append("**Value differences:**")
            lines.append(formatted)
            lines.append("")

    # Column mismatch
    if sem_result.column_mismatch:
        cm = sem_result.column_mismatch
        if cm.missing:
            lines.append(f"**Missing columns**: {', '.join(cm.missing)}")
        if cm.extra:
            lines.append(f"**Extra columns**: {', '.join(cm.extra)}")
        lines.append("")

    # SQL diff
    sql_diff = SQLDiffer.unified_diff(original_sql, rewrite_sql)
    if sql_diff:
        lines.append("**SQL diff (original → rewrite):**")
        lines.append("```diff")
        lines.append(sql_diff)
        lines.append("```")
        lines.append("")

    lines.append("Fix the semantic error. Output ONLY the corrected JSON.")
    return "\n".join(lines)


def build_beam_sniper_prompt(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    all_5_examples: Dict[str, Dict[str, Any]],
    dialect: str,
    strike_results: List[Dict[str, Any]],
    intelligence_brief: str = "",
) -> str:
    """Build the R1 sniper prompt for BEAM mode — sees BDA + intelligence, outputs 2 PatchPlans.

    Called after qwen probe workers. The sniper sees all probe results (BDA),
    EXPLAIN plans for winners, and the full engine intelligence. Outputs 2
    PatchPlans that build on/combine winning probes.

    Args:
        query_id: Query identifier.
        original_sql: Full original SQL.
        explain_text: EXPLAIN ANALYZE output for the original query.
        ir_node_map: IR node map.
        all_5_examples: Gold examples dict.
        dialect: SQL dialect.
        strike_results: List of dicts with probe_id, transform_id, family,
            status, speedup, error, explain_text, sql.
        intelligence_brief: Pre-computed detection + classification summary.

    Returns:
        Complete sniper prompt string.
    """
    engine_name = dialect.upper().replace('POSTGRES', 'PostgreSQL')

    # Static role text — no per-query counts (those go in dynamic section)
    role_text = (
        f"You are a SQL optimization specialist for {engine_name}. "
        "You will receive the results of transform probes (BDA) fired "
        "against a query. Your task: analyze the probe results, identify "
        "what worked and why, then design **exactly 2 patch plans** that "
        "build on the best insights — combining or refining winning strategies."
    )

    static, dynamic, n_families = _build_prompt_body(
        query_id, original_sql, explain_text, ir_node_map,
        all_5_examples, dialect, role_text,
        include_patch_plans=True,
        intelligence_brief=intelligence_brief,
    )

    # Sniper task in static prefix (system instructions)
    static.append("""## Your Task

The BDA (in the query section below) tells you WHAT WORKS. Design 2 patch plans:

1. **Analyze winning strikes**: Compare their EXPLAINs to original —
   where did row counts drop? Which operators changed? What bottleneck
   remains?

2. **Learn from failures**: Which transforms made things worse or broke
   correctness? What should NOT be combined?

3. **Design 2 patch plans**: Stack winning transforms together or refine
   the best. If probe p01 pushed a filter and p03 decorrelated, try both
   in one rewrite.

""" + _build_patchplan_task_section(n_plans=2))

    # Cache boundary
    static.append("---\n\n## Query to Analyze")

    # ── DYNAMIC: Probe summary ────────────────────────────────────
    n_total = len(strike_results)
    n_pass = sum(1 for s in strike_results if s.get("status") in ("PASS", "WIN", "IMPROVED"))
    n_win = sum(1 for s in strike_results if s.get("status") == "WIN")
    dynamic.append(
        f"**Probe summary**: {n_total} probes fired, "
        f"{n_pass} passed validation, {n_win} showed speedup."
    )

    # ── DYNAMIC: BDA Table ────────────────────────────────────────
    bda_lines = [
        "### Strike BDA (Battle Damage Assessment)\n",
        "| Probe | Transform | Family | Status | Speedup | Error/Notes |",
        "|-------|-----------|--------|--------|---------|-------------|",
    ]
    for s in strike_results:
        speedup = s.get("speedup")
        speedup_str = f"{speedup:.2f}x" if speedup else "-"
        error_str = s.get("error") or ""
        bda_lines.append(
            f"| {s.get('probe_id', '?')} | {s.get('transform_id', '?')} | "
            f"{s.get('family', '?')} | {s.get('status', '?')} | "
            f"{speedup_str} | {error_str} |"
        )
    dynamic.append("\n".join(bda_lines))

    # ── DYNAMIC: EXPLAIN Plans for winning strikes ────────────────
    winning = [
        s for s in strike_results
        if s.get("status") in ("WIN", "IMPROVED", "PASS")
        and s.get("speedup") and s["speedup"] >= 1.0
        and s.get("explain_text")
    ]

    if winning:
        explain_sections = ["### EXPLAIN Plans (winning strikes)\n"]
        for s in sorted(winning, key=lambda x: -(x.get("speedup") or 0)):
            explain_sections.append(
                f"#### {s['probe_id']}: {s['transform_id']} ({s['speedup']:.2f}x)\n"
                f"```\n{s['explain_text'].strip()}\n```\n"
            )
        dynamic.append("\n".join(explain_sections))

    # ── DYNAMIC: SQL of winning strikes ───────────────────────────
    if winning:
        sql_sections = ["### SQL of Winning Strikes\n"]
        for s in sorted(winning, key=lambda x: -(x.get("speedup") or 0)):
            sql = s.get("sql")
            if sql:
                sql_sections.append(
                    f"#### {s['probe_id']}: {s['transform_id']} ({s['speedup']:.2f}x)\n"
                    f"```sql\n{sql.strip()}\n```\n"
                )
        dynamic.append("\n".join(sql_sections))

    return "\n\n".join(static + dynamic)


# ── Shared PatchPlan Task Section ──────────────────────────────────────────

def _build_patchplan_task_section(n_plans: int = 2) -> str:
    """Build the shared output format section for PatchPlan JSON.

    Used by build_reasoning_prompt, build_beam_sniper_prompt, and append_shot_results.
    """
    return f"""Output exactly **{n_plans} patch plans** as a JSON array.

### Patch Operations

| Op | Description | Payload |
|----|-------------|---------|
| insert_cte | Add a new CTE to the WITH clause | cte_name, cte_query_sql |
| replace_from | Replace the FROM clause | from_sql |
| replace_where_predicate | Replace the WHERE clause | expr_sql |
| replace_body | Replace entire query body (SELECT, FROM, WHERE, GROUP BY) | sql_fragment |
| replace_expr_subtree | Replace a specific expression | expr_sql (+ by_anchor_hash) |
| delete_expr_subtree | Remove a specific expression | (target only, no payload) |

### Output Format

```json
[
  {{
    "plan_id": "r1",
    "family": "B",
    "transform": "decorrelate",
    "hypothesis": "Correlated subquery re-scans store_sales per row...",
    "target_ir": "S0 [SELECT]\\n  CTE: thresholds (GROUP BY item_sk)...",
    "dialect": "postgres",
    "steps": [
      {{"step_id": "s1", "op": "insert_cte", "target": {{"by_node_id": "S0"}}, "payload": {{"cte_name": "...", "cte_query_sql": "..."}}}},
      {{"step_id": "s2", "op": "replace_from", "target": {{"by_node_id": "S0"}}, "payload": {{"from_sql": "..."}}}}
    ]
  }},
  {{
    "plan_id": "r2",
    "family": "A",
    "transform": "early_filter",
    "hypothesis": "Late date filter after full scan...",
    "target_ir": "...",
    "dialect": "postgres",
    "steps": [...]
  }}
]
```

### Semantic Guards (MUST preserve)
- All WHERE/HAVING/ON conditions preserved exactly
- All literal values unchanged (35*0.01 stays as 35*0.01, NOT 0.35)
- Column names, aliases, ORDER BY, and LIMIT exactly
- Do NOT add new filter conditions
- Same row count as original query

### Rules
- Output exactly {n_plans} plans — each targeting a DIFFERENT optimization strategy
- Each plan must have unique plan_id, family, transform
- Each step's SQL in payloads must be complete, executable (no ellipsis)
- Target all steps at by_node_id: "S0" (the main statement)
- Include hypothesis explaining WHY this optimization should help (cite EXPLAIN evidence)

Output ONLY the JSON array (no markdown fences, no explanation before/after):"""


# ── Helper: Load Gold Examples ─────────────────────────────────────────────

def load_gold_examples(dialect: str, base_path: str = "packages/qt-sql/qt_sql/examples") -> Dict[str, Dict[str, Any]]:
    """Load gold examples for all 5 families from disk.

    Args:
        dialect: "duckdb", "postgres", or "snowflake"
        base_path: Path to examples directory

    Returns:
        Dict mapping family ID (A-E) to example JSON
    """
    import json
    from pathlib import Path

    # Map families to example files (per dialect)
    # This is flexible - if Snowflake lacks a family, borrow from DuckDB/PG
    example_map = {
        "duckdb": {
            "A": "date_cte_isolate",
            "B": "decorrelate",
            "C": "aggregate_pushdown",
            "D": "intersect_to_exists",
            "E": "multi_dimension_prefetch",
            "F": "inner_join_conversion",
        },
        "postgres": {
            "A": "date_cte_explicit_join",
            "B": "shared_scan_decorrelate",
            "C": "materialized_dimension_fact_prefilter",
            "D": "intersect_to_exists",
            "E": "multi_dimension_prefetch",
            "F": "explicit_join_materialized",
        },
        "snowflake": {
            "A": "sk_pushdown_union_all",  # P4: date_sk pushdown (Family A = Early Filtering)
            "B": "inline_decorrelate",  # P3: decorrelation (Family B = Decorrelation)
            "C": "aggregate_pushdown",  # Borrow from DuckDB
            "D": "intersect_to_exists",  # Borrow from DuckDB
            "E": "multi_dimension_prefetch",  # Borrow from DuckDB
            "F": "inner_join_conversion",  # Borrow from DuckDB
        }
    }

    # Normalize dialect to lowercase for case-insensitive lookup
    dialect = dialect.lower().strip()

    examples = {}
    for family_id, example_name in example_map.get(dialect, {}).items():
        # Try native dialect first
        example_path = Path(base_path) / dialect / f"{example_name}.json"

        if not example_path.exists():
            # Fallback: try DuckDB (most complete)
            example_path = Path(base_path) / "duckdb" / f"{example_name}.json"

        if not example_path.exists():
            # Fallback: try PostgreSQL
            example_path = Path(base_path) / "postgres" / f"{example_name}.json"

        if example_path.exists():
            with open(example_path) as f:
                examples[family_id] = json.load(f)
        else:
            logger.warning(f"Example {example_name} for family {family_id} not found")

    return examples


# ── Snipe Prompt Builder ──────────────────────────────────────────────────

def _build_history_summary_table(
    all_iterations: List[List[Any]],
) -> str:
    """Build compact summary table of ALL prior iteration patches.

    One line per patch: iteration, patch_id, family, transform, speedup, status, bottleneck.
    This keeps prompt growth O(n_patches) — ~1 line per patch across all iterations.

    Args:
        all_iterations: List of lists of AppliedPatch objects, one list per iteration.

    Returns:
        Markdown table string.
    """
    lines = [
        "### History — All Prior Patches",
        "",
        "| Iter | Patch | Family | Transform | Speedup | Status | Orig ms | Patch ms | Error (summary) |",
        "|------|-------|--------|-----------|---------|--------|---------|----------|-----------------|",
    ]

    for iter_idx, patches in enumerate(all_iterations):
        for p in patches:
            speedup_str = f"{p.speedup:.2f}x" if p.speedup is not None else "—"
            orig_str = f"{p.original_ms:.0f}" if p.original_ms is not None else "—"
            patch_str = f"{p.patch_ms:.0f}" if p.patch_ms is not None else "—"
            err_str = p.apply_error or ""
            lines.append(
                f"| {iter_idx} | {p.patch_id} | {p.family} | "
                f"{p.transform} | {speedup_str} | {p.status} | "
                f"{orig_str} | {patch_str} | {err_str} |"
            )

    return "\n".join(lines)


def _parse_explain_operators(
    explain_text: str,
) -> List[Tuple[str, float, str]]:
    """Parse compact EXPLAIN text into operator entries sorted by time desc.

    Returns list of (short_name, time_ms, bracket_info) tuples.
    """
    import re

    results: List[Tuple[str, float, str]] = []
    for line in explain_text.strip().split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("Total"):
            continue

        bracket_start = stripped.find("[")
        bracket_end = stripped.find("]")
        if bracket_start < 0 or bracket_end < 0:
            continue

        raw_op = stripped[:bracket_start].strip()
        bracket = stripped[bracket_start + 1 : bracket_end]

        # Extract time in ms
        time_match = re.search(r"(\d+\.?\d*)ms", bracket)
        if not time_match:
            continue
        time_ms = float(time_match.group(1))
        if time_ms < 1.0:
            continue  # skip sub-ms noise

        # Build short name: OPERATOR_TYPE [table_name | join_key]
        # e.g. "SEQ_SCAN  store_sales" -> "SEQ_SCAN store_sales"
        # e.g. "HASH_JOIN INNER on cd_demo_sk = c_current_cdemo_sk" -> "HASH_JOIN INNER (cd_demo_sk)"
        on_match = re.match(r"(.+?)\s+on\s+(\w+)", raw_op)
        if on_match:
            short_name = f"{on_match.group(1).strip()} ({on_match.group(2)})"
        else:
            # Collapse multiple spaces
            short_name = re.sub(r"\s+", " ", raw_op)

        # Extract row info for bracket display
        row_match = re.search(r"([\d.]+[KMB]?)\s+(?:of\s+[\d.]+[KMB]?\s+)?rows", bracket)
        row_str = row_match.group(1) if row_match else ""

        # Extract pct
        pct_match = re.search(r"(\d+)%", bracket)
        pct_str = f" ({pct_match.group(1)}%)" if pct_match else ""

        display = f"{short_name} [{row_str} rows]" if row_str else short_name
        results.append((display, time_ms, pct_str))

    results.sort(key=lambda x: x[1], reverse=True)
    return results


def _build_explain_comparison(
    original_explain: str,
    explains: Dict[str, str],
    patches: List[Any],
) -> str:
    """Build operator cost comparison table: original vs best winner, side by side.

    Shows top operators ranked by time, makes plan differences immediately visible.
    Also detects redundant plans (multiple winners with identical operator structure).
    """
    orig_ops = _parse_explain_operators(original_explain)
    if not orig_ops:
        return ""

    # Find winners with explains
    winners = [
        (p, explains.get(p.patch_id, ""))
        for p in patches
        if p.status == "WIN" and p.patch_id in explains and explains[p.patch_id]
    ]
    if not winners:
        return ""

    # Best winner by speedup
    best_patch, best_explain = max(winners, key=lambda x: x[0].speedup or 0)
    best_ops = _parse_explain_operators(best_explain)
    if not best_ops:
        return ""

    # Detect redundant plans: compare top-5 operator names.
    # Only compare when we parsed enough operators for a meaningful fingerprint.
    def _top_names(explain_text: str) -> tuple[List[str], int]:
        ops = _parse_explain_operators(explain_text)
        return [op[0] for op in ops[:5]], len(ops)

    best_names, best_count = _top_names(best_explain)
    redundant_ids = [best_patch.patch_id]
    complementary_ids = []
    for p, exp in winners:
        if p.patch_id == best_patch.patch_id:
            continue
        names, count = _top_names(exp)
        if best_count >= 3 and count >= 3 and names == best_names:
            redundant_ids.append(p.patch_id)
        else:
            complementary_ids.append(p.patch_id)

    lines = ["#### Operator Cost Comparison (original vs best winner)\n"]

    # Redundancy note
    if len(redundant_ids) > 1:
        lines.append(
            f"**Note**: Patches {', '.join(redundant_ids)} produce "
            f"**identical plans** (REDUNDANT — same structural change)."
        )
    if complementary_ids:
        lines.append(
            f"**Complementary**: Patches {', '.join(complementary_ids)} have "
            f"different plan structures."
        )
    lines.append("")

    # Build side-by-side table
    best_label = f"{best_patch.patch_id} ({best_patch.speedup:.2f}x)"
    lines.append(
        f"| # | Original Operator | Time | "
        f"{best_label} Operator | Time |"
    )
    lines.append("|---|------------------|------|" + "-" * (len(best_label) + 12) + "|------|")

    max_rows = min(max(len(orig_ops), len(best_ops)), 10)
    for i in range(max_rows):
        if i < len(orig_ops):
            o_name, o_time, o_pct = orig_ops[i]
            o_str = f"{o_name}"
            o_time_str = f"{o_time:.0f}ms{o_pct}"
        else:
            o_str, o_time_str = "—", "—"

        if i < len(best_ops):
            b_name, b_time, b_pct = best_ops[i]
            b_str = f"{b_name}"
            b_time_str = f"{b_time:.0f}ms{b_pct}"
        else:
            b_str, b_time_str = "—", "—"

        lines.append(f"| {i + 1} | {o_str} | {o_time_str} | {b_str} | {b_time_str} |")

    # Key changes summary
    lines.append("")
    lines.append("**Key changes**:")

    # Find operators in original that don't appear in winner (eliminated)
    orig_types = {op[0].split("[")[0].strip() for op in orig_ops if op[1] >= 10}
    best_types = {op[0].split("[")[0].strip() for op in best_ops if op[1] >= 10}
    eliminated = orig_types - best_types
    if eliminated:
        lines.append(f"- **ELIMINATED**: {', '.join(sorted(eliminated))}")

    # Biggest remaining operator
    if best_ops:
        top_op = best_ops[0]
        lines.append(
            f"- **Remaining bottleneck**: {top_op[0]} at {top_op[1]:.0f}ms{top_op[2]}"
        )

    lines.append("")
    return "\n".join(lines)


def _build_detailed_iteration_section(
    iteration: int,
    patches: List[Any],
    original_explain: str,
    explains: Dict[str, str],
    race_result: Optional[Any] = None,
) -> str:
    """Build full detail for the MOST RECENT iteration only.

    Includes race timings, EXPLAIN plans, patched SQL, and error details.

    Args:
        iteration: Iteration number (0-based).
        patches: AppliedPatch list from this iteration.
        original_explain: EXPLAIN text for original query.
        explains: Dict mapping patch_id → EXPLAIN text.
        race_result: Optional RaceResult from race validation.

    Returns:
        Detailed section string.
    """
    sections = []

    sections.append(f"### Latest Iteration {iteration} — Detailed Results\n")

    # Race results table
    sections.append("#### Race Results\n")
    sections.append("| Patch | Family | Transform | Speedup | Status | Orig ms | Patch ms | Semantic | Error |")
    sections.append("|-------|--------|-----------|---------|--------|---------|----------|----------|-------|")

    for p in patches:
        speedup_str = f"{p.speedup:.2f}x" if p.speedup is not None else "—"
        orig_str = f"{p.original_ms:.0f}" if p.original_ms is not None else "—"
        patch_str = f"{p.patch_ms:.0f}" if p.patch_ms is not None else "—"
        sem_str = "PASS" if p.semantic_passed else "FAIL"
        error_str = p.apply_error or ""
        sections.append(
            f"| {p.patch_id} | {p.family} | {p.transform} | "
            f"{speedup_str} | {p.status} | "
            f"{orig_str} | {patch_str} | {sem_str} | {error_str} |"
        )

    # Race timings (if available)
    if race_result:
        sections.append("")
        orig_ms = race_result.original.elapsed_ms
        sections.append(f"**Original runtime**: {orig_ms:.0f}ms")

        for i, lane in enumerate(race_result.candidates):
            if i < len(patches):
                pid = patches[i].patch_id if patches[i].output_sql else "(failed)"
                status = "finished" if lane.finished else "DNF"
                sections.append(
                    f"  - {pid}: {lane.elapsed_ms:.0f}ms ({status})"
                )

    # ── Patched SQL for each patch (especially regressions/errors) ────
    sections.append("\n#### Patched SQL\n")
    for p in patches:
        if p.output_sql:
            label = f"{p.patch_id} (Family {p.family}, {p.transform})"
            if p.speedup is not None:
                label += f" — {p.speedup:.2f}x {p.status}"

            sections.append(f"**{label}:**")
            sections.append(f"```sql\n{p.output_sql.strip()}\n```\n")
        else:
            sections.append(
                f"**{p.patch_id}** (Family {p.family}): "
                f"FAILED to apply — {p.apply_error or 'unknown error'}\n"
            )

    # ── Execution Plans (compact parsed format, all patches) ─────
    sections.append("#### Execution Plans\n")

    sections.append("**Original EXPLAIN:**")
    sections.append(f"```\n{original_explain}\n```")

    # Show each patch's EXPLAIN; collapse identical plans only when
    # fingerprints are meaningful (>= 3 operators parsed).  When parsing
    # fails the fingerprint is empty/tiny for every patch, which would
    # incorrectly collapse unrelated plans.
    shown_fingerprints: Dict[tuple, str] = {}  # fingerprint → first patch_id
    for p in patches:
        if p.patch_id not in explains or not explains[p.patch_id]:
            continue
        explain_text = explains[p.patch_id]
        ops = _parse_explain_operators(explain_text)
        fp = tuple(op[0] for op in ops[:5])
        speedup_str = f"{p.speedup:.2f}x" if p.speedup is not None else "?"
        label = f"{p.patch_id} ({speedup_str} {p.status})"

        # Only deduplicate when we parsed enough operators for a reliable fingerprint
        if len(ops) >= 3 and fp in shown_fingerprints:
            sections.append(
                f"\n**{label} EXPLAIN:** same plan as {shown_fingerprints[fp]}"
            )
        else:
            if len(ops) >= 3:
                shown_fingerprints[fp] = p.patch_id
            sections.append(f"\n**{label} EXPLAIN:**")
            sections.append(f"```\n{explain_text}\n```")

    # ── Error Details ────────────────────────────────────────────────
    error_patches = [p for p in patches if p.apply_error]
    if error_patches:
        sections.append("\n#### Error Details\n")
        for p in error_patches:
            sections.append(f"- **{p.patch_id}** ({p.status}): {p.apply_error}")

    return "\n".join(sections)


def append_shot_results(
    base_prompt: str,
    patches: List[Any],
    explains: Dict[str, str],
) -> str:
    """Append shot 1 results to the base prompt for shot 2 (cache-hit pattern).

    The base prompt prefix is preserved exactly so the LLM's KV cache
    can be reused. Only results + new task are appended.

    Args:
        base_prompt: The shot 1 prompt (from build_reasoning_prompt or build_beam_sniper_prompt).
        patches: List of AppliedPatch objects from shot 1 validation.
        explains: Dict mapping patch_id → EXPLAIN text for shot 1 patches.

    Returns:
        Shot 2 prompt = base_prompt + results + new task.
    """
    lines = [base_prompt, "", "## Shot 1 Results", ""]

    # Results table
    lines.append("| # | Family | Transform | Speedup | Status | Error |")
    lines.append("|---|--------|-----------|---------|--------|-------|")

    for p in patches:
        speedup_str = f"{p.speedup:.2f}x" if getattr(p, "speedup", None) is not None else "-"
        status = getattr(p, "status", "?")
        error = getattr(p, "apply_error", "") or ""
        family = getattr(p, "family", "?")
        transform = getattr(p, "transform", "?")
        patch_id = getattr(p, "patch_id", "?")
        lines.append(
            f"| {patch_id} | {family} | {transform} | "
            f"{speedup_str} | {status} | {error} |"
        )

    lines.append("")

    # Show EXPLAIN plans for patches with speedup
    for p in patches:
        pid = getattr(p, "patch_id", "?")
        speedup = getattr(p, "speedup", None)
        status = getattr(p, "status", "?")

        if pid in explains and explains[pid]:
            speedup_str = f"{speedup:.2f}x" if speedup is not None else "?"
            lines.append(f"### {pid} EXPLAIN ({speedup_str} {status}):")
            lines.append(f"```\n{explains[pid].strip()}\n```")
            lines.append("")
        elif getattr(p, "apply_error", None):
            lines.append(f"### {pid} Error:")
            lines.append(p.apply_error)
            lines.append("")

    # Shot 2 task
    lines.extend([
        "## Shot 2 — Design 2 More Patch Plans",
        "",
        "Build on shot 1 results:",
        "1. Your first plan should refine or extend the best winner (or fix its remaining bottleneck)",
        "2. Your second should try a different approach not yet attempted",
        "",
        "If all shot 1 plans failed, diagnose why and try fundamentally different strategies.",
        "",
    ])

    lines.append(_build_patchplan_task_section(n_plans=2))

    return "\n".join(lines)


# ── Utility: Print Prompt for Review ───────────────────────────────────────

def print_prompt_preview(prompt: str, max_lines: int = 100):
    """Print first N lines of prompt for review."""
    lines = prompt.split("\n")
    for i, line in enumerate(lines[:max_lines]):
        print(f"{i+1:3d} | {line}")
    if len(lines) > max_lines:
        print(f"\n... ({len(lines) - max_lines} more lines)")
