"""Build oneshot patch optimization prompt with all 5 families + gold examples.

The prompt shows all 5 optimization families (A-E) with gold examples,
instructs the LLM to choose the 4 most relevant families for this specific query,
and outputs 4 independent patch plans.
"""

from typing import Optional, Dict, Any, List
import json
import logging

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
    }
}


# ── Build Individual Family Sections ───────────────────────────────────────

def format_family_section(
    family_id: str,
    gold_example: Dict[str, Any]
) -> str:
    """Format a single family section with description + gold example patch plan."""

    desc = FAMILY_DESCRIPTIONS[family_id]

    lines = [
        f"### Family {family_id}: {desc['name']} ({desc['pattern']})",
        f"**Description**: {desc['description']}",
        f"**Speedup Range**: {desc['speedup_range']} ({desc['win_rate']})",
        f"**Use When**:",
    ]

    for i, condition in enumerate(desc['use_when'], 1):
        lines.append(f"  {i}. {condition}")

    # Gold example
    example_name = gold_example.get("id", "example")
    speedup = gold_example.get("verified_speedup", "?")

    lines.append("")
    lines.append(f"**Gold Example**: `{example_name}` ({speedup})")

    # Before SQL
    before_sql = gold_example.get("original_sql", "")
    if before_sql:
        # Truncate to first 10 lines for readability
        before_lines = before_sql.split("\n")[:10]
        before_display = "\n".join(before_lines)
        if len(before_sql.split("\n")) > 10:
            before_display += "\n..."

        lines.append("")
        lines.append("**BEFORE (slow):**")
        lines.append(f"```sql\n{before_display}\n```")

    # After SQL
    after_sql = gold_example.get("optimized_sql", "")
    if after_sql:
        after_lines = after_sql.split("\n")[:10]
        after_display = "\n".join(after_lines)
        if len(after_sql.split("\n")) > 10:
            after_display += "\n..."

        lines.append("")
        lines.append("**AFTER (fast):**")
        lines.append(f"```sql\n{after_display}\n```")

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

    # Patch plan (if available)
    patch_plan = gold_example.get("patch_plan")
    if patch_plan:
        lines.append("")
        lines.append("**PATCH PLAN:**")
        lines.append("```json")
        lines.append(json.dumps(patch_plan, indent=2))
        lines.append("```")

    return "\n".join(lines)


# ── Shared Prompt Body (Sections 1-5) ──────────────────────────────────────

def _build_prompt_body(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    all_5_examples: Dict[str, Dict[str, Any]],
    dialect: str,
    role_text: str,
) -> tuple[List[str], int]:
    """Build sections 1-5 shared by single-tier and tiered prompts.

    Returns:
        (sections, n_families) — list of section strings, count of families with plans.
    """
    sections = []

    # ── Section 1: Role ─────────────────────────────────────────────────
    sections.append(f"## Role\n\n{role_text}")

    # ── Section 2: Query ────────────────────────────────────────────────
    sections.append(f"""## Query: {query_id}

**Dialect**: {dialect.upper()}

```sql
{original_sql}
```
""")

    # ── Section 3: Execution Plan ───────────────────────────────────────
    sections.append(f"""## Current Execution Plan

```
{explain_text}
```
""")

    # ── Section 4: IR Structure ────────────────────────────────────────
    sections.append(f"""## IR Structure (for patch targeting)

```
{ir_node_map}
```

**Note**: Use `by_node_id` (e.g., "S0") and `by_anchor_hash` (16-char hex) from map above to target patch operations.
""")

    # ── Section 5: Families with examples ─────────────────────────────
    families_with_plans = [f for f in ["A", "B", "C", "D", "E"]
                           if f in all_5_examples and all_5_examples[f].get("patch_plan")]
    n_families = len(families_with_plans)

    sections.append(f"""## Optimization Families

Review the {n_families} families below. Each shows a pattern with a gold example patch plan.

Choose up to **{min(4, n_families)} most relevant families** for this query based on:
- Query structure (CTEs, subqueries, joins, aggregations, set operations)
- Execution plan signals (WHERE placement, repeated scans, correlated subqueries, materializations)
- Problem signature (cardinality estimation errors, loops vs sets, filter ordering)

""")

    for family_id in ["A", "B", "C", "D", "E"]:
        ex = all_5_examples.get(family_id)
        if ex and ex.get("patch_plan"):
            sections.append(format_family_section(family_id, ex))
            sections.append("")

    return sections, n_families


# ── Main Prompt Builder ────────────────────────────────────────────────────

def build_oneshot_patch_prompt(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    all_5_examples: Dict[str, Dict[str, Any]],
    dialect: str
) -> str:
    """Build the complete oneshot patch optimization prompt.

    Args:
        query_id: Query identifier (e.g., "query_21")
        original_sql: Full original SQL
        explain_text: EXPLAIN ANALYZE output as text
        ir_node_map: IR node map (from render_ir_node_map)
        all_5_examples: Dict mapping family ID (A-E) to gold example JSON
        dialect: SQL dialect (duckdb, postgres, snowflake)

    Returns:
        Complete prompt string
    """
    role_text = (
        "You are a SQL optimization specialist. Your task is to propose "
        "**exactly 4 independent patch plans** for this query, each targeting "
        "a different optimization family.\n\n"
        "Each patch plan must:\n"
        "- Be atomic (steps applied sequentially: s1 → s2 → s3 → ...)\n"
        "- Transform the original query using patch operations\n"
        "- Preserve semantic equivalence (same rows, columns, ordering)\n"
        "- Follow the patterns shown in reference examples below\n\n"
        "You will **choose 4 of the 5 families** based on relevance to THIS SPECIFIC QUERY."
    )

    sections, n_families = _build_prompt_body(
        query_id, original_sql, explain_text, ir_node_map,
        all_5_examples, dialect, role_text,
    )

    # ── Section 6: Output Format (full patch plans) ────────────────────
    n_to_choose = min(4, n_families)
    sections.append(
        f"## Your Task\n\n"
        f"Analyze this query against the {n_families} families above.\n\n"
        f"**Choose up to {n_to_choose} families** that are most relevant. For each chosen family:\n"
        f"1. Create a patch plan with atomic steps\n"
        f"2. Score relevance (0.0\u20131.0) based on how well it matches this query\n"
        f"3. Provide reasoning for your choice\n"
    )

    sections.append("""**Output format**:

```json
[
  {
    "family": "A",
    "transform": "date_cte_isolate",
    "plan_id": "t1_family_a",
    "relevance_score": 0.95,
    "reasoning": "Query has late calendar_date filter on large fact table, CTE cascade structure → early pushdown = high ROI",
    "steps": [
      {
        "step_id": "s1",
        "op": "insert_cte",
        "target": {"by_node_id": "S0"},
        "payload": {"cte_name": "date_filter", "cte_query_sql": "SELECT ... WHERE calendar_date > ..."},
        "description": "Extract date filter into separate CTE"
      },
      {
        "step_id": "s2",
        "op": "replace_from",
        "target": {"by_node_id": "S0"},
        "payload": {"from_sql": "fact_table JOIN date_filter ON ..."},
        "description": "Join via filtered CTE instead of raw table"
      }
    ]
  },
  {
    "family": "B",
    "transform": "decorrelate_subquery",
    "plan_id": "t2_family_b",
    "relevance_score": 0.88,
    "reasoning": "Correlated subquery in WHERE with DELIM_SCAN in plan → decorrelation = medium ROI",
    "steps": [...]
  },
  {
    "family": "E",
    "transform": "materialized_prefetch",
    "plan_id": "t3_family_e",
    "relevance_score": 0.72,
    "reasoning": "Multiple independent dimension filters applied → materialization saves repeated scans",
    "steps": [...]
  },
  {
    "family": "D",
    "transform": "intersect_to_exists",
    "plan_id": "t4_family_d",
    "relevance_score": 0.61,
    "reasoning": "Set operation subquery pattern detected → EXISTS conversion feasible",
    "steps": [...]
  }
]
```

**Rules**:
- Output up to """ + str(n_to_choose) + """ patch plans
- Each plan has its own `plan_id`, `family`, `transform` name
- Each plan includes `relevance_score` (0.0–1.0) and brief `reasoning`
- Each step in `steps` array is complete, executable SQL (no ellipsis)
- Preserve all WHERE filters (removing filters = semantic bug)
- Order patches by relevance_score (highest first)

After JSON, provide analysis:
```
## Analysis

For each available family, explain relevance (HIGH / MEDIUM / LOW) in 1-2 sentences.

**Chosen families**: [list]
**Expected speedups**: t1: Nx, t2: Nx, ...
**Confidence**: High/Medium/Low (brief justification)
```

Now output your JSON array of patch plans:
""")

    return "\n\n".join(sections)


# ── Tiered Prompt Builder (Analyst → Worker split) ────────────────────────

def build_oneshot_patch_prompt_tiered(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    all_5_examples: Dict[str, Dict[str, Any]],
    dialect: str,
) -> str:
    """Build tiered analyst prompt: sections 1-5 shared + Section 6 asks for target IR maps.

    The analyst (DeepSeek) outputs structural targets only — no full patch plans.
    A separate worker (qwen) converts each target into a PatchPlan JSON.

    Args:
        query_id: Query identifier
        original_sql: Full original SQL
        explain_text: EXPLAIN ANALYZE output
        ir_node_map: IR node map (from render_ir_node_map)
        all_5_examples: Dict mapping family ID (A-E) to gold example JSON
        dialect: SQL dialect

    Returns:
        Complete tiered analyst prompt string
    """
    role_text = (
        "You are a SQL optimization analyst. Your task is to analyze this query "
        "and design **up to 4 structural optimization targets**, each targeting "
        "a different optimization family.\n\n"
        "For each target, describe the STRUCTURAL SHAPE of the optimized query "
        "using an IR node map (CTE names, FROM tables, WHERE conditions, GROUP BY, ORDER BY). "
        "A separate code-generation worker will convert your targets into executable patch plans.\n\n"
        "You will **choose up to 4 of the 5 families** based on relevance to THIS SPECIFIC QUERY."
    )

    sections, n_families = _build_prompt_body(
        query_id, original_sql, explain_text, ir_node_map,
        all_5_examples, dialect, role_text,
    )

    # ── Section 6: Tiered Output Format (target IR maps) ────────────────
    n_to_choose = min(4, n_families)
    sections.append(
        f"## Your Task\n\n"
        f"Analyze this query against the {n_families} families above.\n\n"
        f"Choose up to {n_to_choose} families that are most relevant. For each chosen family:\n"
        f"1. Describe the bottleneck hypothesis\n"
        f"2. Design a TARGET IR node map showing what the optimized query SHOULD look like\n"
        f"3. Score relevance (0.0\u20131.0)\n"
        f"4. Recommend which gold example(s) a code-generation worker should use as reference\n"
    )

    sections.append("""**Output format**:

```json
[
  {
    "family": "B",
    "transform": "shared_scan_decorrelate",
    "target_id": "t1",
    "relevance_score": 0.95,
    "hypothesis": "Correlated scalar subquery re-scans web_sales per row. Shared-scan variant: inner=outer table with same date filter.",
    "target_ir": "S0 [SELECT]\\n  CTE: common_scan  (via Q1)\\n    FROM: web_sales, date_dim\\n    WHERE: d_date BETWEEN ... AND d_date_sk = ws_sold_date_sk\\n  CTE: thresholds  (via Q2)\\n    FROM: common_scan\\n    GROUP BY: ws_item_sk\\n  MAIN QUERY (via Q0)\\n    FROM: common_scan cs, item, thresholds t\\n    WHERE: i_manufact_id = 320 AND ... AND cs.ws_ext_discount_amt > t.threshold\\n    ORDER BY: sum(ws_ext_discount_amt)",
    "recommended_examples": ["sf_shared_scan_decorrelate"]
  }
]
```

**Rules**:
- target_ir must follow the IR node map format (same as Section 4)
- target_ir describes the STRUCTURAL SHAPE of the optimized query (CTE names, FROM tables, WHERE conditions, GROUP BY, ORDER BY)
- recommended_examples: list gold example IDs the worker should use as reference patch template
- Each target should represent a DIFFERENT optimization strategy
- Rank by relevance_score (highest first)
- Output up to """ + str(n_to_choose) + """ targets

After JSON, provide analysis:

## Analysis
For each available family, explain relevance (HIGH / MEDIUM / LOW) in 1-2 sentences.
**Chosen families**: [list]
**Confidence**: High/Medium/Low
""")

    return "\n\n".join(sections)


# ── Worker Prompt Builder (for qwen code-generation) ──────────────────────

def build_worker_patch_prompt(
    original_sql: str,
    ir_node_map: str,
    target: Dict[str, Any],
    gold_patch_plan: Dict[str, Any],
    dialect: str,
    dialect_constraints: str = "",
) -> str:
    """Build focused worker prompt for converting a target IR into a PatchPlan JSON.

    Args:
        original_sql: Full original SQL
        ir_node_map: Current IR node map (from render_ir_node_map)
        target: AnalystTarget dict (family, target_ir, hypothesis, etc.)
        gold_patch_plan: The patch_plan field from the recommended gold example
        dialect: SQL dialect
        dialect_constraints: Optional dialect-specific rules

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
    ]

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
            "E": "multi_dimension_prefetch"
        },
        "postgres": {
            "A": "date_cte_explicit_join",
            "B": "shared_scan_decorrelate",
            "C": "materialized_dimension_fact_prefilter",
            "D": "intersect_to_exists",
            "E": "multi_dimension_prefetch"
        },
        "snowflake": {
            "A": "inline_decorrelate",
            "B": "shared_scan_decorrelate",
            "C": "aggregate_pushdown",  # Borrow from DuckDB, mark in prompt
            "D": "intersect_to_exists",  # Borrow from DuckDB
            "E": "multi_dimension_prefetch"  # Borrow from DuckDB
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
            err_str = (p.apply_error or "")[:40]
            lines.append(
                f"| {iter_idx} | {p.patch_id} | {p.family} | "
                f"{p.transform} | {speedup_str} | {p.status} | "
                f"{orig_str} | {patch_str} | {err_str} |"
            )

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
        error_str = (p.apply_error or "")[:80]
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
            # Truncate to 30 lines max
            sql_lines = p.output_sql.strip().split("\n")[:30]
            sql_display = "\n".join(sql_lines)
            if len(p.output_sql.strip().split("\n")) > 30:
                sql_display += "\n-- ... (truncated)"

            label = f"{p.patch_id} (Family {p.family}, {p.transform})"
            if p.speedup is not None:
                label += f" — {p.speedup:.2f}x {p.status}"

            sections.append(f"**{label}:**")
            sections.append(f"```sql\n{sql_display}\n```\n")
        else:
            sections.append(
                f"**{p.patch_id}** (Family {p.family}): "
                f"FAILED to apply — {p.apply_error or 'unknown error'}\n"
            )

    # ── Execution Plans ──────────────────────────────────────────────
    sections.append("#### Execution Plans\n")

    # Original
    explain_display = "\n".join(original_explain.split("\n")[:60])
    sections.append("**Original EXPLAIN:**")
    sections.append(f"```\n{explain_display}\n```")

    # Candidate explains
    for p in patches:
        if p.patch_id in explains and explains[p.patch_id]:
            explain_text = explains[p.patch_id]
            truncated = "\n".join(explain_text.split("\n")[:60])
            speedup_str = f"{p.speedup:.2f}x" if p.speedup is not None else "?"
            sections.append(
                f"\n**{p.patch_id} (Family {p.family}, {speedup_str} {p.status}) EXPLAIN:**"
            )
            sections.append(f"```\n{truncated}\n```")

    # ── Error Details ────────────────────────────────────────────────
    error_patches = [p for p in patches if p.apply_error]
    if error_patches:
        sections.append("\n#### Error Details\n")
        for p in error_patches:
            sections.append(f"- **{p.patch_id}** ({p.status}): {p.apply_error}")

    return "\n".join(sections)


def build_oneshot_patch_snipe_prompt(
    original_prompt: str,
    iteration: int,
    patches: List[Any],
    original_explain: str,
    explains: Dict[str, str],
    race_result: Optional[Any] = None,
    all_prior_iterations: Optional[List[List[Any]]] = None,
) -> str:
    """Build snipe prompt: original prompt + summary history + detailed latest results.

    Context management: instead of re-appending the full original prompt + all results
    each iteration (which would grow unbounded), we use:
    - COMPACT SUMMARY TABLE of ALL prior iterations (1 line per patch)
    - FULL DETAIL of only the MOST RECENT iteration (SQL, EXPLAIN, errors)

    This keeps prompt size roughly constant regardless of iteration count.

    Args:
        original_prompt: The initial prompt (from build_oneshot_patch_prompt)
        iteration: Current iteration number (0-based internal, displayed as 1-based)
        patches: List of AppliedPatch from the most recent iteration
        original_explain: EXPLAIN text for original query
        explains: Dict mapping patch_id → EXPLAIN text
        race_result: Optional RaceResult from race validation
        all_prior_iterations: List of patch lists from ALL prior iterations
            (for building the summary table). If None, only current patches shown.

    Returns:
        Complete snipe prompt string
    """
    sections = [original_prompt]

    sections.append(f"\n\n## Previous Attempt Results\n")

    # ── Summary table of ALL iterations (compact, ~1 line per patch) ──
    if all_prior_iterations and len(all_prior_iterations) > 0:
        # Include all prior iterations in the summary table
        summary = _build_history_summary_table(all_prior_iterations)
        sections.append(summary)
        sections.append("")

    # ── Detailed results for MOST RECENT iteration only ───────────────
    detail = _build_detailed_iteration_section(
        iteration=iteration,
        patches=patches,
        original_explain=original_explain,
        explains=explains,
        race_result=race_result,
    )
    sections.append(detail)

    # ── Task for next iteration ──────────────────────────────────────
    sections.append(f"\n## Your Task (Iteration {iteration + 2})\n")
    sections.append(
        "Analyze the history summary table and the detailed latest results above.\n\n"
        "**Key questions:**\n"
        "- For WINNING patches: can you improve further? Combine with other strategies?\n"
        "- For NEUTRAL patches: what went wrong? Look at the EXPLAIN plan. Can you fix the approach?\n"
        "- For FAILED/ERROR patches: what caused the error? Look at the patched SQL. Propose an alternative.\n"
        "- For REGRESSION patches: why did it get slower? Look at the patched SQL and EXPLAIN. Avoid that pattern.\n\n"
        "Output a new JSON array of up to 4 patch plans (same format as before).\n"
        "You may keep successful approaches and refine them, or try entirely new families.\n"
    )

    return "\n".join(sections)


def build_oneshot_patch_retry_prompt(
    original_prompt: str,
    previous_response: str,
    all_patches: List[Any],
    errors: List[tuple],
) -> str:
    """Build retry prompt showing ALL patches with stats, not just errors.

    The LLM needs full context: what worked, what didn't, and why.

    Args:
        original_prompt: The original prompt that generated these patches.
        previous_response: The raw LLM response (for reference).
        all_patches: All AppliedPatch objects (both successful and failed).
        errors: List of (patch_id, error_msg) for patches that failed to parse/apply.

    Returns:
        Retry prompt string.
    """
    lines = [
        original_prompt,
        "",
        "## RETRY — Fix Malformed Patches",
        "",
        "Your previous response produced these patches:",
        "",
        "| Patch | Family | Transform | Status | Error |",
        "|-------|--------|-----------|--------|-------|",
    ]

    for p in all_patches:
        status = "OK" if p.output_sql else "FAILED"
        err = (p.apply_error or "")[:60]
        lines.append(
            f"| {p.patch_id} | {p.family} | {p.transform} | {status} | {err} |"
        )

    lines.append("")

    # Show SQL for successful patches (so LLM knows what worked)
    ok_patches = [p for p in all_patches if p.output_sql]
    if ok_patches:
        lines.append("### Working patches (keep these unchanged):\n")
        for p in ok_patches:
            sql_preview = "\n".join(p.output_sql.strip().split("\n")[:15])
            lines.append(f"**{p.patch_id}** (Family {p.family}, {p.transform}):")
            lines.append(f"```sql\n{sql_preview}\n```\n")

    # Show error details
    failed_patches = [p for p in all_patches if not p.output_sql]
    if failed_patches:
        lines.append("### Failed patches (fix these):\n")
        for p in failed_patches:
            lines.append(f"**{p.patch_id}** (Family {p.family}, {p.transform}):")
            lines.append(f"  Error: {p.apply_error}")
            lines.append("")

    lines.extend([
        "Please output the COMPLETE JSON array again with fixes.",
        "Keep working patches unchanged. Fix only the broken ones.",
        f"Output exactly {max(len(all_patches), 4)} patch plans:",
    ])
    return "\n".join(lines)


# ── Runtime Error Retry Prompt ─────────────────────────────────────────────

def build_runtime_error_retry_prompt(
    original_prompt: str,
    good_patches: List[Any],
    failed_patches: List[Any],
) -> str:
    """Build a targeted retry prompt for patches that errored at runtime.

    Multi-handling: the LLM sees all kept patches (wins, neutrals, AND
    regressions — all valid signal) plus the errored patches with exact
    error messages. Only needs to produce replacements for the errored ones.

    Args:
        original_prompt: The original prompt (for query/IR context).
        good_patches: AppliedPatch objects that ran successfully (any status except ERROR).
        failed_patches: AppliedPatch objects that errored at runtime.

    Returns:
        Retry prompt string asking for len(failed_patches) replacement patches.
    """
    lines = [
        original_prompt,
        "",
        "## Runtime Error — Replace Errored Patches",
        "",
        f"From your previous response, **{len(good_patches)} patches ran** "
        f"and **{len(failed_patches)} errored at runtime**.",
        "",
        "### Kept patches (do NOT re-emit these):",
        "",
    ]

    for p in good_patches:
        speedup_str = f"{p.speedup:.2f}x" if p.speedup is not None else "?"
        orig_str = f"{p.original_ms:.0f}ms" if p.original_ms is not None else "?"
        patch_str = f"{p.patch_ms:.0f}ms" if p.patch_ms is not None else "?"
        lines.append(
            f"- **{p.patch_id}** (Family {p.family}, {p.transform}): "
            f"{speedup_str} {p.status} (orig={orig_str}, patch={patch_str})"
        )

    lines.extend(["", "### Errored (provide replacements for these):", ""])

    for p in failed_patches:
        lines.append(f"**{p.patch_id}** (Family {p.family}, {p.transform}):")
        lines.append(f"  - **Error**: {p.apply_error or 'unknown'}")
        if p.output_sql:
            sql_preview = "\n".join(p.output_sql.strip().split("\n")[:20])
            lines.append(f"  - **SQL that failed**:")
            lines.append(f"```sql\n{sql_preview}\n```")
        lines.append("")

    n_needed = len(failed_patches)
    lines.extend([
        f"Provide exactly **{n_needed} replacement patch plan(s)** in the same JSON format.",
        "Each replacement should fix the runtime error while targeting the same optimization opportunity.",
        "If the original family is incompatible with this engine, choose a different family.",
        "",
        f"Output a JSON array of {n_needed} patch plans:",
    ])

    return "\n".join(lines)


# ── Utility: Print Prompt for Review ───────────────────────────────────────

def print_prompt_preview(prompt: str, max_lines: int = 100):
    """Print first N lines of prompt for review."""
    lines = prompt.split("\n")
    for i, line in enumerate(lines[:max_lines]):
        print(f"{i+1:3d} | {line}")
    if len(lines) > max_lines:
        print(f"\n... ({len(lines) - max_lines} more lines)")
