"""Fan-out prompt for beam mode.

Asks the analyst to distribute matched examples across 4 workers,
each with a different optimization strategy.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .dag_helpers import append_logical_tree_summary


def build_fan_out_prompt(
    query_id: str,
    sql: str,
    dag: Any,
    costs: Dict[str, Any],
    matched_examples: List[Dict[str, Any]],
    all_available_examples: List[Dict[str, str]],
    regression_warnings: Optional[List[Dict[str, Any]]] = None,
    dialect: str = "duckdb",
) -> str:
    """Build prompt asking analyst to distribute examples across 4 workers.

    Args:
        query_id: Query identifier
        sql: The SQL query to optimize
        dag: Parsed logical tree from Phase 1
        costs: Per-node cost analysis
        matched_examples: Top matched examples (by tag similarity)
        all_available_examples: Full catalog of gold examples (id + description)
        regression_warnings: Regression examples for similar queries (analyst reviews relevance)
        dialect: SQL dialect

    Returns:
        Fan-out prompt string
    """
    lines = []

    # Role
    lines.append(
        "You are coordinating a team of 4 optimization specialists. "
        "Each specialist will attempt to optimize the same query using a "
        "DIFFERENT strategy and set of examples."
    )
    lines.append("")
    lines.append(
        "Your job: analyze the query structure, identify 4 diverse optimization "
        "angles, and assign each specialist a unique strategy with 3 relevant "
        "examples. Maximize diversity to cover the optimization space."
    )
    lines.append("")

    # Query
    lines.append(f"## Query: {query_id}")
    lines.append(f"## Dialect: {dialect}")
    lines.append("")
    lines.append("```sql")
    lines.append(sql.strip())
    lines.append("```")
    lines.append("")

    # Logical tree structure with costs
    lines.append("## Logical Tree Structure & Bottlenecks")
    lines.append("")
    append_logical_tree_summary(lines, dag, costs, include_operations=True)
    lines.append("")

    # Matched examples (top N by tag similarity)
    lines.append(f"## Top {len(matched_examples)} Matched Examples (by structural similarity)")
    lines.append("")
    for i, ex in enumerate(matched_examples, 1):
        ex_id = ex.get("id", "?")
        speedup = ex.get("verified_speedup", ex.get("speedup", "?"))
        desc = ex.get("description", "")[:120]
        lines.append(f"{i}. **{ex_id}** ({speedup}) — {desc}")
    lines.append("")

    # Full catalog
    lines.append("## All Available Examples (full catalog — can swap if needed)")
    lines.append("")
    for ex in all_available_examples:
        ex_id = ex.get("id", "?")
        speedup = ex.get("speedup", "?")
        desc = ex.get("description", "")[:100]
        lines.append(f"- **{ex_id}** ({speedup}) — {desc}")
    lines.append("")

    # Regression warnings
    if regression_warnings:
        lines.append("## Regression Warnings (review relevance to THIS query)")
        lines.append("")
        lines.append(
            "These transforms caused regressions on structurally similar queries. "
            "Review each — if relevant to this query, AVOID the listed transform. "
            "If not relevant (different structure/bottleneck), you may ignore."
        )
        lines.append("")
        for rw in regression_warnings:
            rw_id = rw.get("id", "?")
            mechanism = rw.get("regression_mechanism", rw.get("description", ""))[:120]
            speedup = rw.get("verified_speedup", rw.get("speedup", "?"))
            lines.append(f"- **{rw_id}** ({speedup}) — {mechanism}")
        lines.append("")

    # Task
    lines.append("## Your Task")
    lines.append("")
    lines.append(
        "Design 4 DIFFERENT optimization strategies exploring diverse approaches. "
        "You may keep the matched recommendations OR swap examples from the catalog."
    )
    lines.append("")
    lines.append("**Constraints**:")
    lines.append("- Each worker gets exactly 3 examples")
    lines.append("- No duplicate examples across workers (12 total, 3 per worker)")
    lines.append("- If fewer than 12 unique examples are available, reuse is allowed")
    lines.append("")
    lines.append("**Diversity guidelines**:")
    lines.append("- Worker 1: Conservative — proven patterns, low risk (e.g., pushdown, early filter)")
    lines.append("- Worker 2: Moderate — date/dimension isolation, CTE restructuring")
    lines.append("- Worker 3: Aggressive — multi-CTE restructuring, prefetch patterns")
    lines.append("- Worker 4: Novel — OR-to-UNION, structural transforms, intersect-to-exists")
    lines.append("")
    lines.append("For each worker, specify:")
    lines.append("1. **Strategy name** (e.g., `aggressive_date_prefetch`)")
    lines.append("2. **3 examples** to use (from matched picks or catalog)")
    lines.append("3. **Strategy hint** (1-2 sentences guiding the optimization approach)")
    lines.append("")

    # Output format
    lines.append("### Output Format (follow EXACTLY)")
    lines.append("")
    lines.append("```")
    lines.append("WORKER_1:")
    lines.append("STRATEGY: <strategy_name>")
    lines.append("EXAMPLES: <ex1>, <ex2>, <ex3>")
    lines.append("HINT: <strategy guidance>")
    lines.append("")
    lines.append("WORKER_2:")
    lines.append("STRATEGY: <strategy_name>")
    lines.append("EXAMPLES: <ex4>, <ex5>, <ex6>")
    lines.append("HINT: <strategy guidance>")
    lines.append("")
    lines.append("WORKER_3:")
    lines.append("STRATEGY: <strategy_name>")
    lines.append("EXAMPLES: <ex7>, <ex8>, <ex9>")
    lines.append("HINT: <strategy guidance>")
    lines.append("")
    lines.append("WORKER_4:")
    lines.append("STRATEGY: <strategy_name>")
    lines.append("EXAMPLES: <ex10>, <ex11>, <ex12>")
    lines.append("HINT: <strategy guidance>")
    lines.append("```")

    return "\n".join(lines)

