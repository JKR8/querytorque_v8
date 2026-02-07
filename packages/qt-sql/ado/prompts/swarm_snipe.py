"""Snipe prompt for swarm mode.

After fan-out workers fail to reach the target, the analyst synthesizes
all failure information into a refined single-worker approach.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..schemas import WorkerResult
from .dag_helpers import append_dag_summary


def build_snipe_prompt(
    query_id: str,
    original_sql: str,
    worker_results: List[WorkerResult],
    target_speedup: float,
    dag: Any,
    costs: Dict[str, Any],
    all_available_examples: List[Dict[str, str]],
    dialect: str = "duckdb",
) -> str:
    """Build prompt asking analyst to synthesize failures into refined approach.

    Args:
        query_id: Query identifier
        original_sql: The original SQL query
        worker_results: Results from all previous workers
        target_speedup: Target speedup ratio
        dag: Parsed DAG
        costs: Per-node cost analysis
        all_available_examples: Full catalog of gold examples
        dialect: SQL dialect

    Returns:
        Snipe prompt string
    """
    lines = []

    # Role
    lines.append(
        f"You are analyzing {len(worker_results)} failed optimization attempts "
        f"to design a refined approach that reaches {target_speedup:.1f}x speedup."
    )
    lines.append("")
    lines.append(
        "Your job: understand WHY each attempt fell short, identify unexplored "
        "optimization angles, and synthesize a NEW strategy that combines the "
        "best insights while avoiding repeated mistakes."
    )
    lines.append("")

    # Query
    lines.append(f"## Query: {query_id}")
    lines.append(f"## Target: {target_speedup:.1f}x speedup")
    lines.append(f"## Dialect: {dialect}")
    lines.append("")
    lines.append("```sql")
    lines.append(original_sql.strip())
    lines.append("```")
    lines.append("")

    # Failed attempts
    lines.append("## Previous Attempts")
    lines.append("")
    for wr in worker_results:
        status_emoji = "pass" if wr.speedup >= 1.0 else "regression"
        lines.append(f"### Worker {wr.worker_id}: {wr.strategy}")
        lines.append(f"- **Status**: {wr.status} ({wr.speedup:.2f}x)")
        if wr.error_message:
            lines.append(f"- **Error**: {wr.error_message}")
        lines.append(f"- **Transforms**: {', '.join(wr.transforms) or 'none'}")
        lines.append(f"- **Examples used**: {', '.join(wr.examples_used)}")
        if wr.hint:
            lines.append(f"- **Strategy hint**: {wr.hint}")
        lines.append("")
        # Show abbreviated optimized SQL (first 30 lines)
        sql_lines = wr.optimized_sql.strip().split("\n")
        if len(sql_lines) > 30:
            sql_preview = "\n".join(sql_lines[:30]) + "\n-- ... (truncated)"
        else:
            sql_preview = wr.optimized_sql.strip()
        lines.append("```sql")
        lines.append(sql_preview)
        lines.append("```")
        lines.append("")

    # DAG summary
    lines.append("## DAG Structure & Bottlenecks")
    lines.append("")
    append_dag_summary(lines, dag, costs, include_operations=False)
    lines.append("")

    # Full catalog
    lines.append("## Available Examples (Full Catalog)")
    lines.append("")
    for ex in all_available_examples:
        ex_id = ex.get("id", "?")
        speedup = ex.get("speedup", "?")
        desc = ex.get("description", "")[:100]
        lines.append(f"- **{ex_id}** ({speedup}x) — {desc}")
    lines.append("")

    # Task
    lines.append("## Your Task")
    lines.append("")
    lines.append("Analyze the failed attempts and design a refined approach:")
    lines.append("")
    lines.append("1. **Failure Analysis**: Why did all attempts fall short? Be specific about mechanisms.")
    lines.append("2. **Common Patterns**: What did multiple workers try unsuccessfully?")
    lines.append("3. **Unexplored Space**: What optimization angles were missed entirely?")
    lines.append("4. **Refined Strategy**: Synthesize a NEW approach combining best insights.")
    lines.append("")

    # Output format
    lines.append("### Output Format (follow EXACTLY)")
    lines.append("")
    lines.append("```")
    lines.append("FAILURE_ANALYSIS:")
    lines.append("<Why all workers fell short — be specific about mechanisms>")
    lines.append("")
    lines.append("UNEXPLORED_OPPORTUNITIES:")
    lines.append("<What optimization approaches haven't been tried>")
    lines.append("")
    lines.append("REFINED_STRATEGY:")
    lines.append("<Concrete optimization approach for next attempt>")
    lines.append("")
    lines.append("EXAMPLES: <ex1>, <ex2>, <ex3>")
    lines.append("HINT: <specific guidance for the refined attempt>")
    lines.append("```")

    return "\n".join(lines)


