"""Worker prompt builder — focused SQL generator from analyst briefing.

Workers receive a precise specification from the analyst briefing:
  [1] Role + dialect (short, mechanical — worker follows the DAG)
  [2] SEMANTIC CONTRACT (primacy — frames what MUST be preserved)
  [3] TARGET DAG + NODE CONTRACTS (what to produce — the blueprint)
  [4] HAZARD FLAGS (what to avoid — before they start writing)
  [4b] REGRESSION WARNINGS (observed failures on similar queries)
  [5] ACTIVE CONSTRAINTS (rules that apply — analyst-filtered 3-6)
  [6] REASONED EXAMPLES + before/after SQL (pattern material)
  [7] ORIGINAL SQL (source reference)
  [8] COLUMN COMPLETENESS CONTRACT (recency — final check)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .briefing_checks import build_worker_rewrite_checklist

logger = logging.getLogger(__name__)


def build_worker_prompt(
    worker_briefing: Any,  # BriefingWorker dataclass
    shared_briefing: Any,  # BriefingShared dataclass
    examples: List[Dict[str, Any]],
    original_sql: str,
    output_columns: List[str],
    dialect: str = "duckdb",
    engine_version: Optional[str] = None,
    resource_envelope: Optional[str] = None,
) -> str:
    """Build a worker prompt from analyst briefing sections.

    This prompt is attention-optimized: understanding before material,
    constraints before freedom.

    Args:
        worker_briefing: This worker's assignment from ParsedBriefing
        shared_briefing: Shared analysis from ParsedBriefing
        examples: Loaded gold examples (full before/after SQL)
        original_sql: The original SQL query
        output_columns: Expected output columns for completeness contract
        dialect: SQL dialect
        engine_version: Engine version string (e.g., '1.4.3')
        resource_envelope: System resource envelope text for PG workers (may be None)

    Returns:
        Complete worker prompt string
    """
    sections: list[str] = []

    # ── [1] Role + dialect + output format ───────────────────────────────
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "snowflake": "Snowflake",
    }
    engine = engine_names.get(dialect, dialect)
    ver = f" v{engine_version}" if engine_version else ""

    sections.append(
        f"You are a SQL rewrite engine for {engine}{ver}. "
        f"Follow the Target DAG structure below. Your job is to write correct, "
        f"executable SQL for each node — not to decide whether to restructure. "
        f"Preserve exact semantic equivalence (same rows, same columns, same ordering). "
        f"Preserve defensive guards: if the original uses CASE WHEN x > 0 THEN y/x END "
        f"around a division, keep it — even when a WHERE clause makes the zero case "
        f"unreachable. Guards prevent silent breakage if filters change upstream. "
        f"Strip benchmark comments (-- start query, -- end query) from your output."
    )

    # Engine-specific compact hints
    if dialect == "duckdb":
        sections.append(
            "DuckDB specifics: columnar storage (SELECT only needed columns). "
            "CTEs referenced once are typically inlined; CTEs referenced multiple "
            "times may be materialized. FILTER clause is native "
            "(`COUNT(*) FILTER (WHERE cond)`). Predicate pushdown stops at "
            "UNION ALL boundaries and multi-level CTE references."
        )

    # ── [2] SEMANTIC CONTRACT ────────────────────────────────────────────
    if shared_briefing.semantic_contract:
        sections.append(
            "## Semantic Contract (MUST preserve)\n\n"
            + shared_briefing.semantic_contract
        )

    # ── [3] TARGET DAG + NODE CONTRACTS ──────────────────────────────────
    if worker_briefing.target_dag:
        sections.append(
            "## Target DAG + Node Contracts\n\n"
            "Build your rewrite following this CTE structure. Each node's "
            "OUTPUT list is exhaustive — your SQL must produce exactly those "
            "columns.\n\n"
            + worker_briefing.target_dag
        )

    # ── [4] HAZARD FLAGS ─────────────────────────────────────────────────
    if worker_briefing.hazard_flags:
        sections.append(
            "## Hazard Flags (avoid these specific risks)\n\n"
            + worker_briefing.hazard_flags
        )

    # ── [4b] REGRESSION WARNINGS ──────────────────────────────────────────
    if shared_briefing.regression_warnings:
        sections.append(
            "## Regression Warnings (observed failures on similar queries)\n\n"
            + shared_briefing.regression_warnings
        )

    # ── [5] ACTIVE CONSTRAINTS ───────────────────────────────────────────
    if shared_briefing.active_constraints:
        sections.append(
            "## Constraints (analyst-filtered for this query)\n\n"
            + shared_briefing.active_constraints
        )

    # ── [6] EXAMPLE ADAPTATION ─────────────────────────────────────────
    if worker_briefing.example_reasoning:
        sections.append(
            "## Example Adaptation Notes\n\n"
            "For each example: what to apply to your rewrite, and what to ignore.\n\n"
            + worker_briefing.example_reasoning
        )

    if examples:
        sections.append(_section_examples(examples))

    # ── [7] ORIGINAL SQL ─────────────────────────────────────────────────
    sections.append(
        "## Original SQL\n\n"
        "```sql\n"
        + original_sql + "\n"
        "```"
    )

    # ── [7b] PER-REWRITE CONFIG (PG only) ─────────────────────────────────
    if dialect in ("postgres", "postgresql") and resource_envelope:
        sections.append(_section_set_local_config(resource_envelope))

    sections.append(build_worker_rewrite_checklist())

    # ── [8] COLUMN COMPLETENESS CONTRACT ─────────────────────────────────
    sections.append(_section_output_format(output_columns))

    return "\n\n".join(sections)


def _section_examples(examples: List[Dict[str, Any]]) -> str:
    """Format reference examples with before/after SQL pairs.

    Reuses the same format as node_prompter._section_examples() for
    consistency, but without the generic preamble.
    """
    lines = [
        "## Reference Examples",
        "",
        "Pattern reference only — do not copy table/column names or literals.",
    ]

    for i, example in enumerate(examples):
        pattern_name = (
            example.get("id")
            or example.get("name")
            or f"example_{i+1}"
        )
        speedup = example.get("verified_speedup", "")
        speedup_str = f" ({speedup})" if speedup else ""

        lines.append("")
        lines.append(f"### {i+1}. {pattern_name}{speedup_str}")

        ex = example.get("example", example)

        # Principle
        principle = example.get("principle", "")
        if principle:
            lines.append(f"\n**Principle:** {principle}")

        # BEFORE (slow)
        before_sql = (
            example.get("original_sql")
            or ex.get("before_sql")
            or ex.get("input_slice")
            or ""
        )
        if not before_sql:
            inp = example.get("input", {})
            before_sql = inp.get("sql", "")
        if before_sql:
            lines.append("")
            lines.append("**BEFORE (slow):**")
            lines.append(f"```sql\n{before_sql}\n```")

        # AFTER (fast)
        output = ex.get("output", example.get("output", {}))
        rewrite_sets = output.get("rewrite_sets", [])
        if rewrite_sets and rewrite_sets[0].get("nodes"):
            nodes = rewrite_sets[0]["nodes"]
            lines.append("")
            lines.append("**AFTER (fast):**")
            for nid, sql in nodes.items():
                lines.append(f"[{nid}]:")
                lines.append(f"```sql\n{sql}\n```")
        else:
            out_sql = output.get("sql", "")
            if out_sql:
                lines.append("")
                lines.append("**AFTER (fast):**")
                lines.append(f"```sql\n{out_sql}\n```")

    return "\n".join(lines)


def _section_set_local_config(resource_envelope: str) -> str:
    """Build the per-rewrite SET LOCAL configuration section for PG workers."""
    from ..pg_tuning import PG_TUNABLE_PARAMS

    lines = [
        "## Per-Rewrite Configuration (SET LOCAL)",
        "",
        "You have two optimization levers: SQL rewrite AND per-query configuration.",
        "After writing your rewrite, analyze its execution profile and emit SET LOCAL",
        "commands that fix planner-level bottlenecks specific to YOUR rewrite.",
        "",
        resource_envelope,
        "",
        "### Tunable Parameters (whitelist — only these are allowed)",
        "",
    ]

    for param, (ptype, pmin, pmax, desc) in sorted(PG_TUNABLE_PARAMS.items()):
        if ptype == "bool":
            range_str = "on | off"
        elif ptype == "bytes":
            range_str = f"{pmin}MB–{pmax}MB"
        else:
            range_str = f"{pmin}–{pmax}"
        lines.append(f"- **{param}** ({range_str}): {desc}")

    lines.extend([
        "",
        "### Rules",
        "- Every SET LOCAL MUST cite a specific EXPLAIN node your rewrite creates/changes",
        "- work_mem is PER-OPERATION: count hash/sort ops in your rewrite before sizing",
        "- random_page_cost: ONLY change if your rewrite creates index-favorable access patterns",
        "- Empty is valid: if your rewrite has no planner bottleneck, emit no SET LOCAL",
        "- Stay within the resource envelope bounds above",
        "",
        "### Output Format",
        "If you recommend SET LOCAL, prefix your SQL block with the commands:",
        "",
        "```sql",
        "SET LOCAL work_mem = '512MB';",
        "SET LOCAL jit = 'off';",
        "-- rewritten query follows",
        "WITH ...",
        "SELECT ...",
        "```",
        "",
        "If no config changes help, just output the rewritten SQL directly.",
    ])

    return "\n".join(lines)


def _section_output_format(
    output_columns: Optional[List[str]] = None,
) -> str:
    """Output format section with column completeness contract."""
    lines = [
        "## Output",
        "",
        "Return the complete rewritten SQL query. The query must be syntactically",
        "valid and ready to execute.",
    ]

    if output_columns:
        lines.append("")
        lines.append("### Column Completeness Contract")
        lines.append("")
        lines.append(
            "Your rewritten query MUST produce **exactly** these output columns "
            "(same names, same order):"
        )
        lines.append("")
        for i, col in enumerate(output_columns, 1):
            lines.append(f"  {i}. `{col}`")
        lines.append("")
        lines.append(
            "Do NOT add, remove, or rename any columns. "
            "The result set schema must be identical to the original query."
        )

    lines.extend([
        "",
        "```sql",
        "-- Your rewritten query here",
        "```",
        "",
        "After the SQL, explain the mechanism:",
        "",
        "```",
        "Changes: <1-2 sentences: what structural change + the expected mechanism>",
        "  e.g., 'Consolidated 4 store_sales scans into 1 with CASE branches — reduces I/O by 3x'",
        "  e.g., 'Deferred customer join to resolve_names — joins 4K rows instead of 5.4M'",
        "Expected speedup: <estimate>",
        "```",
        "",
        "Now output your rewritten SQL:",
    ])

    return "\n".join(lines)
