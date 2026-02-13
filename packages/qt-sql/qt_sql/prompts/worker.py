"""Worker prompt builder — focused SQL generator from analyst briefing.

Workers receive a precise specification from the analyst briefing:
  [1] Role + dialect (short, mechanical — worker follows the logical tree)
  [2] SEMANTIC CONTRACT (primacy — frames what MUST be preserved)
  [3] TARGET LOGICAL TREE + NODE CONTRACTS (what to produce — the blueprint)
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
    original_logic_tree: Optional[str] = None,
    resource_envelope: Optional[str] = None,  # deprecated, ignored
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
        original_logic_tree: Pre-built Logic Tree text (DAP format, all [=] markers)
        resource_envelope: Deprecated, ignored. Config tuning moved to config_boost phase.

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
        f"Follow the Target Logical Tree structure below. Your job is to write correct, "
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

    # ── [3] TARGET LOGICAL TREE + NODE CONTRACTS ─────────────────────────
    if worker_briefing.target_logical_tree:
        sections.append(
            "## Target Logical Tree + Node Contracts\n\n"
            "Build your rewrite following this CTE structure. Each node's "
            "OUTPUT list is exhaustive — your SQL must produce exactly those "
            "columns.\n\n"
            + worker_briefing.target_logical_tree
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
    if worker_briefing.example_adaptation:
        sections.append(
            "## Example Adaptation Notes\n\n"
            "For each example: what to apply to your rewrite, and what to ignore.\n\n"
            + worker_briefing.example_adaptation
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

    sections.append(build_worker_rewrite_checklist())

    # ── [8] COLUMN COMPLETENESS CONTRACT + OUTPUT FORMAT ─────────────────
    sections.append(_section_output_format(output_columns, original_logic_tree, dialect=dialect))

    return "\n\n".join(sections)


def _section_examples(examples: List[Dict[str, Any]]) -> str:
    """Format reference examples with before/after SQL pairs.

    Reuses the same format as prompter._section_examples() for
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


def _section_output_format(
    output_columns: Optional[List[str]] = None,
    original_logic_tree: Optional[str] = None,
    dialect: str = "duckdb",
) -> str:
    """Output format section — DAP (Decomposed Attention Protocol).

    Two-part output:
    1. Modified Logic Tree (change markers: [+]/[-]/[~]/[=]/[!])
    2. Component Payload JSON (per DAP spec)

    When original_logic_tree is provided, it's shown as context so the LLM
    sees the current structure in exactly the format it should output.
    """
    lines = []

    # ── Column Completeness Contract ──
    if output_columns:
        cols_str = ", ".join(f"`{c}`" for c in output_columns)
        lines.append(
            f"### Column Completeness Contract\n\n"
            f"`main_query` MUST produce exactly these columns (same names, same order): "
            f"{cols_str}\n"
        )

    # ── Original Query Structure (Logic Tree context) ──
    if original_logic_tree:
        lines.append(
            "## Original Query Structure\n\n"
            "Current structure (all `[=]` unchanged). Your modified tree shows what you changed.\n\n"
            "```\n" + original_logic_tree + "\n```\n"
        )

    # ── Output Format ──
    lines.append(
        "## Output Format\n\n"
        "Two parts in order:\n\n"
        "### Part 1: Modified Logic Tree\n\n"
        "Generate BEFORE writing SQL. Markers: `[+]` new, `[-]` removed, `[~]` modified, `[=]` unchanged, `[!]` structural change.\n\n"
        "### Part 2: Component Payload JSON\n\n"
        "```json\n"
        '{"spec_version": "1.0", "dialect": "<dialect>",\n'
        ' "rewrite_rules": [{"id": "R1", "type": "<transform>", "description": "<what>", "applied_to": ["<id>"]}],\n'
        ' "statements": [{"target_table": null, "change": "modified",\n'
        '   "components": {\n'
        '     "<cte>": {"type": "cte", "change": "modified", "sql": "<full SQL>",\n'
        '       "interfaces": {"outputs": ["col1"], "consumes": ["<upstream>"]}},\n'
        '     "main_query": {"type": "main_query", "change": "modified", "sql": "<SELECT>",\n'
        '       "interfaces": {"outputs": ["col1"], "consumes": ["<cte>"]}}},\n'
        '   "reconstruction_order": ["<cte>", "main_query"],\n'
        '   "assembly_template": "WITH <cte> AS ({<cte>}) {main_query}"}],\n'
        ' "macros": {}, "frozen_blocks": [], "validation_checks": []}\n'
        "```\n\n"
        "### Rules\n"
        "- Tree first, then SQL. Every `sql` must be complete (no ellipsis). Frozen blocks verbatim.\n"
        "- Only include changed/added components. Unchanged → `\"change\": \"unchanged\", \"sql\": \"\"`.\n"
        "- `main_query` columns must match Column Completeness Contract. `reconstruction_order` in dependency order.\n\n"
        "After JSON:\n"
        "```\nChanges: <1-2 sentences>\nExpected speedup: <estimate>\n```\n\n"
        "Now output your Logic Tree and Component Payload JSON:"
    )

    return "\n".join(lines)
