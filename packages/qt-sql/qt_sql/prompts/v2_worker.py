"""V2 Worker prompt builder — SQL generator from V2 analyst briefing.

Workers receive a precise specification:
  [1] Role + dialect + assignment metadata
  [2] SEMANTIC CONTRACT (primacy — what MUST be preserved)
  [3] CURRENT PLAN GAP (what divergences to fix)
  [4] APPROACH + TARGET QUERY MAP + NODE CONTRACTS (blueprint)
  [5] HAZARD FLAGS + REGRESSION WARNINGS (what to avoid)
  [6] ACTIVE CONSTRAINTS (analyst-filtered rules)
  [7] EXAMPLE ADAPTATION + before/after SQL
  [8] ORIGINAL SQL
  [9] COLUMN COMPLETENESS CONTRACT + OUTPUT FORMAT (recency)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .v2_briefing_checks import build_v2_worker_rewrite_checklist

logger = logging.getLogger(__name__)


def build_v2_worker_prompt(
    worker_briefing: Any,  # V2BriefingWorker
    shared_briefing: Any,  # V2BriefingShared
    examples: List[Dict[str, Any]],
    original_sql: str,
    output_columns: List[str],
    dialect: str = "duckdb",
    engine_version: Optional[str] = None,
    original_logic_tree: Optional[str] = None,
) -> str:
    """Build a V2 worker prompt from analyst briefing sections.

    Args:
        worker_briefing: This worker's V2BriefingWorker assignment
        shared_briefing: V2BriefingShared
        examples: Loaded gold examples (full before/after SQL)
        original_sql: The original SQL query
        output_columns: Expected output columns for completeness contract
        dialect: SQL dialect
        engine_version: Engine version string
        original_logic_tree: Pre-built Logic Tree text

    Returns:
        Complete worker prompt string
    """
    sections: list[str] = []

    # ── [1] Role + dialect ──────────────────────────────────────────
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "snowflake": "Snowflake",
    }
    engine = engine_names.get(dialect, dialect)
    ver = f" v{engine_version}" if engine_version else ""

    role_parts = [
        f"You are a SQL rewrite engine for {engine}{ver}.",
        "Follow the Target Query Map structure below.",
        "Write correct, executable SQL for each node.",
        "Preserve exact semantic equivalence (same rows, same columns, same ordering).",
    ]

    # Worker metadata header
    approach = getattr(worker_briefing, "approach", "")
    strategy = getattr(worker_briefing, "strategy", "")

    meta_parts = []
    if strategy:
        meta_parts.append(f"Strategy: {strategy}")
    if approach:
        meta_parts.append(f"Approach: {approach}")

    if meta_parts:
        role_parts.append(f"\n**Assignment:** {' | '.join(meta_parts)}")

    sections.append(" ".join(role_parts[:4]) + ("\n" + role_parts[4] if len(role_parts) > 4 else ""))

    # ── [2] SEMANTIC CONTRACT ────────────────────────────────────────
    if shared_briefing.semantic_contract:
        sections.append(
            "## Semantic Contract (MUST preserve)\n\n"
            + shared_briefing.semantic_contract
        )

    # ── [3] CURRENT PLAN GAP ────────────────────────────────────────
    # Use new field first, fall back to backwards-compat alias
    plan_gap = getattr(shared_briefing, "current_plan_gap", "") or ""
    if not plan_gap:
        plan_gap = getattr(shared_briefing, "goal_violations", "") or ""
    if plan_gap:
        sections.append(
            "## Current Plan Gap (what to fix)\n\n"
            + plan_gap
        )

    # ── [4] APPROACH + TARGET QUERY MAP + NODE CONTRACTS ───────────
    # Approach: the structural idea
    if approach:
        sections.append(
            "## Approach\n\n"
            + approach
        )

    # Target Query Map: the new restructured data flow
    target_map = getattr(worker_briefing, "target_query_map", "") or ""
    node_contracts = getattr(worker_briefing, "node_contracts", "") or ""
    # Fall back to backwards-compat target_logical_tree
    target_tree = getattr(worker_briefing, "target_logical_tree", "") or ""

    if target_map or node_contracts:
        tree_text = ""
        if target_map:
            tree_text += f"TARGET_QUERY_MAP:\n{target_map}"
        if node_contracts:
            if tree_text:
                tree_text += "\n\n"
            tree_text += f"NODE_CONTRACTS:\n{node_contracts}"
        sections.append(
            "## Target Query Map + Node Contracts\n\n"
            "Build your rewrite following this CTE structure. Each node's "
            "OUTPUT list is exhaustive — your SQL must produce exactly those "
            "columns.\n\n"
            + tree_text
        )
    elif target_tree:
        sections.append(
            "## Target Query Map + Node Contracts\n\n"
            "Build your rewrite following this CTE structure. Each node's "
            "OUTPUT list is exhaustive — your SQL must produce exactly those "
            "columns.\n\n"
            + target_tree
        )

    # ── [5] HAZARD FLAGS + REGRESSION WARNINGS ──────────────────────
    if worker_briefing.hazard_flags:
        sections.append(
            "## Hazard Flags (avoid these specific risks)\n\n"
            + worker_briefing.hazard_flags
        )

    if shared_briefing.regression_warnings:
        sections.append(
            "## Regression Warnings (observed failures on similar queries)\n\n"
            + shared_briefing.regression_warnings
        )

    # ── [6] ACTIVE CONSTRAINTS ───────────────────────────────────────
    if shared_briefing.active_constraints:
        sections.append(
            "## Constraints (analyst-filtered for this query)\n\n"
            + shared_briefing.active_constraints
        )

    # ── [7] EXAMPLE ADAPTATION + EXAMPLES ─────────────────────────
    if worker_briefing.example_adaptation:
        sections.append(
            "## Example Adaptation Notes\n\n"
            + worker_briefing.example_adaptation
        )

    if examples:
        sections.append(_section_examples(examples))

    # ── [8] ORIGINAL SQL ─────────────────────────────────────────────
    sections.append(
        "## Original SQL\n\n"
        "```sql\n"
        + original_sql + "\n"
        "```"
    )

    # Rewrite checklist
    sections.append(build_v2_worker_rewrite_checklist())

    # ── [9] COLUMN COMPLETENESS CONTRACT + OUTPUT FORMAT ─────────────
    sections.append(_section_output_format(output_columns, original_logic_tree, dialect))

    return "\n\n".join(sections)


def _section_examples(examples: List[Dict[str, Any]]) -> str:
    """Format reference examples with before/after SQL pairs."""
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

        principle = example.get("principle", "")
        if principle:
            lines.append(f"\n**Principle:** {principle}")

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
    output_columns: Optional[List[str]],
    original_logic_tree: Optional[str],
    dialect: str,
) -> str:
    """Output format section — DAP (Decomposed Attention Protocol)."""
    lines = []

    if output_columns:
        cols_str = ", ".join(f"`{c}`" for c in output_columns)
        lines.append(
            f"### Column Completeness Contract\n\n"
            f"`main_query` MUST produce exactly these columns (same names, same order): "
            f"{cols_str}\n"
        )

    if original_logic_tree:
        lines.append(
            "## Original Query Structure\n\n"
            "Current structure (all `[=]` unchanged).\n\n"
            "```\n" + original_logic_tree + "\n```\n"
        )

    lines.append(
        "## Output Format\n\n"
        "Two parts in order:\n\n"
        "### Part 1: Modified Logic Tree\n\n"
        "Markers: `[+]` new, `[-]` removed, `[~]` modified, `[=]` unchanged, `[!]` structural.\n\n"
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
        "- Tree first, then SQL. Every `sql` must be complete (no ellipsis).\n"
        "- Only changed/added components. Unchanged -> `\"change\": \"unchanged\", \"sql\": \"\"`.\n"
        "- `main_query` columns must match Column Completeness Contract.\n\n"
        "After JSON:\n"
        "```\nChanges: <1-2 sentences>\nExpected speedup: <estimate>\n```\n\n"
        "Now output your Logic Tree and Component Payload JSON:"
    )

    return "\n".join(lines)
