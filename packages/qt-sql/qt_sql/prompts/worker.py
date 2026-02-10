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
    original_logic_tree: Optional[str] = None,
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
        original_logic_tree: Pre-built Logic Tree text (DAP format, all [=] markers)

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

    # ── [8] COLUMN COMPLETENESS CONTRACT + OUTPUT FORMAT ─────────────────
    sections.append(_section_output_format(output_columns, original_logic_tree, dialect=dialect))

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
        "### SET LOCAL Syntax",
        "Include SET LOCAL commands in the `runtime_config` array field of your JSON output.",
        "If no config changes help, omit the field or use an empty array.",
    ])

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

    # ── Column Completeness Contract (always first) ──
    if output_columns:
        lines.append("### Column Completeness Contract")
        lines.append("")
        lines.append(
            "Your `main_query` component MUST produce **exactly** these output columns "
            "(same names, same order):"
        )
        lines.append("")
        for i, col in enumerate(output_columns, 1):
            lines.append(f"  {i}. `{col}`")
        lines.append("")
        lines.append(
            "Do NOT add, remove, or rename any output columns. "
            "The result set schema must be identical to the original query."
        )
        lines.append("")

    # ── Original Query Structure (Logic Tree context) ──
    if original_logic_tree:
        lines.append("## Original Query Structure")
        lines.append("")
        lines.append(
            "This is the current query structure. All nodes are `[=]` (unchanged). "
            "Your modified Logic Tree below should show which nodes you changed."
        )
        lines.append("")
        lines.append("```")
        lines.append(original_logic_tree)
        lines.append("```")
        lines.append("")

    # ── Output Format ──
    lines.append("## Output Format")
    lines.append("")
    lines.append(
        "Your response has **two parts** in order:"
    )
    lines.append("")

    # Part 1: Modified Logic Tree
    lines.append("### Part 1: Modified Logic Tree")
    lines.append("")
    lines.append(
        "Show what changed using change markers. Generate the tree BEFORE writing SQL."
    )
    lines.append("")
    lines.append("Change markers:")
    lines.append("- `[+]` — New component added")
    lines.append("- `[-]` — Component removed")
    lines.append("- `[~]` — Component modified (describe what changed)")
    lines.append("- `[=]` — Unchanged (no children needed)")
    lines.append("- `[!]` — Structural change (e.g. CTE → subquery)")
    lines.append("")

    # Part 2: Component Payload JSON
    lines.append("### Part 2: Component Payload JSON")
    lines.append("")
    lines.append("```json")
    lines.append("{")
    lines.append('  "spec_version": "1.0",')
    lines.append(f'  "dialect": "<dialect>",')
    lines.append('  "rewrite_rules": [')
    lines.append('    {"id": "R1", "type": "<transform_name>", "description": "<what changed>", "applied_to": ["<component_id>"]}')
    lines.append("  ],")
    lines.append('  "statements": [{')
    lines.append('    "target_table": null,')
    lines.append('    "change": "modified",')
    lines.append('    "components": {')
    lines.append('      "<cte_name>": {')
    lines.append('        "type": "cte",')
    lines.append('        "change": "modified",')
    lines.append('        "sql": "<complete SQL for this CTE body>",')
    lines.append('        "interfaces": {"outputs": ["col1", "col2"], "consumes": ["<upstream_id>"]}')
    lines.append("      },")
    lines.append('      "main_query": {')
    lines.append('        "type": "main_query",')
    lines.append('        "change": "modified",')
    lines.append('        "sql": "<final SELECT>",')
    lines.append('        "interfaces": {"outputs": ["col1", "col2"], "consumes": ["<cte_name>"]}')
    lines.append("      }")
    lines.append("    },")
    lines.append('    "reconstruction_order": ["<cte_name>", "main_query"],')
    lines.append('    "assembly_template": "WITH <cte_name> AS ({<cte_name>}) {main_query}"')
    lines.append("  }],")
    lines.append('  "macros": {},')
    lines.append('  "frozen_blocks": [],')
    if dialect in ("postgres", "postgresql"):
        lines.append('  "runtime_config": ["SET LOCAL work_mem = \'512MB\'"],')
    lines.append('  "validation_checks": []')
    lines.append("}")
    lines.append("```")
    lines.append("")

    # Rules
    lines.append("### Rules")
    lines.append("- **Tree first, always.** Generate the Logic Tree before writing any SQL")
    lines.append("- **One component at a time.** When writing SQL for component X, treat others as opaque interfaces")
    lines.append("- **No ellipsis.** Every `sql` value must be complete, executable SQL")
    lines.append("- **Frozen blocks are copy-paste.** Large CASE-WHEN lookups must be verbatim")
    lines.append("- **Validate interfaces.** Verify every `consumes` reference exists in upstream `outputs`")
    lines.append("- Only include components you **changed or added** — set unchanged components to `\"change\": \"unchanged\"` with `\"sql\": \"\"`")
    lines.append("- `main_query` output columns must match the Column Completeness Contract above")
    if dialect in ("postgres", "postgresql"):
        lines.append("- `runtime_config`: SET LOCAL commands for PostgreSQL. Omit or use empty array if not needed")
    lines.append("- `reconstruction_order`: topological order of components for assembly")
    lines.append("")
    lines.append("After the JSON, explain the mechanism:")
    lines.append("")
    lines.append("```")
    lines.append("Changes: <1-2 sentences: what structural change + the expected mechanism>")
    lines.append("Expected speedup: <estimate>")
    lines.append("```")
    lines.append("")
    lines.append("Now output your Logic Tree and Component Payload JSON:")

    return "\n".join(lines)
