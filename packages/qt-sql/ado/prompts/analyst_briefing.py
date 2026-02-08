"""V2 analyst prompt builder — analyst as interpreter, not router.

The V2 analyst produces a structured briefing with 7 sections:
  Shared (all workers):
    1. SEMANTIC_CONTRACT — business intent, invariants, aggregation traps
    2. BOTTLENECK_DIAGNOSIS — dominant cost + mechanism + cardinality flow
    3. ACTIVE_CONSTRAINTS — 3-6 of 25, each with reason for THIS query
    4. REGRESSION_WARNINGS — causal rules, not just "this happened"
  Per-worker:
    5. TARGET_DAG + NODE_CONTRACTS — CTE blueprint + column contracts
    6. EXAMPLES + EXAMPLE_REASONING — why each example matches
    7. HAZARD_FLAGS — strategy-specific risks for this query
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── EXPLAIN plan formatter ──────────────────────────────────────────────

# Operators that are just internal plumbing — collapse/skip them
_SKIP_OPERATORS = {"PROJECTION", "RESULT_COLLECTOR", "COLUMN_DATA_SCAN"}

# Extra-info keys worth showing per operator type
_EXTRA_INFO_KEYS = {
    "SEQ_SCAN": ["Table", "Filters"],
    "INDEX_SCAN": ["Table", "Index", "Filters"],
    "HASH_JOIN": ["Join Type", "Conditions"],
    "CROSS_PRODUCT": [],
    "PIECEWISE_MERGE_JOIN": ["Join Type", "Conditions"],
    "NESTED_LOOP_JOIN": ["Join Type", "Conditions"],
    "HASH_GROUP_BY": ["Aggregates"],
    "PERFECT_HASH_GROUP_BY": ["Aggregates"],
    "UNGROUPED_AGGREGATE": ["Aggregates"],
    "STREAMING_WINDOW": ["Partitions", "Orders"],
    "FILTER": ["Expression"],
    "TOP_N": ["Top", "Order By"],
    "ORDER_BY": ["Orders"],
    "LIMIT": ["Value"],
}


def _fmt_count(n: int) -> str:
    """Format a number compactly: 1234 -> 1,234; 2500000 -> 2.5M."""
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 10_000:
        return f"{n / 1_000:.0f}K"
    if n >= 1_000:
        return f"{n:,}"
    return str(n)


def _fmt_extra_value(key: str, val: Any) -> str:
    """Format an extra_info value to a compact string."""
    if isinstance(val, list):
        items = [str(v) for v in val[:4]]
        s = ", ".join(items)
        if len(val) > 4:
            s += f" (+{len(val) - 4} more)"
        return s
    return str(val)


def _collect_total_timing(node: dict) -> float:
    """Sum all operator_timing values in the tree (exclusive/self-time)."""
    total = node.get("operator_timing", 0) or 0
    for child in node.get("children", []):
        total += _collect_total_timing(child)
    return total


def _render_node(
    node: dict,
    depth: int,
    total_time: float,
    lines: list[str],
    max_depth: int = 30,
) -> None:
    """Recursively render a plan node as an indented ASCII line."""
    if depth > max_depth:
        return

    name = node.get("operator_name", "")
    timing = node.get("operator_timing", 0) or 0
    card = node.get("operator_cardinality", 0) or 0
    scanned = node.get("operator_rows_scanned", 0) or 0
    extra = node.get("extra_info", {}) or {}
    children = node.get("children", [])

    # Skip noise operators — just recurse into their children
    if name in _SKIP_OPERATORS or not name:
        for child in children:
            _render_node(child, depth, total_time, lines, max_depth)
        return

    # Build the line
    indent = "  " * depth
    timing_ms = timing * 1000
    pct = (timing / total_time * 100) if total_time > 0 else 0

    # Cardinality + timing
    if timing_ms >= 0.1:
        if pct >= 1.0:
            stats = f"[{_fmt_count(card)} rows, {timing_ms:.1f}ms, {pct:.0f}%]"
        else:
            stats = f"[{_fmt_count(card)} rows, {timing_ms:.1f}ms]"
    else:
        stats = f"[{_fmt_count(card)} rows]"

    # Special handling for SEQ_SCAN: show table name and scan ratio
    if "SCAN" in name and extra.get("Table"):
        table = extra["Table"]
        if scanned > 0 and scanned != card:
            stats = f"{table} [{_fmt_count(card)} of {_fmt_count(scanned)} rows"
        else:
            stats = f"{table} [{_fmt_count(card)} rows"
        if timing_ms >= 0.1:
            if pct >= 1.0:
                stats += f", {timing_ms:.1f}ms, {pct:.0f}%]"
            else:
                stats += f", {timing_ms:.1f}ms]"
        else:
            stats += "]"
        # Show filters inline for scans
        filters = extra.get("Filters")
        if filters:
            fstr = _fmt_extra_value("Filters", filters)
            if len(fstr) > 80:
                fstr = fstr[:77] + "..."
            stats += f"  Filters: {fstr}"
        lines.append(f"{indent}{name} {stats}")
    else:
        # Join info inline
        suffix = ""
        if "JOIN" in name:
            jtype = extra.get("Join Type", "")
            conds = extra.get("Conditions", "")
            if jtype:
                suffix += f" {jtype}"
            if conds:
                cstr = _fmt_extra_value("Conditions", conds)
                if len(cstr) > 60:
                    cstr = cstr[:57] + "..."
                suffix += f" on {cstr}"
        lines.append(f"{indent}{name}{suffix} {stats}")

        # Show key extra_info on next line(s) for non-join/non-scan
        show_keys = _EXTRA_INFO_KEYS.get(name, [])
        for key in show_keys:
            if key in ("Join Type", "Conditions", "Table", "Filters"):
                continue  # Already shown inline
            val = extra.get(key)
            if val:
                vstr = _fmt_extra_value(key, val)
                if len(vstr) > 100:
                    vstr = vstr[:97] + "..."
                lines.append(f"{indent}  {key}: {vstr}")

    # Recurse
    for child in children:
        _render_node(child, depth + 1, total_time, lines, max_depth)


def format_duckdb_explain_tree(plan_text: str) -> str:
    """Convert DuckDB JSON EXPLAIN plan to a readable ASCII operator tree.

    The plan_text field in cached explain files contains a JSON string
    (not ASCII text). This function parses it and renders a compact tree
    showing operator name, cardinality, timing, cost%, and key metadata.

    Noise operators (PROJECTION, RESULT_COLLECTOR) are collapsed.
    Timing is shown as ms with % of total for bottleneck identification.

    Args:
        plan_text: JSON string from explains/*.json field "plan_text"

    Returns:
        Readable ASCII tree string, or the original text if not JSON.
    """
    # If it's not JSON, return as-is (might be pre-formatted text)
    stripped = plan_text.strip()
    if not stripped.startswith("{"):
        return plan_text

    try:
        plan = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        return plan_text

    # Find the real root — skip wrapper and EXPLAIN_ANALYZE
    root_nodes = []
    for child in plan.get("children", []):
        if child.get("operator_name") == "EXPLAIN_ANALYZE":
            root_nodes.extend(child.get("children", []))
        else:
            root_nodes.append(child)

    if not root_nodes:
        root_nodes = plan.get("children", [])
    if not root_nodes:
        return plan_text

    # Compute total timing for % calculation
    total_time = sum(_collect_total_timing(n) for n in root_nodes)

    lines: list[str] = []
    if total_time > 0:
        lines.append(f"Total execution time: {total_time * 1000:.0f}ms")
        lines.append("")

    for node in root_nodes:
        _render_node(node, depth=0, total_time=total_time, lines=lines)

    return "\n".join(lines)


def _strip_template_comments(sql: str) -> str:
    """Strip TPC-DS/DSB template comments from SQL."""
    import re
    lines = sql.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        # Skip TPC-DS template markers
        if re.match(r"^--\s*(start|end)\s+query\s+\d+", stripped, re.IGNORECASE):
            continue
        # Skip empty comment-only lines at start/end
        if not cleaned and stripped == "--":
            continue
        cleaned.append(line)
    # Strip trailing blank lines
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()
    return "\n".join(cleaned)


def _add_line_numbers(sql: str) -> str:
    """Add line numbers to SQL for analyst reference."""
    lines = sql.split("\n")
    width = len(str(len(lines)))
    return "\n".join(f"{i+1:>{width}} | {line}" for i, line in enumerate(lines))


def _format_constraint_for_analyst(c: Dict[str, Any]) -> str:
    """Format a single constraint for the analyst's full view."""
    cid = c.get("id", "?")
    severity = c.get("severity", "MEDIUM")
    instruction = c.get("prompt_instruction", c.get("description", ""))
    failures = c.get("observed_failures", [])

    parts = [f"**[{severity}] {cid}**: {instruction}"]
    if failures:
        for f in failures[:2]:
            qid = f.get("query_id", "?")
            speedup = f.get("speedup", "?")
            parts.append(f"  - Observed: {qid} regressed to {speedup}x")
    return "\n".join(parts)


def _format_example_compact(ex: Dict[str, str]) -> str:
    """Format a gold example compactly: id (speedup) — description."""
    eid = ex.get("id", "?")
    speedup = ex.get("speedup", "?")
    desc = ex.get("description", "")[:80]
    return f"- **{eid}** ({speedup}x) — {desc}"


def _format_example_full(ex: Dict[str, Any]) -> str:
    """Format a tag-matched example with full metadata."""
    eid = ex.get("id", "?")
    speedup = ex.get("verified_speedup", ex.get("speedup", "?"))
    desc = ex.get("description", "")
    principle = ex.get("principle", "")
    when = ex.get("when", "")
    when_not = ex.get("when_not", "")

    parts = [f"### {eid} ({speedup}x)"]
    if desc:
        parts.append(f"**Description:** {desc}")
    if principle:
        parts.append(f"**Principle:** {principle}")
    if when:
        parts.append(f"**When to apply:** {when}")
    if when_not:
        parts.append(f"**When NOT to apply:** {when_not}")

    # Pattern tags (if present from tag-based matching)
    tags = ex.get("pattern_tags", [])
    if tags:
        parts.append(f"**Pattern tags:** {', '.join(tags[:8])}")

    return "\n".join(parts)


def _format_regression_for_analyst(reg: Dict[str, Any]) -> str:
    """Format a regression example for the analyst's full view."""
    eid = reg.get("id", "?")
    query_id = reg.get("query_id", "?")
    speedup = reg.get("verified_speedup", "?")
    transform = reg.get("transform_attempted", "unknown")
    mechanism = reg.get("regression_mechanism", "")
    desc = reg.get("description", "")

    parts = [f"### {eid}: {transform} on {query_id} ({speedup}x)"]
    if desc:
        parts.append(f"**Anti-pattern:** {desc}")
    if mechanism:
        parts.append(f"**Mechanism:** {mechanism}")
    return "\n".join(parts)


def _format_global_knowledge(gk: Dict[str, Any]) -> str:
    """Format GlobalKnowledge principles for analyst (no anti-patterns — those
    are covered by the Regression Examples section with real mechanisms)."""
    lines = []

    principles = gk.get("principles", [])
    if principles:
        ranked = sorted(principles, key=lambda p: p.get("avg_speedup", 0), reverse=True)
        for p in ranked[:5]:
            name = p.get("name", p.get("id", "?"))
            why = p.get("why", "")
            when = p.get("when", "")
            avg = p.get("avg_speedup", 0)
            win_count = len(p.get("queries", []))
            regression_count = len(p.get("regression_queries", []))

            header = f"**{name}** ({avg:.1f}x avg"
            if win_count:
                header += f", {win_count} wins"
            if regression_count:
                header += f", {regression_count} regressions"
            header += ")"
            parts = [header]
            if why:
                parts.append(f"  Why: {why}")
            if when:
                parts.append(f"  When: {when}")
            lines.append("\n".join(parts))

    return "\n".join(lines)


def build_analyst_briefing_prompt(
    query_id: str,
    sql: str,
    explain_plan_text: Optional[str],
    dag: Any,
    costs: Dict[str, Any],
    semantic_intents: Optional[Dict[str, Any]],
    global_knowledge: Optional[Dict[str, Any]],
    matched_examples: List[Dict[str, Any]],
    all_available_examples: List[Dict[str, str]],
    constraints: List[Dict[str, Any]],
    regression_warnings: Optional[List[Dict[str, Any]]],
    dialect: str = "duckdb",
    dialect_version: Optional[str] = None,
) -> str:
    """Build the V2 analyst briefing prompt.

    The analyst receives ALL available information and produces a structured
    briefing for 4 workers. This is the only call that sees the full picture.

    Args:
        query_id: Query identifier (e.g., 'query_74')
        sql: Original SQL query
        explain_plan_text: ASCII EXPLAIN ANALYZE tree (may be None for ~4 queries)
        dag: Parsed DAG from Phase 1
        costs: Per-node cost analysis
        semantic_intents: Pre-computed per-query + per-node intents (may be None)
        global_knowledge: GlobalKnowledge dict with principles + anti_patterns
        matched_examples: Top tag-matched examples (full metadata, typically 16)
        all_available_examples: Full catalog (id + speedup + description)
        constraints: All engine-filtered constraints (full JSON)
        regression_warnings: Tag-matched regression examples (may be None)
        dialect: SQL dialect
        dialect_version: Engine version string (e.g., '1.4.3')

    Returns:
        Complete analyst prompt string (~3000-5000 tokens input)
    """
    lines: list[str] = []

    # ── 1. Role ──────────────────────────────────────────────────────────
    lines.append(
        "You are a senior query optimization architect. Your job is to deeply "
        "analyze a SQL query and produce a structured briefing for 4 specialist "
        "workers who will each write a different optimized version."
    )
    lines.append("")
    lines.append(
        "You are the ONLY call that sees all the data: EXPLAIN plans, DAG costs, "
        "full constraint list, global knowledge, and the complete example catalog. "
        "The workers will only see what YOU put in their briefings. "
        "Your output quality directly determines their success."
    )
    lines.append("")

    # ── 2. SQL with line numbers ─────────────────────────────────────────
    lines.append(f"## Query: {query_id}")
    dialect_str = dialect
    if dialect_version:
        dialect_str += f" v{dialect_version}"
    lines.append(f"## Dialect: {dialect_str}")
    lines.append("")
    clean_sql = _strip_template_comments(sql)
    lines.append("```sql")
    lines.append(_add_line_numbers(clean_sql))
    lines.append("```")
    lines.append("")

    # ── 3. EXPLAIN ANALYZE plan text ─────────────────────────────────────
    if explain_plan_text:
        formatted_plan = format_duckdb_explain_tree(explain_plan_text)
        lines.append("## EXPLAIN ANALYZE Plan")
        lines.append("")
        lines.append("```")
        # Truncate very long plans to ~150 lines
        plan_lines = formatted_plan.split("\n")
        if len(plan_lines) > 150:
            lines.extend(plan_lines[:150])
            lines.append(f"... ({len(plan_lines) - 150} more lines truncated)")
        else:
            lines.append(formatted_plan)
        lines.append("```")
        lines.append("")
    else:
        lines.append("## EXPLAIN ANALYZE Plan")
        lines.append("")
        lines.append(
            "*EXPLAIN plan not available for this query. "
            "Use DAG cost percentages as proxy for bottleneck identification.*"
        )
        lines.append("")

    # ── 4. DAG node cards ────────────────────────────────────────────────
    lines.append("## Query Structure (DAG)")
    lines.append("")

    from ..analyst import _append_dag_analysis
    from ..node_prompter import _build_node_intent_map

    node_intents = _build_node_intent_map(semantic_intents)
    if semantic_intents:
        query_intent = semantic_intents.get("query_intent", "")
        if query_intent and "main_query" not in node_intents:
            node_intents["main_query"] = query_intent

    _append_dag_analysis(lines, dag, costs, dialect=dialect, node_intents=node_intents)
    lines.append("")

    # ── 5. Semantic intents (if available) ───────────────────────────────
    if semantic_intents:
        query_intent = semantic_intents.get("query_intent", "")
        if query_intent:
            lines.append("## Pre-Computed Semantic Intent")
            lines.append("")
            lines.append(f"**Query intent:** {query_intent}")
            lines.append("")
    else:
        lines.append("## Semantic Intent")
        lines.append("")
        lines.append("*Not pre-computed. Infer business intent from the SQL.*")
        lines.append("")

    # ── 6. Tag-matched examples (specific to this query, shown first) ──
    if matched_examples:
        lines.append(f"## Top {len(matched_examples)} Tag-Matched Examples")
        lines.append("")
        for ex in matched_examples:
            lines.append(_format_example_full(ex))
            lines.append("")

    # ── 7. Full catalog (compact) ────────────────────────────────────────
    if all_available_examples:
        lines.append("## Full Example Catalog")
        lines.append("")
        for ex in all_available_examples:
            lines.append(_format_example_compact(ex))
        lines.append("")

    # ── 8. Global optimization principles ────────────────────────────────
    if global_knowledge:
        lines.append("## Optimization Principles (from benchmark history)")
        lines.append("")
        lines.append(_format_global_knowledge(global_knowledge))
        lines.append("")

    # ── 9. Regression examples (the ONLY anti-pattern section) ─────────
    if regression_warnings:
        lines.append("## Regression Examples")
        lines.append("")
        for reg in regression_warnings:
            lines.append(_format_regression_for_analyst(reg))
            lines.append("")

    # ── 10. All constraints (full) ────────────────────────────────────────
    if constraints:
        lines.append(f"## All Constraints ({len(constraints)} total)")
        lines.append("")
        for c in constraints:
            lines.append(_format_constraint_for_analyst(c))
            lines.append("")

    # ── 11. Chain-of-thought instruction ─────────────────────────────────
    lines.append("## Your Task")
    lines.append("")
    lines.append(
        "First, use a `<reasoning>` block for your internal analysis. "
        "This will be stripped before parsing — use it freely for working "
        "through the query structure, bottleneck hypothesis, constraint "
        "relevance, and strategy design."
    )
    lines.append("")

    # ── 12. Output format specification ──────────────────────────────────
    lines.append("Then produce the structured briefing in EXACTLY this format:")
    lines.append("")
    lines.append("```")
    lines.append("=== SHARED BRIEFING ===")
    lines.append("")
    lines.append("SEMANTIC_CONTRACT:")
    lines.append("[Business intent. Intersection/union semantics from JOIN types.")
    lines.append("Aggregation traps (STDDEV_SAMP needs >=2 rows, COUNT DISTINCT vs COUNT, NULL handling).")
    lines.append("Filter dependencies (which filters gate which outputs).")
    lines.append("Output ordering + LIMIT interaction with semantics.]")
    lines.append("")
    lines.append("BOTTLENECK_DIAGNOSIS:")
    lines.append("[Which operation dominates cost and WHY (not just '50% cost').")
    lines.append("Scan-bound vs join-bound vs aggregation-bound.")
    lines.append("Cardinality flow (how many rows at each stage).")
    lines.append("What the optimizer already handles well (don't re-optimize).")
    lines.append("Whether DAG cost percentages are misleading.]")
    lines.append("")
    lines.append("ACTIVE_CONSTRAINTS:")
    lines.append("- [CONSTRAINT_ID]: [Why it applies to this query, 1 line]")
    lines.append("- [CONSTRAINT_ID]: [Why it applies]")
    lines.append("(Select 3-6 constraints from the full list above. Only include")
    lines.append("constraints that are RELEVANT to this specific query.)")
    lines.append("")
    lines.append("REGRESSION_WARNINGS:")
    lines.append("1. [Pattern name] ([observed regression]):")
    lines.append("   CAUSE: [What happened mechanistically]")
    lines.append("   RULE: [Actionable avoidance rule for THIS query]")
    lines.append("(If no regression warnings are relevant, write 'None applicable.')")
    lines.append("")
    lines.append("=== WORKER 1 BRIEFING ===")
    lines.append("")
    lines.append("STRATEGY: [strategy_name]")
    lines.append("TARGET_DAG:")
    lines.append("  [node] -> [node] -> [node]")
    lines.append("NODE_CONTRACTS:")
    lines.append("  [node_name]:")
    lines.append("    FROM: [tables/CTEs]")
    lines.append("    JOIN: [join conditions]")
    lines.append("    WHERE: [filters]")
    lines.append("    GROUP BY: [columns] (if applicable)")
    lines.append("    AGGREGATE: [functions] (if applicable)")
    lines.append("    OUTPUT: [exhaustive column list]")
    lines.append("    CONSUMERS: [downstream nodes]")
    lines.append("EXAMPLES: [ex1], [ex2], [ex3]")
    lines.append("EXAMPLE_REASONING:")
    lines.append("[Why each example's pattern matches THIS query's bottleneck.")
    lines.append("What adaptation is needed.]")
    lines.append("HAZARD_FLAGS:")
    lines.append("- [Specific risk for this approach on this query]")
    lines.append("")
    lines.append("=== WORKER 2 BRIEFING ===")
    lines.append("[Same structure as Worker 1, DIFFERENT strategy]")
    lines.append("")
    lines.append("=== WORKER 3 BRIEFING ===")
    lines.append("[Same structure as Worker 1, DIFFERENT strategy]")
    lines.append("")
    lines.append("=== WORKER 4 BRIEFING ===")
    lines.append("[Same structure as Worker 1, DIFFERENT strategy]")
    lines.append("```")
    lines.append("")

    # ── 13. Strategy diversity guidance ──────────────────────────────────
    lines.append("## Strategy Design Guidelines")
    lines.append("")
    lines.append(
        "Design 4 DIFFERENT strategies that attack the bottleneck from "
        "different angles. Each worker must use a different structural approach. "
        "Do NOT assign the same transform with minor variations."
    )
    lines.append("")
    lines.append(
        "- **Worker 1**: Conservative — proven patterns (pushdown, early_filter, decorrelate)")
    lines.append(
        "- **Worker 2**: Moderate — CTE restructuring (date_cte_isolate, dimension_cte_isolate)")
    lines.append(
        "- **Worker 3**: Aggressive — multi-node restructuring (prefetch, materialize)")
    lines.append(
        "- **Worker 4**: Novel — structural transforms (single_pass_aggregation, or_to_union, intersect_to_exists)")
    lines.append("")
    lines.append(
        "Each worker gets 2-3 examples. No duplicate examples across workers. "
        "Use example IDs from the catalog above."
    )
    lines.append("")
    lines.append(
        "For TARGET_DAG: Define the CTE structure you want the worker to produce. "
        "The worker's job becomes pure SQL generation within your defined structure. "
        "For NODE_CONTRACTS: Be exhaustive with OUTPUT columns — missing columns "
        "cause semantic breaks."
    )

    return "\n".join(lines)
