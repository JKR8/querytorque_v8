"""V2 analyst prompt builder — §I–§VII investigation-method template.

Clean rewrite: investigation-method framing with 6 goals embedded in mission,
engine-specific profiles, 5-step investigation + worker diversity, reference
appendix with documented cases and regression registry.

Supports three modes:
  - swarm: 4 workers, diversity map, full investigation method
  - expert: 1 worker, simplified investigation method
  - oneshot: analyse + produce SQL directly
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────────────────

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


def _strip_template_comments(sql: str) -> str:
    """Strip TPC-DS/DSB template comments from SQL."""
    import re
    lines = sql.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if re.match(r"^--\s*(start|end)\s+query\s+\d+", stripped, re.IGNORECASE):
            continue
        if not cleaned and stripped == "--":
            continue
        cleaned.append(line)
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()
    return "\n".join(cleaned)


def _add_line_numbers(sql: str) -> str:
    """Add line numbers to SQL for analyst reference."""
    lines = sql.split("\n")
    width = len(str(len(lines)))
    return "\n".join(f"{i+1:>{width}} | {line}" for i, line in enumerate(lines))


def _detect_aggregate_functions(dag: Any, costs: Dict[str, Any]) -> List[str]:
    """Detect aggregate functions used in the query from DAG/costs."""
    aggs = set()
    nodes = getattr(dag, "nodes", {}) or {}
    for nid, node in nodes.items():
        sql_text = getattr(node, "sql", "") or ""
        sql_upper = sql_text.upper()
        for fn in ("COUNT", "SUM", "MAX", "MIN", "AVG",
                    "STDDEV_SAMP", "STDDEV", "VARIANCE", "VAR_SAMP",
                    "PERCENTILE_CONT", "CORR", "COVAR_SAMP"):
            if fn + "(" in sql_upper or fn + " (" in sql_upper:
                aggs.add(fn)
    return sorted(aggs)


def _detect_query_features(dag: Any) -> Dict[str, bool]:
    """Detect structural features from DAG for pruning guide."""
    features = {
        "has_left_join": False,
        "has_or_predicate": False,
        "has_group_by": False,
        "has_window": False,
        "has_intersect": False,
        "has_exists": False,
        "has_cte": False,
        "has_correlated_subquery": False,
    }
    nodes = getattr(dag, "nodes", {}) or {}
    for nid, node in nodes.items():
        sql_text = (getattr(node, "sql", "") or "").upper()
        flags = getattr(node, "flags", []) or []
        if "LEFT JOIN" in sql_text or "LEFT OUTER JOIN" in sql_text:
            features["has_left_join"] = True
        if " OR " in sql_text:
            features["has_or_predicate"] = True
        if "GROUP BY" in sql_text:
            features["has_group_by"] = True
        if "OVER(" in sql_text or "OVER (" in sql_text:
            features["has_window"] = True
        if "INTERSECT" in sql_text:
            features["has_intersect"] = True
        if "EXISTS" in sql_text:
            features["has_exists"] = True
        if nid != "main_query" and getattr(node, "node_type", "") == "cte":
            features["has_cte"] = True
        if "CORRELATED" in " ".join(str(f) for f in flags).upper():
            features["has_correlated_subquery"] = True
    return features


# ──────────────────────────────────────────────────────────────────────
# PostgreSQL EXPLAIN formatting (imported from V1, needed by swarm_snipe)
# ──────────────────────────────────────────────────────────────────────

def _render_pg_node(
    node: Dict[str, Any],
    depth: int,
    lines: list[str],
    max_depth: int = 30,
    is_estimate: bool = False,
) -> None:
    """Recursively render a PG EXPLAIN JSON node as indented text."""
    if depth > max_depth:
        return

    ntype = node.get("Node Type", "???")

    # Prefer ANALYZE actuals; fall back to planner estimates
    if "Actual Rows" in node:
        rows = node.get("Actual Rows", 0) or 0
        loops = node.get("Actual Loops", 1) or 1
        time_ms = node.get("Actual Total Time", 0) or 0
        is_estimate = False
    else:
        rows = node.get("Plan Rows", 0) or 0
        loops = 1
        time_ms = node.get("Total Cost", 0) or 0  # cost units, not ms
        is_estimate = True

    indent = "  " * depth

    # Build the main line
    parts = [f"{indent}-> {ntype}"]

    # Relation/index name
    rel = node.get("Relation Name") or node.get("Index Name")
    if rel:
        parts.append(f"on {rel}")
    alias = node.get("Alias")
    if alias and alias != rel:
        parts.append(f"{alias}")

    # Join type
    jtype = node.get("Join Type")
    if jtype:
        parts[0] = f"{indent}-> {ntype} {jtype}"

    # CTE name
    cte = node.get("CTE Name")
    if cte:
        parts.append(f"(CTE: {cte})")

    line = " ".join(parts)

    # Stats — label clearly when using planner estimates vs actual measurements
    if is_estimate:
        stats = f"(est_rows={_fmt_count(rows)} cost={time_ms:.0f})"
    else:
        stats = f"(rows={_fmt_count(rows)} loops={loops} time={time_ms:.1f}ms)"
    lines.append(f"{line}  {stats}")

    # Important detail lines
    # Sort info
    sort_method = node.get("Sort Method")
    if sort_method:
        space = node.get("Sort Space Used", 0)
        stype = node.get("Sort Space Type", "")
        lines.append(f"{indent}   Sort Method: {sort_method}  Space: {space}kB ({stype})")

    # Hash info
    batches = node.get("Hash Batches")
    if batches and batches > 1:
        mem = node.get("Peak Memory Usage", 0)
        lines.append(f"{indent}   Batches: {batches}  Memory: {mem}kB")

    # Workers
    w_planned = node.get("Workers Planned")
    w_launched = node.get("Workers Launched")
    if w_planned is not None:
        lines.append(f"{indent}   Workers: {w_launched}/{w_planned} launched")

    # Filter/conditions
    for key in ("Filter", "Index Cond", "Hash Cond", "Merge Cond", "Join Filter",
                "Recheck Cond", "One-Time Filter"):
        val = node.get(key)
        if val:
            vstr = str(val)
            if len(vstr) > 100:
                vstr = vstr[:97] + "..."
            lines.append(f"{indent}   {key}: {vstr}")

    # Rows removed by filter
    removed = node.get("Rows Removed by Filter")
    if removed and removed > 0:
        lines.append(f"{indent}   Rows Removed by Filter: {_fmt_count(removed)}")

    # I/O stats (compact)
    shared_read = node.get("Shared Read Blocks", 0)
    shared_hit = node.get("Shared Hit Blocks", 0)
    temp_read = node.get("Temp Read Blocks", 0)
    temp_write = node.get("Temp Written Blocks", 0)
    if shared_read > 1000 or temp_read > 0:
        io_parts = []
        if shared_hit:
            io_parts.append(f"hit={_fmt_count(shared_hit)}")
        if shared_read:
            io_parts.append(f"read={_fmt_count(shared_read)}")
        if temp_read:
            io_parts.append(f"temp_r={_fmt_count(temp_read)}")
        if temp_write:
            io_parts.append(f"temp_w={_fmt_count(temp_write)}")
        lines.append(f"{indent}   Buffers: {' '.join(io_parts)}")

    # Subplan name (for CTEs)
    subplan = node.get("Subplan Name")
    if subplan:
        lines[len(lines) - len(lines)] = lines[len(lines) - len(lines)]  # no-op
        # Already shown via CTE name above

    # Recurse into children
    for child in node.get("Plans", []):
        _render_pg_node(child, depth + 1, lines, max_depth, is_estimate)


def format_pg_explain_tree(plan_json: Any) -> str:
    """Convert PG EXPLAIN (FORMAT JSON) plan to a readable ASCII tree.

    Args:
        plan_json: The plan_json field from cached explains (list or dict).
            Standard PG JSON format: [{"Plan": {...}, "Planning Time": ..., ...}]

    Returns:
        Readable ASCII tree string showing operators, timing, and key details.
    """
    if not plan_json:
        return ""

    # Handle both list wrapper and bare dict
    if isinstance(plan_json, list):
        if not plan_json:
            return ""
        top = plan_json[0]
    elif isinstance(plan_json, dict):
        top = plan_json
    else:
        return str(plan_json)

    root = top.get("Plan")
    if not root:
        return str(plan_json)

    planning_ms = top.get("Planning Time", 0)
    exec_ms = top.get("Execution Time", 0)

    lines: list[str] = []

    # Detect whether this is EXPLAIN ANALYZE (has actuals) or plain EXPLAIN (estimates only)
    has_actuals = "Actual Rows" in root
    is_estimate = not has_actuals

    # Header with total times
    if has_actuals:
        total_ms = root.get("Actual Total Time", 0)
        if total_ms:
            lines.append(f"Total execution time: {total_ms:.1f}ms")
    else:
        total_cost = root.get("Total Cost", 0)
        lines.append(f"NOTE: EXPLAIN only (no ANALYZE) — rows and costs are planner ESTIMATES, not measurements.")
        lines.append(f"Total estimated cost: {total_cost:.0f}")
    if planning_ms:
        lines.append(f"Planning time: {planning_ms:.1f}ms")
    if lines:
        lines.append("")

    # Render the tree
    _render_pg_node(root, depth=0, lines=lines, is_estimate=is_estimate)

    # JIT info
    jit = top.get("JIT") or root.get("JIT")
    if jit:
        lines.append("")
        funcs = jit.get("Functions", 0)
        gen_ms = jit.get("Generation", {}).get("Timing", 0)
        opt_ms = jit.get("Optimization", {}).get("Timing", 0)
        emit_ms = jit.get("Emission", {}).get("Timing", 0)
        total_jit = gen_ms + opt_ms + emit_ms
        lines.append(f"JIT: {funcs} functions, total={total_jit:.1f}ms "
                     f"(gen={gen_ms:.1f} opt={opt_ms:.1f} emit={emit_ms:.1f})")

    # Triggers
    triggers = top.get("Triggers", [])
    if triggers:
        lines.append("")
        for t in triggers:
            lines.append(f"Trigger: {t.get('Trigger Name', '?')} "
                         f"calls={t.get('Calls', 0)} time={t.get('Time', 0):.1f}ms")

    return "\n".join(lines)


def _format_regression_for_analyst(reg: Dict[str, Any]) -> str:
    """Format a regression example for the analyst's full view."""
    eid = reg.get("id", "?")
    speedup = str(reg.get("verified_speedup", "?")).rstrip("x")
    transform = reg.get("transform_attempted", "unknown")
    mechanism = reg.get("regression_mechanism", "")
    desc = reg.get("description", "")

    parts = [f"### {eid}: {transform} ({speedup}x)"]
    if desc:
        parts.append(f"**Anti-pattern:** {desc}")
    if mechanism:
        parts.append(f"**Mechanism:** {mechanism}")
    return "\n".join(parts)


# ──────────────────────────────────────────────────────────────────────
# DuckDB EXPLAIN formatting
# ──────────────────────────────────────────────────────────────────────

# Operators that are just internal plumbing — collapse/skip them
_SKIP_OPERATORS = {"PROJECTION", "RESULT_COLLECTOR", "COLUMN_DATA_SCAN"}

_EXTRA_INFO_KEYS = {
    "HASH_AGGREGATE": ["Distinct Aggregates"],
    "SIMPLE_AGGREGATE": ["Distinct Aggregates"],
    "WINDOW": ["Function", "Order By"],
    "HASH_JOIN": ["Hash Condition"],
    "NESTED_LOOP_JOIN": ["Join Condition"],
    "MERGE_JOIN": ["Merge Condition"],
}


def _fmt_extra_value(key: str, val: Any) -> str:
    """Format an extra_info value for readability."""
    if isinstance(val, list):
        if not val:
            return ""
        return ", ".join(str(v) for v in val)
    return str(val)


def _collect_total_timing(node: Dict[str, Any]) -> float:
    """Sum all operator_timing values in the tree (exclusive/self-time)."""
    total = node.get("operator_timing", 0) or 0
    for child in node.get("children", []):
        total += _collect_total_timing(child)
    return total


def _render_node(
    node: Dict[str, Any],
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
    import json

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


# ═══════════════════════════════════════════════════════════════════════
# Section Builders — each returns a string for its section
# ═══════════════════════════════════════════════════════════════════════

def section_role(mode: str) -> str:
    """§I. ROLE — senior query optimization architect + 6 principles."""
    lines = ["## §I. ROLE", ""]
    lines.append(
        "You are a senior query optimization architect. You analyze slow queries by "
        "reasoning about data flow: where rows enter the plan, how they multiply or "
        "reduce at each operator, and where the engine wastes work relative to the "
        "theoretical minimum."
    )
    lines.append("")
    lines.append("Your diagnostic lens is six principles. Every slow query violates at least one:")
    lines.append("")
    lines.append("1. **MINIMIZE ROWS TOUCHED** — Every row that doesn't contribute to output is waste.")
    lines.append("2. **SMALLEST SET FIRST** — Most selective filter applied earliest. Selectivity compounds.")
    lines.append("3. **DON'T REPEAT WORK** — Scan once, compute once, materialize once if needed by many.")
    lines.append("4. **SETS OVER LOOPS** — Set operations parallelize. Row-by-row re-execution doesn't.")
    lines.append("5. **ARM THE OPTIMIZER** — Restructure so it has full intelligence. Don't force plans.")
    lines.append("6. **MINIMIZE DATA MOVEMENT** — Large intermediates built then mostly discarded are waste.")
    lines.append("")
    lines.append(
        "Your primary asset is a library of **gold examples** — proven before/after SQL "
        "rewrites with measured speedups gathered from hundreds of benchmark runs. "
        "Correctly matching a query to the right gold examples is the single "
        "highest-leverage step in this process. Workers receive the full before/after "
        "SQL for the examples you assign and use them as structural templates. The "
        "diagnosis tells you what's wrong; the examples are the edge — they tell the "
        "workers exactly how to fix it."
    )

    if mode == "swarm":
        lines.append("")
        lines.append(
            "You produce structured briefings for 4 specialist workers. Each worker "
            "designs a new query map showing how their restructuring fixes the "
            "identified problems, THEN writes the SQL to implement that map. They see "
            "ONLY what you provide."
        )
    elif mode == "expert":
        lines.append("")
        lines.append(
            "You produce a structured briefing for a single specialist worker who "
            "designs a new query map and writes the SQL. The worker sees ONLY "
            "what you provide."
        )
    elif mode == "oneshot":
        lines.append("")
        lines.append(
            "You analyze the query, determine the single best optimization "
            "strategy, and produce the optimized SQL directly."
        )
    return "\n".join(lines)


def section_the_case(
    query_id: str,
    sql: str,
    explain_plan_text: Optional[str],
    dag: Any,
    costs: Dict[str, Any],
    semantic_intents: Optional[Dict[str, Any]],
    dialect: str,
    dialect_version: Optional[str],
    qerror_analysis: Optional[Any],
    iteration_history: Optional[Dict[str, Any]],
) -> str:
    """§II. THE CASE — Original SQL, Current Execution Plan, Query Map, Estimation Errors."""
    lines = ["## §II. THE CASE", ""]

    # A. Original SQL
    dialect_str = dialect
    if dialect_version:
        dialect_str += f" v{dialect_version}"
    lines.append(f"### A. Original SQL: {query_id} ({dialect_str})")
    lines.append("")
    clean_sql = _strip_template_comments(sql)
    lines.append("```sql")
    lines.append(clean_sql)
    lines.append("```")
    lines.append("")

    # B. Current Execution Plan (EXPLAIN ANALYZE)
    if explain_plan_text:
        is_estimate = "est_rows=" in explain_plan_text or "EXPLAIN only" in explain_plan_text
        if is_estimate:
            lines.append("### B. Current Execution Plan (EXPLAIN — planner estimates)")
        else:
            lines.append("### B. Current Execution Plan (EXPLAIN ANALYZE)")
        lines.append("")
        lines.append("```")
        plan_lines = explain_plan_text.split("\n")
        if len(plan_lines) > 150:
            lines.extend(plan_lines[:150])
            lines.append(f"... ({len(plan_lines) - 150} more lines truncated)")
        else:
            lines.append(explain_plan_text)
        lines.append("```")
        lines.append("")
        if dialect == "duckdb":
            lines.append(
                "DuckDB times are operator-exclusive (children excluded). "
                "EXPLAIN is ground truth."
            )
        elif is_estimate:
            lines.append(
                "Estimate-only plan — use directionally, cross-check against "
                "schema knowledge."
            )
        else:
            lines.append("EXPLAIN ANALYZE timings are ground truth.")
        lines.append("")
    else:
        lines.append("### B. Current Execution Plan")
        lines.append("")
        lines.append("*EXPLAIN plan not available. Use logical-tree cost % as proxy.*")
        lines.append("")

    # C. Query Map (Semantic Structure with filter/join ratios)
    from ..logic_tree import build_logic_tree
    from ..prompter import _build_node_intent_map

    node_intents = _build_node_intent_map(semantic_intents)
    if semantic_intents:
        query_intent = semantic_intents.get("query_intent", "")
        if query_intent and "main_query" not in node_intents:
            node_intents["main_query"] = query_intent

    lines.append("### C. Query Map")
    lines.append("")
    lines.append("The semantic structure with filter ratios, join ratios, and join directions. "
                 "Use this to deduce the optimal path.")
    lines.append("")
    tree = build_logic_tree(sql, dag, costs, dialect, node_intents)
    lines.append("```")
    lines.append(tree)
    lines.append("```")
    lines.append("")

    # Intent line
    if semantic_intents:
        query_intent = semantic_intents.get("query_intent", "")
        if query_intent:
            lines.append(f"**Intent:** {query_intent}")
            lines.append("")

    # D. Estimation Errors (Q-Error routing)
    if qerror_analysis is not None:
        from ..qerror import format_qerror_for_prompt
        qerror_text = format_qerror_for_prompt(qerror_analysis)
        if qerror_text:
            lines.append("### D. Estimation Errors")
            lines.append("")
            lines.append(qerror_text)
            lines.append("")

    # Condensed per-node detail cards
    from ..analyst import _append_dag_analysis
    lines.append("### Node Details")
    lines.append("")
    _append_dag_analysis(lines, dag, costs, dialect=dialect, node_intents=node_intents)
    lines.append("")

    # Iteration History
    if iteration_history and iteration_history.get("attempts"):
        lines.append("### E. Previous Optimization Attempts")
        lines.append("")
        lines.append(
            "Do NOT repeat strategies that regressed or failed. "
            "Build on what worked; avoid what didn't."
        )
        lines.append("")
        for i, attempt in enumerate(iteration_history["attempts"]):
            status = attempt.get("status", "unknown")
            speedup = attempt.get("speedup", 0)
            transforms = attempt.get("transforms", [])
            t_str = ", ".join(transforms) if transforms else "unknown"
            if status in ("error", "ERROR"):
                error = attempt.get("error", "")
                lines.append(f"- Attempt {i+1}: **{t_str}** -> ERROR: {error}")
            elif speedup < 0.95:
                lines.append(f"- Attempt {i+1}: **{t_str}** -> REGRESSION ({speedup:.2f}x)")
            elif speedup >= 1.10:
                lines.append(f"- Attempt {i+1}: **{t_str}** -> WIN ({speedup:.2f}x)")
            else:
                lines.append(f"- Attempt {i+1}: **{t_str}** -> NEUTRAL ({speedup:.2f}x)")
            failure_analysis = attempt.get("failure_analysis", "")
            if failure_analysis and status not in ("WIN", "IMPROVED"):
                preview = failure_analysis[:200] + "..." if len(failure_analysis) > 200 else failure_analysis
                lines.append(f"  Analysis: {preview}")
        lines.append("")

    return "\n".join(lines)


def section_this_engine(
    engine_profile: Optional[Dict[str, Any]],
    exploit_algorithm_text: Optional[str],
    dialect: str,
    resource_envelope: Optional[str],
    plan_scanner_text: Optional[str] = None,
) -> str:
    """§III. THIS ENGINE — tabular strengths + blind spots."""
    lines = ["## §III. THIS ENGINE", ""]

    engine_names = {"duckdb": "DuckDB", "postgres": "PostgreSQL",
                    "postgresql": "PostgreSQL", "snowflake": "Snowflake"}
    engine_name = engine_names.get(dialect, dialect)

    if exploit_algorithm_text:
        lines.append(f"### {engine_name}")
        lines.append("")
        lines.append(
            "Evidence-based exploit algorithm. Use DETECT rules to match "
            "structural features, then follow EXPLOIT_STEPS."
        )
        lines.append("")
        lines.append(exploit_algorithm_text)
        lines.append("")
        # Plan-Space Scanner (PG only) — append even with exploit algorithm
        if plan_scanner_text:
            lines.append("### Plan-Space Scanner Intelligence")
            lines.append("")
            lines.append(plan_scanner_text)
            lines.append("")
        # Resource Envelope (PG only)
        if dialect in ("postgresql", "postgres") and resource_envelope:
            lines.append("### System Resource Envelope")
            lines.append("")
            lines.append(resource_envelope)
            lines.append("")
        return "\n".join(lines)

    if not engine_profile:
        lines.append(f"### {engine_name}")
        lines.append("")
        lines.append("*No engine profile available.*")
        return "\n".join(lines)

    briefing_note = engine_profile.get("briefing_note", "")

    lines.append(f"### {engine_name}")
    lines.append("")
    if briefing_note:
        lines.append(f"*{briefing_note}*")
        lines.append("")

    # Strengths table: "Handles Well (don't rewrite)"
    strengths = engine_profile.get("strengths", [])
    if strengths:
        lines.append("**Handles Well (don't rewrite)**")
        lines.append("")
        lines.append("| Capability | Implication |")
        lines.append("|---|---|")
        for s in strengths:
            cap = s.get("summary", s.get("id", ""))
            imp = s.get("implication", s.get("field_note", ""))
            lines.append(f"| {cap} | {imp} |")
        lines.append("")

    # Blind spots table: "Blind Spots (your opportunity)"
    gaps = engine_profile.get("gaps", [])
    if gaps:
        lines.append("**Blind Spots (your opportunity)**")
        lines.append("")
        lines.append("| Blind Spot | Consequence |")
        lines.append("|---|---|")
        for g in gaps:
            blind_spot = g.get("id", "")
            consequence = g.get("why", g.get("opportunity", g.get("what", "")))
            lines.append(f"| {blind_spot} | {consequence} |")
        lines.append("")

    # Plan-Space Scanner (PG only)
    if plan_scanner_text:
        lines.append("### Plan-Space Scanner Intelligence")
        lines.append("")
        lines.append(plan_scanner_text)
        lines.append("")

    # Resource Envelope (PG only)
    if dialect in ("postgresql", "postgres") and resource_envelope:
        lines.append("### System Resource Envelope")
        lines.append("")
        lines.append(resource_envelope)
        lines.append("")

    return "\n".join(lines)


def section_constraints(
    constraints: Optional[List[Dict[str, Any]]],
    dag: Any,
    costs: Dict[str, Any],
) -> str:
    """§IV. CONSTRAINTS — 4 non-negotiable rules + aggregation note."""
    lines = ["## §IV. CONSTRAINTS", ""]

    # 4 core constraints
    correctness_ids = (
        "LITERAL_PRESERVATION", "SEMANTIC_EQUIVALENCE",
        "COMPLETE_OUTPUT", "CTE_COLUMN_COMPLETENESS",
    )
    correctness_constraints = [
        c for c in (constraints or [])
        if c.get("id") in correctness_ids
    ]
    if correctness_constraints:
        for c in correctness_constraints:
            cid = c.get("id", "?")
            instruction = c.get("prompt_instruction", c.get("description", ""))
            lines.append(f"- **{cid}**: {instruction}")
        lines.append("")
    else:
        # Fallback: always list the 4 constraints
        lines.append("- **COMPLETE_OUTPUT**: All original SELECT columns, aliases, and order preserved exactly.")
        lines.append("- **LITERAL_PRESERVATION**: All literals copied exactly. `d_year = 2001` stays `2001`.")
        lines.append("- **SEMANTIC_EQUIVALENCE**: Same rows, columns, ordering. Prime directive.")
        lines.append("- **CTE_COLUMN_COMPLETENESS**: Every CTE SELECTs all columns its consumers reference.")
        lines.append("")

    # Aggregation note — detect aggregate functions and classify safety
    aggs = _detect_aggregate_functions(dag, costs)
    safe_aggs = {"COUNT", "SUM", "MAX", "MIN"}
    unsafe_aggs = {"STDDEV_SAMP", "STDDEV", "VARIANCE", "VAR_SAMP",
                   "PERCENTILE_CONT", "CORR", "COVAR_SAMP", "AVG"}
    found_safe = [a for a in aggs if a in safe_aggs]
    found_unsafe = [a for a in aggs if a in unsafe_aggs]

    if aggs:
        safe_str = ", ".join(found_safe) if found_safe else "none"
        if found_unsafe:
            unsafe_str = ", ".join(found_unsafe)
            lines.append(
                f"**Aggregation:** This query uses {safe_str} (safe) and "
                f"{unsafe_str} (grouping-sensitive). Verify aggregation equivalence "
                f"for any restructuring."
            )
        else:
            lines.append(
                f"**Aggregation:** This query uses {safe_str} — all safe. "
                f"No STDDEV/VARIANCE traps."
            )
    else:
        lines.append("**Aggregation:** No aggregate functions detected.")

    return "\n".join(lines)


def section_investigate(
    mode: str,
    dag: Any,
    qerror_analysis: Optional[Any],
    explain_plan_text: Optional[str],
) -> str:
    """§V. INVESTIGATE — 7-step reasoning + Worker Diversity."""
    lines = ["## §V. INVESTIGATE", ""]
    lines.append("Work in `<reasoning>`. Follow the expert tuning process:")
    lines.append("")

    # Step 1: Analyze the Current Plan
    lines.append(
        "**Step 1: Analyze the Current Plan.** Read the cost spine and EXPLAIN in §II.B. "
        "Identify the red flags: where is time going? What's the running rowcount at each "
        "stage? Where does it fail to decrease?"
    )
    lines.append("")

    # Step 2: Read the Map
    lines.append(
        "**Step 2: Read the Map.** Use the query map (§II.C) to understand the data shape. "
        "Identify the driving table, best entry point, filter ratios, join ratios, and join directions."
    )
    lines.append("")

    # Step 3: Deduce the Optimal Path
    lines.append(
        "**Step 3: Deduce the Optimal Path.** From the map, work out the ideal join order:"
    )
    lines.append("")
    lines.append("- Start from the best entry point (most selective filter)")
    lines.append("- Follow reducing joins first (downward/semi)")
    lines.append("- Pick up filters early to shrink the running rowcount at every step")
    lines.append("- Defer expanding joins and pure attribute lookups until last")
    lines.append("- Compute the running rowcount at each step of your optimal path")
    lines.append("")

    # Step 4: Diagnose the Gap
    lines.append(
        "**Step 4: Diagnose the Gap.** Compare your optimal path (Step 3) to the actual "
        "plan (Step 1). For each divergence:"
    )
    lines.append("")
    lines.append("- Name the violated goal (§I)")
    lines.append(
        "- Check if an engine blind spot from §III explains it. If yes, name it. "
        "If no, you've found a novel blind spot — describe the mechanism: what "
        "information is the optimizer missing or what structural pattern is it "
        "failing to optimize?"
    )
    lines.append("- Quantify: how many excess rows flow because of this divergence?")
    lines.append("")
    lines.append(
        "This diagnosis is complete and actionable on its own. Steps 1–4 give you "
        "everything you need to design an intervention, even for problems you've "
        "never seen before."
    )
    lines.append("")

    # Step 5: Match Gold Examples
    lines.append(
        "**Step 5: Match Gold Examples.** This is the highest-leverage step. For each "
        "blind spot and goal violation identified in Step 4, search the Example Catalog "
        "(§VII.B) for gold examples with matching query structure."
    )
    lines.append("")
    lines.append(
        "- **Match found**: The matching examples become the primary basis for worker "
        "strategies. Assign them to workers with APPLY/IGNORE/ADAPT guidance. The gold "
        "example's before/after SQL is a structural template — the worker adapts it, "
        "not invents from scratch."
    )
    lines.append(
        "- **No match**: Design the intervention from your diagnosis. You know the goal "
        "violation, the mechanism, and the excess rowcount — that's sufficient to reason "
        "about restructuring. Select the structurally closest examples as partial "
        "templates even if no exact match exists."
    )
    lines.append("")

    # Step 6: Select Examples Per Worker (NEW)
    if mode == "swarm":
        lines.append(
            "**Step 6: Select Examples Per Worker.** For each of the 4 strategies, "
            "select 1–3 examples from the catalog:"
        )
    elif mode == "expert":
        lines.append(
            "**Step 6: Select Examples.** For your strategy, "
            "select 1–3 examples from the catalog:"
        )
    else:
        lines.append(
            "**Step 6: Select Examples.** For your strategy, "
            "select 1–3 examples from the catalog:"
        )
    lines.append("")
    lines.append("*Matching criteria* (in priority order):")
    lines.append(
        "1. **Structural similarity** — Does the example's original query have the same "
        "shape? (same join pattern, same subquery type, same fact/dim relationship). "
        "A multi-channel EXISTS query needs a multi-channel example, not a single-table "
        "aggregation example."
    )
    lines.append(
        "2. **Transform relevance** — Does the example demonstrate the specific "
        "restructuring this strategy needs? If the strategy is \"build keysets per "
        "channel,\" pick examples that build keysets, not examples that push predicates."
    )
    lines.append(
        "3. **Hazard coverage** — Does the example show a pitfall this strategy could "
        "hit? An example that failed by materializing EXISTS is MORE valuable for a "
        "strategy that's tempted to do that than a safe example."
    )
    lines.append("")
    lines.append(
        "*Adaptation guidance* — For each assigned example, you MUST specify:"
    )
    lines.append(
        "- **APPLY**: Which structural pattern from the example maps to this query "
        "(e.g., \"the date_dim CTE pattern — isolate qualifying dates first, then join "
        "to fact\")"
    )
    lines.append(
        "- **IGNORE**: Which parts of the example don't apply and WHY (e.g., \"ignore "
        "the ROLLUP handling — this query has no ROLLUP\"). Without this, workers copy "
        "irrelevant complexity."
    )
    lines.append(
        "- **ADAPT**: What's different between the example's query and this query that "
        "requires modification (e.g., \"example has 2 channels, this query has 3 — "
        "extend the pattern but don't exceed 2 CTE chains\")"
    )
    lines.append("")
    lines.append("*Anti-patterns*:")
    lines.append(
        "- Don't assign an example just because it matches the same blind spot if the "
        "query structure is fundamentally different"
    )
    lines.append(
        "- Don't pad with 3 examples when 1 is a strong match — irrelevant examples "
        "dilute attention"
    )
    lines.append(
        "- Don't assign examples that demonstrate transforms the strategy ISN'T using"
    )
    lines.append("")

    # Step 7: Design Strategies
    if mode == "swarm":
        lines.append(
            "**Step 7: Design Four Strategies.** Each strategy must include a NEW QUERY MAP "
            "showing the restructured data flow before specifying any SQL. The map is the "
            "design document — it proves the restructuring produces monotonically decreasing "
            "rowcounts and addresses the diagnosed goal violations."
        )
        lines.append("")
        lines.append("Selection rules:")
        lines.append(
            "- If the EXPLAIN shows the optimizer already handles something "
            "(e.g., EXISTS → semi-join), don't re-do it"
        )
        lines.append(
            "- Verify structural prerequisites before assigning transforms "
            "(no decorrelation if there's no correlated subquery)"
        )
        lines.append(
            "- Strategies may compose 2–3 transforms — compound strategies produce "
            "the biggest wins and biggest regressions"
        )
        lines.append("")

        # Worker Diversity
        lines.append("### Worker Diversity")
        lines.append("")
        lines.append("### Transform Families")
        lines.append("")
        lines.append(
            "Six families of structural transformation, classified by the optimizer blind "
            "spot they exploit (not by syntactic change). Each family has a measured "
            "win:regression ratio from empirical benchmarks:"
        )
        lines.append("")
        lines.append(
            "**Family A — SCAN REDUCTION** (filter early, scan less)"
        )
        lines.append(
            "Transforms: date_cte_isolate, dimension_cte_isolate, early_filter, "
            "pushdown, multi_date_range_cte, prefetch_fact_join"
        )
        lines.append(
            "Mechanism: Pre-filter dimension tables into CTEs so fact table joins probe "
            "tiny hash tables. Move predicates earlier in the plan."
        )
        lines.append(
            "Blind spot: CROSS_CTE_PREDICATE_BLINDNESS — optimizer cannot push predicates "
            "backward from outer query into CTE definitions."
        )
        lines.append("Win ratio: 1:1 (high volume, medium risk). ~35% of all DuckDB wins.")
        lines.append("")
        lines.append(
            "**Family B — JOIN RESTRUCTURING** (arm the optimizer)"
        )
        lines.append(
            "Transforms: inner_join_conversion, multi_dimension_prefetch, "
            "pg_date_cte_explicit_join, pg_explicit_join_materialized"
        )
        lines.append(
            "Mechanism: Give the planner explicit join structure and better cardinality "
            "estimates. Convert LEFT→INNER when NULLs are eliminated downstream."
        )
        lines.append(
            "Blind spot: Join ordering + LEFT→INNER inference. On PG: COMMA_JOIN_WEAKNESS."
        )
        lines.append("Win ratio: 1:1 (safe on DuckDB, most reliable on PG).")
        lines.append("")
        lines.append(
            "**Family C — AGGREGATION REWRITE** (minimize rows touched)"
        )
        lines.append(
            "Transforms: aggregate_pushdown, deferred_window_aggregation"
        )
        lines.append(
            "Mechanism: Push GROUP BY below joins when aggregation keys align with join "
            "keys. Defer window functions to after filtering joins."
        )
        lines.append(
            "Blind spot: AGGREGATE_BELOW_JOIN_BLINDNESS — optimizer cannot push GROUP BY "
            "below joins."
        )
        lines.append(
            "Win ratio: INFINITY (ZERO regressions). aggregate_pushdown produced 42.90x "
            "(largest single win). Always safe."
        )
        lines.append("")
        lines.append(
            "**Family D — SCAN CONSOLIDATION** (don't repeat work)"
        )
        lines.append(
            "Transforms: single_pass_aggregation, channel_bitmap_aggregation, "
            "self_join_decomposition, union_cte_split, self_join_pivot"
        )
        lines.append(
            "Mechanism: Merge N separate scans of the same table into 1 pass using CASE "
            "expressions, shared CTEs, or pivoted aggregation."
        )
        lines.append(
            "Blind spot: REDUNDANT_SCAN_ELIMINATION — optimizer cannot detect when the "
            "same table is scanned N times with similar filters across subquery boundaries."
        )
        lines.append(
            "Win ratio: INFINITY (ZERO standalone regressions). single_pass_aggregation "
            "2.72x avg. SAFEST family."
        )
        lines.append("")
        lines.append(
            "**Family E — SUBQUERY ELIMINATION** (sets over loops)"
        )
        lines.append(
            "Transforms: decorrelate, inline_decorrelate_materialized, "
            "intersect_to_exists, set_operation_materialization"
        )
        lines.append(
            "Mechanism: Convert correlated subqueries to CTEs with GROUP BY + JOIN. "
            "Replace INTERSECT/EXCEPT with EXISTS semi-joins."
        )
        lines.append(
            "Blind spot: CORRELATED_SUBQUERY_PARALYSIS — optimizer cannot decorrelate "
            "complex aggregate correlations. INTERSECT_MATERIALIZATION."
        )
        lines.append(
            "Win ratio: 1.7:1 (medium-safe). HIGHEST IMPACT on PG (100-8000x timeout "
            "rescues). intersect_to_exists has zero regressions."
        )
        lines.append("")
        lines.append(
            "**Family F — PREDICATE RESTRUCTURE** (arm the optimizer — predicates)"
        )
        lines.append(
            "Transforms: or_to_union"
        )
        lines.append(
            "Mechanism: Split OR conditions on DIFFERENT columns into UNION ALL branches "
            "so optimizer can use different access paths per branch."
        )
        lines.append(
            "Blind spot: CROSS_COLUMN_OR_DECOMPOSITION — optimizer cannot decompose OR "
            "conditions spanning different columns into targeted scans."
        )
        lines.append(
            "Win ratio: 0.4:1 (RISKY — regresses more often than it wins). Max 3 UNION "
            "branches. Cross-column ONLY. NEVER split same-column OR. NEVER on PG."
        )
        lines.append(
            "EXTREME VARIANCE: same transform produced 6.28x on Q88 and 0.23x on Q13."
        )
        lines.append("")
        lines.append("### Worker Roles")
        lines.append("")
        lines.append(
            "Workers are differentiated by WHICH families they attack, not by how "
            "aggressively they attack them."
        )
        lines.append("")
        lines.append("**W1 — Proven compound** (highest expected win rate)")
        lines.append(
            "Apply the best 2 transforms from different families, chosen from gold examples "
            "with strong measured speedups. This is NOT a conservative worker — it's the "
            "highest-expectation play. Prefer families C/D (zero regressions) as primary "
            "when the query structure supports them."
        )
        lines.append("")
        lines.append("**W2 — Structural alternative** (different angle of attack)")
        lines.append(
            "Primary family MUST differ from W1's primary family. If W1 leads with Scan "
            "Reduction (A), W2 leads with Join Restructuring (B) or Subquery Elimination "
            "(E). Guarantees the system explores a genuinely different structural approach."
        )
        lines.append("")
        lines.append("**W3 — Aggressive compound** (highest ceiling, highest variance)")
        lines.append(
            "Compose 3+ transforms across multiple families. This is where the extreme "
            "outliers live (8044x, 359x on PG). Higher risk of regression, but captures wins "
            "that simpler strategies can't reach. Must include at least one family not in "
            "W1's primary."
        )
        lines.append("")
        lines.append("**W4 — Novel / orthogonal** (exploration mandate)")
        lines.append(
            "MUST use a family not covered by W1–W3, OR attempt a novel technique not in "
            "the gold library. W4 priority:"
        )
        lines.append(
            "  1. PREFERRED: Attempt a novel technique — new discoveries expand the library"
        )
        lines.append(
            "  2. MEDIUM: Target uncovered family (if C or D uncovered, they have HIGHER "
            "priority — zero regressions)"
        )
        lines.append(
            "  3. LOWEST: If F (Predicate Restructure) is uncovered, W4 targets it WITH "
            "safeguard rules (max 3 branches, cross-column only)"
        )
        lines.append("")
        lines.append("### Family Coverage Rule")
        lines.append("")
        lines.append(
            "**Across W1–W4, at least 3 of the 6 transform families must be represented as "
            "a primary or secondary family.** No two workers may share the same primary "
            "family unless the query structure only supports 2 applicable families (rare — "
            "document why in DIVERSITY_MAP)."
        )
        lines.append("")
        lines.append("Verify coverage before finalizing:")
        lines.append("```")
        lines.append("Family A (Scan Reduction):         covered by W_?")
        lines.append("Family B (Join Restructuring):     covered by W_?")
        lines.append("Family C (Aggregation Rewrite):    covered by W_?")
        lines.append("Family D (Scan Consolidation):     covered by W_?")
        lines.append("Family E (Subquery Elimination):   covered by W_?")
        lines.append("Family F (Predicate Restructure):  covered by W_?")
        lines.append("Uncovered families:                [list → W4 should target these]")
        lines.append("```")

    elif mode == "expert":
        lines.append(
            "**Step 7: Design Best Strategy.** Design the single strategy with highest "
            "expected value. Combine 2-3 transforms if their mechanisms are complementary."
        )
        lines.append("")
        lines.append("Selection rules:")
        lines.append(
            "- If the EXPLAIN shows the optimizer already handles something, don't re-do it"
        )
        lines.append(
            "- Verify structural prerequisites before assigning transforms"
        )
    elif mode == "oneshot":
        lines.append(
            "**Step 7: Implement.** Design and implement the single best strategy "
            "as working SQL."
        )
        lines.append("")
        lines.append("Selection rules:")
        lines.append(
            "- If the EXPLAIN shows the optimizer already handles something, don't re-do it"
        )
        lines.append(
            "- Verify structural prerequisites before assigning transforms"
        )

    return "\n".join(lines)


def section_output_format(
    mode: str,
    is_discovery_mode: bool,
    dialect: str,
) -> str:
    """§VI. OUTPUT FORMAT — shared briefing + per-worker briefings."""
    lines = ["## §VI. OUTPUT FORMAT", ""]

    # Shared briefing
    lines.append("```")
    lines.append("=== SHARED BRIEFING ===")
    lines.append("")
    lines.append("SEMANTIC_CONTRACT: (80-150 tokens)")
    lines.append("(a) Business intent.")
    lines.append("(b) JOIN semantics.")
    lines.append("(c) Aggregation traps.")
    lines.append("(d) Filter dependencies.")
    lines.append("")
    lines.append("OPTIMAL_PATH:")
    lines.append("[Your deduced ideal join order from Step 3, with running rowcount at each step.")
    lines.append("This is the destination — what every worker is trying to get the optimizer to execute.]")
    lines.append("")
    lines.append("CURRENT_PLAN_GAP:")
    lines.append("[Where the actual plan diverges from optimal. Per divergence: which goal violated,")
    lines.append("which blind spot causes it, how many excess rows result.]")
    lines.append("")
    lines.append("ACTIVE_CONSTRAINTS:")
    lines.append("- [ID]: [1-line relevance]")
    lines.append("")
    lines.append("REGRESSION_WARNINGS:")
    lines.append("- [Pattern] ([result]):")
    lines.append("  CAUSE: [...]")
    lines.append("  RULE: [...]")
    lines.append("")

    if mode == "swarm":
        lines.append("DIVERSITY_MAP:")
        lines.append("| Worker | Role              | Primary Family | Secondary | Key Structural Idea |")
        lines.append("|--------|-------------------|----------------|-----------|---------------------|")
        lines.append("| W1     | Proven compound   | [A-F]          | [A-F]     | [1-line]            |")
        lines.append("| W2     | Structural alt    | [≠ W1 primary]  | [opt.]    | [1-line]            |")
        lines.append("| W3     | Aggressive cmpd   | [multi]         | [multi]   | [1-line]            |")
        lines.append("| W4     | Novel/orthogonal  | [uncovered]     | -         | [1-line]            |")
        lines.append("")
        lines.append("FAMILY_COVERAGE: A [W_] B [W_] C [W_] D [W_] E [W_] F [W_] | Uncovered: [list → W4 targets]")
        lines.append("")

    # Worker briefing template
    _WORKER_TEMPLATE = [
        "STRATEGY: [name — matches diversity map]",
        "ROLE: [proven_compound | structural_alt | aggressive_compound | novel_orthogonal]",
        "PRIMARY_FAMILY: [A-F] — [family name]",
        "APPROACH: [2-3 sentences: structural idea, which gap it closes, which goal it serves]",
        "",
        "TARGET_QUERY_MAP:",
        "[The NEW query map for this strategy — same tree format as §II.C but showing the",
        "restructured data flow. Must show running rowcount at each node decreasing",
        "monotonically. This is the worker's design document — they write SQL to implement",
        "THIS map.]",
        "",
        "NODE_CONTRACTS:",
        "  [node_name]:",
        "    FROM/JOIN/WHERE/GROUP BY/AGGREGATE/OUTPUT/EXPECTED_ROWS/CONSUMERS",
        "    (all as SQL fragments)",
        "",
        "EXAMPLES: [1-3 IDs from §VII.B — selected for structural similarity to THIS strategy]",
        "EXAMPLE_ADAPTATION:",
        "  [example_id]:",
        "    APPLY: [which structural pattern from this example maps to this query]",
        "    IGNORE: [which parts don't apply and why]",
        "    ADAPT: [what differs between example and this query]",
        "HAZARD_FLAGS: [query-specific risks for THIS approach]",
    ]

    _WORKER4_EXTRA = [
        "EXPLORATION_TYPE: [novel_technique | compound_from_uncovered | retry_different_structure]",
        "HYPOTHESIS_TAG: [descriptive]",
        "UNCOVERED_FAMILY: [which family W1-W3 missed that W4 targets]",
    ]

    if mode == "swarm":
        # Generate explicit WORKER 1, 2, 3, 4 briefing templates
        # (Parser requires numeric headers, not template placeholder "N")
        for worker_num in range(1, 5):
            lines.append("")
            lines.append(f"=== WORKER {worker_num} BRIEFING ===")
            lines.append("")
            for tl in _WORKER_TEMPLATE:
                lines.append(tl)
            lines.append("")
            if worker_num == 4:
                lines.append("Worker 4 adds:")
                for extra in _WORKER4_EXTRA:
                    lines.append(f"  {extra}")
            if is_discovery_mode:
                lines.append("")
                lines.append(
                    "(Discovery mode: ALL workers include EXPLORATION_TYPE and HYPOTHESIS_TAG.)"
                )
            lines.append("")
    elif mode == "expert":
        lines.append("")
        lines.append("=== WORKER 1 BRIEFING ===")
        lines.append("")
        for tl in _WORKER_TEMPLATE:
            lines.append(tl)
        lines.append("")
    elif mode == "oneshot":
        lines.append("")
        lines.append("=== OPTIMIZED SQL ===")
        lines.append("")
        lines.append("STRATEGY: strategy_name")
        lines.append("TRANSFORM: transform_names")
        lines.append("")
        lines.append("```sql")
        lines.append("SELECT ...")
        lines.append("```")
        lines.append("")

    lines.append("```")

    return "\n".join(lines)


def section_reference_appendix(
    engine_profile: Optional[Dict[str, Any]],
    exploit_algorithm_text: Optional[str],
    dag: Any,
    detected_transforms: Optional[List] = None,
    matched_examples: Optional[List[Dict[str, Any]]] = None,
    dialect: str = "duckdb",
) -> str:
    """§VII. REFERENCE APPENDIX — case files by blind spot, gold example catalog, regression registry, structural matches, verification."""
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "snowflake": "Snowflake",
    }
    engine_display = engine_names.get(dialect, dialect)
    lines = [f"## §VII. REFERENCE APPENDIX ({engine_display})", ""]
    lines.append(
        "Case files and gold examples from past investigations, organized by engine "
        "blind spot (matching §III). Consult during Step 5 when your diagnosis identifies "
        "a matching blind spot."
    )
    lines.append("")

    # A. Documented Cases by Blind Spot
    if exploit_algorithm_text:
        # Exploit algorithm already has documented cases — skip detailed gap rendering
        pass
    elif engine_profile:
        gaps = engine_profile.get("gaps", [])
        if gaps:
            lines.append("### A. Documented Cases by Blind Spot")
            lines.append("")
            for g in gaps:
                gid = g.get("id", "")
                goal = g.get("goal", "")
                detect = g.get("detect", "")
                gates = g.get("gates", "")
                what_worked = g.get("what_worked", [])
                what_didnt = g.get("what_didnt_work", [])
                field_notes = g.get("field_notes", [])
                also_manifests = g.get("also_manifests_as", [])

                goal_str = f" → {goal}" if goal else ""
                pct_str = ""
                for note in field_notes:
                    if "%" in note:
                        pct_str = f" ({note})"
                        break
                lines.append(f"**Blind spot: {_format_blind_spot_id(gid)}**{goal_str}{pct_str}")
                if detect:
                    lines.append(f"Detect: {detect}")
                if gates:
                    lines.append(f"Gates: {gates}")
                if what_worked:
                    treatments_str = ", ".join(what_worked[:4])
                    lines.append(f"Treatments: {treatments_str}.")
                if what_didnt:
                    failures_str = ", ".join(what_didnt[:3])
                    lines.append(f"Failures: {failures_str}.")
                for am in also_manifests:
                    name = am.get("name", "")
                    desc = am.get("description", "")
                    treatment = am.get("treatment", "")
                    lines.append(f"Also manifests as **{name}** — {desc}")
                    if treatment:
                        lines.append(f"Treatment: {treatment}")
                remaining_notes = [n for n in field_notes if "%" not in n]
                if remaining_notes:
                    notes_str = " ".join(remaining_notes[:3])
                    lines.append(f"Notes: {notes_str}")
                lines.append("")

    # B. Gold Example Catalog
    lines.append(f"### B. Gold Example Catalog ({engine_display})")
    lines.append("")
    lines.append(
        "Each example is a proven before/after SQL pair with measured speedups. "
        "Workers receive the full SQL for assigned examples. You select based on "
        "structural similarity to this query."
    )
    lines.append("")
    if matched_examples:
        lines.append("| Example ID | Match | Query Shape | Result | Key Feature |")
        lines.append("|---|---|---|---|---|")
        for ex in matched_examples:
            ex_id = ex.get("id", "?")
            desc = ex.get("description", "")
            speedup = ex.get("verified_speedup", "")
            principle = ex.get("principle", "")
            match_score = ex.get("_match_score", 0)
            match_pct = f"{match_score:.0%}" if match_score else "—"
            # Extract a short "query shape" from description or principle
            raw_shape = desc if desc else (principle if principle else "")
            shape = (raw_shape[:57] + "...") if len(raw_shape) > 60 else raw_shape
            # Key feature: use key_insight if available, else principle
            raw_insight = (ex.get("example", {}).get("key_insight", "") or "")
            if not raw_insight:
                raw_insight = principle if principle else ""
            key_insight = (raw_insight[:77] + "...") if len(raw_insight) > 80 else raw_insight
            lines.append(f"| {ex_id} | {match_pct} | {shape} | {speedup} | {key_insight} |")
        lines.append("")
    else:
        lines.append("*No gold examples available for this engine.*")
        lines.append("")

    # C. Regression Registry — engine-specific known failures
    if not exploit_algorithm_text:
        # Only emit static registry when exploit algorithm doesn't have its own
        registry = _get_regression_registry(dialect)
        if registry:
            lines.append("### C. Regression Registry")
            lines.append("")
            lines.append("| What | Result | Cause |")
            lines.append("|------|--------|-------|")
            for entry in registry:
                lines.append(f"| {entry[0]} | {entry[1]} | {entry[2]} |")
            lines.append("")

    # D. Structural Matches for This Query
    if detected_transforms:
        lines.append("### D. Structural Matches for This Query")
        lines.append("")
        for m in detected_transforms:
            pct = f"{m.overlap_ratio:.0%}"
            matched = ", ".join(m.matched_features)
            gap_str = f" [{m.gap}]" if m.gap else ""
            lines.append(f"- **{m.id}** ({pct}){gap_str} — {matched}")
            if m.contraindications:
                for ci in m.contraindications:
                    lines.append(f"  ⚠ {ci['instruction']}")
        lines.append("")

    # E. What Doesn't Apply
    features = _detect_query_features(dag)
    inapplicable = []
    if not features["has_left_join"]:
        inapplicable.append("No LEFT JOINs")
    if not features["has_intersect"]:
        inapplicable.append("No INTERSECT")
    if not features["has_window"]:
        inapplicable.append("No WINDOW/OVER")
    if not features["has_cte"]:
        inapplicable.append("No CTE chain in original")

    if inapplicable:
        lines.append("### E. What Doesn't Apply")
        lines.append("")
        lines.append(", ".join(inapplicable) + ".")
        lines.append("")

    # F. Verification Checklist
    if not exploit_algorithm_text:
        # Only emit static checklist when exploit algorithm doesn't have its own
        lines.append("### F. Verification Checklist")
        lines.append("")
        lines.append(
            "Before finalizing: every CTE has WHERE; no orphaned CTEs; EXISTS remains EXISTS; "
            "same-column ORs intact; rowcounts decrease through CTE chains; comma joins → "
            "explicit JOIN...ON."
        )

    return "\n".join(lines)


def _get_regression_registry(dialect: str) -> List[tuple]:
    """Return engine-specific regression entries: [(what, result, cause), ...]."""
    _DUCKDB_REGRESSIONS = [
        ("Materialized EXISTS", "0.14x", "Semi-join destroyed"),
        ("3 dim CTE cross-join", "0.0076x", "Cartesian product"),
        ("9 UNION branches", "0.23x", "Excessive fact scans"),
        ("Decorrelated LEFT semi", "0.34x", "Broke early termination"),
        ("Orphaned CTE after split", "0.49x", "Double materialization"),
        ("3-fact CTE chain", "0.50x", "Locked join order"),
        ("Split same-column OR", "0.59x", "Destroyed native optimization"),
    ]
    _PG_REGRESSIONS = [
        ("Multi-scan rewrite", "0.50x", "Double fact table scan"),
        ("Window AVG OVER PARTITION", "1.0x", "No improvement on PG"),
        ("Materialized EXISTS", "0.14x", "Semi-join destroyed"),
        ("Excessive CTE materialization", "0.60x", "Forced scan order"),
    ]
    if dialect == "duckdb":
        return _DUCKDB_REGRESSIONS
    if dialect in ("postgresql", "postgres"):
        return _PG_REGRESSIONS
    return []  # Snowflake etc — no known regressions yet


def _format_blind_spot_id(gid: str) -> str:
    """Convert engine profile gap ID to readable display name.

    CROSS_CTE_PREDICATE_BLINDNESS → Cross-CTE predicate blindness
    """
    # Preserve acronyms
    acronyms = {"CTE", "CSE", "OR"}
    words = gid.split("_")
    result = []
    for i, w in enumerate(words):
        if w in acronyms:
            result.append(w)
        elif i == 0:
            result.append(w.capitalize())
        else:
            result.append(w.lower())
    return " ".join(result)


# ═══════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════

def build_v2_analyst_briefing_prompt(
    query_id: str,
    sql: str,
    explain_plan_text: Optional[str],
    dag: Any,
    costs: Dict[str, Any],
    semantic_intents: Optional[Dict[str, Any]],
    constraints: Optional[List[Dict[str, Any]]] = None,
    dialect: str = "duckdb",
    dialect_version: Optional[str] = None,
    engine_profile: Optional[Dict[str, Any]] = None,
    resource_envelope: Optional[str] = None,
    exploit_algorithm_text: Optional[str] = None,
    plan_scanner_text: Optional[str] = None,
    iteration_history: Optional[Dict[str, Any]] = None,
    mode: str = "swarm",
    detected_transforms: Optional[List] = None,
    qerror_analysis: Optional[Any] = None,
    matched_examples: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Build the V2 analyst briefing prompt with §I-§VII structure.

    Args:
        query_id: Query identifier (e.g., 'query_35')
        sql: Original SQL query
        explain_plan_text: Pre-rendered EXPLAIN ANALYZE tree
        dag: Parsed logical tree from Phase 1
        costs: Per-node cost analysis
        semantic_intents: Pre-computed per-query intents
        constraints: All engine-filtered constraints (full JSON)
        dialect: SQL dialect (duckdb, postgresql, snowflake)
        dialect_version: Engine version string
        engine_profile: Engine profile JSON with strengths/gaps
        resource_envelope: System resource envelope text (PG only)
        exploit_algorithm_text: Evidence-based exploit algorithm text
        plan_scanner_text: Pre-computed plan-space scanner results (PG)
        iteration_history: Prior optimization attempts
        mode: 'swarm' (4 workers), 'expert' (1 worker), 'oneshot'
        detected_transforms: Top-N transforms by feature overlap
        qerror_analysis: Cardinality estimation error analysis
        matched_examples: Pre-ranked gold examples with _match_score

    Returns:
        Complete analyst prompt string with §I-§VII sections.
    """
    if mode not in ("swarm", "expert", "oneshot"):
        raise ValueError(f"Invalid mode: {mode!r}. Must be 'swarm', 'expert', or 'oneshot'.")

    has_empirical_gaps = bool(engine_profile and engine_profile.get("gaps"))
    is_discovery_mode = not has_empirical_gaps

    sections = [
        section_role(mode),
        section_the_case(
            query_id, sql, explain_plan_text, dag, costs,
            semantic_intents, dialect, dialect_version,
            qerror_analysis, iteration_history,
        ),
        section_this_engine(
            engine_profile, exploit_algorithm_text, dialect,
            resource_envelope, plan_scanner_text,
        ),
        section_constraints(constraints, dag, costs),
        section_investigate(mode, dag, qerror_analysis, explain_plan_text),
        section_output_format(mode, is_discovery_mode, dialect),
        section_reference_appendix(
            engine_profile, exploit_algorithm_text, dag,
            detected_transforms, matched_examples, dialect,
        ),
    ]

    return "\n\n".join(sections)
