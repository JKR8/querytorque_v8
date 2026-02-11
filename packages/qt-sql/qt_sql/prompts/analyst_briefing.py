"""V2 analyst prompt builder — analyst as interpreter, not router.

Two-layer architecture:
  Layer 1: Engine gap profiles (offensive hunting guide — what optimizer gaps to exploit)
  Layer 2: Correctness constraints (defensive validation gates — 4 non-negotiable rules)

The V2 analyst produces a structured briefing:
  Shared (all workers):
    1. SEMANTIC_CONTRACT — business intent, invariants, aggregation traps
    2. BOTTLENECK_DIAGNOSIS — dominant cost + mechanism + cardinality flow
    3. ACTIVE_CONSTRAINTS — correctness gates + matched engine gaps
    4. REGRESSION_WARNINGS — causal rules, not just "this happened"
  Per-worker:
    5. TARGET_LOGICAL_TREE + NODE_CONTRACTS — CTE blueprint + column contracts
    6. EXAMPLES + EXAMPLE_ADAPTATION — what to apply/ignore per example
    7. HAZARD_FLAGS — strategy-specific risks for this query
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from .briefing_checks import (
    build_analyst_section_checklist,
    build_expert_section_checklist,
    build_oneshot_section_checklist,
)

logger = logging.getLogger(__name__)

ALGO_DIR = Path(__file__).resolve().parent.parent / "algorithms"


@lru_cache(maxsize=16)
def _load_algorithm(name: str) -> Optional[str]:
    """Load a prompt-level algorithm YAML and inject into prompt as-is."""
    path = ALGO_DIR / f"{name}.yaml"
    if not path.exists():
        return None
    return path.read_text()


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


def _render_pg_node(
    node: dict,
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
    overridable = c.get("overridable", False)
    instruction = c.get("prompt_instruction", c.get("description", ""))
    failures = c.get("observed_failures", [])
    successes = c.get("observed_successes", [])
    override_conditions = c.get("override_conditions", [])

    tag = "[overridable] " if overridable else ""
    parts = [f"**[{severity}] {tag}{cid}**: {instruction}"]
    if failures:
        for f in failures[:2]:
            qid = f.get("query", f.get("query_id", "?"))
            reg = f.get("regression", f.get("speedup"))
            err = f.get("error", "")
            if reg:
                parts.append(f"  - Failure: {qid} regressed to {reg}")
            elif err:
                parts.append(f"  - Failure: {qid} — {err}")
    if successes:
        for s in successes[:2]:
            qid = s.get("query", "?")
            spd = str(s.get("speedup", "?")).rstrip("x")
            ctx = s.get("context", "")
            parts.append(f"  - Success: {qid} achieved {spd}x — {ctx}")
    if overridable and override_conditions:
        parts.append("  - Override conditions (Worker 4 exploration):")
        for oc in override_conditions:
            parts.append(f"    * {oc}")
    return "\n".join(parts)


def _format_example_compact(ex: Dict[str, str]) -> str:
    """Format a gold example compactly: id (speedup) — description."""
    eid = ex.get("id", "?")
    speedup = str(ex.get("speedup", "?")).rstrip("x")
    desc = ex.get("description", "")[:80]
    return f"- **{eid}** ({speedup}x) — {desc}"


def _format_example_full(ex: Dict[str, Any]) -> str:
    """Format a tag-matched example with full metadata."""
    eid = ex.get("id", "?")
    speedup = str(ex.get("verified_speedup", ex.get("speedup", "?"))).rstrip("x")
    desc = ex.get("description", "")
    principle = ex.get("principle", "")

    # when_not_to_use lives inside the example sub-dict
    example_data = ex.get("example", {})
    when_not = example_data.get("when_not_to_use", "")

    parts = [f"### {eid} ({speedup}x)"]
    if desc:
        parts.append(f"**Description:** {desc}")
    if principle:
        parts.append(f"**Principle:** {principle}")
    if when_not:
        parts.append(f"**When NOT to apply:** {when_not}")

    return "\n".join(parts)


def _format_regression_for_analyst(reg: Dict[str, Any]) -> str:
    """Format a regression example for the analyst's full view."""
    eid = reg.get("id", "?")
    query_id = reg.get("query_id", "?")
    speedup = str(reg.get("verified_speedup", "?")).rstrip("x")
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


def _section_strategy_leaderboard(
    leaderboard: Dict[str, Any],
    archetype: str,
) -> str:
    """Format strategy leaderboard section for a specific archetype.

    Shows observed success rates per transform so the analyst can make
    data-driven strategy selections instead of guessing.
    """
    lines: list[str] = []

    arch_summary = leaderboard.get("archetype_summary", {}).get(archetype)
    if not arch_summary:
        return ""

    lines.append(f"## Strategy Leaderboard (observed success rates)")
    lines.append("")
    lines.append(
        f"Archetype: **{archetype}** ({arch_summary['query_count']} queries in pool, "
        f"{arch_summary['total_attempts']} total attempts)"
    )
    lines.append("")

    # Get transforms for this archetype
    transforms = leaderboard.get("transform_by_archetype", {}).get(archetype, {})
    if not transforms:
        lines.append("*No transform data available for this archetype.*")
        return "\n".join(lines)

    # Get elimination list
    elim = leaderboard.get("elimination_table", {}).get(archetype, {})
    avoid_set = set(elim.get("avoid", []))
    elim_reasons = elim.get("reason", {})

    # Sort by success_rate descending, then avg_speedup
    ranked = sorted(
        transforms.items(),
        key=lambda x: (-x[1]["success_rate"], -x[1]["avg_speedup_all"]),
    )

    # Filter to transforms with at least 3 attempts (enough signal)
    ranked = [(t, d) for t, d in ranked if d["attempts"] >= 3]

    if not ranked:
        lines.append("*Insufficient data (< 3 attempts per transform).*")
        return "\n".join(lines)

    lines.append("| Transform | Attempts | Win Rate | Avg Speedup | Avoid? |")
    lines.append("|-----------|----------|----------|-------------|--------|")

    for transform, data in ranked:
        avoid_flag = "AVOID" if transform in avoid_set else ""
        win_pct = f"{data['success_rate']:.0%}"
        avg_sp = f"{data['avg_speedup_all']:.2f}x"
        lines.append(
            f"| {transform} | {data['attempts']} | {win_pct} | {avg_sp} | {avoid_flag} |"
        )

    # Show elimination reasons if any
    if avoid_set:
        lines.append("")
        lines.append("**Elimination reasons:**")
        for t in sorted(avoid_set):
            reason = elim_reasons.get(t, "low success rate")
            lines.append(f"- **{t}**: {reason}")

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
    strategy_leaderboard: Optional[Dict[str, Any]] = None,
    query_archetype: Optional[str] = None,
    engine_profile: Optional[Dict[str, Any]] = None,
    resource_envelope: Optional[str] = None,
    exploit_algorithm_text: Optional[str] = None,
    plan_scanner_text: Optional[str] = None,
    iteration_history: Optional[Dict[str, Any]] = None,
    mode: str = "swarm",
) -> str:
    """Build the analyst briefing prompt for swarm, expert, or oneshot mode.

    All modes share the same data sections (EXPLAIN, logical tree, examples, constraints,
    etc.). What varies: role framing, output format, strategy rules, and
    exploration budget.

    Args:
        query_id: Query identifier (e.g., 'query_74')
        sql: Original SQL query
        explain_plan_text: ASCII EXPLAIN ANALYZE tree (may be None for ~4 queries)
        dag: Parsed logical tree from Phase 1
        costs: Per-node cost analysis
        semantic_intents: Pre-computed per-query + per-node intents (may be None)
        global_knowledge: GlobalKnowledge dict with principles + anti_patterns
        matched_examples: Top tag-matched examples (full metadata, typically 16)
        all_available_examples: Full catalog (id + speedup + description)
        constraints: All engine-filtered constraints (full JSON)
        regression_warnings: Tag-matched regression examples (may be None)
        dialect: SQL dialect
        dialect_version: Engine version string (e.g., '1.4.3')
        strategy_leaderboard: Pre-built leaderboard JSON (from build_strategy_leaderboard.py)
        query_archetype: Archetype classification for this query (e.g., 'scan_consolidation')
        engine_profile: Engine profile JSON with optimizer strengths/gaps (may be None)
        resource_envelope: System resource envelope text for PG workers (may be None)
        exploit_algorithm_text: Evidence-based exploit algorithm YAML from frontier
            probing (may be None). When present, replaces engine profile section.
        plan_scanner_text: Pre-computed plan-space scanner results for PG (may be None).
            Shows what happens when planner flags are toggled via SET LOCAL.
        iteration_history: Prior optimization attempts for this query (expert/oneshot
            iterative mode). Dict with 'attempts' list. None for first iteration or swarm.
        mode: Prompt mode — "swarm" (4 workers), "expert" (1 worker), or "oneshot"
            (analyze + produce SQL directly). Default "swarm".

    Returns:
        Complete analyst prompt string (~3000-5000 tokens input)
    """
    if mode not in ("swarm", "expert", "oneshot"):
        raise ValueError(f"Invalid mode: {mode!r}. Must be 'swarm', 'expert', or 'oneshot'.")
    lines: list[str] = []

    # ── 1. Role ──────────────────────────────────────────────────────────
    if mode == "swarm":
        lines.append(
            "You are a senior query optimization architect. Your job is to deeply "
            "analyze a SQL query and produce a structured briefing for 4 specialist "
            "workers who will each write a different optimized version."
        )
        lines.append("")
        lines.append(
            "You are the ONLY call that sees all the data: EXPLAIN plans, logical-tree costs, "
            "full constraint list, global knowledge, and the complete example catalog. "
            "The workers will only see what YOU put in their briefings. "
            "Your output quality directly determines their success."
        )
    elif mode == "expert":
        lines.append(
            "You are a senior query optimization architect. Your job is to deeply "
            "analyze a SQL query and produce a structured briefing for a single "
            "specialist worker who will write the best possible optimized version."
        )
        lines.append("")
        lines.append(
            "You are the ONLY call that sees all the data: EXPLAIN plans, logical-tree costs, "
            "full constraint list, global knowledge, and the complete example catalog. "
            "The worker will only see what YOU put in the briefing. "
            "Your output quality directly determines success."
        )
    elif mode == "oneshot":
        lines.append(
            "You are a senior query optimization architect. Your job is to deeply "
            "analyze a SQL query, determine the single best optimization strategy, "
            "and then produce the optimized SQL directly."
        )
        lines.append("")
        lines.append(
            "You have all the data: EXPLAIN plans, logical-tree costs, full constraint list, "
            "global knowledge, and the complete example catalog. Analyze thoroughly, "
            "then implement the best strategy as working SQL."
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

    # ── 3. EXPLAIN plan ─────────────────────────────────────────────────
    is_estimate_plan = False  # hoisted for use in reasoning step 2
    if explain_plan_text:
        formatted_plan = format_duckdb_explain_tree(explain_plan_text)
        # Detect estimate-only plans (no actual timing data)
        is_estimate_plan = "est_rows=" in formatted_plan or "EXPLAIN only" in formatted_plan
        if is_estimate_plan:
            lines.append("## EXPLAIN Plan (planner estimates)")
        else:
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
        lines.append(
            "**NOTE:** The EXPLAIN plan shows the PHYSICAL execution structure, which "
            "may differ significantly from the logical tree below. The optimizer may have "
            "already split CTEs, reordered joins, or pushed predicates. When the EXPLAIN "
            "and the logical tree disagree, the EXPLAIN is ground truth for what the optimizer is "
            "already doing."
        )
        lines.append("")
        if dialect == "duckdb":
            lines.append(
                "DuckDB EXPLAIN ANALYZE reports **operator-exclusive** wall-clock time "
                "per node (children's time is NOT included in the parent's reported time). "
                "The percentage annotations are also exclusive. You can sum sibling nodes "
                "to get pipeline cost. logical-tree cost percentages are derived metrics that may "
                "not reflect actual execution time — use EXPLAIN timings as ground truth."
            )
        elif is_estimate_plan:
            lines.append(
                "This is a plan-only EXPLAIN (no ANALYZE). Row counts and costs are "
                "planner predictions — use them directionally but flag your diagnosis as "
                "lower-confidence. Fall back to schema-based reasoning (table sizes, index "
                "selectivity, join fan-out) to validate planner estimates."
            )
        else:
            lines.append(
                "Use EXPLAIN ANALYZE timings as ground truth. logical-tree cost percentages are "
                "derived metrics that may not reflect actual execution time."
            )
        lines.append("")
    else:
        lines.append("## EXPLAIN Plan")
        lines.append("")
        lines.append(
            "*EXPLAIN plan not available for this query. "
            "Use logical-tree cost percentages as proxy for bottleneck identification.*"
        )
        lines.append("")

    # ── 3.5. Plan-Space Scanner Intelligence ─────────────────────────────
    if plan_scanner_text:
        lines.append("## Plan-Space Scanner Intelligence")
        lines.append("")
        lines.append(plan_scanner_text)
        lines.append("")
        algo_text = _load_algorithm("postgres_dsb_sf10_scanner")
        if algo_text:
            lines.append(algo_text)
            lines.append("")

    # ── 4. Query Structure (Logic Tree + condensed node details) ─────────
    from ..logic_tree import build_logic_tree
    from ..analyst import _append_dag_analysis
    from ..prompter import _build_node_intent_map

    node_intents = _build_node_intent_map(semantic_intents)
    if semantic_intents:
        query_intent = semantic_intents.get("query_intent", "")
        if query_intent and "main_query" not in node_intents:
            node_intents["main_query"] = query_intent

    # Logic Tree overview
    lines.append("## Query Structure (Logic Tree)")
    lines.append("")
    tree = build_logic_tree(sql, dag, costs, dialect, node_intents)
    lines.append("```")
    lines.append(tree)
    lines.append("```")
    lines.append("")

    # Condensed per-node detail cards (analyst needs join/filter details)
    lines.append("## Node Details")
    lines.append("")
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
            lines.append(
                "START from this pre-computed intent. In your SEMANTIC_CONTRACT output, "
                "ENRICH it with: intersection/union semantics from JOIN types, "
                "aggregation function traps, NULL propagation paths, and filter "
                "dependencies. Do NOT re-derive what is already stated above."
            )
            lines.append("")
    # When no pre-computed intents exist, omit this section entirely.
    # The analyst's task already requires producing SEMANTIC_CONTRACT output.

    # ── 5b. Aggregation Semantics Check ───────────────────────────────
    lines.append("## Aggregation Semantics Check")
    lines.append("")
    lines.append(
        "You MUST verify aggregation equivalence for any proposed restructuring:"
    )
    lines.append("")
    lines.append(
        "- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. Returns NULL "
        "for 0-1 values. Changing group membership changes the result."
    )
    lines.append(
        "- `STDDEV_SAMP(x) FILTER (WHERE year=1999)` over a combined (1999,2000) "
        "group is NOT equivalent to `STDDEV_SAMP(x)` over only 1999 rows — "
        "FILTER still uses the combined group's membership for the stddev denominator."
    )
    lines.append(
        "- **AVG and STDDEV are NOT duplicate-safe**: if a join introduces row "
        "duplication, the aggregate result changes."
    )
    lines.append(
        "- When splitting a UNION ALL CTE with GROUP BY + aggregate, each split "
        "branch must preserve the exact GROUP BY columns and filter to the exact "
        "same row set as the original."
    )
    lines.append(
        "- **SAFE ALTERNATIVE**: If GROUP BY includes the discriminator column "
        "(e.g., d_year), each group is already partitioned. STDDEV_SAMP computed "
        "per-group is correct. You can then pivot using "
        "`MAX(CASE WHEN year = 1999 THEN year_total END) AS year_total_1999` "
        "because the GROUP BY guarantees exactly one row per (customer, year) — "
        "the MAX is just a row selector, not a real aggregation."
    )
    lines.append("")

    # ── 6. Tag-matched examples (specific to this query, shown first) ──
    if matched_examples:
        lines.append(f"## Top {len(matched_examples)} Tag-Matched Examples")
        lines.append("")
        for ex in matched_examples:
            lines.append(_format_example_full(ex))
            lines.append("")

    # ── 7. Additional examples not in tag-matched set (compact) ──────────
    if all_available_examples:
        matched_ids = {ex.get("id") for ex in matched_examples} if matched_examples else set()
        additional = [ex for ex in all_available_examples if ex.get("id") not in matched_ids]
        if additional:
            lines.append("## Additional Examples (not tag-matched to this query)")
            lines.append("")
            for ex in additional:
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

    # ── 9b. Iteration History (expert/oneshot iterative mode) ────────────
    if iteration_history and iteration_history.get("attempts"):
        lines.append("## Previous Optimization Attempts on This Query")
        lines.append("")
        lines.append(
            "The following attempts have already been tried. "
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
                lines.append(f"- Attempt {i+1}: **{t_str}** → ERROR: {error}")
            elif speedup < 0.95:
                lines.append(f"- Attempt {i+1}: **{t_str}** → REGRESSION ({speedup:.2f}x)")
            elif speedup >= 1.10:
                lines.append(f"- Attempt {i+1}: **{t_str}** → WIN ({speedup:.2f}x)")
            else:
                lines.append(f"- Attempt {i+1}: **{t_str}** → NEUTRAL ({speedup:.2f}x)")
            # Include failure analysis if present
            failure_analysis = attempt.get("failure_analysis", "")
            if failure_analysis and status not in ("WIN", "IMPROVED"):
                preview = failure_analysis[:200] + "..." if len(failure_analysis) > 200 else failure_analysis
                lines.append(f"  Analysis: {preview}")
        lines.append("")

    # ── 10. Exploit Algorithm or Engine Profile ──────────────────────────
    if exploit_algorithm_text:
        lines.append("## Exploit Algorithm: Evidence-Based Gap Intelligence")
        lines.append("")
        lines.append(
            "The following YAML describes known optimizer gaps with detection rules, "
            "procedural exploit steps, and evidence. Use DETECT rules to match "
            "structural features of the query, then follow EXPLOIT_STEPS."
        )
        lines.append("")
        lines.append(exploit_algorithm_text)
        lines.append("")
    elif engine_profile:
        briefing_note = engine_profile.get("briefing_note", "")
        lines.append("## Engine Profile: Field Intelligence Briefing")
        lines.append("")
        if briefing_note:
            lines.append(f"*{briefing_note}*")
            lines.append("")

        # Strengths (things the optimizer already does — don't fight these)
        strengths = engine_profile.get("strengths", [])
        if strengths:
            lines.append("### Optimizer Strengths (DO NOT fight these)")
            lines.append("")
            for s in strengths:
                sid = s.get("id", "")
                summary = s.get("summary", "")
                field_note = s.get("field_note", "")
                lines.append(f"- **{sid}**: {summary}")
                if field_note:
                    lines.append(f"  *Field note:* {field_note}")
            lines.append("")

        # Gaps (opportunities — the hunting guide)
        gaps = engine_profile.get("gaps", [])
        if gaps:
            lines.append("### Optimizer Gaps (hunt for these)")
            lines.append("")
            for g in gaps:
                gid = g.get("id", "")
                priority = g.get("priority", "")
                what = g.get("what", "")
                why = g.get("why", "")
                opportunity = g.get("opportunity", "")

                lines.append(f"**{gid}** [{priority}]")
                lines.append(f"  What: {what}")
                if why:
                    lines.append(f"  Why: {why}")
                if opportunity:
                    lines.append(f"  Opportunity: {opportunity}")

                # What worked (compact)
                what_worked = g.get("what_worked", [])
                if what_worked:
                    lines.append("  What worked:")
                    for w in what_worked[:4]:
                        lines.append(f"    + {w}")

                # What didn't work (compact)
                what_didnt = g.get("what_didnt_work", [])
                if what_didnt:
                    lines.append("  What didn't work:")
                    for w in what_didnt[:3]:
                        lines.append(f"    - {w}")

                # Field notes (the real intel)
                field_notes = g.get("field_notes", [])
                if field_notes:
                    lines.append("  Field notes:")
                    for fn in field_notes:
                        lines.append(f"    * {fn}")
                lines.append("")

        # Scale sensitivity warning
        scale_warn = engine_profile.get("scale_sensitivity_warning")
        if scale_warn:
            lines.append(f"**SCALE WARNING**: {scale_warn}")
            lines.append("")

    # ── 10a. Resource Envelope (PostgreSQL only — passed through to workers) ──
    if dialect in ("postgresql", "postgres") and resource_envelope:
        lines.append("## System Resource Envelope (PostgreSQL)")
        lines.append("")
        lines.append(
            "Workers will use this to size SET LOCAL parameters for their rewrites. "
            "Included here for your awareness — you do NOT output config. "
            "Each worker decides its own per-rewrite config."
        )
        lines.append("")
        lines.append(resource_envelope)
        lines.append("")

    # ── 10b. Correctness Constraints (non-negotiable validation gates) ────
    correctness_constraints = [
        c for c in constraints
        if c.get("id") in (
            "LITERAL_PRESERVATION", "SEMANTIC_EQUIVALENCE",
            "COMPLETE_OUTPUT", "CTE_COLUMN_COMPLETENESS",
        )
    ]
    if correctness_constraints:
        lines.append(f"## Correctness Constraints ({len(correctness_constraints)} — NEVER violate)")
        lines.append("")
        for c in correctness_constraints:
            lines.append(_format_constraint_for_analyst(c))
            lines.append("")

    # ── 11. Chain-of-thought instruction with reasoning checklist ────────
    lines.append("## Your Task")
    lines.append("")
    lines.append(
        "First, use a `<reasoning>` block for your internal analysis. "
        "This will be stripped before parsing. Work through these steps IN ORDER:"
    )
    lines.append("")
    lines.append(
        "1. **CLASSIFY**: What structural archetype is this query?\n"
        "   (channel-comparison self-join / correlated-aggregate filter / "
        "star-join with late dim filter / repeated fact scan / "
        "multi-channel UNION ALL / EXISTS-set operations / other)"
    )
    lines.append("")
    if is_estimate_plan:
        lines.append(
            "2. **EXPLAIN PLAN ANALYSIS**: From the EXPLAIN plan (planner estimates), identify:\n"
            "   - Compare estimated row counts and costs per node. These are planner "
            "predictions, not measurements — use them directionally. Cross-check against "
            "schema knowledge (table sizes, index selectivity) where estimates look suspect.\n"
            "   - Which nodes have the highest estimated cost and WHY\n"
            "   - Where estimated row counts drop sharply (existing selectivity)\n"
            "   - Where estimated row counts DON'T drop (missed optimization opportunity)\n"
            "   - Whether the optimizer already splits CTEs, pushes predicates, "
            "or performs transforms you might otherwise assign\n"
            "   - Count scans per base table. If a fact table is scanned N times, "
            "a restructuring that reduces it to 1 scan saves (N-1)/N of that table's "
            "I/O cost. Prioritize transforms that reduce scan count on the largest tables.\n"
            "   - Whether the CTE is materialized once and probed multiple times, "
            "or re-executed per reference"
        )
    else:
        lines.append(
            "2. **EXPLAIN PLAN ANALYSIS**: From the EXPLAIN ANALYZE output, identify:\n"
            "   - Compute wall-clock ms per EXPLAIN node. Sum repeated operations "
            "(e.g., 2x store_sales joins = total cost). The EXPLAIN is ground truth, "
            "not the logical-tree cost percentages.\n"
            "   - Which nodes consume >10% of runtime and WHY\n"
            "   - Where row counts drop sharply (existing selectivity)\n"
            "   - Where row counts DON'T drop (missed optimization opportunity)\n"
            "   - Whether the optimizer already splits CTEs, pushes predicates, "
            "or performs transforms you might otherwise assign\n"
            "   - Count scans per base table. If a fact table is scanned N times, "
            "a restructuring that reduces it to 1 scan saves (N-1)/N of that table's "
            "I/O cost. Prioritize transforms that reduce scan count on the largest tables.\n"
            "   - Whether the CTE is materialized once and probed multiple times, "
            "or re-executed per reference"
        )
    lines.append("")
    lines.append(
        "3. **GAP MATCHING**: Compare the EXPLAIN analysis to the Engine Profile "
        "gaps above. For each gap:\n"
        "   - Does this query exhibit the gap? (e.g., is a predicate NOT pushed "
        "into a CTE? Is the same fact table scanned multiple times?)\n"
        "   - Check the 'opportunity' — does this query's structure match?\n"
        "   - Check 'what_didnt_work' and 'field_notes' — any disqualifiers for this query?\n"
        "   - Also verify: is the optimizer ALREADY handling this well? "
        "(Check the Optimizer Strengths above — if the engine already does it, "
        "your transform adds overhead, not value.)"
    )
    lines.append("")
    lines.append(
        "4. **AGGREGATION TRAP CHECK**: For every aggregate function in the query, "
        "verify: does my proposed restructuring change which rows participate "
        "in each group? STDDEV_SAMP, VARIANCE, PERCENTILE_CONT, CORR are "
        "grouping-sensitive. SUM, COUNT, MIN, MAX are grouping-insensitive "
        "(modulo duplicates). If the query uses FILTER clauses or conditional "
        "aggregation, verify equivalence explicitly."
    )
    lines.append("")
    if mode == "swarm":
        lines.append(
            "5. **TRANSFORM SELECTION**: From the matched engine gaps, select transforms "
            "that exploit the specific gaps present in THIS query. Rank by expected value "
            "(rows affected × historical speedup from evidence). Select 4 that are "
            "structurally diverse — each attacking a different gap or bottleneck.\n"
            "   REJECT tag-matched examples whose primary technique requires a structural "
            "feature this query lacks (e.g., reject intersect_to_exists if query has no "
            "INTERSECT; reject decorrelate if query has no correlated subquery). Tag "
            "matching is approximate — always verify structural applicability."
        )
    else:
        lines.append(
            "5. **TRANSFORM SELECTION**: From the matched engine gaps, select the single "
            "best transform (or compound strategy) that maximizes expected value "
            "(rows affected × historical speedup from evidence) for THIS query.\n"
            "   REJECT tag-matched examples whose primary technique requires a structural "
            "feature this query lacks. Tag matching is approximate — always verify "
            "structural applicability."
        )
    lines.append("")
    if mode == "swarm":
        lines.append(
            "6. **LOGICAL TREE DESIGN**: For each worker's strategy, define the target logical tree "
            "topology. Verify that every node contract has exhaustive output "
            "columns by checking downstream references.\n"
            "   CTE materialization matters for your design: a CTE referenced by "
            "2+ consumers will likely be materialized (good — computed once, probed "
            "many). A CTE referenced once may be inlined (no materialization benefit "
            "from 'sharing'). Design shared CTEs only when multiple downstream nodes "
            "consume them. See CTE_INLINING in Engine Profile strengths."
        )
    else:
        lines.append(
            "6. **LOGICAL TREE DESIGN**: Define the target logical tree topology for your chosen "
            "strategy. Verify that every node contract has exhaustive output "
            "columns by checking downstream references.\n"
            "   CTE materialization matters: a CTE referenced by 2+ consumers "
            "will likely be materialized. A CTE referenced once may be inlined."
        )
    lines.append("")
    if mode == "oneshot":
        lines.append(
            "7. **WRITE REWRITE**: Implement your strategy as a JSON rewrite_set. "
            "Each changed or added CTE is a node. Produce per-node SQL matching "
            "your logical tree design from step 6. Declare output columns for every node "
            "in `node_contracts`. The rewrite must be semantically equivalent to "
            "the original."
        )
        lines.append("")

    # ── 11b. PG SET LOCAL config for oneshot (analyst IS the worker) ─────
    if mode == "oneshot" and dialect in ("postgres", "postgresql") and resource_envelope:
        from .worker import _section_set_local_config
        lines.append(_section_set_local_config(resource_envelope))
        lines.append("")

    # ── 12. Output format specification ──────────────────────────────────
    lines.append("Then produce the structured briefing in EXACTLY this format:")
    lines.append("")
    lines.append("```")

    # Shared briefing section (identical across all modes)
    lines.append("=== SHARED BRIEFING ===")
    lines.append("")
    lines.append("SEMANTIC_CONTRACT: (80-150 tokens, cover ONLY:)")
    lines.append("(a) One sentence of business intent (start from pre-computed intent if available).")
    lines.append("(b) JOIN type semantics that constrain rewrites (INNER = intersection = all sides must match).")
    lines.append("(c) Any aggregation function traps specific to THIS query.")
    lines.append("(d) Any filter dependencies that a rewrite could break.")
    lines.append("Do NOT repeat information already in ACTIVE_CONSTRAINTS or REGRESSION_WARNINGS.")
    lines.append("")
    lines.append("BOTTLENECK_DIAGNOSIS:")
    lines.append("[Which operation dominates cost and WHY (not just '50% cost').")
    lines.append("Scan-bound vs join-bound vs aggregation-bound.")
    lines.append("Cardinality flow (how many rows at each stage).")
    lines.append("What the optimizer already handles well (don't re-optimize).")
    lines.append("Whether logical-tree cost percentages are misleading.]")
    lines.append("")
    lines.append("ACTIVE_CONSTRAINTS:")
    lines.append("- [CORRECTNESS_CONSTRAINT_ID]: [Why it applies to this query, 1 line]")
    lines.append("- [ENGINE_GAP_ID]: [Evidence from EXPLAIN that this gap is active]")
    lines.append("(List all 4 correctness constraints + the 1-3 engine gaps that")
    lines.append("are active for THIS query based on your EXPLAIN analysis.)")
    lines.append("")
    lines.append("REGRESSION_WARNINGS:")
    lines.append("1. [Pattern name] ([observed regression]):")
    lines.append("   CAUSE: [What happened mechanistically]")
    lines.append("   RULE: [Actionable avoidance rule for THIS query]")
    lines.append("(If no regression warnings are relevant, write 'None applicable.')")
    lines.append("")

    # Worker briefing format — varies per mode
    _WORKER_BRIEFING_TEMPLATE = [
        "STRATEGY: [strategy_name]",
        "TARGET_LOGICAL_TREE:",
        "  [node] -> [node] -> [node]",
        "NODE_CONTRACTS:",
        "(Write all fields as SQL fragments, not natural language.",
        "Example: 'WHERE: d_year IN (1999, 2000)' not 'WHERE: filter to target years'.",
        "The worker uses these as specifications to code against.)",
        "  [node_name]:",
        "    FROM: [tables/CTEs]",
        "    JOIN: [join conditions]",
        "    WHERE: [filters]",
        "    GROUP BY: [columns] (if applicable)",
        "    AGGREGATE: [functions] (if applicable)",
        "    OUTPUT: [exhaustive column list]",
        "    EXPECTED_ROWS: [approximate row count from EXPLAIN analysis]",
        "    CONSUMERS: [downstream nodes]",
        "EXAMPLES: [ex1], [ex2], [ex3]",
        "EXAMPLE_ADAPTATION:",
        "[For each example: what aspect to apply to THIS strategy,",
        "and what to IGNORE (e.g., 'apply the date CTE pattern; ignore the",
        "decorrelation — Q74 has no correlated subquery').]",
        "HAZARD_FLAGS:",
        "- [Specific risk for this approach on this query]",
    ]

    if mode == "swarm":
        for wid in range(1, 5):
            if wid == 4:
                lines.append(f"=== WORKER {wid} BRIEFING === (EXPLORATION WORKER)")
            else:
                lines.append(f"=== WORKER {wid} BRIEFING ===")
            lines.append("")
            for tl in _WORKER_BRIEFING_TEMPLATE:
                lines.append(tl)
            if wid == 4:
                lines.append("CONSTRAINT_OVERRIDE: [CONSTRAINT_ID or 'None']")
                lines.append("OVERRIDE_REASONING: [Why this query's structure differs from the observed failure, or 'N/A']")
                lines.append("EXPLORATION_TYPE: [constraint_relaxation | compound_strategy | novel_combination]")
            lines.append("")
    elif mode == "expert":
        lines.append("=== WORKER 1 BRIEFING ===")
        lines.append("")
        for tl in _WORKER_BRIEFING_TEMPLATE:
            lines.append(tl)
    elif mode == "oneshot":
        lines.append("=== REWRITE ===")
        lines.append("")
        lines.append("First output a **Modified Logic Tree** showing what changed:")
        lines.append("- `[+]` new  `[-]` removed  `[~]` modified  `[=]` unchanged  `[!]` structural")
        lines.append("")
        lines.append("Then output a **Component Payload JSON**:")
        lines.append("")
        lines.append("```json")
        lines.append("{")
        lines.append('  "spec_version": "1.0",')
        lines.append('  "dialect": "<dialect>",')
        lines.append('  "rewrite_rules": [{"id": "R1", "type": "<transform>", "description": "<what>", "applied_to": ["<comp_id>"]}],')
        lines.append('  "statements": [{')
        lines.append('    "target_table": null,')
        lines.append('    "change": "modified",')
        lines.append('    "components": {')
        lines.append('      "<cte_name>": {"type": "cte", "change": "modified", "sql": "<CTE body>", "interfaces": {"outputs": ["col1"], "consumes": []}},')
        lines.append('      "main_query": {"type": "main_query", "change": "modified", "sql": "<final SELECT>", "interfaces": {"outputs": ["col1"], "consumes": ["<cte_name>"]}}')
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
        lines.append("Rules:")
        lines.append("- Tree first, always — generate Logic Tree before writing SQL")
        lines.append("- One component at a time — treat other components as opaque interfaces")
        lines.append("- No ellipsis — every `sql` value must be complete, executable SQL")
        lines.append("- Only include changed/added components; set unchanged to `\"change\": \"unchanged\"` with `\"sql\": \"\"`")
        lines.append("- `main_query` output columns must match original exactly")
        if dialect in ("postgres", "postgresql"):
            lines.append("- `runtime_config`: SET LOCAL commands for PostgreSQL. Omit if not needed")
        lines.append("")
        lines.append("After the JSON, explain the mechanism:")
        lines.append("")
        lines.append("```")
        lines.append("Changes: <1-2 sentences: what structural change + the expected mechanism>")
        lines.append("Expected speedup: <estimate>")
        lines.append("```")

    lines.append("```")
    lines.append("")

    # Section validation checklist — varies per mode
    if mode == "swarm":
        lines.append(build_analyst_section_checklist())
    elif mode == "expert":
        lines.append(build_expert_section_checklist())
    elif mode == "oneshot":
        lines.append(build_oneshot_section_checklist())
    lines.append("")

    # ── 13. Transform Catalog ─────────────────────────────────────────────
    lines.append("## Transform Catalog")
    lines.append("")
    if mode == "swarm":
        lines.append(
            "Select 4 transforms that are applicable to THIS query, maximizing "
            "structural diversity (each must attack a different part of the "
            "execution plan)."
        )
    else:
        lines.append(
            "Select the best transform (or compound strategy of 2-3 transforms) "
            "that maximizes expected speedup for THIS query."
        )
    lines.append("")
    lines.append("### Predicate Movement")
    lines.append(
        "- **global_predicate_pushdown**: Trace selective predicates from late "
        "in the CTE chain back to the earliest scan via join equivalences. "
        "Biggest win when a dimension filter is applied after a large "
        "intermediate materialization.\n"
        "  Maps to examples: pushdown, early_filter, date_cte_isolate"
    )
    lines.append(
        "- **transitive_predicate_propagation**: Infer predicates through join "
        "equivalence chains (A.key = B.key AND B.key = 5 -> A.key = 5). "
        "Especially across CTE boundaries where optimizers stop propagating.\n"
        "  Maps to examples: early_filter, dimension_cte_isolate"
    )
    lines.append(
        "- **null_rejecting_join_simplification**: When downstream WHERE "
        "rejects NULLs from the outer side of a LEFT JOIN, convert to INNER. "
        "Enables reordering and predicate pushdown. CHECK: does the query "
        "actually have LEFT/OUTER joins before assigning this.\n"
        "  Maps to examples: (no direct gold example — novel transform)"
    )
    lines.append("")
    lines.append("### Join Restructuring")
    lines.append(
        "- **self_join_elimination**: When a UNION ALL CTE is self-joined N "
        "times with each join filtering to a different discriminator, split "
        "into N pre-partitioned CTEs. Eliminates discriminator filtering "
        "and repeated hash probes on rows that don't match.\n"
        "  Maps to examples: union_cte_split, shared_dimension_multi_channel"
    )
    lines.append(
        "- **decorrelation**: Convert correlated EXISTS/IN/scalar subqueries "
        "to CTE + JOIN. CHECK: does the query actually have correlated "
        "subqueries before assigning this.\n"
        "  Maps to examples: decorrelate, composite_decorrelate_union"
    )
    lines.append(
        "- **aggregate_pushdown**: When GROUP BY follows a multi-table join but "
        "aggregation only uses columns from one side, push the GROUP BY below "
        "the join. CHECK: verify the join doesn't change row multiplicity "
        "for the aggregate (one-to-many breaks AVG/STDDEV).\n"
        "  Maps to examples: (no direct gold example — novel transform)"
    )
    lines.append(
        "- **late_attribute_binding**: When a dimension table is joined only to "
        "resolve display columns (names, descriptions) that aren't used in "
        "filters, aggregations, or join conditions, defer that join until after "
        "all filtering and aggregation is complete. Join on the surrogate key "
        "once against the final reduced result set. This eliminates N-1 "
        "dimension scans when the CTE references the dimension N times. "
        "CHECK: verify the deferred columns aren't used in WHERE, GROUP BY, "
        "or JOIN ON — only in the final SELECT.\n"
        "  Maps to examples: dimension_cte_isolate (partial pattern), early_filter"
    )
    lines.append("")
    lines.append("### Scan Optimization")
    lines.append(
        "- **star_join_prefetch**: Pre-filter ALL dimension tables into CTEs, "
        "then probe fact table with the combined key intersection.\n"
        "  Maps to examples: dimension_cte_isolate, multi_dimension_prefetch, "
        "prefetch_fact_join, date_cte_isolate"
    )
    lines.append(
        "- **single_pass_aggregation**: Merge N subqueries on the same fact "
        "table into 1 scan with CASE/FILTER inside aggregates. "
        "CHECK: STDDEV_SAMP/VARIANCE are grouping-sensitive — FILTER "
        "over a combined group != separate per-group computation.\n"
        "  Maps to examples: single_pass_aggregation, channel_bitmap_aggregation"
    )
    lines.append(
        "- **scan_consolidation_pivot**: When a CTE is self-joined N times "
        "with each reference filtering to a different discriminator (e.g., year, "
        "channel), consolidate into fewer scans that GROUP BY the discriminator, "
        "then pivot rows to columns using MAX(CASE WHEN discriminator = X THEN "
        "agg_value END). This halves the fact scans and dimension joins. "
        "SAFE when GROUP BY includes the discriminator — each group is naturally "
        "partitioned, so aggregates like STDDEV_SAMP are computed correctly "
        "per-partition. The pivot MAX is just a row selector (one row per group), "
        "not a real aggregation.\n"
        "  Maps to examples: single_pass_aggregation, union_cte_split"
    )
    lines.append("")
    lines.append("### Structural Transforms")
    lines.append(
        "- **union_consolidation**: Share dimension lookups across UNION ALL "
        "branches that scan different fact tables with the same dim joins.\n"
        "  Maps to examples: shared_dimension_multi_channel"
    )
    lines.append(
        "- **window_optimization**: Push filters before window functions when "
        "they don't affect the frame. Convert ROW_NUMBER + filter to LATERAL "
        "+ LIMIT. Merge same-PARTITION windows into one sort pass.\n"
        "  Maps to examples: deferred_window_aggregation"
    )
    lines.append(
        "- **exists_restructuring**: Convert INTERSECT to EXISTS for semi-join "
        "short-circuit, or restructure complex EXISTS with shared CTEs. "
        "CHECK: does the query actually have INTERSECT or complex EXISTS.\n"
        "  Maps to examples: intersect_to_exists, multi_intersect_exists_cte"
    )
    lines.append("")

    # ── 13b. Strategy Leaderboard (observed success rates) ────────────────
    if strategy_leaderboard and query_archetype:
        leaderboard_section = _section_strategy_leaderboard(
            strategy_leaderboard, query_archetype
        )
        if leaderboard_section:
            lines.append(leaderboard_section)
            lines.append("")

    # ── 14. Strategy Selection Rules ──────────────────────────────────────
    lines.append("## Strategy Selection Rules")
    lines.append("")
    lines.append(
        "1. **CHECK APPLICABILITY**: Each transform has a structural prerequisite "
        "(correlated subquery, UNION ALL CTE, LEFT JOIN, etc.). Verify the "
        "query actually has the prerequisite before assigning a transform. "
        "DO NOT assign decorrelation if there are no correlated subqueries."
    )
    lines.append(
        "2. **CHECK OPTIMIZER OVERLAP**: Read the EXPLAIN plan. If the optimizer "
        "already performs a transform (e.g., already splits a UNION CTE, "
        "already pushes a predicate), that transform will have marginal "
        "benefit. Note this in your reasoning and prefer transforms the "
        "optimizer is NOT already doing."
    )
    if mode == "swarm":
        lines.append(
            "3. **MAXIMIZE DIVERSITY**: Each worker must attack a different part of "
            "the execution plan. Do not assign 'pushdown variant A' and "
            "'pushdown variant B'. Assign transforms from different categories above."
        )
    else:
        lines.append(
            "3. **MAXIMIZE EXPECTED VALUE**: Select the single strategy with the "
            "highest expected speedup, considering both the magnitude of the "
            "bottleneck it addresses and the historical success rate."
        )
    lines.append(
        "4. **ASSESS RISK PER-QUERY**: Risk is a function of (transform x "
        "query complexity), not an inherent property of the transform. "
        "Decorrelation is low-risk on a simple EXISTS and high-risk on "
        "nested correlation inside a CTE. Assess per-assignment."
    )
    lines.append(
        "5. **COMPOSITION IS ALLOWED AND ENCOURAGED**: A strategy can "
        "combine 2-3 transforms from different categories (e.g., "
        "star_join_prefetch + scan_consolidation_pivot, or date_cte_isolate + "
        "early_filter + decorrelate). The TARGET_LOGICAL_TREE should reflect the combined "
        "structure. Compound strategies are often the source of the biggest wins."
    )
    if mode == "swarm":
        lines.append(
            "6. **MINIMAL-CHANGE BASELINE**: If the EXPLAIN shows the optimizer "
            "already handles the primary bottleneck (e.g., already splits CTEs, "
            "already pushes predicates), consider assigning one worker as a "
            "minimal-change baseline: explicit JOINs only, no structural changes. "
            "This provides a regression-safe fallback."
        )
    lines.append("")

    if mode == "swarm":
        lines.append(
            "Each worker gets 1-3 examples. If fewer than 2 examples genuinely "
            "match the worker's strategy, assign 1 and state 'No additional examples "
            "apply.' Do NOT pad with irrelevant examples — an irrelevant example is "
            "worse than no example because the worker will try to apply its pattern. "
            "No duplicate examples across workers. Use example IDs from the catalog above."
        )
    else:
        lines.append(
            "Select 1-3 examples that genuinely match the strategy. "
            "Do NOT pad with irrelevant examples — an irrelevant example is "
            "worse than no example. Use example IDs from the catalog above."
        )
    lines.append("")
    lines.append(
        "For TARGET_LOGICAL_TREE: Define the CTE structure you want produced. "
        "For NODE_CONTRACTS: Be exhaustive with OUTPUT columns — missing columns "
        "cause semantic breaks."
    )
    lines.append("")

    # ── 14b. Exploration Budget (swarm only) ──────────────────────────────
    if mode == "swarm":
        lines.append("## Exploration Budget (Worker 4)")
        lines.append("")
        lines.append(
            "Workers 1-3 follow the engine profile's proven patterns. "
            "**Worker 4 is the EXPLORATION worker** with a different mandate:"
        )
        lines.append("")
        lines.append(
            "Worker 4 MAY (in priority order — prefer higher-value exploration):\n"
            "  (c) **PREFERRED**: Attempt a novel technique not listed in the engine "
            "profile, if the EXPLAIN plan reveals an optimizer blind spot not yet "
            "documented. This is the highest-value exploration — new discoveries "
            "expand the engine profile for all future queries.\n"
            "  (b) Combine 2-3 transforms from different engine gaps into a compound "
            "strategy that hasn't been tested before. Medium value — tests "
            "interaction effects between known patterns.\n"
            "  (a) Retry a technique from 'what_didnt_work', IF the structural "
            "context of THIS query differs materially from the observed failure — "
            "explain the structural difference in HAZARD_FLAGS. Lowest priority — "
            "only when the query structure clearly diverges from the failed case."
        )
        lines.append("")
        lines.append(
            "Worker 4 may NEVER violate correctness constraints "
            "(LITERAL_PRESERVATION, SEMANTIC_EQUIVALENCE, COMPLETE_OUTPUT, "
            "CTE_COLUMN_COMPLETENESS)."
        )
        lines.append("")
        lines.append(
            "The exploration worker's output is tagged EXPLORATORY and tracked "
            "separately. Past failures documented in the engine profile are "
            "context-specific — they happened on specific queries with specific "
            "structures. Worker 4's job is to test whether those failures "
            "generalize or not. If Worker 4 discovers a new win, it becomes "
            "field intelligence for the engine profile."
        )
        lines.append("")

    # ── 15. Output Consumption Spec (swarm only) ─────────────────────────
    if mode == "swarm":
        lines.append("## Output Consumption Spec")
        lines.append("")
        lines.append(
            "Each worker receives:\n"
            "1. SHARED BRIEFING (SEMANTIC_CONTRACT + BOTTLENECK_DIAGNOSIS + "
            "ACTIVE_CONSTRAINTS + REGRESSION_WARNINGS)\n"
            "2. Their specific WORKER N BRIEFING (STRATEGY + TARGET_LOGICAL_TREE + "
            "NODE_CONTRACTS + EXAMPLES + EXAMPLE_ADAPTATION + HAZARD_FLAGS)\n"
            "3. Full before/after SQL for their assigned examples (retrieved by example ID)\n"
            "4. The original query SQL (full, as reference)\n"
            "5. Column completeness contract + output format spec\n\n"
            "Workers do NOT see other workers' briefings.\n"
            "Presentation order: briefing first (understanding), then examples "
            "(patterns), then original SQL (source), then output format (mechanics)."
        )

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Script-level oneshot — full multi-statement pipeline in one prompt
# ═══════════════════════════════════════════════════════════════════════════


def build_script_oneshot_prompt(
    sql_script: str,
    script_dag: Any,  # ScriptDAG from script_parser.py
    dialect: str = "duckdb",
    explain_plans: Optional[Dict[str, str]] = None,
    engine_profile: Optional[Dict[str, Any]] = None,
    matched_examples: Optional[List[Dict[str, Any]]] = None,
    constraints: Optional[List[Dict[str, Any]]] = None,
    regression_warnings: Optional[List[Dict[str, Any]]] = None,
    resource_envelope: Optional[str] = None,
) -> str:
    """Build a oneshot prompt for an entire multi-statement SQL pipeline.

    Unlike the single-query oneshot which optimizes one SELECT in isolation,
    this prompt gives the model the FULL script + dependency graph so it can
    reason about cross-statement optimizations:
      - Predicate pushdown across materialization boundaries
      - Redundant scan elimination across statements
      - Materialization point optimization (table vs view vs CTE)
      - Filter propagation from downstream consumers to upstream producers

    Args:
        sql_script: Full SQL script text (all statements)
        script_dag: ScriptDAG with dependency graph and optimization targets
        dialect: SQL dialect
        explain_plans: Map of object_name -> EXPLAIN ANALYZE text for each
            optimization target. Collected by running the pipeline stages
            in dependency order against a real database.
        engine_profile: Engine optimizer strengths/gaps (optional)
        matched_examples: Gold examples for pattern matching (optional)
        constraints: Correctness constraints (optional)
        regression_warnings: Known regression patterns (optional)

    Returns:
        Complete prompt text for a single LLM call
    """
    lines: List[str] = []

    # ── 1. Role framing ──────────────────────────────────────────────
    lines.append(
        "You are a senior SQL pipeline optimization architect. Your job is to "
        "analyze a multi-statement SQL data pipeline end-to-end and produce "
        "optimized rewrites that exploit cross-statement optimization "
        "opportunities that no single-query optimizer can see."
    )
    lines.append("")
    lines.append(
        f"**Dialect**: {dialect}. All SQL must be valid {dialect} syntax."
    )
    lines.append("")

    # ── 2. Pipeline dependency graph context ──────────────────────────────────────
    lines.append("## Pipeline Dependency Graph")
    lines.append("")
    lines.append(
        "This script is a data pipeline. Each CREATE TABLE/VIEW is a pipeline "
        "stage that materializes intermediate results. The dependency graph below "
        "shows what each stage creates, what it depends on, and which stages "
        "have enough structural complexity to be worth optimizing."
    )
    lines.append("")
    lines.append("```")
    lines.append(script_dag.summary())
    lines.append("```")
    lines.append("")

    # Lineage narrative for optimization targets
    targets = script_dag.optimization_targets()
    if targets:
        lines.append("### Key Optimization Chains")
        lines.append("")
        for target in targets:
            deps = sorted(target.references & set(script_dag._creates_index.keys()))
            if deps:
                lines.append(
                    f"- **{target.creates_object}** (complexity={target.complexity_score}) "
                    f"← depends on: {', '.join(deps)}"
                )
        lines.append("")

    # ── 3. EXPLAIN ANALYZE plans ───────────────────────────────────
    if explain_plans:
        lines.append("## EXPLAIN ANALYZE Plans")
        lines.append("")
        lines.append(
            "Execution plans for each optimization target, collected by "
            "running the pipeline in dependency order against a real database. "
            "Use these to identify actual cost hotspots, scan sizes, and "
            "join strategies."
        )
        lines.append("")
        for target in targets:
            name = target.creates_object
            if name and name in explain_plans:
                lines.append(f"### {name} (complexity={target.complexity_score})")
                lines.append("")
                lines.append("```")
                lines.append(explain_plans[name].strip())
                lines.append("```")
                lines.append("")

    # ── 4. Cross-statement optimization opportunities ────────────────
    lines.append("## Cross-Statement Optimization Opportunities")
    lines.append("")
    lines.append(
        "These are the high-value patterns to look for in multi-statement "
        "pipelines. Single-query optimizers CANNOT do these — they require "
        "seeing the full pipeline:"
    )
    lines.append("")
    lines.append(
        "1. **Predicate pushdown across materialization boundaries**: "
        "A downstream stage filters on a column that exists in an upstream "
        "view/table. Push that filter into the upstream definition so it "
        "scans less data from the start. This is the #1 win in pipeline "
        "optimization."
    )
    lines.append(
        "2. **Redundant scan elimination**: The same base table is scanned "
        "by multiple views/stages with overlapping columns. Consolidate "
        "into a shared CTE or materialized stage."
    )
    lines.append(
        "3. **Materialization point optimization**: Some intermediate tables "
        "exist only because the author couldn't express the logic as CTEs. "
        "Converting temp tables to CTEs within the consuming query lets the "
        "optimizer see through the boundary."
    )
    lines.append(
        "4. **Filter propagation**: Downstream consumers apply filters "
        "(e.g., `WHERE calendar_date = max(...)`, `WHERE customer_type IN (...)`). "
        "If the upstream stage doesn't filter, it's scanning unnecessary data. "
        "Propagate the filter upstream."
    )
    lines.append(
        "5. **Join elimination**: If an upstream stage joins a table only to "
        "produce columns that no downstream consumer uses, that join can be "
        "removed."
    )
    lines.append("")

    # ── 4. Engine profile (if available) ─────────────────────────────
    if engine_profile:
        lines.append("## Engine Profile")
        lines.append("")
        if "briefing_note" in engine_profile:
            lines.append(engine_profile["briefing_note"])
            lines.append("")
        gaps = engine_profile.get("gaps", [])
        if gaps:
            lines.append("### Optimizer Gaps (exploit these)")
            lines.append("")
            for gap in gaps:
                lines.append(
                    f"- **{gap.get('id', '?')}**: {gap.get('what', '')}"
                )
            lines.append("")

    # ── 5. Examples (if available) ───────────────────────────────────
    if matched_examples:
        lines.append("## Optimization Examples")
        lines.append("")
        for ex in matched_examples[:5]:
            ex_id = ex.get("id", "?")
            desc = ex.get("description", "")
            speedup = ex.get("speedup", "?")
            lines.append(f"- **{ex_id}** ({speedup}x): {desc}")
        lines.append("")

    # ── 6. Full SQL script ───────────────────────────────────────────
    lines.append("## Complete SQL Pipeline")
    lines.append("")
    lines.append(
        "Below is the COMPLETE pipeline. Every statement is shown — views, "
        "temp tables, drops, selects. Read the full pipeline before proposing "
        "any changes. Your rewrites must maintain semantic equivalence for "
        "every downstream consumer."
    )
    lines.append("")
    lines.append("```sql")
    lines.append(sql_script.strip())
    lines.append("```")
    lines.append("")

    # ── 7. Analysis steps ────────────────────────────────────────────
    lines.append("## Your Analysis Steps")
    lines.append("")
    lines.append(
        "1. **TRACE DATA FLOW**: Follow the pipeline dependency graph from base tables "
        "to final outputs."
    )
    lines.append(
        "2. **IDENTIFY FILTER BOUNDARIES**: Where do filters first appear? "
        "Can they be pushed earlier in the chain?"
    )
    lines.append(
        "3. **MAP BASE TABLE SCANS**: Which base tables are scanned by "
        "multiple stages? Can scans be consolidated?"
    )
    lines.append(
        "4. **CHECK MATERIALIZATION NECESSITY**: Does each temp table NEED "
        "to be materialized, or could it be inlined as a CTE?"
    )
    lines.append(
        "5. **WRITE REWRITES**: For each statement you change, produce a "
        "component payload in the output format below."
    )
    lines.append("")

    # ── 7b. PG SET LOCAL config (script-level oneshot) ──────────────────
    if dialect in ("postgres", "postgresql") and resource_envelope:
        from .worker import _section_set_local_config
        lines.append(_section_set_local_config(resource_envelope))
        lines.append("")

    # ── 8. Output format (DAP multi-statement) ─────────────────────────
    lines.append("## Output Format")
    lines.append("")
    lines.append(
        "First output a **Modified Logic Tree** for the pipeline, showing "
        "which statements and components changed."
    )
    lines.append("")
    lines.append(
        "Then output a **Component Payload JSON** with one statement entry "
        "per pipeline stage you modify."
    )
    lines.append("")
    lines.append("```json")
    lines.append("{")
    lines.append('  "spec_version": "1.0",')
    lines.append('  "dialect": "<dialect>",')
    lines.append('  "rewrite_rules": [')
    lines.append('    {"id": "R1", "type": "<transform>", "description": "<what>", "applied_to": ["<stmt.comp>"]}')
    lines.append("  ],")
    lines.append('  "statements": [')
    lines.append("    {")
    lines.append('      "target_table": "<table_or_view_name>",')
    lines.append('      "change": "modified",')
    lines.append('      "components": {')
    lines.append('        "<cte_name>": {"type": "cte", "change": "modified", "sql": "<CTE body>", "interfaces": {"outputs": ["col1"], "consumes": []}},')
    lines.append('        "main_query": {"type": "main_query", "change": "modified", "sql": "<SELECT>", "interfaces": {"outputs": ["col1"], "consumes": ["<cte_name>"]}}')
    lines.append("      },")
    lines.append('      "reconstruction_order": ["<cte_name>", "main_query"],')
    lines.append('      "assembly_template": "CREATE TABLE <target> AS WITH <cte> AS ({<cte>}) {main_query}"')
    lines.append("    }")
    lines.append("  ],")
    lines.append('  "macros": {},')
    lines.append('  "frozen_blocks": [],')
    if dialect in ("postgres", "postgresql"):
        lines.append('  "runtime_config": ["SET LOCAL work_mem = \'512MB\'"],')
    lines.append('  "validation_checks": []')
    lines.append("}")
    lines.append("```")
    lines.append("")
    lines.append("### Rules")
    lines.append("- Tree first — generate the pipeline Logic Tree before writing any SQL")
    lines.append(
        "- Each `statements[]` entry targets a specific pipeline stage "
        "(must match a CREATE in the script)"
    )
    lines.append("- Only include statements you actually change")
    lines.append(
        "- Output columns of each stage must remain identical "
        "(downstream consumers depend on them)"
    )
    lines.append(
        "- Rewrites must be semantically equivalent for ALL downstream "
        "consumers, not just the immediate next stage"
    )
    lines.append("- No ellipsis — every `sql` value must be complete, executable SQL")
    if dialect in ("postgres", "postgresql"):
        lines.append("- `runtime_config`: SET LOCAL commands for PostgreSQL. Omit if not needed")
    lines.append("")
    lines.append("After the JSON, explain the overall pipeline optimization:")
    lines.append("")
    lines.append("```")
    lines.append(
        "Pipeline changes: <2-4 sentences: what cross-statement "
        "optimizations and why>"
    )
    lines.append("Expected overall speedup: <estimate>")
    lines.append("```")
    lines.append("")

    # ── 9. Validation checklist ──────────────────────────────────────
    lines.append("## Pipeline Validation Checklist")
    lines.append("")
    lines.append(
        "- Every modified stage preserves its output schema "
        "(column names, types, row semantics)"
    )
    lines.append(
        "- Filters pushed upstream do not remove rows that "
        "downstream consumers need"
    )
    lines.append(
        "- If a temp table is inlined as CTE, ALL consumers of "
        "that table must be updated"
    )
    lines.append(
        "- Redundant scan consolidation must not change join cardinality"
    )
    lines.append("- All literal values preserved exactly")
    lines.append("")

    return "\n".join(lines)
