"""Q-Error analysis for EXPLAIN ANALYZE plans (DuckDB + PostgreSQL).

Extracts cardinality estimation errors (Q-Error), derives categorical
variables (DIRECTION, LOCUS, MAGNITUDE), routes to pathology candidates,
and produces prompt-ready text for the analyst briefing (§2b-i).

Supported formats:
  DuckDB (EXPLAIN (ANALYZE, FORMAT JSON)):
    node.operator_cardinality                   = actual rows
    node.extra_info["Estimated Cardinality"]    = planner estimate (may have ~ prefix)
    node.operator_timing                        = operator self-time (seconds)
    node.children                               = child nodes (recursive)

  PostgreSQL (EXPLAIN (ANALYZE, FORMAT JSON)):
    node["Actual Rows"]      = actual rows (× Actual Loops)
    node["Plan Rows"]        = planner estimate
    node["Actual Total Time"] - node["Actual Startup Time"] = self-time (ms)
    node["Plans"]            = child nodes (recursive)

Q-Error = max(estimated/actual, actual/estimated), symmetric, ≥ 1.0.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Node type → locus category mapping ───────────────────────────────────

_LOCUS_MAP: Dict[str, str] = {
    # ── DuckDB node types ──
    # Joins
    "HASH_JOIN": "JOIN",
    "NESTED_LOOP_JOIN": "JOIN",
    "PIECEWISE_MERGE_JOIN": "JOIN",
    "CROSS_PRODUCT": "JOIN",
    "LEFT_DELIM_JOIN": "JOIN",
    "RIGHT_DELIM_JOIN": "JOIN",
    "BLOCKWISE_NL_JOIN": "JOIN",
    "IE_JOIN": "JOIN",
    "ASOF_JOIN": "JOIN",
    "POSITIONAL_JOIN": "JOIN",
    # Scans
    "TABLE_SCAN": "SCAN",
    "SEQ_SCAN": "SCAN",
    "INDEX_SCAN": "SCAN",
    "DELIM_SCAN": "SCAN",
    "CTE_SCAN": "CTE",
    "COLUMN_DATA_SCAN": "SCAN",
    "CHUNK_SCAN": "SCAN",
    # Aggregates
    "HASH_GROUP_BY": "AGGREGATE",
    "PERFECT_HASH_GROUP_BY": "AGGREGATE",
    "UNGROUPED_AGGREGATE": "AGGREGATE",
    "STREAMING_WINDOW": "AGGREGATE",
    # Filters
    "FILTER": "FILTER",
    # Projections / other
    "PROJECTION": "PROJECTION",
    "ORDER_BY": "PROJECTION",
    "TOP_N": "PROJECTION",
    "LIMIT": "PROJECTION",
    "UNION": "PROJECTION",
    "EXCEPT": "PROJECTION",
    "INTERSECT": "PROJECTION",
    "DISTINCT": "PROJECTION",
    "RECURSIVE_CTE": "CTE",
    "RESULT_COLLECTOR": "PROJECTION",
    "EXPLAIN_ANALYZE": "PROJECTION",
    # ── PostgreSQL node types ──
    # Joins
    "Hash Join": "JOIN",
    "Merge Join": "JOIN",
    "Nested Loop": "JOIN",
    # Scans
    "Seq Scan": "SCAN",
    "Index Scan": "SCAN",
    "Index Only Scan": "SCAN",
    "Bitmap Heap Scan": "SCAN",
    "Bitmap Index Scan": "SCAN",
    "CTE Scan": "CTE",
    "Subquery Scan": "SCAN",
    "Function Scan": "SCAN",
    "Values Scan": "SCAN",
    # Aggregates
    "Aggregate": "AGGREGATE",
    "GroupAggregate": "AGGREGATE",
    "HashAggregate": "AGGREGATE",
    "WindowAgg": "AGGREGATE",
    # Filters (PG puts filter as property, not node — but Result can act as filter)
    "Result": "FILTER",
    # Projections / other
    "Sort": "PROJECTION",
    "Limit": "PROJECTION",
    "Unique": "PROJECTION",
    "SetOp": "PROJECTION",
    "Append": "PROJECTION",
    "MergeAppend": "PROJECTION",
    "Materialize": "PROJECTION",
    "Gather": "PROJECTION",
    "Gather Merge": "PROJECTION",
    "BitmapAnd": "FILTER",
    "BitmapOr": "FILTER",
}

# ── Routing table: (locus, direction) → pathology candidates ─────────────
# From DEEP_ANALYSIS.txt §7 enhanced symptom routing table.

_ROUTING_TABLE: Dict[tuple[str, str], list[str]] = {
    ("AGGREGATE", "OVER_EST"): ["P3"],
    ("AGGREGATE", "UNDER_EST"): ["P3"],
    ("CTE", "ZERO_EST"): ["P0", "P7"],
    ("CTE", "UNDER_EST"): ["P2", "P0"],
    ("CTE", "OVER_EST"): ["P7", "P0"],
    ("FILTER", "OVER_EST"): ["P9", "P0"],
    ("FILTER", "UNDER_EST"): ["P0"],
    ("JOIN", "UNDER_EST"): ["P2", "P0"],
    ("JOIN", "ZERO_EST"): ["P0", "P2"],
    ("JOIN", "OVER_EST"): ["P5", "P0"],
    ("PROJECTION", "OVER_EST"): ["P7", "P0", "P4"],
    ("PROJECTION", "UNDER_EST"): ["P6", "P5", "P0"],
    ("PROJECTION", "ZERO_EST"): ["P6", "P0"],
    ("SCAN", "OVER_EST"): ["P1", "P4"],
    ("SCAN", "UNDER_EST"): ["P0"],
    ("SCAN", "ZERO_EST"): ["P2"],
}


# ── Dataclasses ──────────────────────────────────────────────────────────

@dataclass
class QErrorSignal:
    """Q-Error analysis for a single plan node."""
    node_type: str
    estimated: int
    actual: int
    q_error: float
    direction: str          # OVER_EST | UNDER_EST | ZERO_EST | ACCURATE
    timing_ms: float        # operator self-time in ms

    @property
    def locus(self) -> str:
        return _LOCUS_MAP.get(self.node_type, "PROJECTION")


@dataclass
class QErrorAnalysis:
    """Full Q-Error analysis for a query plan."""
    signals: list[QErrorSignal] = field(default_factory=list)
    max_q_error: float = 1.0
    severity: str = "ACCURATE"
    direction: str = "ACCURATE"
    locus: str = "PROJECTION"
    magnitude: str = "MINOR"
    structural_flags: list[str] = field(default_factory=list)
    pathology_candidates: list[str] = field(default_factory=list)


# ── Core analysis ────────────────────────────────────────────────────────

def _parse_estimated(extra_info: dict) -> Optional[int]:
    """Parse 'Estimated Cardinality' from extra_info, handling ~ prefix."""
    raw = extra_info.get("Estimated Cardinality")
    if raw is None:
        return None
    try:
        return int(str(raw).lstrip("~").strip())
    except (ValueError, TypeError):
        return None


def _classify_direction(estimated: int, actual: int) -> str:
    if estimated == 0 and actual == 0:
        return "ACCURATE"
    if estimated == 0:
        return "ZERO_EST"
    if actual == 0:
        return "ZERO_EST"
    if estimated > actual * 1.5:
        return "OVER_EST"
    if actual > estimated * 1.5:
        return "UNDER_EST"
    return "ACCURATE"


def _classify_magnitude(q_error: float) -> str:
    if q_error > 1000:
        return "EXTREME"
    if q_error > 100:
        return "3_ORDER"
    if q_error > 10:
        return "2_ORDER"
    if q_error > 2:
        return "1_ORDER"
    return "MINOR"


def _classify_severity(max_q: float) -> str:
    if max_q > 10_000:
        return "CATASTROPHIC_BLINDNESS"
    if max_q > 100:
        return "MAJOR_HALLUCINATION"
    if max_q > 10:
        return "MODERATE_GUESS"
    if max_q > 2:
        return "MINOR_DRIFT"
    return "ACCURATE"


def _compute_q_error(estimated: int, actual: int) -> float:
    """Compute symmetric Q-Error: max(est/act, act/est)."""
    if estimated == 0 and actual == 0:
        return 1.0
    elif estimated == 0:
        return float(actual)
    elif actual == 0:
        return float(estimated)
    else:
        return max(estimated / actual, actual / estimated)


def _walk_duckdb_nodes(node: dict, signals: list[QErrorSignal], min_q: float = 2.0) -> None:
    """Recursively walk DuckDB plan tree, collecting Q-Error signals."""
    extra_info = node.get("extra_info", {})
    if not isinstance(extra_info, dict):
        extra_info = {}

    estimated = _parse_estimated(extra_info)
    actual = node.get("operator_cardinality", 0)
    op_name = node.get("operator_name") or node.get("operator_type") or "UNKNOWN"
    timing_s = node.get("operator_timing", 0) or 0

    if estimated is not None:
        q_error = _compute_q_error(estimated, actual)
        if q_error >= min_q:
            signals.append(QErrorSignal(
                node_type=op_name,
                estimated=estimated,
                actual=actual,
                q_error=q_error,
                direction=_classify_direction(estimated, actual),
                timing_ms=timing_s * 1000,
            ))

    for child in node.get("children", []):
        _walk_duckdb_nodes(child, signals, min_q)


def _walk_pg_nodes(node: dict, signals: list[QErrorSignal], min_q: float = 2.0) -> None:
    """Recursively walk PostgreSQL plan tree, collecting Q-Error signals.

    PG structure: "Plan Rows" (estimated), "Actual Rows" (actual per loop),
    "Actual Loops", "Node Type", "Plans" (children).
    """
    op_name = node.get("Node Type", "UNKNOWN")
    estimated = node.get("Plan Rows", 0)
    actual_per_loop = node.get("Actual Rows", 0)
    loops = node.get("Actual Loops", 1) or 1
    actual = actual_per_loop  # Per-loop actual is the relevant cardinality estimate comparison

    # Self-time approximation: total_time - sum of children's total_time (per loop)
    total_time = node.get("Actual Total Time", 0)
    child_time = sum(c.get("Actual Total Time", 0) for c in node.get("Plans", []))
    timing_ms = max((total_time - child_time) * loops, 0)

    if estimated is not None and (estimated > 0 or actual > 0):
        q_error = _compute_q_error(estimated, actual)
        if q_error >= min_q:
            signals.append(QErrorSignal(
                node_type=op_name,
                estimated=estimated,
                actual=actual,
                q_error=q_error,
                direction=_classify_direction(estimated, actual),
                timing_ms=timing_ms,
            ))

    for child in node.get("Plans", []):
        _walk_pg_nodes(child, signals, min_q)


def _detect_plan_format(plan_json) -> str:
    """Auto-detect whether plan_json is DuckDB or PostgreSQL format.

    Returns "duckdb", "postgres", or "unknown".
    """
    if isinstance(plan_json, dict):
        if "children" in plan_json or "operator_name" in plan_json:
            return "duckdb"
        if "Plan" in plan_json or "Node Type" in plan_json:
            return "postgres"
    if isinstance(plan_json, list) and plan_json:
        first = plan_json[0]
        if isinstance(first, dict) and ("Plan" in first or "Node Type" in first):
            return "postgres"
    return "unknown"


def analyze_plan_qerror(plan_json, dialect: Optional[str] = None) -> QErrorAnalysis:
    """Extract Q-Error signals from EXPLAIN ANALYZE JSON tree.

    Walks children recursively, computes Q-Error per node,
    derives categorical variables, routes to pathology candidates.

    Auto-detects DuckDB vs PostgreSQL format, or use dialect hint.

    Args:
        plan_json: DuckDB analyzed_plan dict (top-level has 'children')
                   or PostgreSQL plan list/dict (top-level has 'Plan')
        dialect: Optional hint — "duckdb" or "postgresql"/"postgres"

    Returns:
        QErrorAnalysis with signals, severity, direction, locus, magnitude,
        structural_flags, and pathology_candidates.
    """
    if not plan_json:
        return QErrorAnalysis()

    # Normalize: PG sometimes wraps in a list [{"Plan": {...}}]
    raw = plan_json
    if isinstance(raw, list):
        if len(raw) > 0 and isinstance(raw[0], dict):
            raw = raw[0]
        else:
            return QErrorAnalysis()

    if not isinstance(raw, dict):
        return QErrorAnalysis()

    # Detect format
    fmt = dialect or ""
    if fmt.lower().startswith("postgres"):
        fmt = "postgres"
    elif fmt.lower() == "duckdb":
        fmt = "duckdb"
    else:
        fmt = _detect_plan_format(raw)

    # Walk all nodes
    signals: list[QErrorSignal] = []

    if fmt == "postgres":
        # PG: raw might be {"Plan": {...}, "Execution Time": ...} or the Plan node itself
        plan_root = raw.get("Plan", raw)
        _walk_pg_nodes(plan_root, signals)
    else:
        # DuckDB: top-level has 'children' (list of operator trees)
        children = raw.get("children", [])
        if children:
            for child in children:
                _walk_duckdb_nodes(child, signals)
        else:
            _walk_duckdb_nodes(raw, signals)

    if not signals:
        analysis = QErrorAnalysis()
        analysis.structural_flags = extract_structural_flags(plan_json)
        return analysis

    # Sort by Q-Error descending
    signals.sort(key=lambda s: s.q_error, reverse=True)

    max_q = signals[0].q_error
    worst = signals[0]

    # Dominant direction: direction of worst signal
    direction = worst.direction

    # Locus: locus category of worst signal
    locus = worst.locus

    # Magnitude of worst signal
    magnitude = _classify_magnitude(max_q)

    # Severity
    severity = _classify_severity(max_q)

    # Structural flags (free proxy signals)
    structural_flags = extract_structural_flags(plan_json)

    # Route to pathology candidates via (locus, direction) table
    pathology_set: list[str] = []
    seen: set[str] = set()

    # Primary routing from worst signal
    key = (locus, direction)
    for p in _ROUTING_TABLE.get(key, []):
        if p not in seen:
            pathology_set.append(p)
            seen.add(p)

    # Secondary routing from other significant signals (Q > 100)
    for sig in signals[1:5]:
        key2 = (sig.locus, sig.direction)
        for p in _ROUTING_TABLE.get(key2, []):
            if p not in seen:
                pathology_set.append(p)
                seen.add(p)

    # Add structural flag-based routing
    for flag in structural_flags:
        if flag == "DELIM_SCAN" and "P2" not in seen:
            pathology_set.append("P2")
            seen.add("P2")
        elif flag == "REPEATED_TABLE" and "P1" not in seen:
            pathology_set.append("P1")
            seen.add("P1")
        elif flag == "EST_ZERO" and "P0" not in seen:
            pathology_set.append("P0")
            seen.add("P0")
        elif flag == "INTERSECT_EXCEPT" and "P6" not in seen:
            pathology_set.append("P6")
            seen.add("P6")
        elif flag == "LEFT_JOIN" and "P5" not in seen:
            pathology_set.append("P5")
            seen.add("P5")

    return QErrorAnalysis(
        signals=signals,
        max_q_error=max_q,
        severity=severity,
        direction=direction,
        locus=locus,
        magnitude=magnitude,
        structural_flags=structural_flags,
        pathology_candidates=pathology_set,
    )


# ── Structural flags (EXPLAIN-only, no execution needed) ────────────────

def _walk_structural_duckdb(node: dict, flags: list[str], tables_seen: dict[str, int]) -> None:
    """Walk DuckDB plan tree collecting structural proxy signals."""
    extra_info = node.get("extra_info", {})
    if not isinstance(extra_info, dict):
        extra_info = {}
    op_name = node.get("operator_name") or node.get("operator_type") or ""

    # EST_ZERO: planner estimated 0 rows
    estimated = _parse_estimated(extra_info)
    if estimated == 0 and op_name not in ("RESULT_COLLECTOR", "EXPLAIN_ANALYZE", "PROJECTION"):
        if "EST_ZERO" not in flags:
            flags.append("EST_ZERO")

    # EST_ONE_NONLEAF: est=1 on non-leaf node (planner guessing)
    children = node.get("children", [])
    if estimated == 1 and children and op_name not in ("LIMIT", "TOP_N", "RESULT_COLLECTOR"):
        if "EST_ONE_NONLEAF" not in flags:
            flags.append("EST_ONE_NONLEAF")

    # DELIM_SCAN: decorrelation marker
    if op_name in ("DELIM_SCAN", "LEFT_DELIM_JOIN", "RIGHT_DELIM_JOIN", "DELIM_JOIN"):
        if "DELIM_SCAN" not in flags:
            flags.append("DELIM_SCAN")

    # Track table scans for REPEATED_TABLE
    if op_name in ("TABLE_SCAN", "SEQ_SCAN", "INDEX_SCAN"):
        table = extra_info.get("Table", "")
        if table:
            tables_seen[table] = tables_seen.get(table, 0) + 1

    # LEFT_JOIN
    join_type = extra_info.get("Join Type", "")
    if "LEFT" in str(join_type).upper() and op_name not in ("LEFT_DELIM_JOIN",):
        if "LEFT_JOIN" not in flags:
            flags.append("LEFT_JOIN")

    # INTERSECT_EXCEPT
    if op_name in ("INTERSECT", "EXCEPT"):
        if "INTERSECT_EXCEPT" not in flags:
            flags.append("INTERSECT_EXCEPT")

    for child in children:
        _walk_structural_duckdb(child, flags, tables_seen)


def _walk_structural_pg(node: dict, flags: list[str], tables_seen: dict[str, int]) -> None:
    """Walk PostgreSQL plan tree collecting structural proxy signals."""
    op_name = node.get("Node Type", "")
    estimated = node.get("Plan Rows", 0)
    children = node.get("Plans", [])

    # EST_ZERO: planner estimated 0 rows
    if estimated == 0 and op_name not in ("Result", "Limit"):
        if "EST_ZERO" not in flags:
            flags.append("EST_ZERO")

    # EST_ONE_NONLEAF: est=1 on non-leaf node
    if estimated == 1 and children and op_name not in ("Limit", "Result"):
        if "EST_ONE_NONLEAF" not in flags:
            flags.append("EST_ONE_NONLEAF")

    # Track table scans for REPEATED_TABLE
    if op_name in ("Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan"):
        table = node.get("Relation Name", "")
        if table:
            tables_seen[table] = tables_seen.get(table, 0) + 1

    # LEFT_JOIN
    join_type = node.get("Join Type", "")
    if "Left" in str(join_type):
        if "LEFT_JOIN" not in flags:
            flags.append("LEFT_JOIN")

    # SetOp (INTERSECT/EXCEPT)
    if op_name == "SetOp":
        cmd = node.get("Command", "")
        if cmd in ("Intersect", "Intersect All", "Except", "Except All"):
            if "INTERSECT_EXCEPT" not in flags:
                flags.append("INTERSECT_EXCEPT")

    # CTE Scan
    if op_name == "CTE Scan":
        pass  # counted after walk for MULTI_CTE

    # Subplan (correlated subquery marker — PG equivalent of DELIM_SCAN)
    parent_rel = node.get("Parent Relationship", "")
    if parent_rel == "SubPlan":
        if "CORRELATED_SUBPLAN" not in flags:
            flags.append("CORRELATED_SUBPLAN")

    for child in children:
        _walk_structural_pg(child, flags, tables_seen)


def extract_structural_flags(plan_json, dialect: Optional[str] = None) -> list[str]:
    """Extract EXPLAIN-only proxy signals (no execution needed).

    Detects: EST_ZERO, EST_ONE_NONLEAF, DELIM_SCAN/CORRELATED_SUBPLAN,
    REPEATED_TABLE, LEFT_JOIN, INTERSECT_EXCEPT.
    """
    if not plan_json:
        return []

    raw = plan_json
    if isinstance(raw, list):
        if len(raw) > 0 and isinstance(raw[0], dict):
            raw = raw[0]
        else:
            return []
    if not isinstance(raw, dict):
        return []

    fmt = dialect or ""
    if fmt.lower().startswith("postgres"):
        fmt = "postgres"
    elif fmt.lower() == "duckdb":
        fmt = "duckdb"
    else:
        fmt = _detect_plan_format(raw)

    flags: list[str] = []
    tables_seen: dict[str, int] = {}

    if fmt == "postgres":
        plan_root = raw.get("Plan", raw)
        _walk_structural_pg(plan_root, flags, tables_seen)
    else:
        children = raw.get("children", [])
        if children:
            for child in children:
                _walk_structural_duckdb(child, flags, tables_seen)
        else:
            _walk_structural_duckdb(raw, flags, tables_seen)

    # REPEATED_TABLE: same table scanned 2+ times
    for table, count in tables_seen.items():
        if count >= 2:
            if "REPEATED_TABLE" not in flags:
                flags.append("REPEATED_TABLE")
            break

    return flags


# ── Prompt formatter ─────────────────────────────────────────────────────

def _fmt_count(n: int) -> str:
    """Format a number compactly."""
    if abs(n) >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if abs(n) >= 10_000:
        return f"{n / 1_000:.0f}K"
    if abs(n) >= 1_000:
        return f"{n:,}"
    return str(n)


def format_qerror_for_prompt(analysis: QErrorAnalysis) -> str:
    """Render Q-Error analysis as prompt-ready text block for §2b-i.

    Only includes the signals empirically validated as predictive:
    - Locus + Direction → Pathology routing (85% accuracy on 41 validated wins)
    - Structural flags (direct transform triggers)

    Deliberately EXCLUDES severity/magnitude (flat win rate across all levels,
    not predictive of optimization opportunity).
    """
    if not analysis.signals and not analysis.structural_flags:
        return ""

    lines: list[str] = []
    lines.append("### §2b-i. Cardinality Estimation Routing (Q-Error)")
    lines.append("")

    if analysis.signals:
        worst = analysis.signals[0]
        direction_desc = {
            "UNDER_EST": "actual >> estimated — planner under-provisions this operator",
            "OVER_EST": "estimated >> actual — planner over-provisions, redundant work likely",
            "ZERO_EST": "planner estimated 0 — no statistics propagated (CTE/subquery boundary)",
        }.get(analysis.direction, analysis.direction)
        lines.append(f"Direction: {analysis.direction} ({direction_desc})")
        lines.append(f"Locus: {analysis.locus} — worst mismatch at {worst.node_type} "
                      f"(est={_fmt_count(worst.estimated)}, act={_fmt_count(worst.actual)})")
        lines.append("")

    if analysis.pathology_candidates:
        lines.append(f"Pathology routing: {', '.join(analysis.pathology_candidates)}")
        lines.append("(Locus+Direction routing is 85% accurate at predicting where the winning transform operates)")
        lines.append("")

    if analysis.structural_flags:
        lines.append("Structural signals:")
        flag_actions = {
            "EST_ZERO": "blind to CTE/subquery stats → push predicate into CTE (P0, P7)",
            "EST_ONE_NONLEAF": "planner guessing on non-leaf node → check P0 (predicate pushback), P1 (repeated scans). Only P2 (decorrelation) if nested loops + correlated subquery confirmed in EXPLAIN",
            "DELIM_SCAN": "correlated subquery the optimizer couldn't decorrelate → P2",
            "CORRELATED_SUBPLAN": "correlated subquery (PG SubPlan) → decorrelation candidate (P2)",
            "REPEATED_TABLE": "same table scanned multiple times → single-pass opportunity (P1)",
            "LEFT_JOIN": "LEFT JOIN present → check if INNER conversion safe (P5)",
            "INTERSECT_EXCEPT": "INTERSECT/EXCEPT operator → EXISTS replacement (P6)",
            "MULTI_CTE": "multiple CTE references → materialization boundary check (P7)",
        }
        for flag in analysis.structural_flags:
            action = flag_actions.get(flag, flag)
            lines.append(f"  - {flag}: {action}")
        lines.append("")
        lines.append("IMPORTANT: Cross-check structural signals against the PRUNING GUIDE in §III. "
                      "If the EXPLAIN shows no nested loops, skip P2. If each table appears once, skip P1. "
                      "The pruning guide overrides routing suggestions.")
        lines.append("")

    return "\n".join(lines)
