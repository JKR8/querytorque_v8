#!/usr/bin/env python3
"""
STATE ANALYSIS System - Comprehensive analysis of all 99 TPC-DS queries
Generates strategic recommendations for next optimization moves

Strategy: Prioritize by RUNTIME (absolute time savings), not just speedup percentage
"""

import json
import csv
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from statistics import mean, median
import sys

# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class WorkerState:
    """One worker's attempt on a query"""
    worker_id: str
    speedup: float
    transforms: List[str]
    status: str
    error: Optional[str] = None
    syntax_valid: Optional[bool] = None

@dataclass
class TransformStats:
    """Statistics for a single transform across all queries"""
    transform_id: str
    verified_speedup: float = 0.0
    total_attempts: int = 0
    successes: int = 0
    failures: int = 0
    success_rate: float = 0.0
    avg_speedup_when_successful: float = 1.0
    known_issues: List[str] = field(default_factory=list)

@dataclass
class QueryState:
    """Complete state for one query"""
    query_id: str
    query_num: int
    category: str  # WIN, IMPROVED, NEUTRAL, REGRESSION, NO_DATA, ERROR
    original_ms: float
    best_speedup: float
    best_worker: Optional[str]
    expected_speedup: float
    states: Dict[str, WorkerState] = field(default_factory=dict)
    transforms_tried: Set[str] = field(default_factory=set)
    transforms_succeeded: Set[str] = field(default_factory=set)
    transforms_failed: Set[str] = field(default_factory=set)

    @property
    def runtime_percentile_rank(self) -> float:
        """Will be set after all states loaded"""
        return getattr(self, '_runtime_percentile_rank', 0.0)

@dataclass
class Recommendation:
    """Recommendation for a query"""
    transform: str
    confidence: float  # 0-100
    expected_speedup: float
    risk: str  # LOW, MEDIUM, HIGH
    rationale: str
    evidence: List[str] = field(default_factory=list)
    from_state: str = "baseline"


# ============================================================================
# TPC-DS QUERY CHARACTERISTICS
# Maps each query to its structural features for pattern matching
# ============================================================================

# Features:
#   correlated_subquery: Has correlated subquery pattern → decorrelate
#   date_filter: Joins date_dim with year/month filter → date_cte_isolate
#   multi_date_alias: Multiple date_dim aliases (d1,d2,d3) → multi_date_range_cte
#   or_condition: OR on different columns → or_to_union
#   multi_dim_filter: Multiple dimension filters → dimension_cte_isolate / multi_dimension_prefetch
#   dim_fact_chain: Dimension filter → fact join chain → prefetch_fact_join / early_filter
#   repeated_scan: Same table scanned multiple times → single_pass_aggregation / pushdown
#   intersect: Uses INTERSECT → intersect_to_exists
#   union_year: UNION ALL with year discriminator → union_cte_split
#   exists_repeat: Repeated EXISTS subqueries → materialize_cte
#   complex_multi_join: 5+ table joins, hard to decompose → EXPLAIN_ANALYZE
#   window_fn: Window functions (ROW_NUMBER, RANK) → EXPLAIN_ANALYZE (TopN issue)

TPCDS_QUERY_FEATURES = {
    1:  ["correlated_subquery", "date_filter", "dim_fact_chain"],
    2:  ["date_filter", "complex_multi_join"],
    3:  ["date_filter", "dim_fact_chain"],
    4:  ["multi_date_alias", "complex_multi_join", "correlated_subquery"],
    5:  ["date_filter", "union_year", "multi_dim_filter"],
    6:  ["correlated_subquery", "date_filter"],
    7:  ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    8:  ["correlated_subquery", "date_filter"],
    9:  ["repeated_scan"],  # 15+ scalar subqueries on same table
    10: ["date_filter", "multi_dim_filter", "exists_repeat"],
    11: ["correlated_subquery", "date_filter", "multi_date_alias"],
    12: ["date_filter", "dim_fact_chain"],
    13: ["multi_dim_filter", "dim_fact_chain"],  # demographics filters
    14: ["intersect", "date_filter", "complex_multi_join"],
    15: ["or_condition", "date_filter"],
    16: ["date_filter", "exists_repeat", "complex_multi_join"],
    17: ["multi_date_alias", "dim_fact_chain"],
    18: ["date_filter", "multi_dim_filter"],
    19: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    20: ["date_filter", "dim_fact_chain"],
    21: ["date_filter", "dim_fact_chain"],
    22: ["date_filter", "window_fn"],  # ROLLUP/grouping
    23: ["correlated_subquery", "date_filter", "complex_multi_join"],
    24: ["correlated_subquery", "complex_multi_join"],
    25: ["multi_date_alias", "dim_fact_chain"],
    26: ["multi_dim_filter", "dim_fact_chain"],
    27: ["multi_dim_filter", "dim_fact_chain"],
    28: ["repeated_scan"],  # multiple aggregation subqueries
    29: ["multi_date_alias", "dim_fact_chain"],
    30: ["correlated_subquery", "date_filter"],
    31: ["date_filter", "complex_multi_join"],
    32: ["correlated_subquery", "date_filter"],
    33: ["date_filter", "multi_dim_filter", "union_year"],
    34: ["multi_dim_filter", "dim_fact_chain"],
    35: ["date_filter", "exists_repeat", "multi_dim_filter"],
    36: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    37: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    38: ["date_filter", "intersect", "complex_multi_join"],
    39: ["date_filter", "dim_fact_chain"],
    40: ["date_filter", "dim_fact_chain"],
    41: ["correlated_subquery"],
    42: ["date_filter", "dim_fact_chain"],
    43: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    44: ["repeated_scan", "window_fn"],
    45: ["or_condition", "date_filter", "correlated_subquery"],
    46: ["multi_dim_filter", "dim_fact_chain"],
    47: ["date_filter", "dim_fact_chain", "window_fn"],
    48: ["multi_dim_filter", "or_condition"],
    49: ["date_filter", "union_year", "window_fn"],
    50: ["date_filter", "dim_fact_chain"],
    51: ["date_filter", "window_fn"],
    52: ["date_filter", "dim_fact_chain"],
    53: ["date_filter", "dim_fact_chain"],
    54: ["date_filter", "complex_multi_join"],
    55: ["date_filter", "dim_fact_chain"],
    56: ["multi_dim_filter", "union_year"],
    57: ["date_filter", "dim_fact_chain", "window_fn"],
    58: ["date_filter", "multi_date_alias"],
    59: ["date_filter", "complex_multi_join"],
    60: ["date_filter", "multi_dim_filter", "union_year"],
    61: ["date_filter", "multi_dim_filter"],
    62: ["date_filter", "dim_fact_chain"],
    63: ["date_filter", "dim_fact_chain"],  # prefetch_fact_join proven here
    64: ["complex_multi_join", "multi_date_alias"],
    65: ["date_filter", "dim_fact_chain"],
    66: ["date_filter", "multi_dim_filter", "union_year"],
    67: ["date_filter", "dim_fact_chain", "window_fn"],
    68: ["multi_dim_filter", "dim_fact_chain"],
    69: ["multi_dim_filter", "dim_fact_chain"],
    70: ["date_filter", "window_fn", "correlated_subquery"],
    71: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    72: ["multi_date_alias", "multi_dim_filter", "complex_multi_join"],
    73: ["multi_dim_filter", "dim_fact_chain"],
    74: ["union_year", "date_filter"],
    75: ["date_filter", "union_year"],
    76: ["date_filter", "or_condition", "union_year"],
    77: ["date_filter", "union_year"],
    78: ["date_filter", "complex_multi_join"],
    79: ["multi_dim_filter", "dim_fact_chain"],
    80: ["date_filter", "multi_date_alias", "union_year"],
    81: ["correlated_subquery", "date_filter"],
    82: ["multi_dim_filter", "dim_fact_chain"],
    83: ["multi_dim_filter", "dim_fact_chain"],
    84: ["multi_dim_filter", "dim_fact_chain"],
    85: ["multi_dim_filter", "dim_fact_chain"],
    86: ["multi_dim_filter", "dim_fact_chain"],
    87: ["date_filter", "multi_dim_filter", "complex_multi_join"],
    88: ["repeated_scan", "or_condition"],  # multiple CASE WHEN subqueries + OR
    89: ["date_filter", "dim_fact_chain"],
    90: ["date_filter", "dim_fact_chain"],
    91: ["date_filter", "dim_fact_chain"],
    92: ["date_filter", "dim_fact_chain"],
    93: ["dim_fact_chain"],  # early_filter proven here (reason code)
    94: ["date_filter", "exists_repeat", "complex_multi_join"],
    95: ["exists_repeat", "date_filter"],
    96: ["multi_dim_filter", "dim_fact_chain"],
    97: ["date_filter", "union_year"],
    98: ["date_filter", "dim_fact_chain"],
    99: ["date_filter", "multi_dim_filter"],
}

# Maps query features → best-fit gold patterns (ordered by priority)
FEATURE_TO_PATTERN = {
    "correlated_subquery": ["decorrelate", "date_cte_isolate"],
    "date_filter":         ["date_cte_isolate", "prefetch_fact_join"],
    "multi_date_alias":    ["multi_date_range_cte"],
    "or_condition":        ["or_to_union"],
    "multi_dim_filter":    ["dimension_cte_isolate", "multi_dimension_prefetch", "early_filter"],
    "dim_fact_chain":      ["prefetch_fact_join", "early_filter", "multi_dimension_prefetch"],
    "repeated_scan":       ["single_pass_aggregation", "pushdown"],
    "intersect":           ["intersect_to_exists"],
    "union_year":          ["union_cte_split"],
    "exists_repeat":       ["materialize_cte"],
    "complex_multi_join":  [],  # No gold pattern, needs EXPLAIN ANALYZE
    "window_fn":           [],  # No gold pattern, needs EXPLAIN ANALYZE
}


# ============================================================================
# DATA LOADING
# ============================================================================

def load_all_query_states() -> Dict[str, QueryState]:
    """Load and merge data for all 99 queries from multiple sources"""
    states = {}

    # 1. Load state history YAMLs
    state_dir = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/state_histories_all_99")
    for yaml_file in sorted(state_dir.glob("q*_state_history.yaml")):
        try:
            with open(yaml_file) as f:
                data = yaml.safe_load(f)

            query_id = f"q{data['query_num']}"
            qs = QueryState(
                query_id=query_id,
                query_num=data['query_num'],
                category="NO_DATA",
                original_ms=0.0,
                best_speedup=data.get('best_speedup', 1.0),
                best_worker=None,
                expected_speedup=0.0,
            )

            # Parse states from YAML
            for state in data.get('states', []):
                state_id = state.get('state_id', 'unknown')
                transforms = [t.strip() for t in state.get('transforms', []) if t and t.strip()]
                ws = WorkerState(
                    worker_id=state_id,
                    speedup=state.get('speedup', 1.0),
                    transforms=transforms,
                    status=state.get('status', 'unknown'),
                    error=state.get('error'),
                    syntax_valid=state.get('syntax_valid'),
                )
                qs.states[state_id] = ws

                # Track transforms (skip empty ones)
                for transform in transforms:
                    qs.transforms_tried.add(transform)
                    speedup = state.get('speedup', 1.0)
                    status = state.get('status', 'unknown')
                    if status == 'success' and speedup > 1.1:
                        qs.transforms_succeeded.add(transform)
                    elif status in ('error', 'regression') or speedup < 0.95:
                        qs.transforms_failed.add(transform)
                    # else: neutral (tried, no meaningful improvement or regression)

            states[query_id] = qs
        except Exception as e:
            print(f"Warning: Failed to load {yaml_file}: {e}", file=sys.stderr)

    # 2. Merge with CSV master leaderboard
    csv_path = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Master_v2_20260206.csv")
    if csv_path.exists():
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                query_num = int(row['Query_Num'])
                query_id = f"q{query_num}"

                if query_id not in states:
                    continue

                qs = states[query_id]
                # Note: Don't use CSV classification - we'll recalculate based on actual best_speedup
                # qs.category = row['Classification']
                try:
                    qs.original_ms = float(row.get('Kimi_Original_ms', 0) or 0)
                except (ValueError, TypeError):
                    qs.original_ms = 0.0
                try:
                    expected = row.get('Expected_Speedup', '1.0') or '1.0'
                    qs.expected_speedup = float(expected)
                except (ValueError, TypeError):
                    qs.expected_speedup = 1.0

                # Track which worker achieved best result in 4W retry
                if row.get('Retry3W_Best_Worker'):
                    qs.best_worker = f"W{row['Retry3W_Best_Worker']}"

    # Recalculate classifications based on actual best_speedup (State 0 → State 1)
    # State 0 = unoptimized baseline (1.0x)
    # State 1 = current best achieved (best_speedup)
    # Intermediate failures don't matter - only net result matters
    for qs in states.values():
        if qs.best_speedup >= 1.5:
            qs.category = "WIN"
        elif qs.best_speedup >= 1.1:
            qs.category = "IMPROVED"
        elif qs.best_speedup >= 0.95:
            qs.category = "NEUTRAL"
        elif qs.best_speedup < 0.95:
            qs.category = "REGRESSION"
        else:
            qs.category = "NO_DATA"

    return states


def load_gold_patterns() -> Dict[str, TransformStats]:
    """Load verified gold patterns with their speedups"""
    patterns = {}

    example_dir = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql/qt_sql/optimization/examples")
    for json_file in example_dir.glob("*.json"):
        try:
            with open(json_file) as f:
                data = json.load(f)

            pattern_id = data['id']
            speedup_str = data.get('verified_speedup', '1.0x').replace('x', '')
            try:
                speedup = float(speedup_str)
            except:
                speedup = 1.0

            patterns[pattern_id] = TransformStats(
                transform_id=pattern_id,
                verified_speedup=speedup,
                total_attempts=1,  # Will be updated from failure analysis
                successes=1,
                failures=0,
                success_rate=1.0,
                avg_speedup_when_successful=speedup,
            )
        except Exception as e:
            print(f"Warning: Failed to load {json_file}: {e}", file=sys.stderr)

    return patterns


def load_failure_analysis(patterns: Dict[str, TransformStats]) -> None:
    """Update transform stats from failure analysis"""
    fail_path = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/state_histories/failure_analysis.yaml")

    if not fail_path.exists():
        return

    try:
        with open(fail_path) as f:
            data = yaml.safe_load(f)

        for transform_id, info in data.get('by_transform', {}).items():
            if transform_id not in patterns:
                patterns[transform_id] = TransformStats(transform_id=transform_id)

            pattern = patterns[transform_id]
            pattern.total_attempts = info.get('count', 0)

            error_types = info.get('error_types', {})
            pattern.failures = error_types.get('error', 0) + error_types.get('regression', 0)
            pattern.successes = pattern.total_attempts - pattern.failures
            pattern.success_rate = pattern.successes / pattern.total_attempts if pattern.total_attempts > 0 else 0.0

            # Track known issues
            if 'or_to_union' in transform_id:
                pattern.known_issues.append("Severe regressions with >3 branches")
    except Exception as e:
        print(f"Warning: Failed to load failure analysis: {e}", file=sys.stderr)


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def calculate_runtime_percentiles(states: Dict[str, QueryState]) -> None:
    """Calculate runtime percentile rankings - CRITICAL FOR PRIORITIZATION"""
    # Sort by original_ms descending (longest first)
    sorted_queries = sorted(
        [s for s in states.values() if s.original_ms > 0],
        key=lambda x: x.original_ms,
        reverse=True
    )

    total = len(sorted_queries)
    top_20_threshold = int(total * 0.2)
    top_50_threshold = int(total * 0.5)

    for idx, qs in enumerate(sorted_queries):
        if idx < top_20_threshold:
            qs._runtime_percentile_rank = 50.0  # Top 20% = 50 points
            qs._runtime_tier = "TOP_20%"
        elif idx < top_50_threshold:
            qs._runtime_percentile_rank = 25.0  # Top 50% = 25 points
            qs._runtime_tier = "TOP_50%"
        else:
            qs._runtime_percentile_rank = 0.0   # Bottom 50% = 0 points
            qs._runtime_tier = "BOTTOM_50%"


def calculate_priority_score(
    qs: QueryState,
    patterns: Dict[str, TransformStats]
) -> Tuple[float, str]:
    """
    Calculate priority score (0-100) for working on this query.

    CRITICAL: Prioritize by RUNTIME (absolute time savings), not percentage.
    """
    if qs.original_ms <= 0:
        return 0.0, "NO_RUNTIME_DATA"

    # 1. Runtime percentile (50 points for top 20%, 25 for top 50%)
    runtime_points = qs._runtime_percentile_rank

    # 2. Gap to expectation
    gap = max(0, qs.expected_speedup - qs.best_speedup)
    gap_points = min(20, gap * 5)  # Cap at 20 points

    # 3. Win potential (how far to 1.5x threshold)
    win_threshold = 1.5
    win_gap = max(0, win_threshold - qs.best_speedup)
    win_points = min(20, win_gap * 10) if qs.best_speedup < win_threshold else 0

    # 4. Untried patterns
    untried = set(patterns.keys()) - qs.transforms_tried
    untried_points = min(5, len(untried))

    # 5. Category bonus
    category_bonus = {
        "NEUTRAL": 15,
        "REGRESSION": 10,
        "IMPROVED": 5,
        "WIN": 0,
        "GOLD_EXAMPLE": 0,
        "NO_DATA": 5,
    }.get(qs.category, 0)

    total = runtime_points + gap_points + win_points + untried_points + category_bonus

    return min(100, total), qs._runtime_tier


def score_recommendation(
    query_state: QueryState,
    transform: str,
    pattern_stats: Optional[TransformStats],
    from_best_state: bool = False,
) -> Optional[Recommendation]:
    """Score a single transform recommendation"""

    if not pattern_stats:
        return None

    # Base confidence from success rate
    base_confidence = pattern_stats.success_rate * 40

    # Speedup bonus (up to 30 points)
    speedup_bonus = min(30, (pattern_stats.avg_speedup_when_successful - 1.0) * 10)

    # Failure avoidance (up to 20 points)
    failure_penalty = pattern_stats.failures * 5
    failure_bonus = max(0, 20 - failure_penalty)

    # Pattern match (simple heuristic - 10 points max)
    pattern_match = 10 if from_best_state else 5

    confidence = base_confidence + speedup_bonus + failure_bonus + pattern_match
    confidence = max(0, min(100, confidence))

    # Determine risk
    if pattern_stats.success_rate > 0.8:
        risk = "LOW"
    elif pattern_stats.success_rate > 0.5:
        risk = "MEDIUM"
    else:
        risk = "HIGH"

    rec = Recommendation(
        transform=transform,
        confidence=confidence,
        expected_speedup=pattern_stats.avg_speedup_when_successful,
        risk=risk,
        rationale=f"Transform has {pattern_stats.success_rate*100:.0f}% success rate with {pattern_stats.avg_speedup_when_successful:.2f}x average speedup",
        from_state="best" if from_best_state else "baseline",
    )

    return rec


def get_matching_patterns(query_num: int) -> List[str]:
    """Get gold patterns that match this query's structural features"""
    features = TPCDS_QUERY_FEATURES.get(query_num, [])
    matched = []
    seen = set()
    for feature in features:
        for pattern in FEATURE_TO_PATTERN.get(feature, []):
            if pattern not in seen:
                matched.append(pattern)
                seen.add(pattern)
    return matched


def needs_explain_analyze(query_num: int) -> List[str]:
    """Return EXPLAIN ANALYZE investigation reasons if no gold pattern fits"""
    features = TPCDS_QUERY_FEATURES.get(query_num, [])
    reasons = []
    if "complex_multi_join" in features:
        reasons.append("Complex multi-join: Check join ordering and cardinality estimates (actual vs EC)")
    if "window_fn" in features:
        reasons.append("Window functions: Check for full sort on grouped TopN (ROW_NUMBER OVER PARTITION)")
    return reasons


def generate_recommendations(
    qs: QueryState,
    patterns: Dict[str, TransformStats],
    failure_queries: Optional[Dict[str, Set[str]]] = None,
) -> List[Recommendation]:
    """Generate recommendations based on query structure, not generic success rates.

    Strategy:
    1. Match gold patterns to this query's TPC-DS structural features
    2. Filter out patterns already tried (unless there's a reason to retry)
    3. For queries where no gold pattern fits, recommend EXPLAIN ANALYZE investigation
    4. Always start from baseline (State 0) for regressions
    """
    recommendations = {}

    # Get structurally matched patterns for this query
    matched_patterns = get_matching_patterns(qs.query_num)
    explain_reasons = needs_explain_analyze(qs.query_num)

    # Score matched patterns that haven't been tried
    for transform in matched_patterns:
        if transform not in patterns:
            continue

        pattern = patterns[transform]

        if transform not in qs.transforms_tried:
            # Not tried yet - recommend it
            features = TPCDS_QUERY_FEATURES.get(qs.query_num, [])
            # Find which feature matched this pattern
            matching_features = []
            for feat in features:
                if transform in FEATURE_TO_PATTERN.get(feat, []):
                    matching_features.append(feat)

            rationale = f"Query has {', '.join(matching_features)} structure"
            rec = score_recommendation(qs, transform, pattern, from_best_state=False)
            if rec:
                rec.rationale = rationale
                # Boost confidence for structural match
                rec.confidence = min(100, rec.confidence + 15)
                recommendations[transform] = rec

        elif transform in qs.transforms_failed:
            # Tried and failed - note it but don't recommend unless strong reason
            pass  # History shows failure, skip

    # If no structural matches left untried, suggest EXPLAIN ANALYZE
    if not recommendations and explain_reasons:
        rec = Recommendation(
            transform="EXPLAIN_ANALYZE",
            confidence=60,
            expected_speedup=0.0,
            risk="MEDIUM",
            rationale="No untried gold pattern matches query structure. " + " ".join(explain_reasons),
        )
        recommendations["EXPLAIN_ANALYZE"] = rec

    # If we still have no recommendations, check if ALL matched patterns were tried
    if not recommendations and matched_patterns:
        tried_and_failed = [p for p in matched_patterns if p in qs.transforms_failed]
        tried_and_succeeded = [p for p in matched_patterns if p in qs.transforms_succeeded]

        if tried_and_failed and not tried_and_succeeded:
            # All structural matches failed - need novel approach
            rec = Recommendation(
                transform="EXPLAIN_ANALYZE",
                confidence=50,
                expected_speedup=0.0,
                risk="MEDIUM",
                rationale=f"Matched patterns already tried and failed ({', '.join(tried_and_failed)}). "
                          f"Run EXPLAIN ANALYZE to identify specific bottleneck: "
                          f"cardinality misestimates, nested loop joins, missing filter pushdown.",
            )
            recommendations["EXPLAIN_ANALYZE"] = rec

    sorted_recs = sorted(recommendations.values(), key=lambda r: r.confidence, reverse=True)
    return sorted_recs[:5]


# ============================================================================
# REPORT GENERATION
# ============================================================================

def format_query_analysis(
    qs: QueryState,
    recommendations: List[Recommendation],
    patterns: Dict[str, TransformStats],
) -> str:
    """Generate markdown section for one query"""

    # Calculate time savings potential
    savings_2x = qs.original_ms / 2 if qs.original_ms > 0 else 0
    savings_3x = qs.original_ms * 2 / 3 if qs.original_ms > 0 else 0

    lines = []
    lines.append(f"\n### Q{qs.query_num}: {qs.query_id.upper()}")
    lines.append(f"**Classification**: {qs.category}")
    lines.append(f"**Runtime**: {qs.original_ms:.1f}ms baseline ({qs._runtime_tier})")
    lines.append(f"**Time Savings Potential**: {savings_2x:.0f}ms at 2x, {savings_3x:.0f}ms at 3x")
    lines.append(f"**Current Best**: {qs.best_speedup:.2f}x ({qs.best_worker or 'baseline'})")
    lines.append(f"**Gap to Expectation**: {abs(qs.expected_speedup - qs.best_speedup):.2f}x")

    # State history - show all attempts clearly
    lines.append(f"\n**Attempt History** (State 0 = baseline, current best = next starting point):")
    # Order: baseline first, then attempts, prioritize by speedup
    state_order = ['baseline', 'kimi', 'v2_standard', 'retry3w_1', 'retry3w_2', 'retry3w_3', 'retry3w_4',
                   'W1', 'W2', 'W3', 'W4']
    shown = set()
    for state_id in state_order:
        if state_id in qs.states and state_id not in shown:
            ws = qs.states[state_id]
            transforms_str = ", ".join(t for t in ws.transforms if t.strip()) if ws.transforms else "none"
            lines.append(f"- {state_id}: {ws.speedup:.2f}x [{transforms_str}] {ws.status}")
            shown.add(state_id)
    # Show any other states not in order
    for state_id in sorted(qs.states.keys()):
        if state_id not in shown:
            ws = qs.states[state_id]
            transforms_str = ", ".join(t for t in ws.transforms if t.strip()) if ws.transforms else "none"
            lines.append(f"- {state_id}: {ws.speedup:.2f}x [{transforms_str}] {ws.status}")

    # Transforms attempted - clear success/failure record
    if qs.transforms_tried:
        lines.append(f"\n**Transforms Tried** (learning record):")
        succeeded = sorted(qs.transforms_succeeded)
        failed = sorted(qs.transforms_failed)
        neutral = sorted(qs.transforms_tried - qs.transforms_succeeded - qs.transforms_failed)

        if succeeded:
            lines.append(f"  Worked: {', '.join(succeeded)}")
        if neutral:
            lines.append(f"  No effect: {', '.join(neutral)}")
        if failed:
            lines.append(f"  Failed: {', '.join(failed)}")

    # Untried gold patterns - opportunities
    untried = set(patterns.keys()) - qs.transforms_tried
    if untried:
        lines.append(f"\n**Gold Patterns NOT Tried** (candidates for next attempt):")
        for transform in sorted(untried):
            lines.append(f"  - {transform}")

    # Recommendations
    if recommendations:
        lines.append(f"\n**Recommended Next Moves**:")
        for i, rec in enumerate(recommendations[:3], 1):
            if rec.transform == "EXPLAIN_ANALYZE":
                lines.append(f"\n{i}. **EXPLAIN ANALYZE investigation** [CONFIDENCE: {rec.confidence:.0f}%]")
                lines.append(f"   - {rec.rationale}")
            else:
                pattern = patterns.get(rec.transform)
                success_str = f"{pattern.success_rate*100:.0f}%" if pattern else "N/A"
                lines.append(f"\n{i}. **{rec.transform}** [CONFIDENCE: {rec.confidence:.0f}%] [RISK: {rec.risk}]")
                lines.append(f"   - Why: {rec.rationale}")
                lines.append(f"   - Verified: {rec.expected_speedup:.2f}x on benchmark queries")
                lines.append(f"   - Success rate: {success_str}")

    return "\n".join(lines)


def generate_executive_dashboard(
    all_states: Dict[str, QueryState],
    patterns: Dict[str, TransformStats],
) -> str:
    """Generate top-level summary"""
    lines = []

    lines.append("# EXECUTIVE DASHBOARD\n")

    # Overall progress
    classifications = defaultdict(int)
    for qs in all_states.values():
        classifications[qs.category] += 1

    lines.append("## Progress Summary\n")
    for cat in ['WIN', 'IMPROVED', 'NEUTRAL', 'REGRESSION', 'ERROR', 'GOLD_EXAMPLE']:
        count = classifications[cat]
        lines.append(f"- {cat}: {count}")

    # Complete leaderboard of all 99 queries by runtime
    lines.append("\n## Complete Query Leaderboard (All 99 Queries by Runtime)\n")
    lines.append("| Rank | Query | Runtime | Speedup | Status | Savings @2x |")
    lines.append("|------|-------|---------|---------|--------|-------------|")

    sorted_by_runtime = sorted(
        [s for s in all_states.values() if s.original_ms > 0],
        key=lambda x: x.original_ms,
        reverse=True
    )

    for i, qs in enumerate(sorted_by_runtime, 1):
        savings_2x = qs.original_ms / 2
        top_mark = "⭐ TOP 20" if i <= 20 else ""
        lines.append(
            f"| {i} | Q{qs.query_num} | {qs.original_ms:.0f}ms | {qs.best_speedup:.2f}x | "
            f"{qs.category} | {savings_2x:.0f}ms {top_mark} |"
        )

    # Transform stats
    lines.append("\n## Transform Effectiveness\n")
    sorted_patterns = sorted(patterns.values(), key=lambda x: x.success_rate, reverse=True)
    for pattern in sorted_patterns[:10]:
        lines.append(
            f"- {pattern.transform_id}: {pattern.success_rate*100:.0f}% success "
            f"({pattern.successes}/{pattern.total_attempts}), {pattern.avg_speedup_when_successful:.2f}x avg"
        )

    return "\n".join(lines)


def generate_full_report() -> str:
    """Main orchestrator - generate complete analysis report"""
    print("Loading state histories...", file=sys.stderr)
    states = load_all_query_states()

    print("Loading gold patterns...", file=sys.stderr)
    patterns = load_gold_patterns()

    print("Loading failure analysis...", file=sys.stderr)
    load_failure_analysis(patterns)

    print("Calculating runtime percentiles...", file=sys.stderr)
    calculate_runtime_percentiles(states)

    # Calculate priority scores
    print("Calculating priorities...", file=sys.stderr)
    priorities = {}
    for query_id, qs in states.items():
        priority, tier = calculate_priority_score(qs, patterns)
        priorities[query_id] = (priority, tier)

    # Sort by priority
    sorted_by_priority = sorted(
        states.items(),
        key=lambda x: priorities[x[0]][0],
        reverse=True
    )

    # Generate report
    report = []
    report.append("# STATE ANALYSIS REPORT: TPC-DS Optimization Strategy\n")
    report.append(f"**Generated**: 2026-02-06\n")
    report.append(f"**Scope**: 99 TPC-DS Queries\n")
    report.append(f"**Strategy**: Prioritize by RUNTIME (absolute time savings), not speedup %\n")

    # Executive dashboard
    report.append(generate_executive_dashboard(states, patterns))

    # Tier 1: High-value targets
    report.append("\n\n# TIER 1: HIGH-VALUE TARGETS (Priority > 70)\n")
    tier1 = [q for q in sorted_by_priority if priorities[q[0]][0] > 70]
    for query_id, qs in tier1[:30]:
        priority, tier = priorities[query_id]
        recommendations = generate_recommendations(qs, patterns)
        report.append(f"\n**Priority Score**: {priority:.1f} ({tier})")
        report.append(format_query_analysis(qs, recommendations, patterns))

    # Tier 2: Incremental opportunities
    report.append("\n\n# TIER 2: INCREMENTAL OPPORTUNITIES (Priority 40-70)\n")
    tier2 = [q for q in sorted_by_priority if 40 <= priorities[q[0]][0] <= 70]
    for query_id, qs in tier2[:30]:
        priority, tier = priorities[query_id]
        recommendations = generate_recommendations(qs, patterns)
        report.append(f"\n**Priority Score**: {priority:.1f} ({tier})")
        report.append(format_query_analysis(qs, recommendations, patterns))

    # Tier 3: Mature wins
    report.append("\n\n# TIER 3: MATURE WINS (Priority < 40)\n")
    tier3 = [q for q in sorted_by_priority if priorities[q[0]][0] < 40]
    report.append(f"\n**{len(tier3)} queries with low priority** (mostly short-running or already optimized)")
    report.append("\nThese queries are not recommended for immediate focus due to:")
    report.append("- Short baseline runtime (<500ms) → lower absolute time savings potential")
    report.append("- Already at or near expected speedup targets")
    report.append("- Limited remaining optimization opportunities\n")

    # Add appendix with methodology
    report.append("\n\n# APPENDIX: METHODOLOGY & INTERPRETATION GUIDE\n")
    report.append("\n## Priority Scoring Formula\n")
    report.append("`Priority = Runtime_Percentile(50pts) + Gap_To_Expectation(20pts) + Win_Potential(20pts) + Untried_Patterns(5pts) + Category_Bonus(15pts)`\n")
    report.append("\n### Runtime Percentile (50 points - DOMINANT FACTOR)\n")
    report.append("- **Top 20% by baseline runtime**: 50 points")
    report.append("- **Top 21-50% by baseline runtime**: 25 points")
    report.append("- **Bottom 50% by baseline runtime**: 0 points\n")
    report.append("**Key insight**: A 1.2x speedup on a 10,000ms query saves more absolute time than 3x speedup on a 100ms query.\n")
    report.append("\n### Time Savings Potential\n")
    report.append("Shown for each query:")
    report.append("- **At 2x speedup**: Original_ms / 2 seconds saved")
    report.append("- **At 3x speedup**: Original_ms * 2 / 3 seconds saved\n")
    report.append("\n### Confidence Scores\n")
    report.append("- **90-100%**: Very high confidence - proven pattern with high success rate")
    report.append("- **75-89%**: High confidence - successful pattern, likely to work")
    report.append("- **60-74%**: Good confidence - proven technique, moderate risk")
    report.append("- **40-59%**: Moderate confidence - less evidence but promising")
    report.append("- **<40%**: Low confidence - experimental, use as last resort\n")
    report.append("\n### Risk Assessment\n")
    report.append("- **LOW**: >80% historical success rate")
    report.append("- **MEDIUM**: 50-80% success rate")
    report.append("- **HIGH**: <50% success rate or untested pattern\n")
    report.append("\n## How to Use This Report\n")
    report.append("1. **Start with Tier 1** (highest priority scores) - these are longest-running queries with proven patterns")
    report.append("2. **Check Time Savings Potential** - focus on queries where potential savings are largest")
    report.append("3. **Review Top Recommendations** - follow highest-confidence transforms first")
    report.append("4. **Validate improvements** using 3-run (discard warmup, avg last 2) or 5-run trimmed mean methodology")
    report.append("5. **Move to Tier 2** only after exhausting high-value targets in Tier 1\n")

    return "\n".join(report)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    report = generate_full_report()

    output_path = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/state/STATE_ANALYSIS_REPORT.md")
    with open(output_path, 'w') as f:
        f.write(report)

    print(f"Report generated: {output_path}", file=sys.stderr)
    print(f"Lines: {len(report.splitlines())}", file=sys.stderr)
