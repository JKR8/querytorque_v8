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
                    if state.get('status') == 'success' and state.get('speedup', 1.0) > 1.0:
                        qs.transforms_succeeded.add(transform)
                    elif state.get('status') != 'success':
                        qs.transforms_failed.add(transform)

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


def generate_recommendations(
    qs: QueryState,
    patterns: Dict[str, TransformStats]
) -> List[Recommendation]:
    """Generate 2-5 recommendations for a query

    Strategy:
    - Untried transforms: Safe bets, prioritize these
    - Failed transforms: Learn what didn't work, only recommend with good reason
    - Succeeded transforms: Build on them
    - Remember: For regressions, baseline is State 0, never build from the failed state
    """
    recommendations = {}

    # First: Untried transforms (no failure history, safest bets)
    for transform in ['prefetch_fact_join', 'single_pass_aggregation', 'date_cte_isolate', 'early_filter',
                      'dimension_cte_isolate', 'multi_dimension_prefetch', 'materialize_cte',
                      'union_cte_split', 'decorrelate', 'pushdown', 'or_to_union', 'intersect_to_exists',
                      'multi_date_range_cte']:
        if transform in patterns and transform not in qs.transforms_tried:
            pattern = patterns[transform]
            rec = score_recommendation(qs, transform, pattern, from_best_state=False)
            if rec and rec.confidence > 40 and transform not in recommendations:
                recommendations[transform] = rec

    # Second: Build on what succeeded (compound benefits)
    if qs.best_speedup > 1.0 and qs.best_worker:
        best_state = None
        for state_id in [qs.best_worker, 'kimi', 'v2_standard']:
            if state_id in qs.states:
                best_state = qs.states[state_id]
                break

        if best_state and best_state.transforms:
            current_transforms = set(best_state.transforms)
            for transform in patterns:
                if (transform not in current_transforms and
                    transform not in qs.transforms_tried and
                    transform not in recommendations):
                    pattern = patterns[transform]
                    rec = score_recommendation(qs, transform, pattern, from_best_state=True)
                    if rec and rec.confidence > 50:
                        recommendations[transform] = rec

    # Third: Failed transforms only if high success rate elsewhere (indicates query-specific issue)
    if len(recommendations) < 2:
        for transform in qs.transforms_failed:
            if transform in patterns and transform not in recommendations:
                pattern = patterns[transform]
                if pattern.success_rate > 0.7:  # High success elsewhere, worth retry
                    rec = score_recommendation(qs, transform, pattern, from_best_state=False)
                    if rec and rec.confidence > 50:
                        rec.rationale = f"(Tried before but high success rate elsewhere - worth retry)"
                        recommendations[transform] = rec

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

        if succeeded:
            lines.append(f"✓ SUCCEEDED: {', '.join(succeeded)}")
        if failed:
            lines.append(f"✗ FAILED: {', '.join(failed)}")

    # Untried gold patterns - opportunities
    untried = set(patterns.keys()) - qs.transforms_tried
    if untried:
        lines.append(f"\n**Gold Patterns NOT Tried** (candidates for next attempt):")
        for transform in sorted(untried):
            lines.append(f"  - {transform}")

    # Recommendations
    if recommendations:
        lines.append(f"\n**Top Recommendations**:")
        for i, rec in enumerate(recommendations[:3], 1):
            pattern = patterns[rec.transform]
            lines.append(f"\n{i}. **{rec.transform}** [CONFIDENCE: {rec.confidence:.0f}%] [RISK: {rec.risk}]")
            lines.append(f"   - Expected: {rec.expected_speedup:.2f}x improvement")
            lines.append(f"   - Success Rate: {pattern.success_rate*100:.0f}%")
            lines.append(f"   - Rationale: {rec.rationale}")

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
