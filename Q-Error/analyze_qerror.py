#!/usr/bin/env python3
"""
Q-Error Analysis: Does optimizer wrongness predict optimization opportunity?

Tests hypothesis: If DB's EXPLAIN estimates are far from reality, optimization ROI is high.
"""
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class QErrorNode:
    """Node with Q-Error signal"""
    operator: str
    estimated: int
    actual: int
    q_error: float
    node_type: str  # JOIN, SCAN, AGGREGATE, etc.

    def severity(self) -> str:
        """Classify Q-Error severity"""
        if self.q_error < 2:
            return "ACCURATE"
        elif self.q_error < 10:
            return "MINOR_DRIFT"
        elif self.q_error < 100:
            return "MODERATE_GUESS"
        elif self.q_error < 1000:
            return "MAJOR_HALLUCINATION"
        else:
            return "CATASTROPHIC_BLINDNESS"

@dataclass
class QueryAnalysis:
    """Q-Error analysis for a query"""
    query_id: str
    max_q_error: float
    max_q_node: Optional[QErrorNode]
    join_errors: List[QErrorNode]
    scan_errors: List[QErrorNode]
    speedup: Optional[float] = None
    transform: Optional[str] = None

def calculate_q_error(estimated: int, actual: int) -> float:
    """
    Q-Error = max(estimate/actual, actual/estimate)

    Interpretation:
    - Q ≈ 1: Perfect estimate
    - Q > 10: DB is guessing
    - Q > 1000: DB is hallucinating
    """
    if estimated == 0 and actual == 0:
        return 1.0
    if estimated == 0:
        return float(actual) if actual > 0 else 1.0
    if actual == 0:
        return float(estimated) if estimated > 0 else 1.0

    return max(estimated / actual, actual / estimated)

def extract_q_errors_from_node(node: Dict, path: str = "") -> List[QErrorNode]:
    """Recursively extract Q-Error signals from EXPLAIN ANALYZE tree"""
    errors = []

    if not isinstance(node, dict):
        return errors

    # Extract cardinality data
    operator_type = node.get("operator_type", "")
    operator_name = node.get("operator_name", operator_type)

    estimated = None
    actual = node.get("operator_cardinality", 0)

    # DuckDB stores estimates in extra_info
    extra_info = node.get("extra_info", {})
    if "Estimated Cardinality" in extra_info:
        try:
            estimated = int(extra_info["Estimated Cardinality"])
        except (ValueError, TypeError):
            pass

    # Calculate Q-Error if we have both values
    if estimated is not None and actual is not None:
        q_error = calculate_q_error(estimated, actual)

        # Only report significant errors (Q > 2)
        if q_error > 2.0:
            errors.append(QErrorNode(
                operator=f"{path}/{operator_name}",
                estimated=estimated,
                actual=actual,
                q_error=q_error,
                node_type=operator_type
            ))

    # Recurse into children
    for i, child in enumerate(node.get("children", [])):
        child_path = f"{path}/{operator_name}[{i}]" if path else operator_name
        errors.extend(extract_q_errors_from_node(child, child_path))

    return errors

def analyze_explain_file(explain_path: Path) -> Optional[QueryAnalysis]:
    """Analyze a single EXPLAIN ANALYZE file"""
    try:
        with open(explain_path) as f:
            data = json.load(f)

        # Extract query ID from path
        query_id = explain_path.parent.parent.name if "audit" in str(explain_path) else explain_path.parent.name

        # Extract Q-Error nodes
        plan_json = data.get("plan_json", {})
        all_errors = extract_q_errors_from_node(plan_json)

        if not all_errors:
            return None

        # Find max Q-Error
        max_node = max(all_errors, key=lambda x: x.q_error)

        # Separate by operator type
        join_errors = [e for e in all_errors if "JOIN" in e.node_type]
        scan_errors = [e for e in all_errors if "SCAN" in e.node_type]

        return QueryAnalysis(
            query_id=query_id,
            max_q_error=max_node.q_error,
            max_q_node=max_node,
            join_errors=join_errors,
            scan_errors=scan_errors
        )

    except Exception as e:
        print(f"Error parsing {explain_path}: {e}")
        return None

def load_gold_speedups() -> Dict[str, Tuple[float, str]]:
    """Load verified speedups from gold examples"""
    gold_dir = Path("packages/qt-sql/qt_sql/examples/duckdb")
    speedups = {}

    for json_file in gold_dir.glob("*.json"):
        try:
            with open(json_file) as f:
                data = json.load(f)

            queries = data.get("benchmark_queries", [])
            speedup = data.get("sf10_speedup", 0)
            transform = data.get("id", "")

            for q in queries:
                # Store (speedup, transform) for each query
                if speedup > 1.0:  # Only wins
                    speedups[q.lower()] = (speedup, transform)

        except Exception as e:
            print(f"Error loading {json_file}: {e}")

    return speedups

def find_explain_files() -> List[Path]:
    """Find all EXPLAIN ANALYZE files in benchmark directories"""
    base = Path("packages/qt-sql/qt_sql/benchmarks")
    explain_files = []

    for pattern in ["**/explain_analyze.json", "**/*_explain.json"]:
        explain_files.extend(base.glob(pattern))

    return explain_files

def main():
    print("=" * 80)
    print("Q-ERROR ANALYSIS: Does Optimizer Wrongness Predict Optimization ROI?")
    print("=" * 80)
    print()

    # Load gold speedups
    speedups = load_gold_speedups()
    print(f"Loaded {len(speedups)} gold example speedups")
    print()

    # Find EXPLAIN files
    explain_files = find_explain_files()
    print(f"Found {len(explain_files)} EXPLAIN ANALYZE files")
    print()

    # Analyze each file
    analyses = []
    for explain_path in explain_files:
        analysis = analyze_explain_file(explain_path)
        if analysis:
            # Try to match with gold speedup
            query_id = analysis.query_id.lower().replace("_", "")
            for gold_q, (speedup, transform) in speedups.items():
                if gold_q.lower().replace("q", "") in query_id or query_id in gold_q.lower():
                    analysis.speedup = speedup
                    analysis.transform = transform
                    break

            analyses.append(analysis)

    if not analyses:
        print("No Q-Error data found!")
        return

    # Sort by Q-Error (highest first)
    analyses.sort(key=lambda x: x.max_q_error, reverse=True)

    # Print detailed results
    print("TOP Q-ERROR QUERIES (Highest Optimizer Wrongness)")
    print("-" * 80)
    print(f"{'Query':<15} {'Max Q-Error':<15} {'Severity':<25} {'Speedup':<10} {'Transform':<20}")
    print("-" * 80)

    for a in analyses[:20]:
        severity = a.max_q_node.severity() if a.max_q_node else "N/A"
        speedup_str = f"{a.speedup:.2f}x" if a.speedup else "N/A"
        transform_str = a.transform or "N/A"

        print(f"{a.query_id:<15} {a.max_q_error:<15.1f} {severity:<25} {speedup_str:<10} {transform_str:<20}")

        if a.max_q_node:
            print(f"  └─ {a.max_q_node.node_type}: Est={a.max_q_node.estimated:,} Actual={a.max_q_node.actual:,}")

    print()
    print("=" * 80)
    print("CORRELATION ANALYSIS")
    print("=" * 80)

    # Analyze correlation for queries with speedup data
    with_speedup = [a for a in analyses if a.speedup is not None]

    if len(with_speedup) >= 3:
        print(f"\nQueries with both Q-Error and Speedup data: {len(with_speedup)}")

        # Sort by Q-Error and by Speedup
        by_qerror = sorted(with_speedup, key=lambda x: x.max_q_error, reverse=True)
        by_speedup = sorted(with_speedup, key=lambda x: x.speedup, reverse=True)

        print("\nTop Q-Error (Expected: Should correlate with high speedup)")
        for a in by_qerror[:5]:
            print(f"  {a.query_id}: Q-Error={a.max_q_error:.1f}, Speedup={a.speedup:.2f}x ({a.transform})")

        print("\nTop Speedups (Expected: Should correlate with high Q-Error)")
        for a in by_speedup[:5]:
            print(f"  {a.query_id}: Speedup={a.speedup:.2f}x, Q-Error={a.max_q_error:.1f} ({a.transform})")

        # Simple rank correlation
        q_ranks = {a.query_id: i for i, a in enumerate(by_qerror)}
        s_ranks = {a.query_id: i for i, a in enumerate(by_speedup)}

        common_queries = set(q_ranks.keys()) & set(s_ranks.keys())
        if common_queries:
            rank_diffs = [abs(q_ranks[q] - s_ranks[q]) for q in common_queries]
            avg_rank_diff = sum(rank_diffs) / len(rank_diffs)

            print(f"\nAverage Rank Difference: {avg_rank_diff:.1f}")
            print(f"(Lower = stronger correlation, Random = {len(common_queries)/2:.1f})")

            if avg_rank_diff < len(common_queries) / 3:
                print("✅ STRONG CORRELATION: Q-Error predicts optimization ROI!")
            elif avg_rank_diff < len(common_queries) / 2:
                print("⚠️  MODERATE CORRELATION: Q-Error has some predictive power")
            else:
                print("❌ WEAK CORRELATION: Q-Error does not predict speedup")

    else:
        print("\nNot enough data to test correlation (need ≥3 queries with both Q-Error and speedup)")

    print()
    print("=" * 80)
    print("ACTIONABLE INSIGHTS")
    print("=" * 80)

    catastrophic = [a for a in analyses if a.max_q_node and a.max_q_node.severity() == "CATASTROPHIC_BLINDNESS"]
    if catastrophic:
        print(f"\n🚨 {len(catastrophic)} queries with CATASTROPHIC Q-Error (>1000x):")
        for a in catastrophic[:10]:
            speedup_note = f" → Already optimized: {a.speedup:.2f}x" if a.speedup else " → Optimization candidate!"
            print(f"  {a.query_id}: Q-Error={a.max_q_error:.1f}{speedup_note}")

    print("\nRECOMMENDATION:")
    print("  If Q-Error > 1000: Apply AGGRESSIVE transforms (decorrelate, materialize, force join order)")
    print("  If Q-Error > 10: Apply SAFE transforms (pushdown, CTE isolation)")
    print("  If Q-Error < 10: Low ROI, skip optimization")

if __name__ == "__main__":
    main()
