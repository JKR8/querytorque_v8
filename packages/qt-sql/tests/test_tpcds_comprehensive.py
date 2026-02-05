"""Comprehensive TPC-DS testing for AST detection and rewrites.

Tests all 99 TPC-DS queries against:
1. AST detector rules (119 rules)
2. Semantic rewriters (16 rewriters)
"""

import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
import time

# Add package to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlglot

from qt_sql.analyzers.ast_detector import ASTDetector
from qt_sql.rewriters import (
    get_rewriter_for_rule,
    list_registered_rules,
    list_registered_rewriters,
    get_coverage_stats,
)


# TPC-DS query locations
TPCDS_PATHS = [
    Path("/mnt/d/TPC-DS/queries_sf100"),
    Path("/mnt/d/TPC-DS/queries_duckdb_converted"),
]


@dataclass
class QueryResult:
    """Result for a single query."""
    query_num: int
    file_path: Path
    parse_success: bool
    parse_error: str = ""
    detection_time_ms: float = 0
    issues_found: list = field(default_factory=list)
    rewrite_results: list = field(default_factory=list)


@dataclass
class TestSummary:
    """Summary of all test results."""
    total_queries: int = 0
    parse_failures: int = 0
    queries_with_issues: int = 0
    total_issues: int = 0
    issues_by_rule: dict = field(default_factory=lambda: defaultdict(int))
    issues_by_severity: dict = field(default_factory=lambda: defaultdict(int))
    issues_by_category: dict = field(default_factory=lambda: defaultdict(int))
    rewrite_attempts: int = 0
    rewrite_successes: int = 0
    total_detection_time_ms: float = 0
    query_results: list = field(default_factory=list)


def find_tpcds_queries() -> list[Path]:
    """Find all TPC-DS query files."""
    queries = []
    for base_path in TPCDS_PATHS:
        if base_path.exists():
            queries.extend(sorted(base_path.glob("query_*.sql")))
            if queries:
                break
    return queries


def extract_query_num(path: Path) -> int:
    """Extract query number from filename."""
    name = path.stem  # query_1 or query_01
    return int(name.split("_")[1])


def _run_single_query(query_path: Path, detector: ASTDetector) -> QueryResult:
    """Test a single TPC-DS query."""
    query_num = extract_query_num(query_path)
    result = QueryResult(query_num=query_num, file_path=query_path, parse_success=True)

    # Read query
    sql = query_path.read_text(encoding="utf-8")

    # Run detection
    start = time.perf_counter()
    try:
        matches = detector.detect(sql)  # dialect already set in detector constructor
        result.detection_time_ms = (time.perf_counter() - start) * 1000
        result.issues_found = matches
    except Exception as e:
        result.parse_success = False
        result.parse_error = str(e)
        result.detection_time_ms = (time.perf_counter() - start) * 1000
        return result

    # Try rewrites for each detected issue
    for match in matches:
        if match.rule_id.startswith("SQL-PARSE"):
            continue

        rewriter = get_rewriter_for_rule(match.rule_id)
        if rewriter:
            try:
                # Parse the SQL to get AST
                parsed = sqlglot.parse_one(sql, dialect="duckdb")
                rewrite_result = rewriter.rewrite(parsed)
                result.rewrite_results.append({
                    "rule_id": match.rule_id,
                    "rewriter_id": rewriter.rewriter_id,
                    "success": rewrite_result.success,
                    "confidence": rewrite_result.confidence.value if rewrite_result.success else None,
                })
            except Exception as e:
                result.rewrite_results.append({
                    "rule_id": match.rule_id,
                    "rewriter_id": rewriter.rewriter_id,
                    "success": False,
                    "error": str(e)[:100],
                })

    return result


def run_tpcds_tests() -> TestSummary:
    """Run tests on all TPC-DS queries."""
    summary = TestSummary()

    # Find queries
    queries = find_tpcds_queries()
    if not queries:
        print("ERROR: No TPC-DS queries found!")
        print(f"Searched: {TPCDS_PATHS}")
        return summary

    summary.total_queries = len(queries)
    print(f"\n{'='*70}")
    print(f"TPC-DS Comprehensive Test Suite")
    print(f"{'='*70}")
    print(f"Found {len(queries)} queries in {queries[0].parent}")
    print()

    # Initialize detector with DuckDB dialect
    detector = ASTDetector(dialect="duckdb")

    # Test each query
    for query_path in queries:
        result = _run_single_query(query_path, detector)
        summary.query_results.append(result)
        summary.total_detection_time_ms += result.detection_time_ms

        if not result.parse_success:
            summary.parse_failures += 1
            print(f"  Q{result.query_num:02d}: PARSE ERROR - {result.parse_error[:50]}")
            continue

        if result.issues_found:
            summary.queries_with_issues += 1
            summary.total_issues += len(result.issues_found)

            for issue in result.issues_found:
                summary.issues_by_rule[issue.rule_id] += 1
                summary.issues_by_severity[issue.severity] += 1
                summary.issues_by_category[issue.category] += 1

        # Count rewrite stats
        for rw in result.rewrite_results:
            summary.rewrite_attempts += 1
            if rw.get("success"):
                summary.rewrite_successes += 1

        # Print progress
        issue_count = len(result.issues_found)
        rewrite_count = len([r for r in result.rewrite_results if r.get("success")])
        status = f"{issue_count} issues"
        if rewrite_count:
            status += f", {rewrite_count} rewrites"
        print(f"  Q{result.query_num:02d}: {result.detection_time_ms:6.1f}ms - {status}")

    return summary


def print_summary(summary: TestSummary):
    """Print test summary."""
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")

    print(f"\n## Query Statistics")
    print(f"  Total queries:        {summary.total_queries}")
    print(f"  Parse failures:       {summary.parse_failures}")
    print(f"  Queries with issues:  {summary.queries_with_issues}")
    print(f"  Total issues found:   {summary.total_issues}")
    print(f"  Avg issues/query:     {summary.total_issues/max(1,summary.total_queries):.1f}")
    print(f"  Total detection time: {summary.total_detection_time_ms:.0f}ms")
    print(f"  Avg time/query:       {summary.total_detection_time_ms/max(1,summary.total_queries):.1f}ms")

    print(f"\n## Issues by Severity")
    for sev in ["critical", "high", "medium", "low", "info"]:
        count = summary.issues_by_severity.get(sev, 0)
        if count:
            print(f"  {sev:10s}: {count:4d}")

    print(f"\n## Top 15 Rules Triggered")
    sorted_rules = sorted(summary.issues_by_rule.items(), key=lambda x: -x[1])
    for rule_id, count in sorted_rules[:15]:
        print(f"  {rule_id:20s}: {count:4d}")

    print(f"\n## Issues by Category")
    sorted_cats = sorted(summary.issues_by_category.items(), key=lambda x: -x[1])
    for cat, count in sorted_cats:
        print(f"  {cat:25s}: {count:4d}")

    print(f"\n## Rewriter Statistics")
    print(f"  Rewrite attempts:   {summary.rewrite_attempts}")
    print(f"  Rewrite successes:  {summary.rewrite_successes}")
    if summary.rewrite_attempts:
        pct = 100 * summary.rewrite_successes / summary.rewrite_attempts
        print(f"  Success rate:       {pct:.1f}%")

    # Coverage stats
    coverage = get_coverage_stats()
    print(f"\n## Rewriter Coverage")
    print(f"  Registered rewriters: {coverage['total_rewriters']}")
    print(f"  Rules with rewriters: {coverage['total_rules_covered']}")

    # Queries with most issues
    print(f"\n## Queries with Most Issues")
    sorted_queries = sorted(summary.query_results, key=lambda x: -len(x.issues_found))
    for qr in sorted_queries[:10]:
        if qr.issues_found:
            print(f"  Q{qr.query_num:02d}: {len(qr.issues_found)} issues")


def main():
    """Main entry point."""
    summary = run_tpcds_tests()
    print_summary(summary)

    # Return exit code based on parse failures
    return 1 if summary.parse_failures > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
