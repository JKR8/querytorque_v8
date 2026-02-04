#!/usr/bin/env python3
"""
Analyze all 99 TPC-DS queries with AST detector.

Usage:
    python scripts/analyze_tpcds_queries.py
"""

import sys
from pathlib import Path
from collections import defaultdict, Counter
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))

from qt_sql.analyzers.ast_detector import ASTDetector


def analyze_query(sql: str, query_name: str, include_style: bool = False):
    """Analyze a single query with AST detector."""
    try:
        detector = ASTDetector(dialect="duckdb", include_style=include_style)
        issues = detector.detect(sql)
        return issues
    except Exception as e:
        print(f"  ⚠ Failed to analyze {query_name}: {e}")
        return []


def main():
    # TPC-DS query location from CLAUDE.md
    queries_dir = Path("D:/TPC-DS/queries_duckdb_converted")

    if not queries_dir.exists():
        print(f"❌ Queries directory not found: {queries_dir}")
        print("\nTrying alternative location...")
        queries_dir = Path("/mnt/d/TPC-DS/queries_duckdb_converted")

    if not queries_dir.exists():
        print(f"❌ Queries directory not found: {queries_dir}")
        print("\nPlease update the path in the script.")
        return

    print(f"📁 Reading queries from: {queries_dir}\n")

    # Find all query files
    query_files = sorted(queries_dir.glob("query*.sql"))

    if not query_files:
        print(f"❌ No query files found in {queries_dir}")
        return

    print(f"Found {len(query_files)} queries\n")
    print("=" * 80)

    # Analyze each query
    all_matches = []
    query_results = {}
    rule_frequency = Counter()
    category_frequency = Counter()
    severity_counts = Counter()
    queries_with_findings = set()

    for i, query_file in enumerate(query_files, 1):
        query_name = query_file.stem
        print(f"\n[{i}/{len(query_files)}] Analyzing {query_name}...")

        try:
            sql = query_file.read_text(encoding="utf-8")
        except Exception as e:
            print(f"  ⚠ Failed to read {query_name}: {e}")
            continue

        issues = analyze_query(sql, query_name, include_style=False)

        if issues:
            queries_with_findings.add(query_name)
            print(f"  ✓ Found {len(issues)} findings")

            # Store results
            query_results[query_name] = issues
            all_matches.extend(issues)

            # Update counters
            for issue in issues:
                rule_frequency[issue.rule_id] += 1
                category_frequency[issue.category] += 1
                severity_counts[issue.severity] += 1

            # Show top 3 findings for this query
            for issue in issues[:3]:
                print(f"    - [{issue.rule_id}] {issue.severity.upper()}: {issue.name[:60]}...")
        else:
            print(f"  ✓ No findings")

    # Summary Report
    print("\n" + "=" * 80)
    print("\n📊 SUMMARY REPORT")
    print("=" * 80)

    print(f"\n📈 Overall Statistics:")
    print(f"  Total queries analyzed: {len(query_files)}")
    print(f"  Queries with findings: {len(queries_with_findings)}")
    print(f"  Total findings: {len(all_matches)}")
    print(f"  Average findings per query: {len(all_matches) / len(query_files):.1f}")

    print(f"\n⚠ Severity Breakdown:")
    for severity in ['error', 'warning', 'info']:
        count = severity_counts.get(severity, 0)
        pct = (count / len(all_matches) * 100) if all_matches else 0
        print(f"  {severity.capitalize()}: {count} ({pct:.1f}%)")

    print(f"\n📂 Top 10 Categories:")
    for category, count in category_frequency.most_common(10):
        pct = (count / len(all_matches) * 100) if all_matches else 0
        print(f"  {category}: {count} ({pct:.1f}%)")

    print(f"\n🔍 Top 20 Rules (Most Frequent):")
    for rule_id, count in rule_frequency.most_common(20):
        queries_hit = sum(1 for q, matches in query_results.items()
                         if any(m.rule_id == rule_id for m in matches))
        print(f"  {rule_id}: {count} findings across {queries_hit} queries")

    # Queries with most findings
    print(f"\n🔥 Queries with Most Findings (Top 10):")
    query_counts = [(q, len(matches)) for q, matches in query_results.items()]
    query_counts.sort(key=lambda x: x[1], reverse=True)
    for query_name, count in query_counts[:10]:
        print(f"  {query_name}: {count} findings")

    # Save detailed results to JSON
    output_file = project_root / "docs" / "tpcds_ast_analysis.json"

    # Convert issues to serializable format
    serializable_results = {}
    for query_name, issues in query_results.items():
        serializable_results[query_name] = [
            {
                "rule_id": i.rule_id,
                "name": i.name,
                "severity": i.severity,
                "category": i.category,
                "description": i.description,
                "suggestion": i.suggestion,
                "location": i.location,
            }
            for i in issues
        ]

    report_data = {
        "summary": {
            "total_queries": len(query_files),
            "queries_with_findings": len(queries_with_findings),
            "total_findings": len(all_matches),
            "avg_per_query": round(len(all_matches) / len(query_files), 2),
        },
        "severity_counts": dict(severity_counts),
        "category_counts": dict(category_frequency),
        "rule_frequency": dict(rule_frequency.most_common(50)),
        "queries": serializable_results,
    }

    with open(output_file, "w") as f:
        json.dump(report_data, f, indent=2)

    print(f"\n💾 Detailed results saved to: {output_file}")
    print("\n✅ Analysis complete!")


if __name__ == "__main__":
    main()
