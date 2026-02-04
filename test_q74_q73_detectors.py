#!/usr/bin/env python3
"""Test GLD-006 and GLD-007 on Q74 and Q73."""

import sys
sys.path.insert(0, "packages/qt-sql")

from pathlib import Path
from qt_sql.analyzers.ast_detector import detect_antipatterns

# Test Q74
print("=" * 70)
print("Testing Q74 (UNION CTE Specialization - GLD-006)")
print("=" * 70)

q74_sql = Path("research/experiments/benchmarks/kimi_benchmark_20260202_221828/q74/original.sql").read_text()

results = detect_antipatterns(q74_sql, dialect="duckdb")
gld006_found = False
for issue in results:
    if issue.rule_id == "GLD-006":
        print(f"✓ {issue.rule_id}: {issue.description}")
        gld006_found = True

if not gld006_found:
    print("✗ GLD-006 NOT detected on Q74")
    print("\nAll gold rules detected:")
    for issue in results:
        if "GLD" in issue.rule_id or issue.severity == "gold":
            print(f"  - {issue.rule_id}: {issue.description[:80]}")

# Test Q73
print("\n" + "=" * 70)
print("Testing Q73 (Subquery Materialization - GLD-007)")
print("=" * 70)

q73_path = None
for path in Path("research/experiments/benchmarks").rglob("q73/original.sql"):
    q73_path = path
    break

if q73_path:
    q73_sql = q73_path.read_text()

    results = detect_antipatterns(q73_sql, dialect="duckdb")
    gld007_found = False
    for issue in results:
        if issue.rule_id == "GLD-007":
            print(f"✓ {issue.rule_id}: {issue.description}")
            gld007_found = True

    if not gld007_found:
        print("✗ GLD-007 NOT detected on Q73")
        print("\nAll gold rules detected:")
        for issue in results:
            if "GLD" in issue.rule_id or issue.severity == "gold":
                print(f"  - {issue.rule_id}: {issue.description[:80]}")
else:
    print("✗ Could not find Q73 query file")

print("\n" + "=" * 70)
print("Summary")
print("=" * 70)
print(f"GLD-006 (Q74): {'✓ DETECTED' if gld006_found else '✗ NOT DETECTED'}")
print(f"GLD-007 (Q73): {'✓ DETECTED' if gld007_found else '✗ NOT DETECTED'}")
