#!/usr/bin/env python3
"""Test complete gold detector coverage on all 7 winning transforms."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "packages" / "qt-sql"))
from qt_sql.analyzers.ast_detector import detect_antipatterns

# All winning transforms from WINNING_TRANSFORMS
TRANSFORMS = {
    93: {"transform": "early_filter", "expected": "GLD-003", "speedup": 2.71},
    90: {"transform": "early_filter", "expected": "GLD-003", "speedup": 1.84},
    74: {"transform": "union_cte_split", "expected": "GLD-006", "speedup": 1.42},
    80: {"transform": "early_filter", "expected": "GLD-003", "speedup": 1.24},
    73: {"transform": "subquery_materialize", "expected": "GLD-007", "speedup": 1.24},
    27: {"transform": "early_filter", "expected": "GLD-003", "speedup": 1.23},
    78: {"transform": "projection_prune", "expected": "GLD-004", "speedup": 1.21},
}

print("=" * 80)
print("GOLD DETECTOR COVERAGE TEST - All 7 Winning Transforms")
print("=" * 80)

results_summary = []
benchmark_dir = ROOT / "research" / "experiments" / "benchmarks" / "kimi_benchmark_20260202_221828"

for qnum, info in sorted(TRANSFORMS.items(), key=lambda x: x[1]["speedup"], reverse=True):
    query_dir = benchmark_dir / f"q{qnum}"
    original_sql = query_dir / "original.sql"

    if not original_sql.exists():
        print(f"\nQ{qnum}: ✗ File not found")
        results_summary.append((qnum, False, "File not found"))
        continue

    sql = original_sql.read_text()
    issues = detect_antipatterns(sql, dialect="duckdb")

    # Check if expected rule is detected
    detected = any(issue.rule_id == info["expected"] for issue in issues)

    status = "✓" if detected else "✗"
    print(f"\nQ{qnum} ({info['speedup']:.2f}x): {status} {info['transform']}")
    print(f"  Expected: {info['expected']}")

    if detected:
        for issue in issues:
            if issue.rule_id == info["expected"]:
                print(f"  Detected: {issue.description[:100]}...")
    else:
        print(f"  NOT DETECTED")
        # Show what gold rules were detected
        gold_issues = [i for i in issues if "GLD" in i.rule_id]
        if gold_issues:
            print(f"  Other gold rules: {', '.join(i.rule_id for i in gold_issues)}")

    results_summary.append((qnum, detected, info["expected"]))

# Final summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

detected_count = sum(1 for _, detected, _ in results_summary if detected)
total = len(results_summary)

print(f"\nCoverage: {detected_count}/{total} ({detected_count/total*100:.0f}%)")
print(f"\nDetailed results:")
for qnum, detected, rule_id in results_summary:
    status = "✓" if detected else "✗"
    speedup = TRANSFORMS[qnum]["speedup"]
    print(f"  {status} Q{qnum} ({speedup:.2f}x) - {rule_id}")

if detected_count == total:
    print(f"\n🎉 SUCCESS: All {total} high-value transforms are detected!")
else:
    print(f"\n⚠️  INCOMPLETE: {total - detected_count} transforms still missing detection")
