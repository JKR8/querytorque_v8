#!/usr/bin/env python3
"""Generate ML training data from TPC-DS benchmark results.

Creates CSV with:
- Query (normalized)
- AST detections (rule IDs)
- Transforms applied
- Winning transform
- Speedup factor
"""

import json
import csv
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, "packages/qt-sql")

from qt_sql.analyzers.ast_detector import detect_antipatterns

BASE = Path(__file__).parent.parent
BENCHMARK_DIR = BASE / "research" / "experiments" / "benchmarks" / "kimi_benchmark_20260202_221828"
OUTPUT_CSV = BASE / "research" / "ml_training_data.csv"

# Known winning transforms from analysis
WINNING_TRANSFORMS = {
    1: {"transform": "decorrelate", "speedup": 2.81},
    15: {"transform": "or_to_union", "speedup": 2.67},
    93: {"transform": "early_filter", "speedup": 2.71},
    90: {"transform": "early_filter", "speedup": 1.84},
    74: {"transform": "union_cte_split", "speedup": 1.42},
    80: {"transform": "early_filter", "speedup": 1.24},
    73: {"transform": "subquery_materialize", "speedup": 1.24},
    27: {"transform": "early_filter", "speedup": 1.23},
    78: {"transform": "projection_prune", "speedup": 1.21},
}


def load_summary():
    """Load benchmark summary with speedups."""
    summary_path = BENCHMARK_DIR / "summary.json"
    with open(summary_path) as f:
        data = json.load(f)
    return data["results"]


def detect_rules(sql_text: str) -> list[str]:
    """Get list of detected rule IDs."""
    issues = detect_antipatterns(sql_text, dialect="duckdb")
    return [issue.rule_id for issue in issues]


def get_gold_rules(rule_ids: list[str]) -> list[str]:
    """Filter to only gold rules."""
    return [r for r in rule_ids if r.startswith("GLD-")]


def generate_training_data():
    """Generate ML training CSV."""

    print("Loading benchmark results...")
    results = load_summary()

    # Build speedup map
    speedup_map = {}
    for r in results:
        if r["status"] == "pass":
            speedup_map[r["query"]] = r["speedup"]

    print(f"Found {len(speedup_map)} successful queries")

    # Generate training rows
    rows = []

    for qnum in range(1, 100):  # TPC-DS has 99 queries
        query_dir = BENCHMARK_DIR / f"q{qnum}"
        original_sql = query_dir / "original.sql"

        if not original_sql.exists():
            continue

        # Get SQL
        sql = original_sql.read_text()

        # Detect AST rules
        rule_ids = detect_rules(sql)
        gold_rules = get_gold_rules(rule_ids)

        # Get speedup
        speedup = speedup_map.get(qnum, 1.0)

        # Get winning transform if exists
        winning_transform = ""
        if qnum in WINNING_TRANSFORMS:
            winning_transform = WINNING_TRANSFORMS[qnum]["transform"]

        # Create row
        row = {
            "query_id": f"q{qnum}",
            "speedup": f"{speedup:.3f}",
            "has_win": "1" if speedup >= 1.2 else "0",
            "winning_transform": winning_transform,
            "all_detections": "|".join(rule_ids),
            "gold_detections": "|".join(gold_rules),
            "detection_count": len(rule_ids),
            "gold_count": len(gold_rules),
            "sql_length": len(sql),
            "has_cte": "1" if "WITH" in sql.upper() else "0",
            "has_union": "1" if "UNION" in sql.upper() else "0",
            "has_subquery": "1" if "SELECT" in sql[100:] else "0",  # After first SELECT
        }

        rows.append(row)

        if qnum % 10 == 0:
            print(f"  Processed {qnum} queries...")

    # Write CSV
    print(f"\nWriting {len(rows)} rows to {OUTPUT_CSV}")

    fieldnames = [
        "query_id",
        "speedup",
        "has_win",
        "winning_transform",
        "all_detections",
        "gold_detections",
        "detection_count",
        "gold_count",
        "sql_length",
        "has_cte",
        "has_union",
        "has_subquery",
    ]

    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Print stats
    print("\n" + "=" * 60)
    print("Training Data Statistics")
    print("=" * 60)

    total = len(rows)
    wins = sum(1 for r in rows if r["has_win"] == "1")
    with_gold = sum(1 for r in rows if int(r["gold_count"]) > 0)

    print(f"Total queries: {total}")
    print(f"Queries with speedup ≥ 1.2x: {wins} ({wins/total*100:.1f}%)")
    print(f"Queries with gold detections: {with_gold} ({with_gold/total*100:.1f}%)")

    # Winning transforms distribution
    transform_counts = defaultdict(int)
    for r in rows:
        if r["winning_transform"]:
            transform_counts[r["winning_transform"]] += 1

    print("\nWinning transforms distribution:")
    for transform, count in sorted(transform_counts.items(), key=lambda x: -x[1]):
        print(f"  {transform}: {count}")

    print(f"\n✓ Training data saved: {OUTPUT_CSV}")


if __name__ == "__main__":
    generate_training_data()
