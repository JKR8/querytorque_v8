#!/usr/bin/env python3
"""
Quick test script for v5 optimization on a single query.

Usage:
    export DEEPSEEK_API_KEY=your_key_here
    python3 scripts/test_v5_single_query.py [query_number]

Examples:
    python3 scripts/test_v5_single_query.py 1
    python3 scripts/test_v5_single_query.py 15
"""

import sys
import time
from pathlib import Path

# Add packages to path if not installed
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))

from qt_sql.optimization import optimize_v5_json_queue


def load_query(query_num: int) -> str:
    """Load TPC-DS query from standard location."""
    queries_dir = Path("/mnt/d/TPC-DS/queries_duckdb_converted")

    patterns = [
        f"query_{query_num}.sql",
        f"query{query_num:02d}.sql",
        f"query{query_num}.sql",
    ]

    for pattern in patterns:
        path = queries_dir / pattern
        if path.exists():
            return path.read_text()

    raise FileNotFoundError(f"Query {query_num} not found in {queries_dir}")


def main():
    # Parse query number
    query_num = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    print("=" * 60)
    print(f"V5 Optimization Test - Query {query_num}")
    print("=" * 60)
    print()

    # Load query
    print(f"Loading query {query_num}...")
    try:
        sql = load_query(query_num)
        print(f"✅ Query loaded ({len(sql)} chars)")
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return 1
    print()

    # Database paths
    sample_db = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
    full_db = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"

    # Verify databases exist
    if not Path(sample_db).exists():
        print(f"❌ Sample DB not found: {sample_db}")
        return 1
    if not Path(full_db).exists():
        print(f"❌ Full DB not found: {full_db}")
        return 1

    print(f"Sample DB: {sample_db}")
    print(f"Full DB: {full_db}")
    print()

    # Run optimization
    print("Running v5 optimization...")
    print("  - 5 workers in parallel on sample DB")
    print("  - Sequential validation on full DB")
    print("  - Early stopping when speedup ≥2.0x")
    print()
    print("This will take 1-5 minutes...")
    print()

    start_time = time.time()

    try:
        valid, full_results, winner = optimize_v5_json_queue(
            sql=sql,
            sample_db=sample_db,
            full_db=full_db,
            max_workers=5,
            target_speedup=2.0,
        )

        elapsed = time.time() - start_time

        # Print results
        print("=" * 60)
        print("Results")
        print("=" * 60)
        print()
        print(f"Elapsed time: {elapsed:.1f}s")
        print()

        print(f"Valid candidates from sample: {len(valid)}")
        if valid:
            print("  Sample speedups:")
            for v in valid:
                status = "✅" if v.speedup >= 1.0 else "⚠️"
                print(f"    {status} Worker {v.worker_id}: {v.speedup:.2f}x")
        print()

        print(f"Full DB validations: {len(full_results)}")
        if full_results:
            print("  Full speedups:")
            for fr in full_results:
                if fr.full_status.value == "pass":
                    status = "✅" if fr.full_speedup >= 2.0 else "⚠️"
                    print(f"    {status} Worker {fr.sample.worker_id}: {fr.full_speedup:.2f}x")
                else:
                    print(f"    ❌ Worker {fr.sample.worker_id}: {fr.full_status.value}")
        print()

        if winner:
            print(f"🏆 Winner found!")
            print(f"   Worker: {winner.sample.worker_id}")
            print(f"   Sample speedup: {winner.sample.speedup:.2f}x")
            print(f"   Full speedup: {winner.full_speedup:.2f}x")
            print()
            print("Optimized SQL:")
            print("-" * 60)
            print(winner.sample.optimized_sql[:500])
            if len(winner.sample.optimized_sql) > 500:
                print("...")
                print(f"({len(winner.sample.optimized_sql)} total chars)")
        else:
            print("❌ No winner found (speedup <2.0x)")
            if valid:
                best = max(valid, key=lambda v: v.speedup)
                print(f"   Best sample speedup: {best.speedup:.2f}x (worker {best.worker_id})")
            if full_results:
                best_full = max(
                    [fr for fr in full_results if fr.full_status.value == "pass"],
                    key=lambda fr: fr.full_speedup,
                    default=None
                )
                if best_full:
                    print(f"   Best full speedup: {best_full.full_speedup:.2f}x (worker {best_full.sample.worker_id})")

        print()
        print("=" * 60)
        print("✅ Test completed successfully")
        print("=" * 60)

        return 0

    except Exception as e:
        print()
        print("=" * 60)
        print("❌ Test failed")
        print("=" * 60)
        print()
        print(f"Error: {e}")
        print()

        import traceback
        traceback.print_exc()

        return 1


if __name__ == "__main__":
    sys.exit(main())
