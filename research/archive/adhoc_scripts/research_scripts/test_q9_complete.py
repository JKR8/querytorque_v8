#!/usr/bin/env python3
"""
Test Query 9 with Complete v5 Workflow

Runs the full DSPy v5 pipeline:
1. Generate 5 candidates (different example batches)
2. Validate on sample DB
3. Save to disk
4. Benchmark on main DB (5 runs with trimmed mean)
5. Select winner
"""

import sys
from pathlib import Path

# Add packages to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))

from qt_sql.optimization.candidate_benchmarker import optimize_v5_complete

# Configuration
QUERY_FILE = Path("/mnt/d/TPC-DS/queries_duckdb_converted/query_9.sql")
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
MAIN_DB = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
OUTPUT_DIR = Path("dspy_v5_results")
PROVIDER = "deepseek"
RUNS = 5

def main():
    print("=" * 80)
    print("Query 9 - Complete DSPy v5 Optimization Workflow")
    print("=" * 80)
    print()

    # Load query
    sql = QUERY_FILE.read_text()
    print(f"Query: {QUERY_FILE.name}")
    print(f"Length: {len(sql)} chars")
    print()

    # Run complete workflow
    benchmarked, winner, run_dir = optimize_v5_complete(
        sql=sql,
        sample_db=SAMPLE_DB,
        main_db=MAIN_DB,
        output_dir=OUTPUT_DIR,
        provider=PROVIDER,
        runs=RUNS,
    )

    # Display summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total candidates: {len(benchmarked)}")
    print(f"Successful benchmarks: {sum(1 for b in benchmarked if b.error is None)}")
    print()

    print("All Candidates:")
    print("-" * 80)
    for b in sorted(benchmarked, key=lambda x: x.main_db_speedup, reverse=True):
        status_icon = "✅" if b.error is None else "❌"
        print(f"{status_icon} Worker #{b.worker_id}: {b.main_db_speedup:.2f}x speedup, {b.main_db_latency_s:.3f}s")
        if b.error:
            print(f"   Error: {b.error}")
    print()

    if winner:
        print("Winner:")
        print("-" * 80)
        print(f"Worker: #{winner.worker_id}")
        print(f"Speedup: {winner.main_db_speedup:.2f}x")
        print(f"Latency: {winner.main_db_latency_s:.3f}s")
        print()

    print(f"Full results saved to: {run_dir.absolute()}")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
