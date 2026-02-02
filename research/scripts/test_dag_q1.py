#!/usr/bin/env python3
"""
Quick test of DAG-based optimizer on TPC-DS Q1.

Usage:
    python research/scripts/test_dag_q1.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-sql"))

import duckdb
from qt_sql.optimization.sql_dag import SQLDag, build_dag_prompt

# Config
FULL_DB = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
QUERY_FILE = "/mnt/d/TPC-DS/queries_duckdb_converted/query_1.sql"


def benchmark(conn, sql, runs=3):
    """Run query multiple times, return avg of runs 2-3."""
    times = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        times.append(time.perf_counter() - start)
    return sum(times[1:]) / 2, result


def validate(orig_result, opt_result):
    """Check semantic equivalence."""
    return set(orig_result) == set(opt_result)


def main():
    print("=" * 60)
    print("DAG Optimizer Test - TPC-DS Q1")
    print("=" * 60)

    # Load query
    original_sql = Path(QUERY_FILE).read_text()
    print(f"\nLoaded: {QUERY_FILE}")

    # Build DAG
    dag = SQLDag.from_sql(original_sql)
    print(f"\nDAG nodes: {list(dag.nodes.keys())}")
    print(f"DAG edges: {[(e.source, e.target) for e in dag.edges]}")

    # Show prompt (truncated)
    print("\n" + "-" * 60)
    print("Generated Prompt (first 1500 chars):")
    print("-" * 60)
    prompt = build_dag_prompt(original_sql)
    print(prompt[:1500])
    print("...\n")

    # Example rewrite (the validated optimization)
    rewrites = {
        "customer_total_return": """
            SELECT sr_customer_sk AS ctr_customer_sk,
                   sr_store_sk AS ctr_store_sk,
                   SUM(SR_FEE) AS ctr_total_return,
                   AVG(SUM(SR_FEE)) OVER (PARTITION BY sr_store_sk) * 1.2 AS ctr_avg_threshold
            FROM store_returns
            JOIN date_dim ON sr_returned_date_sk = d_date_sk
            JOIN store ON sr_store_sk = s_store_sk
            WHERE d_year = 2000 AND s_state = 'SD'
            GROUP BY sr_customer_sk, sr_store_sk
        """,
        "main_query": """
            SELECT c_customer_id
            FROM customer_total_return AS ctr1
            JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
            WHERE ctr1.ctr_total_return > ctr1.ctr_avg_threshold
            ORDER BY c_customer_id
            LIMIT 100
        """
    }

    optimized_sql = dag.apply_rewrites(rewrites)

    print("-" * 60)
    print("Applied Rewrites:")
    print("-" * 60)
    for node_id in rewrites:
        print(f"  - {node_id}")

    print("\n" + "-" * 60)
    print("Optimized SQL:")
    print("-" * 60)
    print(optimized_sql[:800])
    print("...")

    # Benchmark on full DB
    print("\n" + "=" * 60)
    print("Benchmarking on SF100...")
    print("=" * 60)

    conn = duckdb.connect(FULL_DB, read_only=True)

    orig_time, orig_result = benchmark(conn, original_sql)
    opt_time, opt_result = benchmark(conn, optimized_sql)

    conn.close()

    speedup = orig_time / opt_time
    correct = validate(orig_result, opt_result)

    print(f"\nOriginal:  {orig_time:.3f}s ({len(orig_result)} rows)")
    print(f"Optimized: {opt_time:.3f}s ({len(opt_result)} rows)")
    print(f"Speedup:   {speedup:.2f}x")
    print(f"Validated: {'✓ CORRECT' if correct else '✗ MISMATCH'}")

    return 0 if correct else 1


if __name__ == "__main__":
    sys.exit(main())
