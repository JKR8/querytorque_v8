#!/usr/bin/env python3
"""Re-benchmark saved queries with correct methodology (3 runs, discard 1st, avg 2)"""

import time
from pathlib import Path
import duckdb

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
BATCH_DIR = Path("research/experiments/dspy_runs/batch_20260201_202648")

def benchmark_query(conn, sql, runs=3):
    """3 runs, discard first (warmup), average remaining 2."""
    times = []
    result = None
    for i in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        print(f"    Run {i+1}: {elapsed:.4f}s")
    avg = sum(times[1:]) / len(times[1:])
    print(f"    Avg (runs 2-3): {avg:.4f}s")
    return avg, len(result), result

print(f"Database: {SAMPLE_DB}")
print(f"Methodology: 3 runs, discard 1st, average runs 2-3\n")

conn = duckdb.connect(SAMPLE_DB, read_only=True)

results = []
for qdir in sorted(BATCH_DIR.iterdir()):
    if not qdir.is_dir():
        continue

    orig_file = qdir / "original.sql"
    opt_file = qdir / "optimized.sql"

    if not orig_file.exists() or not opt_file.exists():
        continue

    qname = qdir.name
    print(f"{'='*50}")
    print(f"{qname.upper()}")
    print('='*50)

    orig_sql = orig_file.read_text()
    opt_sql = opt_file.read_text()

    print("  Original:")
    orig_time, orig_rows, orig_result = benchmark_query(conn, orig_sql)

    print("  Optimized:")
    try:
        opt_time, opt_rows, opt_result = benchmark_query(conn, opt_sql)

        correct = sorted([tuple(r) for r in orig_result]) == sorted([tuple(r) for r in opt_result])
        speedup = orig_time / opt_time if opt_time > 0 else 0

        print(f"\n  Result: {speedup:.2f}x speedup, Correct: {'YES' if correct else 'NO'}")
        print(f"  Rows: {orig_rows} -> {opt_rows}")

        results.append((qname, orig_time, opt_time, speedup, correct))
    except Exception as e:
        print(f"  ERROR: {e}")
        results.append((qname, orig_time, None, None, False))

conn.close()

print(f"\n{'='*60}")
print("SUMMARY (correct methodology)")
print('='*60)
print(f"{'Query':<8} {'Original':<12} {'Optimized':<12} {'Speedup':<10} {'Correct'}")
print('-'*60)
for qname, orig, opt, speedup, correct in results:
    if opt:
        print(f"{qname:<8} {orig:<12.4f} {opt:<12.4f} {speedup:<10.2f}x {'YES' if correct else 'NO'}")
    else:
        print(f"{qname:<8} {orig:<12.4f} {'ERROR':<12} {'-':<10} NO")
