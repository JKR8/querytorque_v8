"""Re-benchmark internally-consistent retry pairs against their OWN originals.

Uses 3x3 validation: run 3 times, discard 1st (warmup), average last 2.
Runs on DuckDB SF10.
"""
import os
import sys
import glob
import json
import time
import csv
import duckdb
from pathlib import Path

DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10_1.duckdb"
RETRY_BASE = "research/archive/benchmark_results/retry_runs"
TIMEOUT_S = 300
N_RUNS = 3  # 3x3: warmup + 2 measured

# Map leaderboard entries to retry directories
# Retry3W = retry_collect, Retry4W = retry_neutrals
BATCH_MAP = {
    "Retry3W": "retry_collect",
    "Retry4W": "retry_neutrals",
}

# PARAM_MISMATCH queries from leaderboard v4 with their best source + worker
# Format: (query_num, source, worker_num)
PARAM_MISMATCH_QUERIES = [
    (9, "Retry3W", 2),
    (63, "Retry3W", 2),
    (40, "Retry4W", 2),
    (46, "Retry4W", 3),
    (42, "Retry4W", 3),  # also in retry_collect
    (77, "Retry4W", 4),
    (52, "Retry4W", 3),
    (21, "Retry4W", 2),
    (29, "Retry3W", 1),
    (23, "Retry4W", 1),
    (47, "Retry4W", 3),
    (99, "Retry4W", 3),
    (80, "Retry4W", 3),
    (39, "Retry4W", 4),
    (97, "Retry4W", 4),
    (26, "Retry3W", 1),
    (69, "Retry4W", 2),
    (5, "Retry3W", 1),
    (14, "Retry4W", 4),
    (85, "Retry4W", 3),
    (54, "Retry4W", 3),
    (22, "Retry3W", 2),
    (96, "Retry3W", 2),
    (58, "Retry4W", 1),
    (73, "Retry3W", 2),
    (36, "Retry4W", 4),
    (38, "Retry3W", 2),
    (68, "Retry4W", 2),
    (72, "Retry4W", 1),
    (31, "Retry4W", 3),
    (10, "Retry4W", 2),
    (37, "Retry3W", 2),
    (98, "Retry4W", 2),
    (12, "Retry3W", 3),
    (82, "Retry3W", 1),
    (19, "Retry4W", 4),
    (92, "Retry4W", 1),
    (20, "Retry4W", 3),
    (3, "Retry4W", 1),
    # Kimi PARAM_MISMATCH (no retry pairs available, skip)
    # (74, "Kimi", None),
    # (76, "Kimi", None),
]


def run_query_timed(con, sql, timeout_s=TIMEOUT_S):
    """Run a query and return elapsed ms, or None on timeout/error."""
    try:
        start = time.perf_counter()
        con.execute(sql)
        _ = con.fetchall()
        elapsed = (time.perf_counter() - start) * 1000
        return elapsed
    except Exception as e:
        return None


def benchmark_pair(orig_sql, opt_sql, query_label):
    """Run 3x3 benchmark for a single original/optimized pair."""
    results = {"query": query_label, "status": "OK"}

    # Run original 3 times
    con = duckdb.connect(DB_PATH, read_only=True)
    orig_times = []
    for i in range(N_RUNS):
        t = run_query_timed(con, orig_sql)
        if t is None:
            results["status"] = "ORIG_ERROR"
            results["orig_ms"] = None
            con.close()
            return results
        orig_times.append(t)
    con.close()

    # Run optimized 3 times
    con = duckdb.connect(DB_PATH, read_only=True)
    opt_times = []
    for i in range(N_RUNS):
        t = run_query_timed(con, opt_sql)
        if t is None:
            results["status"] = "OPT_ERROR"
            results["orig_ms"] = sum(orig_times[1:]) / (N_RUNS - 1)
            results["opt_ms"] = None
            con.close()
            return results
        opt_times.append(t)
    con.close()

    # 3x3: discard first run (warmup), average last 2
    orig_avg = sum(orig_times[1:]) / (N_RUNS - 1)
    opt_avg = sum(opt_times[1:]) / (N_RUNS - 1)

    results["orig_times"] = [round(t, 1) for t in orig_times]
    results["opt_times"] = [round(t, 1) for t in opt_times]
    results["orig_ms"] = round(orig_avg, 1)
    results["opt_ms"] = round(opt_avg, 1)
    results["speedup"] = round(orig_avg / opt_avg, 2) if opt_avg > 0 else 0

    if results["speedup"] >= 1.5:
        results["verdict"] = "WIN"
    elif results["speedup"] >= 1.05:
        results["verdict"] = "IMPROVED"
    elif results["speedup"] >= 0.95:
        results["verdict"] = "NEUTRAL"
    else:
        results["verdict"] = "REGRESSION"

    return results


def find_pair(qnum, source, worker):
    """Find original + optimized SQL files for a retry pair."""
    batch_dir = BATCH_MAP.get(source)
    if not batch_dir:
        return None, None

    base = os.path.join(RETRY_BASE, batch_dir, f"q{qnum}")
    orig_path = os.path.join(base, "original.sql")
    opt_path = os.path.join(base, f"w{worker}_optimized.sql")

    if os.path.exists(orig_path) and os.path.exists(opt_path):
        return orig_path, opt_path

    # Try alternate batch directories
    for alt_batch in ["retry_neutrals", "retry_collect", "retry_neutrals_sf10_winners", "retry_under_1_3x"]:
        alt_base = os.path.join(RETRY_BASE, alt_batch, f"q{qnum}")
        alt_orig = os.path.join(alt_base, "original.sql")
        alt_opt = os.path.join(alt_base, f"w{worker}_optimized.sql")
        if os.path.exists(alt_orig) and os.path.exists(alt_opt):
            return alt_orig, alt_opt

    return None, None


def main():
    os.chdir("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")

    # Verify DB
    con = duckdb.connect(DB_PATH, read_only=True)
    row_count = con.execute("SELECT count(*) FROM store_sales").fetchone()[0]
    con.close()
    print(f"DuckDB SF10 verified: store_sales = {row_count:,} rows")
    print(f"Benchmarking {len(PARAM_MISMATCH_QUERIES)} retry pairs with 3x3 validation\n")

    all_results = []
    wins = 0
    improved = 0
    neutral = 0
    regression = 0
    errors = 0

    for i, (qnum, source, worker) in enumerate(PARAM_MISMATCH_QUERIES):
        label = f"Q{qnum} ({source}/w{worker})"
        print(f"[{i+1}/{len(PARAM_MISMATCH_QUERIES)}] {label}...", end=" ", flush=True)

        orig_path, opt_path = find_pair(qnum, source, worker)
        if not orig_path:
            print("SKIP (files not found)")
            continue

        with open(orig_path) as f:
            orig_sql = f.read()
        with open(opt_path) as f:
            opt_sql = f.read()

        if not opt_sql.strip():
            print("SKIP (empty optimized SQL)")
            continue

        result = benchmark_pair(orig_sql, opt_sql, f"Q{qnum}")
        result["source"] = source
        result["worker"] = worker
        result["batch_dir"] = os.path.dirname(orig_path)
        all_results.append(result)

        if result["status"] != "OK":
            print(f"{result['status']}")
            errors += 1
        else:
            v = result["verdict"]
            s = result["speedup"]
            print(f"{s:.2f}x {v} (orig={result['orig_ms']:.0f}ms opt={result['opt_ms']:.0f}ms)")
            if v == "WIN":
                wins += 1
            elif v == "IMPROVED":
                improved += 1
            elif v == "NEUTRAL":
                neutral += 1
            else:
                regression += 1

    # Summary
    print(f"\n{'='*80}")
    print("RESULTS SUMMARY")
    print(f"{'='*80}")
    print(f"Total benchmarked: {len(all_results)}")
    print(f"  WIN (>=1.5x):      {wins}")
    print(f"  IMPROVED (>=1.05x): {improved}")
    print(f"  NEUTRAL:           {neutral}")
    print(f"  REGRESSION (<0.95x): {regression}")
    print(f"  ERRORS:            {errors}")

    # Sort by speedup descending
    valid = [r for r in all_results if r["status"] == "OK"]
    valid.sort(key=lambda r: r["speedup"], reverse=True)

    print(f"\n{'='*80}")
    print("DETAILED RESULTS (sorted by speedup)")
    print(f"{'='*80}")
    print(f"{'Query':<8} {'Speedup':>8} {'Verdict':<12} {'Orig_ms':>10} {'Opt_ms':>10} {'Source'}")
    print("-" * 70)
    for r in valid:
        print(f"{r['query']:<8} {r['speedup']:>7.2f}x {r['verdict']:<12} {r['orig_ms']:>10.1f} {r['opt_ms']:>10.1f} {r['source']}/w{r['worker']}")

    # Save results
    output_path = "research/retry_rebenchmark_results.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {output_path}")

    # CSV output
    csv_path = "research/retry_rebenchmark_results.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Query", "Speedup", "Verdict", "Orig_ms", "Opt_ms", "Source", "Worker", "Status"])
        for r in valid:
            w.writerow([r["query"], r["speedup"], r["verdict"], r["orig_ms"], r["opt_ms"], r["source"], r["worker"], r["status"]])
        for r in all_results:
            if r["status"] != "OK":
                w.writerow([r["query"], "", "", r.get("orig_ms", ""), r.get("opt_ms", ""), r.get("source", ""), r.get("worker", ""), r["status"]])
    print(f"CSV saved to {csv_path}")


if __name__ == "__main__":
    main()
