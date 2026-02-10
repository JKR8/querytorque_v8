#!/usr/bin/env python3
"""Validate ALL PG DSB leaderboard results using proper 3-run methodology.

For each query with an optimized SQL in best/:
  1. Run original 3x → discard 1st (warmup), average last 2
  2. Run optimized 3x → discard 1st (warmup), average last 2
  3. Compare row counts for correctness
  4. Compute validated speedup

Usage:
    python validate_all_pg_dsb.py                    # all 46 pairs
    python validate_all_pg_dsb.py --wins-only        # only >=1.10x claimed
    python validate_all_pg_dsb.py --query query059_multi  # single query
    python validate_all_pg_dsb.py --top N            # top N by claimed speedup
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
BENCHMARK_DIR = Path(__file__).parent.parent / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "postgres_dsb"
QUERIES_DIR = BENCHMARK_DIR / "queries"
BEST_DIR = BENCHMARK_DIR / "best"
TIMEOUT_MS = 300000  # 5 min timeout per query


def run_query(conn, sql, timeout_ms=TIMEOUT_MS):
    """Run a query and return (elapsed_ms, row_count). Returns (None, None) on error."""
    cur = conn.cursor()
    try:
        cur.execute(f"SET statement_timeout = {timeout_ms}")
        t0 = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        return elapsed, len(rows)
    except Exception as e:
        conn.rollback()
        return None, str(e)
    finally:
        cur.close()


def validate_query(query_id, original_sql, optimized_sql):
    """Run 3x validation on a single query pair. Returns dict with results."""
    conn = psycopg2.connect(DSN)
    conn.autocommit = True

    result = {
        "query_id": query_id,
        "original_times": [],
        "optimized_times": [],
        "original_rows": None,
        "optimized_rows": None,
        "rows_match": None,
        "validated_speedup": None,
        "status": None,
        "error": None,
    }

    # --- Run original 3x ---
    for i in range(3):
        ms, rows = run_query(conn, original_sql)
        if ms is None:
            result["error"] = f"Original run {i+1} failed: {rows}"
            result["status"] = "ERROR"
            conn.close()
            return result
        result["original_times"].append(round(ms, 1))
        if i == 2:  # last run
            result["original_rows"] = rows

    # --- Run optimized 3x ---
    for i in range(3):
        ms, rows = run_query(conn, optimized_sql)
        if ms is None:
            result["error"] = f"Optimized run {i+1} failed: {rows}"
            result["status"] = "ERROR"
            conn.close()
            return result
        result["optimized_times"].append(round(ms, 1))
        if i == 2:
            result["optimized_rows"] = rows

    conn.close()

    # --- Compute: discard 1st (warmup), average last 2 ---
    orig_avg = sum(result["original_times"][1:]) / 2
    opt_avg = sum(result["optimized_times"][1:]) / 2

    result["original_avg_ms"] = round(orig_avg, 1)
    result["optimized_avg_ms"] = round(opt_avg, 1)
    result["rows_match"] = result["original_rows"] == result["optimized_rows"]

    if opt_avg > 0:
        result["validated_speedup"] = round(orig_avg / opt_avg, 2)
    else:
        result["validated_speedup"] = 0

    # Classify
    spd = result["validated_speedup"]
    if not result["rows_match"]:
        result["status"] = "WRONG_RESULTS"
    elif spd >= 1.10:
        result["status"] = "WIN"
    elif spd >= 1.05:
        result["status"] = "IMPROVED"
    elif spd >= 0.95:
        result["status"] = "NEUTRAL"
    else:
        result["status"] = "REGRESSION"

    return result


def load_manifest():
    """Load the best/ manifest to get claimed speedups."""
    manifest_path = BEST_DIR / "manifest.json"
    if manifest_path.exists():
        with open(manifest_path) as f:
            return json.load(f)
    return {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wins-only", action="store_true", help="Only validate >=1.10x claimed")
    parser.add_argument("--query", type=str, help="Validate a single query")
    parser.add_argument("--top", type=int, help="Validate top N by claimed speedup")
    parser.add_argument("--skip-timeout", action="store_true", help="Skip queries with original >120s")
    args = parser.parse_args()

    manifest = load_manifest()
    queries_info = manifest.get("queries", {})

    # Build list of pairs to validate
    pairs = []
    for sql_file in sorted(BEST_DIR.glob("*.sql")):
        qid = sql_file.stem
        orig_file = QUERIES_DIR / f"{qid}.sql"
        if not orig_file.exists():
            continue
        claimed = queries_info.get(qid, {}).get("speedup", 0)
        source = queries_info.get(qid, {}).get("source", "")
        pairs.append((qid, orig_file, sql_file, claimed, source))

    # Apply filters
    if args.query:
        pairs = [(q, o, b, c, s) for q, o, b, c, s in pairs if q == args.query]
    if args.wins_only:
        pairs = [(q, o, b, c, s) for q, o, b, c, s in pairs if c >= 1.10]
    if args.skip_timeout:
        # Skip queries where original runtime > 120s
        SLOW = {"query092_multi", "query032_multi", "query081_multi", "query014_multi"}
        pairs = [(q, o, b, c, s) for q, o, b, c, s in pairs if q not in SLOW]

    # Sort by claimed speedup descending
    pairs.sort(key=lambda x: x[3], reverse=True)

    if args.top:
        pairs = pairs[:args.top]

    print(f"Validating {len(pairs)} query pairs")
    print(f"Method: 3x runs, discard warmup, average last 2")
    print(f"DSN: {DSN}")
    print("=" * 100)

    results = []
    for i, (qid, orig_file, best_file, claimed, source) in enumerate(pairs):
        orig_sql = orig_file.read_text().strip()
        opt_sql = best_file.read_text().strip()

        # Check if original == optimized (the bug we just fixed)
        if orig_sql == opt_sql:
            print(f"  [{i+1}/{len(pairs)}] {qid}: SKIPPED — original == optimized (bug not fixed for this query)")
            results.append({
                "query_id": qid,
                "claimed_speedup": claimed,
                "status": "STALE_SQL",
                "error": "original == optimized",
            })
            continue

        print(f"  [{i+1}/{len(pairs)}] {qid} (claimed {claimed}x from {source})...", end=" ", flush=True)

        r = validate_query(qid, orig_sql, opt_sql)
        r["claimed_speedup"] = claimed
        r["source"] = source
        results.append(r)

        if r["status"] == "ERROR":
            print(f"ERROR: {r['error']}")
        else:
            match_icon = "✓" if r["rows_match"] else "✗ MISMATCH"
            delta = ""
            if claimed > 0 and r["validated_speedup"]:
                diff = r["validated_speedup"] - claimed
                delta = f" (Δ{diff:+.2f})"
            print(f"{r['validated_speedup']}x [rows:{match_icon}] "
                  f"orig={r['original_avg_ms']:.0f}ms opt={r['optimized_avg_ms']:.0f}ms{delta}")

    # --- Summary ---
    print("\n" + "=" * 100)
    print("VALIDATION SUMMARY")
    print("=" * 100)

    validated = [r for r in results if r.get("validated_speedup") is not None]
    errors = [r for r in results if r.get("status") == "ERROR"]
    stale = [r for r in results if r.get("status") == "STALE_SQL"]
    wrong = [r for r in results if r.get("status") == "WRONG_RESULTS"]

    wins = [r for r in validated if r["validated_speedup"] >= 1.10]
    improved = [r for r in validated if 1.05 <= r["validated_speedup"] < 1.10]
    neutral = [r for r in validated if 0.95 <= r["validated_speedup"] < 1.05]
    regression = [r for r in validated if r["validated_speedup"] < 0.95]

    print(f"  WIN (>=1.10x):    {len(wins)}")
    print(f"  IMPROVED:         {len(improved)}")
    print(f"  NEUTRAL:          {len(neutral)}")
    print(f"  REGRESSION:       {len(regression)}")
    print(f"  WRONG RESULTS:    {len(wrong)}")
    print(f"  ERROR:            {len(errors)}")
    print(f"  STALE SQL:        {len(stale)}")

    # Show comparison table
    print(f"\n{'Query':<25} {'Claimed':>8} {'Validated':>10} {'Delta':>8} {'Status':<12} {'Rows':>5}")
    print("-" * 75)
    for r in sorted(validated, key=lambda x: x["validated_speedup"], reverse=True):
        claimed = r.get("claimed_speedup", 0)
        val = r["validated_speedup"]
        delta = val - claimed if claimed > 0 else 0
        rows_ok = "✓" if r.get("rows_match") else "✗"
        print(f"  {r['query_id']:<23} {claimed:>7.2f}x {val:>9.2f}x {delta:>+7.2f} {r['status']:<12} {rows_ok:>5}")

    # Save results
    out_path = Path(__file__).parent / "pg_dsb_validation_results.json"
    with open(out_path, "w") as f:
        json.dump({
            "validated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "method": "3x runs, discard warmup, average last 2",
            "dsn": DSN,
            "summary": {
                "total": len(results),
                "wins": len(wins),
                "improved": len(improved),
                "neutral": len(neutral),
                "regression": len(regression),
                "wrong_results": len(wrong),
                "errors": len(errors),
                "stale": len(stale),
            },
            "results": results,
        }, f, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
