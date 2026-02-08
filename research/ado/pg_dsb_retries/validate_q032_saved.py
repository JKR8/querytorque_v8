#!/usr/bin/env python3
"""Validate saved Q032 swarm results — EXPLAIN gate + SF10 5x trimmed mean.

NO API CALLS. Reads saved optimized SQL from swarm_sessions/ on disk.

Usage (from project root):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb/validate_q032_saved.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import psycopg2

DSN_SF5 = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf5"
DSN_SF10 = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"

SESSION_DIR = Path("packages/qt-sql/ado/benchmarks/postgres_dsb/swarm_sessions/query032_multi/iteration_00_fan_out")
QUERY_DIR = Path("packages/qt-sql/ado/benchmarks/postgres_dsb/queries")
OUT_DIR = Path("packages/qt-sql/ado/benchmarks/postgres_dsb")


def get_explain_cost(dsn: str, sql: str) -> float | None:
    try:
        con = psycopg2.connect(dsn)
        con.autocommit = True
        cur = con.cursor()
        cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
        plan = cur.fetchone()[0]
        con.close()
        return plan[0]["Plan"]["Total Cost"]
    except Exception as e:
        print(f"    EXPLAIN failed: {e}")
        return None


def run_5x(dsn: str, sql: str, timeout_ms: int = 300_000) -> list[float]:
    con = psycopg2.connect(dsn)
    con.autocommit = True
    cur = con.cursor()
    times = []
    for i in range(5):
        try:
            cur.execute(f"SET statement_timeout = {timeout_ms}")
            t0 = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            elapsed = (time.perf_counter() - t0) * 1000
            times.append(elapsed)
            print(f"    Run {i+1}: {elapsed:.1f}ms")
            cur.execute("SET statement_timeout = 0")
        except Exception as e:
            cur.execute("ROLLBACK")
            cur.execute("SET statement_timeout = 0")
            times.append(float("inf"))
            print(f"    Run {i+1}: TIMEOUT/ERROR ({e})")
    con.close()
    return times


def trimmed_mean(times: list[float]) -> float:
    finite = [t for t in times if t != float("inf")]
    if len(finite) < 3:
        return float("inf")
    finite.sort()
    return sum(finite[1:-1]) / (len(finite) - 2)


def main():
    print("\n" + "=" * 70)
    print("  Q032 VALIDATION — Recover saved results, EXPLAIN + SF10 5x")
    print("  NO API CALLS — reading from disk only")
    print("=" * 70)

    # Load original SQL
    orig_sql = (QUERY_DIR / "query032_multi.sql").read_text()

    # Load saved worker results
    workers = []
    for wdir in sorted(SESSION_DIR.iterdir()):
        if not wdir.is_dir() or not wdir.name.startswith("worker_"):
            continue
        result = json.loads((wdir / "result.json").read_text())
        opt_sql = (wdir / "optimized.sql").read_text()
        workers.append({
            "worker_id": result["worker_id"],
            "strategy": result["strategy"],
            "transforms": result["transforms"],
            "swarm_status": result["status"],
            "swarm_speedup": result["speedup"],
            "sql": opt_sql,
            "error": result.get("error_message"),
        })
        status_marker = "*" if result["status"] == "WIN" else " "
        print(f"  {status_marker} W{result['worker_id']} ({result['strategy']}): "
              f"{result['status']} {result['speedup']:.1f}x")

    # EXPLAIN cost gate
    print(f"\n--- EXPLAIN Cost Gate (SF5) ---")
    orig_cost = get_explain_cost(DSN_SF5, orig_sql)
    print(f"  Original cost: {orig_cost}")

    for w in workers:
        if w["swarm_status"] == "ERROR":
            w["explain_cost"] = None
            w["cost_ratio"] = 0
            print(f"  W{w['worker_id']} ({w['strategy']}): SKIPPED (swarm ERROR)")
            continue
        cost = get_explain_cost(DSN_SF5, w["sql"])
        w["explain_cost"] = cost
        ratio = orig_cost / cost if orig_cost and cost else 0
        w["cost_ratio"] = ratio
        marker = "*" if ratio > 1.1 else " "
        print(f"  {marker} W{w['worker_id']} ({w['strategy']}): cost={cost:.0f} ratio={ratio:.2f}x")

    # Pick best by EXPLAIN
    valid = [w for w in workers if w["explain_cost"] is not None and w["explain_cost"] > 0]
    if not valid:
        print("  [!] No valid candidates!")
        return

    best = max(valid, key=lambda w: w["cost_ratio"])
    print(f"\n  BEST by EXPLAIN: W{best['worker_id']} ({best['strategy']}) — {best['cost_ratio']:.2f}x")

    # SF10 5x trimmed mean — validate top 2 (W2 and W3 both WINs)
    print(f"\n--- SF10 5x Trimmed Mean Validation ---")
    wins = [w for w in workers if w["swarm_status"] == "WIN"]

    results = {}
    for w in wins:
        print(f"\n  W{w['worker_id']} ({w['strategy']}):")
        times = run_5x(DSN_SF10, w["sql"])
        tm = trimmed_mean(times)

        # Original times out at 300s
        orig_tm = 300_000.0
        speedup = orig_tm / tm if tm < float("inf") else 0.0
        status = "WIN" if speedup >= 1.1 else "PASS" if speedup >= 0.95 else "REGRESSION"

        print(f"    Trimmed mean: {tm:.1f}ms")
        print(f"    => {status} {speedup:.1f}x (vs 300s timeout baseline)")

        results[f"W{w['worker_id']}"] = {
            "worker_id": w["worker_id"],
            "strategy": w["strategy"],
            "transforms": w["transforms"],
            "status": status,
            "speedup_5x": speedup,
            "trimmed_mean_ms": tm,
            "all_runs_ms": times,
            "explain_cost": w.get("explain_cost"),
            "cost_ratio": w.get("cost_ratio"),
        }

    # Summary
    print(f"\n{'=' * 70}")
    print(f"  FINAL Q032 RESULTS")
    print(f"{'=' * 70}")
    for k, r in results.items():
        print(f"  {k} ({r['strategy']}): {r['status']} {r['speedup_5x']:.1f}x "
              f"({r['trimmed_mean_ms']:.0f}ms vs 300s timeout)")

    # Save
    out_path = OUT_DIR / "q032_validation_results.json"
    out_path.write_text(json.dumps({
        "query_id": "query032_multi",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "method": "5x_trimmed_mean_sf10",
        "baseline_ms": 300_000.0,
        "baseline_note": "original times out at 300s",
        "workers": results,
    }, indent=2, default=str))
    print(f"\n  Saved: {out_path}")


if __name__ == "__main__":
    main()
