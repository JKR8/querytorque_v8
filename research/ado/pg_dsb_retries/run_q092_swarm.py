#!/usr/bin/env python3
"""Swarm optimization for Q092 with EXPLAIN cost gate + SF10 5x validation.

Every intermediate step is saved to disk BEFORE proceeding to the next.
If this script crashes, all prior steps can be recovered from:
  swarm_sessions/query092_multi/iteration_00_fan_out/

Usage (from project root):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb/run_q092_swarm.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(".env")

import psycopg2

from qt_sql.pipeline import Pipeline
from qt_sql.schemas import OptimizationMode

BENCHMARK_DIR = "packages/qt-sql/ado/benchmarks/postgres_dsb"
QUERIES_DIR = Path(BENCHMARK_DIR) / "queries"
SESSION_DIR = Path(BENCHMARK_DIR) / "swarm_sessions" / "query092_multi"
DSN_SF5 = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf5"
DSN_SF10 = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"

QUERY_ID = "query092_multi"


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


def fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s"


def main():
    t_start = time.time()

    print(f"\n{'#'*70}")
    print(f"  Q092 SWARM — EXPLAIN gate + SF10 5x validation")
    print(f"  Intermediates saved to: {SESSION_DIR}")
    print(f"{'#'*70}\n")

    sql = (QUERIES_DIR / f"{QUERY_ID}.sql").read_text()

    # ── Step 1: Check if swarm already ran (resume support) ──────────
    iter_dir = SESSION_DIR / "iteration_00_fan_out"
    workers_on_disk = []
    if iter_dir.exists():
        for wdir in sorted(iter_dir.iterdir()):
            if wdir.is_dir() and wdir.name.startswith("worker_"):
                result_file = wdir / "result.json"
                sql_file = wdir / "optimized.sql"
                if result_file.exists() and sql_file.exists():
                    workers_on_disk.append(wdir.name)

    if len(workers_on_disk) >= 4:
        print(f"  [RESUME] Found {len(workers_on_disk)} saved workers — skipping swarm API calls")
        print(f"           Workers: {workers_on_disk}")
    else:
        # ── Step 2: Run swarm (pipeline saves to swarm_sessions/) ────
        print(f"  [1] Running swarm (analyst + 4 workers)...", flush=True)
        pipeline = Pipeline(BENCHMARK_DIR)

        t_swarm = time.time()
        result = pipeline.run_optimization_session(
            query_id=QUERY_ID,
            sql=sql,
            mode=OptimizationMode.SWARM,
            max_iterations=1,
            target_speedup=999.0,
        )
        swarm_elapsed = time.time() - t_swarm
        print(f"      Swarm complete ({fmt(swarm_elapsed)})")

        # Verify files saved
        if not iter_dir.exists():
            print(f"  [!] ERROR: swarm_sessions not created!")
            return

    # ── Step 3: Load saved results from disk ─────────────────────────
    print(f"\n  [2] Loading saved worker results from disk...")
    workers = []
    for wdir in sorted(iter_dir.iterdir()):
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
        print(f"    {status_marker} W{result['worker_id']} ({result['strategy']}): "
              f"{result['status']} {result['speedup']:.1f}x")

    if not workers:
        print(f"  [!] No workers found!")
        return

    # Save checkpoint: worker summary
    checkpoint = SESSION_DIR / "checkpoint_workers.json"
    checkpoint.write_text(json.dumps(
        [{"worker_id": w["worker_id"], "strategy": w["strategy"],
          "status": w["swarm_status"], "speedup": w["swarm_speedup"]}
         for w in workers], indent=2))
    print(f"    Checkpoint saved: {checkpoint}")

    # ── Step 4: EXPLAIN cost gate ────────────────────────────────────
    print(f"\n  [3] EXPLAIN Cost Gate (SF5)...")
    orig_cost = get_explain_cost(DSN_SF5, sql)
    print(f"    Original cost: {orig_cost}")

    for w in workers:
        if w["swarm_status"] == "ERROR":
            w["explain_cost"] = None
            w["cost_ratio"] = 0
            print(f"    W{w['worker_id']} ({w['strategy']}): SKIPPED (swarm ERROR)")
            continue
        cost = get_explain_cost(DSN_SF5, w["sql"])
        w["explain_cost"] = cost
        ratio = orig_cost / cost if orig_cost and cost else 0
        w["cost_ratio"] = ratio
        cost_str = f"{cost:.0f}" if cost is not None else "FAILED"
        marker = "*" if ratio > 1.1 else " "
        print(f"    {marker} W{w['worker_id']} ({w['strategy']}): "
              f"cost={cost_str} ratio={ratio:.2f}x")

    # Save checkpoint: explain costs
    explain_checkpoint = SESSION_DIR / "checkpoint_explain.json"
    explain_checkpoint.write_text(json.dumps({
        "orig_cost": orig_cost,
        "workers": [{
            "worker_id": w["worker_id"], "strategy": w["strategy"],
            "explain_cost": w.get("explain_cost"), "cost_ratio": w.get("cost_ratio"),
        } for w in workers],
    }, indent=2, default=str))
    print(f"    Checkpoint saved: {explain_checkpoint}")

    # ── Step 5: SF10 5x trimmed mean validation (WINs only) ─────────
    wins = [w for w in workers if w["swarm_status"] == "WIN"]
    if not wins:
        # Fall back to best EXPLAIN ratio
        valid = [w for w in workers if w["explain_cost"] is not None and w["explain_cost"] > 0]
        if valid:
            wins = [max(valid, key=lambda w: w["cost_ratio"])]
            print(f"\n  No WINs from swarm — validating best EXPLAIN candidate")

    if not wins:
        print(f"\n  [!] No candidates to validate!")
        return

    print(f"\n  [4] SF10 5x Trimmed Mean Validation ({len(wins)} candidates)...")
    results = {}
    for w in wins:
        print(f"\n    W{w['worker_id']} ({w['strategy']}):")
        times = run_5x(DSN_SF10, w["sql"])
        tm = trimmed_mean(times)

        orig_tm = 300_000.0
        speedup = orig_tm / tm if tm < float("inf") else 0.0
        status = "WIN" if speedup >= 1.1 else "PASS" if speedup >= 0.95 else "REGRESSION"

        print(f"      Trimmed mean: {tm:.1f}ms")
        print(f"      => {status} {speedup:.1f}x (vs 300s timeout baseline)")

        results[f"W{w['worker_id']}"] = {
            "worker_id": w["worker_id"],
            "strategy": w["strategy"],
            "transforms": w["transforms"],
            "status": status,
            "speedup_5x": speedup,
            "trimmed_mean_ms": tm,
            "all_runs_ms": [t if t != float("inf") else "TIMEOUT" for t in times],
            "explain_cost": w.get("explain_cost"),
            "cost_ratio": w.get("cost_ratio"),
        }

        # Save per-worker validation checkpoint
        val_checkpoint = SESSION_DIR / f"checkpoint_validation_W{w['worker_id']}.json"
        val_checkpoint.write_text(json.dumps(results[f"W{w['worker_id']}"], indent=2, default=str))
        print(f"      Checkpoint saved: {val_checkpoint}")

    # ── Summary ──────────────────────────────────────────────────────
    total = time.time() - t_start
    print(f"\n{'='*70}")
    print(f"  FINAL Q092 RESULTS — {fmt(total)}")
    print(f"{'='*70}")
    for k, r in results.items():
        print(f"  {k} ({r['strategy']}): {r['status']} {r['speedup_5x']:.1f}x "
              f"({r['trimmed_mean_ms']:.0f}ms vs 300s timeout)")

    # Save final results
    out_path = Path(BENCHMARK_DIR) / "q092_validation_results.json"
    out_path.write_text(json.dumps({
        "query_id": QUERY_ID,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "method": "5x_trimmed_mean_sf10",
        "baseline_ms": 300_000.0,
        "baseline_note": "original times out at 300s",
        "total_elapsed_s": total,
        "workers": results,
    }, indent=2, default=str))
    print(f"\n  Saved: {out_path}")


if __name__ == "__main__":
    main()
