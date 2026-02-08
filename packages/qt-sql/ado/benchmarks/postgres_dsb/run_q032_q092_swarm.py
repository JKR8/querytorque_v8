#!/usr/bin/env python3
"""Swarm optimization for Q032 + Q092 with EXPLAIN cost gate + SF10 5x validation.

Intermediate: PG EXPLAIN cost (fast, no execution)
Final: 5x trimmed mean on SF10 (remove min/max, avg 3)

Usage (from project root):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb/run_q032_q092_swarm.py
"""

from __future__ import annotations

import json
import time
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(".env")

import psycopg2

from ado.pipeline import Pipeline
from ado.schemas import OptimizationMode

BENCHMARK_DIR = "packages/qt-sql/ado/benchmarks/postgres_dsb"
QUERIES_DIR = Path(BENCHMARK_DIR) / "queries"
DSN_SF5 = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf5"
DSN_SF10 = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"

QUERIES = ["query032_multi", "query092_multi"]


def get_explain_cost(dsn: str, sql: str) -> float | None:
    """Get PG EXPLAIN total cost (no execution)."""
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


def validate_5x_sf10(dsn: str, sql: str, timeout_ms: int = 300_000) -> list[float]:
    """Run query 5 times on SF10, return list of timings (ms)."""
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
            cur.execute("SET statement_timeout = 0")
        except Exception as e:
            cur.execute("ROLLBACK")
            cur.execute("SET statement_timeout = 0")
            times.append(float("inf"))
            print(f"    Run {i+1}: TIMEOUT/ERROR ({e})")
    con.close()
    return times


def trimmed_mean(times: list[float]) -> float:
    """5x trimmed mean: remove min/max, avg remaining 3."""
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


def run_swarm_with_explain_gate(pipeline: Pipeline, query_id: str, sql: str):
    """Run swarm generation + EXPLAIN cost gate for one query."""
    t0 = time.time()
    print(f"\n{'='*70}")
    print(f"  SWARM + EXPLAIN GATE: {query_id}")
    print(f"{'='*70}")

    # Step 1: Parse DAG
    print(f"  [1] Parsing DAG...", flush=True)
    dag, costs, _explain = pipeline._parse_dag(sql, dialect="postgres", query_id=query_id)

    # Step 2: Find examples (top 20)
    print(f"  [2] Finding examples...", flush=True)
    matched_examples = pipeline._find_examples(sql, engine="postgres", k=20)
    all_available = pipeline._list_gold_examples("postgres")
    regression_warnings = pipeline._find_regression_warnings(sql, engine="postgres", k=3)
    print(f"      {len(matched_examples)} examples, {len(regression_warnings)} warnings")
    for ex in matched_examples[:5]:
        print(f"      - {ex.get('id', '?')}: {ex.get('verified_speedup', '?')}")

    # Step 3: Run full swarm session (analyst + 4 workers)
    # Use the pipeline's session infrastructure but we'll validate via EXPLAIN
    print(f"  [3] Running swarm (analyst + 4 workers)...", flush=True)
    t_swarm = time.time()

    result = pipeline.run_optimization_session(
        query_id=query_id,
        sql=sql,
        mode=OptimizationMode.SWARM,
        max_iterations=1,  # fan-out only, no snipe
        target_speedup=999.0,  # don't stop early
    )
    swarm_elapsed = time.time() - t_swarm
    print(f"      Swarm complete ({fmt(swarm_elapsed)})")

    # Step 4: Collect all candidate SQLs from worker results
    candidates = []
    if result.iterations:
        for it_data in result.iterations:
            for wr in it_data.get("worker_results", []):
                opt_sql = wr.get("optimized_sql", "")
                if opt_sql and opt_sql.strip() != sql.strip():
                    candidates.append({
                        "worker_id": wr.get("worker_id", 0),
                        "strategy": wr.get("strategy", "?"),
                        "transforms": wr.get("transforms", []),
                        "sql": opt_sql,
                        "swarm_speedup": wr.get("speedup", 0.0),
                        "swarm_status": wr.get("status", "?"),
                    })

    if not candidates:
        print(f"  [!] No candidates generated!")
        return None

    print(f"\n  [4] EXPLAIN cost gate ({len(candidates)} candidates)...", flush=True)

    # Get original EXPLAIN cost
    orig_cost = get_explain_cost(DSN_SF5, sql)
    if orig_cost is None:
        # Try SF10
        orig_cost = get_explain_cost(DSN_SF10, sql)
    print(f"      Original cost: {orig_cost}")

    # Get candidate EXPLAIN costs
    for cand in candidates:
        cand["explain_cost"] = get_explain_cost(DSN_SF5, cand["sql"])
        if cand["explain_cost"] is None:
            cand["explain_cost"] = get_explain_cost(DSN_SF10, cand["sql"])
        ratio = orig_cost / cand["explain_cost"] if orig_cost and cand["explain_cost"] else 0
        cand["cost_ratio"] = ratio
        marker = "*" if ratio > 1.1 else " "
        cost_str = f"{cand['explain_cost']:.0f}" if cand["explain_cost"] is not None else "FAILED"
        print(f"    {marker} W{cand['worker_id']} ({cand['strategy']}): "
              f"cost={cost_str} ratio={ratio:.2f}x "
              f"transforms={cand['transforms']}")

    # Pick best by EXPLAIN cost ratio
    valid = [c for c in candidates if c["explain_cost"] is not None and c["explain_cost"] > 0]
    if not valid:
        print(f"  [!] No valid candidates (all EXPLAIN failed)")
        return None

    best = max(valid, key=lambda c: c["cost_ratio"])
    print(f"\n  [5] BEST by EXPLAIN: W{best['worker_id']} ({best['strategy']}) "
          f"— cost ratio {best['cost_ratio']:.2f}x")

    total_elapsed = time.time() - t0
    print(f"      Total time: {fmt(total_elapsed)}")

    return {
        "query_id": query_id,
        "n_candidates": len(candidates),
        "best_worker": best["worker_id"],
        "best_strategy": best["strategy"],
        "best_transforms": best["transforms"],
        "best_cost_ratio": best["cost_ratio"],
        "best_sql": best["sql"],
        "orig_explain_cost": orig_cost,
        "best_explain_cost": best["explain_cost"],
        "all_candidates": candidates,
        "swarm_elapsed_s": swarm_elapsed,
        "total_elapsed_s": total_elapsed,
    }


def main():
    t_batch = time.time()

    print(f"\n{'#'*70}")
    print(f"  Q032 + Q092 SWARM — EXPLAIN gate + SF10 5x validation")
    print(f"{'#'*70}\n")

    pipeline = Pipeline(BENCHMARK_DIR)

    # Phase 1: Generate + EXPLAIN cost gate
    swarm_results = {}
    for query_id in QUERIES:
        sql_path = QUERIES_DIR / f"{query_id}.sql"
        sql = sql_path.read_text()
        result = run_swarm_with_explain_gate(pipeline, query_id, sql)
        if result:
            swarm_results[query_id] = result

    # Phase 2: 5x trimmed mean validation on SF10
    print(f"\n\n{'#'*70}")
    print(f"  PHASE 2: SF10 5x TRIMMED MEAN VALIDATION")
    print(f"{'#'*70}\n")

    final_results = {}
    for query_id, sr in swarm_results.items():
        print(f"\n--- {query_id} (best: W{sr['best_worker']} {sr['best_strategy']}) ---")

        # Validate best candidate
        print(f"  Timing optimized (5x)...", flush=True)
        opt_times = validate_5x_sf10(DSN_SF10, sr["best_sql"])
        opt_tm = trimmed_mean(opt_times)
        print(f"    Runs: {[f'{t:.0f}ms' if t != float('inf') else 'TIMEOUT' for t in opt_times]}")
        print(f"    Trimmed mean: {opt_tm:.1f}ms")

        # Original will timeout — use 300s ceiling
        print(f"  Original: TIMEOUT baseline (300,000ms)")
        orig_tm = 300_000.0

        if opt_tm < float("inf"):
            speedup = orig_tm / opt_tm
            status = "WIN" if speedup >= 1.1 else "PASS" if speedup >= 0.95 else "REGRESSION"
        else:
            speedup = 0.0
            status = "ERROR"

        print(f"  => {status} {speedup:.1f}x ({opt_tm:.0f}ms vs 300,000ms baseline)")

        final_results[query_id] = {
            "query_id": query_id,
            "status": status,
            "speedup": speedup,
            "orig_ms_sf10": orig_tm,
            "opt_ms_sf10": opt_tm,
            "opt_runs_sf10": opt_times,
            "best_worker": sr["best_worker"],
            "best_strategy": sr["best_strategy"],
            "best_transforms": sr["best_transforms"],
            "cost_ratio": sr["best_cost_ratio"],
            "optimized_sql": sr["best_sql"],
        }

    # Final summary
    total = time.time() - t_batch
    print(f"\n\n{'#'*70}")
    print(f"  FINAL RESULTS — {fmt(total)}")
    print(f"{'#'*70}\n")

    for qid, fr in final_results.items():
        print(f"  {qid}: {fr['status']} {fr['speedup']:.1f}x "
              f"({fr['opt_ms_sf10']:.0f}ms vs 300s timeout) "
              f"W{fr['best_worker']} {fr['best_strategy']}")

    # Save
    out_path = Path(BENCHMARK_DIR) / "q032_q092_swarm_results.json"
    out_path.write_text(json.dumps({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "total_elapsed_s": total,
        "queries": final_results,
        "swarm_details": {k: {kk: vv for kk, vv in v.items() if kk != "best_sql"}
                          for k, v in swarm_results.items()},
    }, indent=2, default=str))
    print(f"\n  Results saved: {out_path}")


if __name__ == "__main__":
    main()
