#!/usr/bin/env python3
"""Run swarm optimization on a batch of PostgreSQL DSB queries.

Usage (from project root):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb/run_swarm_batch.py

Reads queries from the queries/ directory, runs SwarmSession on each,
prints a summary table at the end.
"""

import json
import sys
import time
from pathlib import Path

# Must run from project root
from dotenv import load_dotenv
load_dotenv(".env")

from qt_sql.pipeline import Pipeline
from qt_sql.schemas import OptimizationMode


QUERIES = [
    "query001_multi",
    "query023_multi",
    "query031_multi",
    "query038_multi",
    "query065_multi",
    "query075_multi",
    "query081_multi",
    "query083_multi",
    "query084_agg",
    "query087_multi",
]

BENCHMARK_DIR = "packages/qt-sql/ado/benchmarks/postgres_dsb"
QUERIES_DIR = Path(BENCHMARK_DIR) / "queries"
MAX_ITERATIONS = 3
TARGET_SPEEDUP = 2.0


def main():
    t_batch = time.time()

    print(f"\n{'#'*60}", flush=True)
    print(f"  SWARM BATCH: {len(QUERIES)} PostgreSQL DSB queries", flush=True)
    print(f"  target={TARGET_SPEEDUP:.1f}x  max_iterations={MAX_ITERATIONS}", flush=True)
    print(f"{'#'*60}\n", flush=True)

    pipeline = Pipeline(BENCHMARK_DIR)
    results = []

    for i, query_id in enumerate(QUERIES, 1):
        sql_path = QUERIES_DIR / f"{query_id}.sql"
        if not sql_path.exists():
            print(f"  [{i}/{len(QUERIES)}] SKIP {query_id} — file not found", flush=True)
            results.append((query_id, "SKIP", 0.0, 0.0))
            continue

        sql = sql_path.read_text()
        t_query = time.time()

        print(f"\n{'='*60}", flush=True)
        print(f"  [{i}/{len(QUERIES)}] Starting: {query_id}", flush=True)
        print(f"  Batch elapsed: {_fmt(time.time() - t_batch)}", flush=True)
        print(f"{'='*60}", flush=True)

        try:
            result = pipeline.run_optimization_session(
                query_id=query_id,
                sql=sql,
                mode=OptimizationMode.SWARM,
                max_iterations=MAX_ITERATIONS,
                target_speedup=TARGET_SPEEDUP,
            )
            elapsed = time.time() - t_query
            results.append((query_id, result.status, result.best_speedup, elapsed))
        except Exception as e:
            elapsed = time.time() - t_query
            print(f"  ERROR: {e}", flush=True)
            results.append((query_id, "CRASH", 0.0, elapsed))

    # Summary table
    total_elapsed = time.time() - t_batch
    print(f"\n\n{'#'*60}", flush=True)
    print(f"  BATCH COMPLETE — {_fmt(total_elapsed)}", flush=True)
    print(f"{'#'*60}\n", flush=True)

    print(f"  {'Query':<25} {'Status':<12} {'Speedup':>8} {'Time':>8}", flush=True)
    print(f"  {'─'*25} {'─'*12} {'─'*8} {'─'*8}", flush=True)

    wins = 0
    for query_id, status, speedup, elapsed in results:
        marker = "*" if speedup >= 1.10 else " "
        print(f" {marker}{query_id:<25} {status:<12} {speedup:>7.2f}x {_fmt(elapsed):>8}", flush=True)
        if speedup >= 1.10:
            wins += 1

    print(f"\n  Wins: {wins}/{len(results)} ({100*wins/max(len(results),1):.0f}%)", flush=True)
    avg_speedup = sum(r[2] for r in results if r[2] > 0) / max(sum(1 for r in results if r[2] > 0), 1)
    print(f"  Avg speedup (non-zero): {avg_speedup:.2f}x", flush=True)
    print(f"  Total time: {_fmt(total_elapsed)}", flush=True)

    # Save results
    out_path = Path(BENCHMARK_DIR) / "swarm_batch_results.json"
    out_data = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "total_elapsed_s": round(total_elapsed, 1),
        "n_queries": len(results),
        "n_wins": wins,
        "results": [
            {"query_id": q, "status": s, "speedup": round(sp, 3), "elapsed_s": round(e, 1)}
            for q, s, sp, e in results
        ],
    }
    out_path.write_text(json.dumps(out_data, indent=2))
    print(f"\n  Results saved: {out_path}", flush=True)


def _fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


if __name__ == "__main__":
    main()
