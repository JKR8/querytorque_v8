#!/usr/bin/env python3
"""Batch validation — run original once, then each worker's optimized SQL.

Validation method: 3-run (run 3 times, discard 1st warmup, average last 2).
Processes queries sequentially. For each query: time original ONCE,
then time each worker's SQL against that baseline.

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.batch_validate \
        packages/qt-sql/ado/benchmarks/duckdb_tpcds/swarm_batch_20260208_030342
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import logging
from pathlib import Path

PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
os.chdir(PROJECT_ROOT)
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

import duckdb

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"
N_RUNS = 3  # 3-run: discard 1st (warmup), average last 2
TIMEOUT_S = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("batch_validate")


def extract_sql_from_response(response_text: str) -> str | None:
    """Extract SQL from ```sql ... ``` block in LLM response."""
    match = re.search(r'```sql\s*\n(.*?)```', response_text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def time_query(con: duckdb.DuckDBPyConnection, sql: str) -> list[float]:
    """Run query N_RUNS times, return list of elapsed times in ms."""
    times = []
    for run in range(N_RUNS):
        t0 = time.time()
        try:
            result = con.execute(sql)
            result.fetchall()
            elapsed = (time.time() - t0) * 1000
            times.append(elapsed)
        except Exception as e:
            times.append(-1)  # mark as error
            logger.warning(f"    Run {run+1} error: {e}")
    return times


def validate_results(con: duckdb.DuckDBPyConnection,
                     original_sql: str, optimized_sql: str) -> dict:
    """Check that optimized produces same results as original."""
    try:
        orig_rows = con.execute(original_sql).fetchall()
        opt_rows = con.execute(optimized_sql).fetchall()
        return {
            "rows_match": orig_rows == opt_rows,
            "original_rows": len(orig_rows),
            "optimized_rows": len(opt_rows),
        }
    except Exception as e:
        return {
            "rows_match": False,
            "error": str(e),
        }


def score_3run(times: list[float]) -> float | None:
    """3-run scoring: discard 1st (warmup), average last 2."""
    valid = [t for t in times if t > 0]
    if len(valid) < 2:
        return None
    # Discard first, average rest
    return sum(valid[1:]) / len(valid[1:])


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 -m ado.batch_validate <swarm_batch_dir>")
        sys.exit(1)

    batch_dir = Path(sys.argv[1])
    if not batch_dir.exists():
        print(f"Not found: {batch_dir}")
        sys.exit(1)

    # Find all query dirs
    query_dirs = sorted(
        [d for d in batch_dir.iterdir() if d.is_dir() and d.name.startswith("query_")],
        key=lambda d: d.name,
    )
    logger.info(f"Validating {len(query_dirs)} queries from {batch_dir}")
    logger.info(f"DB: {DB_PATH}, Method: 3-run (discard warmup, avg last 2)")

    con = duckdb.connect(DB_PATH, read_only=True)

    results = {}
    for qi, qdir in enumerate(query_dirs, 1):
        qid = qdir.name
        logger.info(f"\n{'='*60}")
        logger.info(f"[{qi}/{len(query_dirs)}] {qid}")
        logger.info(f"{'='*60}")

        # Load original SQL
        orig_path = qdir / "original.sql"
        if not orig_path.exists():
            logger.error(f"  No original.sql, skipping")
            continue
        original_sql = orig_path.read_text().strip()

        # Time original ONCE (3 runs)
        logger.info(f"  Timing original ({N_RUNS} runs)...")
        orig_times = time_query(con, original_sql)
        orig_avg = score_3run(orig_times)
        if orig_avg is None:
            logger.error(f"  Original query failed, skipping")
            results[qid] = {"status": "ORIGINAL_ERROR", "original_times": orig_times}
            continue
        logger.info(f"  Original: {orig_times[0]:.0f} / {orig_times[1]:.0f} / {orig_times[2]:.0f} ms  → avg {orig_avg:.0f}ms")

        # Validate each worker
        qresult = {
            "original_times_ms": [round(t, 1) for t in orig_times],
            "original_avg_ms": round(orig_avg, 1),
            "workers": {},
        }

        for wid in [1, 2, 3, 4]:
            resp_path = qdir / f"worker_{wid}_response.txt"
            if not resp_path.exists():
                logger.warning(f"  W{wid}: no response file")
                continue

            response = resp_path.read_text()
            optimized_sql = extract_sql_from_response(response)
            if not optimized_sql:
                logger.warning(f"  W{wid}: could not extract SQL from response")
                qresult["workers"][f"w{wid}"] = {"status": "PARSE_ERROR"}
                continue

            # Time optimized (3 runs)
            logger.info(f"  W{wid}: timing ({N_RUNS} runs)...")
            opt_times = time_query(con, optimized_sql)
            opt_avg = score_3run(opt_times)

            if opt_avg is None:
                logger.warning(f"  W{wid}: execution failed")
                qresult["workers"][f"w{wid}"] = {
                    "status": "EXEC_ERROR",
                    "times_ms": [round(t, 1) for t in opt_times],
                }
                # Save the error SQL for debugging
                (qdir / f"worker_{wid}_extracted.sql").write_text(optimized_sql)
                continue

            speedup = orig_avg / opt_avg if opt_avg > 0 else 0

            # Classify
            if speedup >= 1.10:
                status = "WIN"
            elif speedup >= 1.05:
                status = "IMPROVED"
            elif speedup >= 0.95:
                status = "NEUTRAL"
            else:
                status = "REGRESSION"

            # Check correctness
            correctness = validate_results(con, original_sql, optimized_sql)

            times_str = " / ".join(f"{t:.0f}" for t in opt_times)
            logger.info(
                f"  W{wid}: {times_str} ms  → avg {opt_avg:.0f}ms  "
                f"→ {speedup:.2f}x {status}"
                f"{'  ✓' if correctness['rows_match'] else '  ✗ WRONG RESULTS'}"
            )

            wresult = {
                "status": status,
                "speedup": round(speedup, 4),
                "times_ms": [round(t, 1) for t in opt_times],
                "avg_ms": round(opt_avg, 1),
                "rows_match": correctness.get("rows_match", False),
            }
            if not correctness.get("rows_match"):
                wresult["correctness"] = correctness

            qresult["workers"][f"w{wid}"] = wresult

            # Save extracted SQL
            (qdir / f"worker_{wid}_extracted.sql").write_text(optimized_sql)

        # Pick best valid worker
        valid_workers = {
            k: v for k, v in qresult["workers"].items()
            if v.get("rows_match") and v.get("speedup", 0) > 0
        }
        if valid_workers:
            best_k = max(valid_workers, key=lambda k: valid_workers[k]["speedup"])
            best = valid_workers[best_k]
            qresult["best_worker"] = best_k
            qresult["best_speedup"] = best["speedup"]
            qresult["best_status"] = best["status"]
            logger.info(
                f"  BEST: {best_k} → {best['speedup']:.2f}x {best['status']}"
            )
        else:
            qresult["best_worker"] = None
            qresult["best_speedup"] = 0
            qresult["best_status"] = "NO_VALID"
            logger.info(f"  BEST: none (no valid workers)")

        results[qid] = qresult

        # Save per-query validation
        (qdir / "validation.json").write_text(json.dumps(qresult, indent=2))

    con.close()

    # Save full results
    (batch_dir / "validation_results.json").write_text(json.dumps(results, indent=2))

    # Print summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"{'Query':<12} {'Baseline':>10} {'Best':>10} {'Speedup':>8} {'Status':>12} {'Worker':>8}")
    logger.info("-" * 64)

    wins = 0
    for qid in sorted(results):
        r = results[qid]
        if r.get("status") == "ORIGINAL_ERROR":
            logger.info(f"{qid:<12} {'ERROR':>10}")
            continue
        orig = r.get("original_avg_ms", 0)
        best_s = r.get("best_speedup", 0)
        best_st = r.get("best_status", "?")
        best_w = r.get("best_worker", "?")
        best_ms = orig / best_s if best_s > 0 else 0
        if best_st == "WIN":
            wins += 1
        logger.info(
            f"{qid:<12} {orig:>9.0f}ms {best_ms:>9.0f}ms {best_s:>7.2f}x {best_st:>12} {best_w:>8}"
        )

    logger.info(f"\nWINS: {wins}/{len(results)}")


if __name__ == "__main__":
    main()
