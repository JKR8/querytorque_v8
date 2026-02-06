#!/usr/bin/env python3
"""
Retry ERROR queries with 4-hour timeout
Saves results after each query (checkpoint)
Monitors C: drive space (stops at 1GB)
"""

import json
import time
import sys
import shutil
import statistics
from pathlib import Path
import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DSN = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"
ROUND_DIR = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/rounds/round_01")
OUTPUT_DIR = ROUND_DIR.parent / "validation_results"
RESULTS_FILE = OUTPUT_DIR / "retry_errors_4h.json"
RUNS = 3
TIMEOUT_SECS = 14400  # 4 hours

ERROR_QUERIES = [
    "query001_multi", "query032_multi", "query039_multi",
    "query075_multi", "query081_multi", "query083_multi",
    "query085_spj_spj", "query092_multi", "query100_agg",
    "query101_agg", "query102_agg"
]

def get_c_free_gb():
    try:
        return shutil.disk_usage("/mnt/c").free / (1024**3)
    except:
        return 999

def connect_db():
    import urllib.parse
    parsed = urllib.parse.urlparse(DSN)
    return psycopg2.connect(
        host=parsed.hostname, port=parsed.port or 5432,
        database=parsed.path.lstrip('/'),
        user=parsed.username, password=parsed.password
    )

def run_query_timed(conn, sql, timeout=TIMEOUT_SECS):
    try:
        cursor = conn.cursor()
        cursor.execute("SET statement_timeout = %s", (timeout * 1000,))
        try:
            start = time.time()
            cursor.execute(sql)
            rows = cursor.fetchall()
            elapsed = (time.time() - start) * 1000
            cursor.close()
            conn.commit()
            return elapsed, True, None, len(rows)
        except Exception as e:
            conn.rollback()
            cursor.close()
            return 0, False, str(e), 0
    except Exception as e:
        conn.rollback()
        return 0, False, str(e), 0

def benchmark_query(conn, query_id, query_dir):
    free_gb = get_c_free_gb()
    if free_gb < 1:
        return {"query_id": query_id, "status": "STOPPED", "error": f"C: drive {free_gb:.2f}GB"}

    logger.info(f"[{free_gb:.1f}GB C:] {query_id}...")

    orig_file = query_dir / "original.sql"
    opt_file = query_dir / "optimized.sql"
    if not orig_file.exists() or not opt_file.exists():
        return {"query_id": query_id, "status": "ERROR", "error": "Missing SQL files"}

    original_sql = orig_file.read_text()
    optimized_sql = opt_file.read_text()

    # Run original
    original_times = []
    for run in range(RUNS):
        elapsed, ok, err, rows = run_query_timed(conn, original_sql)
        if ok:
            original_times.append(elapsed)
            orig_rows = rows
        else:
            return {"query_id": query_id, "status": "ERROR", "error": f"Original failed: {err[:200]}"}

    # Run optimized
    optimized_times = []
    for run in range(RUNS):
        elapsed, ok, err, rows = run_query_timed(conn, optimized_sql)
        if ok:
            optimized_times.append(elapsed)
            opt_rows = rows
        else:
            return {"query_id": query_id, "status": "ERROR", "error": f"Optimized failed: {err[:200]}"}

    if orig_rows != opt_rows:
        return {"query_id": query_id, "status": "ERROR", "error": f"Row mismatch: {orig_rows} vs {opt_rows}"}

    orig_avg = statistics.mean(original_times[1:])
    opt_avg = statistics.mean(optimized_times[1:])
    speedup = round(orig_avg / opt_avg, 2) if opt_avg > 0 else 0

    if speedup >= 1.3:
        classification = "WIN"
    elif speedup >= 0.95:
        classification = "PASS"
    else:
        classification = "REGRESSION"

    return {
        "query_id": query_id,
        "status": "PASS",
        "classification": classification,
        "speedup": speedup,
        "methodology": f"3-run (discard warmup, avg last 2): {orig_avg:.2f}ms -> {opt_avg:.2f}ms",
        "original_times_ms": [round(t, 2) for t in original_times],
        "optimized_times_ms": [round(t, 2) for t in optimized_times],
        "original_avg_ms": round(statistics.mean(original_times), 2),
        "optimized_avg_ms": round(statistics.mean(optimized_times), 2),
        "row_count": orig_rows
    }

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("RETRY ERROR QUERIES - 4 HOUR TIMEOUT")
    logger.info("=" * 60)
    logger.info(f"Queries: {len(ERROR_QUERIES)}")
    logger.info(f"Timeout: {TIMEOUT_SECS}s ({TIMEOUT_SECS//3600}h)")
    logger.info(f"C: Drive: {get_c_free_gb():.1f}GB free")
    logger.info("")

    # Load existing results if resuming
    results = []
    completed_ids = set()
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE) as f:
            existing = json.load(f)
        results = existing.get("results", [])
        completed_ids = {r["query_id"] for r in results}
        logger.info(f"Resuming: {len(completed_ids)} already done")

    conn = connect_db()
    logger.info("Connected to PostgreSQL")

    for idx, query_id in enumerate(ERROR_QUERIES, 1):
        if query_id in completed_ids:
            logger.info(f"[{idx}/{len(ERROR_QUERIES)}] {query_id} - SKIPPED (already done)")
            continue

        query_dir = ROUND_DIR / query_id
        result = benchmark_query(conn, query_id, query_dir)
        results.append(result)

        status = result.get("classification", result["status"])
        speedup = result.get("speedup", "-")
        logger.info(f"  -> {status} {speedup}")

        # Save after each query
        output = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "timeout_secs": TIMEOUT_SECS,
            "total_queries": len(ERROR_QUERIES),
            "completed": len(results),
            "results": results
        }
        with open(RESULTS_FILE, "w") as f:
            json.dump(output, f, indent=2)
        logger.info(f"  💾 Saved ({len(results)}/{len(ERROR_QUERIES)})")

        if result["status"] == "STOPPED":
            logger.error("STOPPED - C: drive space critical")
            break

    conn.close()

    wins = len([r for r in results if r.get("classification") == "WIN"])
    passes = len([r for r in results if r.get("classification") == "PASS"])
    regressions = len([r for r in results if r.get("classification") == "REGRESSION"])
    errors = len([r for r in results if r["status"] != "PASS"])

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"COMPLETE: {len(results)}/{len(ERROR_QUERIES)}")
    logger.info(f"  Wins: {wins} | Passes: {passes} | Regressions: {regressions} | Errors: {errors}")
    logger.info(f"Results: {RESULTS_FILE}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
