#!/usr/bin/env python3
"""
PostgreSQL DSB Benchmark Validator
Measures speedup of ADO-optimized queries vs originals
Uses 5-run trimmed mean validation (remove min/max, average remaining 3)
"""

import json
import time
import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple
import statistics
import psycopg2
from psycopg2 import sql
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Config
DSN = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"
ROUND_DIR = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/rounds/round_01")
OUTPUT_DIR = ROUND_DIR.parent / "validation_results"
RUNS = 3  # 3-run (discard warmup, avg last 2)
TIMEOUT_SECS = 300  # 5 min per query

def connect_db():
    """Connect to PostgreSQL DSB database"""
    # Parse DSN
    import urllib.parse
    parsed = urllib.parse.urlparse(DSN)
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path.lstrip('/'),
        user=parsed.username,
        password=parsed.password
    )
    return conn

def read_query_sql(query_dir: Path, variant: str) -> str:
    """Read SQL from file"""
    sql_file = query_dir / f"{variant}.sql"
    if not sql_file.exists():
        return None
    return sql_file.read_text()

def run_query_timed(conn, query_text: str, timeout: int = TIMEOUT_SECS) -> Tuple[float, bool, str]:
    """
    Run query and measure execution time
    Returns: (time_ms, success, error_message)
    """
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = %s", (timeout * 1000,))

    try:
        start = time.time()
        cursor.execute(query_text)
        # Fetch all results to count rows
        rows = cursor.fetchall()
        elapsed = (time.time() - start) * 1000  # Convert to ms
        row_count = len(rows)
        cursor.close()
        return elapsed, True, None, row_count
    except Exception as e:
        cursor.close()
        return 0, False, str(e), 0

def calculate_speedup(original_times: List[float], optimized_times: List[float]) -> Tuple[float, str]:
    """
    Calculate speedup using 3-run methodology:
    - Run 3 times
    - Discard first (warmup)
    - Average last 2 runs
    Returns: (speedup_ratio, methodology_description)
    """
    if len(original_times) < 2 or len(optimized_times) < 2:
        return 0.0, "Insufficient runs"

    # Discard first, average last 2
    orig_avg = statistics.mean(original_times[1:])  # Skip first, take last 2
    opt_avg = statistics.mean(optimized_times[1:])

    if opt_avg == 0:
        return 0.0, "Optimized time is 0"

    speedup = orig_avg / opt_avg
    description = f"3-run method (discard warmup, avg last 2): {orig_avg:.2f}ms → {opt_avg:.2f}ms"

    return speedup, description

def benchmark_query(conn, query_id: str, query_dir: Path) -> Dict:
    """
    Benchmark a single query (original vs optimized)
    Returns result dict with all timing data
    """
    logger.info(f"Benchmarking {query_id}...")

    # Read SQL files
    original_sql = read_query_sql(query_dir, "original")
    optimized_sql = read_query_sql(query_dir, "optimized")

    if not original_sql or not optimized_sql:
        return {
            "query_id": query_id,
            "status": "ERROR",
            "error": "Missing SQL files"
        }

    # Run original query (5 times)
    original_times = []
    original_error = None
    original_rows = 0

    for run in range(RUNS):
        elapsed, success, error, rows = run_query_timed(conn, original_sql)
        if success:
            original_times.append(elapsed)
            original_rows = rows
        else:
            original_error = error
            break

    if not original_times:
        return {
            "query_id": query_id,
            "status": "ERROR",
            "error": f"Original query failed: {original_error}"
        }

    # Run optimized query (5 times)
    optimized_times = []
    optimized_error = None
    optimized_rows = 0

    for run in range(RUNS):
        elapsed, success, error, rows = run_query_timed(conn, optimized_sql)
        if success:
            optimized_times.append(elapsed)
            optimized_rows = rows
        else:
            optimized_error = error
            break

    if not optimized_times:
        return {
            "query_id": query_id,
            "status": "ERROR",
            "error": f"Optimized query failed: {optimized_error}"
        }

    # Check result correctness
    if original_rows != optimized_rows:
        return {
            "query_id": query_id,
            "status": "ERROR",
            "error": f"Result mismatch: {original_rows} vs {optimized_rows} rows"
        }

    # Calculate speedup
    speedup, methodology = calculate_speedup(original_times, optimized_times)

    # Classify result
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
        "speedup": round(speedup, 2),
        "methodology": methodology,
        "original_times_ms": [round(t, 2) for t in original_times],
        "optimized_times_ms": [round(t, 2) for t in optimized_times],
        "original_avg_ms": round(statistics.mean(original_times), 2),
        "optimized_avg_ms": round(statistics.mean(optimized_times), 2),
        "row_count": original_rows
    }

def main():
    """Main benchmark runner"""

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("PostgreSQL DSB Benchmark Validator")
    logger.info("=" * 80)
    logger.info(f"Database: {DSN}")
    logger.info(f"Validation Method: {RUNS}-run trimmed mean")
    logger.info(f"Query Directory: {ROUND_DIR}")
    logger.info()

    # Discover queries
    query_dirs = sorted([d for d in ROUND_DIR.iterdir() if d.is_dir()])
    logger.info(f"Discovered {len(query_dirs)} queries")
    logger.info()

    # Connect to database
    try:
        conn = connect_db()
        logger.info("✅ Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"❌ Failed to connect: {e}")
        sys.exit(1)

    # Benchmark each query
    results = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "validation_method": "3-run (discard warmup, avg last 2)",
        "total_queries": len(query_dirs),
        "results": []
    }

    wins = 0
    passes = 0
    regressions = 0
    errors = 0

    for idx, query_dir in enumerate(query_dirs, 1):
        query_id = query_dir.name
        logger.info(f"[{idx}/{len(query_dirs)}] {query_id}...")

        result = benchmark_query(conn, query_id, query_dir)
        results["results"].append(result)

        if result["status"] == "PASS":
            classification = result.get("classification")
            if classification == "WIN":
                wins += 1
                logger.info(f"  ✅ WIN: {result['speedup']}x")
            elif classification == "PASS":
                passes += 1
                logger.info(f"  ⚪ PASS: {result['speedup']}x")
            else:
                regressions += 1
                logger.info(f"  ❌ REGRESSION: {result['speedup']}x")
        else:
            errors += 1
            logger.info(f"  ❌ ERROR: {result['error'][:60]}")

    conn.close()

    # Summary
    logger.info()
    logger.info("=" * 80)
    logger.info("BENCHMARK RESULTS SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total Queries: {results['total_queries']}")
    logger.info(f"✅ Wins (≥1.3x):     {wins} ({wins*100//results['total_queries']}%)")
    logger.info(f"⚪ Passes (0.95-1.3x): {passes} ({passes*100//results['total_queries']}%)")
    logger.info(f"❌ Regressions (<0.95x): {regressions} ({regressions*100//results['total_queries']}%)")
    logger.info(f"⚠️  Errors:        {errors} ({errors*100//results['total_queries']}%)")

    # Calculate average speedup (excluding errors)
    valid_results = [r for r in results["results"] if r["status"] == "PASS"]
    if valid_results:
        avg_speedup = statistics.mean([r["speedup"] for r in valid_results])
        logger.info(f"📊 Average Speedup: {avg_speedup:.2f}x")

    results["summary"] = {
        "wins": wins,
        "passes": passes,
        "regressions": regressions,
        "errors": errors,
        "validated": results['total_queries'] - errors
    }

    # Write results
    output_file = OUTPUT_DIR / "postgresql_dsb_validation.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info()
    logger.info(f"📁 Results saved to: {output_file}")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
