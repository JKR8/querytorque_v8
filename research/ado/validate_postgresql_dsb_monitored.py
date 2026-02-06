#!/usr/bin/env python3
"""
PostgreSQL DSB Benchmark Validator with C: Drive Monitoring
- Saves results after each query (checkpointing)
- Monitors C: drive space continuously
- Stops if C: drive space drops below 1GB threshold
"""

import json
import time
import sys
import os
import shutil
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
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
RESULTS_FILE = OUTPUT_DIR / "postgresql_dsb_validation.json"
RUNS = 3  # 3-run (discard warmup, avg last 2)
TIMEOUT_SECS = 600  # 10 min per query
C_DRIVE_THRESHOLD_GB = 1  # Stop if C: space < 1GB

def get_c_drive_free_gb() -> float:
    """Get free space on C: drive in GB"""
    try:
        stat = shutil.disk_usage("/mnt/c")
        return stat.free / (1024**3)
    except:
        return 999  # If can't determine, assume safe

def check_c_drive_space() -> Tuple[bool, float]:
    """
    Check C: drive space
    Returns: (is_safe, free_gb)
    """
    free_gb = get_c_drive_free_gb()
    is_safe = free_gb > C_DRIVE_THRESHOLD_GB
    return is_safe, free_gb

def connect_db():
    """Connect to PostgreSQL DSB database"""
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

def run_query_timed(conn, query_text: str, timeout: int = TIMEOUT_SECS) -> Tuple[float, bool, str, int]:
    """Run query and measure execution time"""
    try:
        cursor = conn.cursor()
        cursor.execute("SET statement_timeout = %s", (timeout * 1000,))

        try:
            start = time.time()
            cursor.execute(query_text)
            rows = cursor.fetchall()
            elapsed = (time.time() - start) * 1000  # Convert to ms
            row_count = len(rows)
            cursor.close()
            conn.commit()
            return elapsed, True, None, row_count
        except Exception as e:
            conn.rollback()
            cursor.close()
            return 0, False, str(e), 0
    except Exception as e:
        conn.rollback()
        return 0, False, str(e), 0

def calculate_speedup(original_times: List[float], optimized_times: List[float]) -> Tuple[float, str]:
    """Calculate speedup using 3-run methodology"""
    if len(original_times) < 2 or len(optimized_times) < 2:
        return 0.0, "Insufficient runs"

    # Discard first, average last 2
    orig_avg = statistics.mean(original_times[1:])
    opt_avg = statistics.mean(optimized_times[1:])

    if opt_avg == 0:
        return 0.0, "Optimized time is 0"

    speedup = orig_avg / opt_avg
    description = f"3-run method (discard warmup, avg last 2): {orig_avg:.2f}ms → {opt_avg:.2f}ms"

    return speedup, description

def benchmark_query(conn, query_id: str, query_dir: Path) -> Dict:
    """Benchmark a single query"""
    # Check disk space before each query
    is_safe, free_gb = check_c_drive_space()
    if not is_safe:
        return {
            "query_id": query_id,
            "status": "STOPPED",
            "error": f"C: drive space critical: {free_gb:.2f}GB remaining (threshold: {C_DRIVE_THRESHOLD_GB}GB)"
        }

    logger.info(f"[{free_gb:.2f}GB free on C:] Benchmarking {query_id}...")

    # Read SQL files
    original_sql = read_query_sql(query_dir, "original")
    optimized_sql = read_query_sql(query_dir, "optimized")

    if not original_sql or not optimized_sql:
        return {
            "query_id": query_id,
            "status": "ERROR",
            "error": "Missing SQL files"
        }

    # Run original query (3 times)
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

    # Run optimized query (3 times)
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

def load_checkpoint() -> Dict:
    """Load previous checkpoint if exists"""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return None

def save_checkpoint(results: Dict, processed_count: int):
    """Save checkpoint after each query"""
    checkpoint = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "processed": processed_count,
        "results": results
    }
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)

def main():
    """Main benchmark runner with checkpointing"""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("PostgreSQL DSB Benchmark Validator (with C: Drive Monitoring)")
    logger.info("=" * 80)
    logger.info(f"Database: {DSN}")
    logger.info(f"Validation Method: 3-run (discard warmup, avg last 2)")
    logger.info(f"C: Drive Safety Threshold: {C_DRIVE_THRESHOLD_GB}GB remaining")
    logger.info("")

    # Check initial space
    is_safe, free_gb = check_c_drive_space()
    logger.info(f"C: Drive Status: {free_gb:.2f}GB free")
    if not is_safe:
        logger.error(f"❌ C: drive space already below threshold!")
        sys.exit(1)

    # Discover queries
    query_dirs = sorted([d for d in ROUND_DIR.iterdir() if d.is_dir()])
    logger.info(f"Discovered {len(query_dirs)} queries")
    logger.info("")

    # Load checkpoint if exists
    checkpoint = load_checkpoint()
    if checkpoint:
        logger.info(f"⚡ Resuming from checkpoint (processed: {checkpoint['processed']}/{len(query_dirs)})")
        processed_dirs = checkpoint["processed"]
        results_list = checkpoint["results"]
    else:
        processed_dirs = 0
        results_list = []

    # Connect to database
    try:
        conn = connect_db()
        logger.info("✅ Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"❌ Failed to connect: {e}")
        sys.exit(1)

    # Benchmark each query
    for idx, query_dir in enumerate(query_dirs, 1):
        # Skip already processed queries
        if idx <= processed_dirs:
            continue

        query_id = query_dir.name

        result = benchmark_query(conn, query_id, query_dir)

        # Check if we hit the stop condition
        if result["status"] == "STOPPED":
            logger.error(f"⚠️  STOPPED: {result['error']}")
            conn.close()

            # Save final results before exiting
            final_results = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "validation_method": "3-run (discard warmup, avg last 2)",
                "total_queries": len(query_dirs),
                "processed_queries": len(results_list),
                "stopped_at_query": query_id,
                "stop_reason": result['error'],
                "results": results_list
            }

            with open(RESULTS_FILE, "w") as f:
                json.dump(final_results, f, indent=2)

            logger.info(f"📁 Partial results saved to: {RESULTS_FILE}")
            logger.info(f"   Can resume by running the script again")
            sys.exit(1)

        results_list.append(result)

        # Log and save after each query (checkpoint)
        if result["status"] == "PASS":
            classification = result.get("classification")
            if classification == "WIN":
                logger.info(f"  ✅ WIN: {result['speedup']}x")
            elif classification == "PASS":
                logger.info(f"  ⚪ PASS: {result['speedup']}x")
            else:
                logger.info(f"  ❌ REGRESSION: {result['speedup']}x")
        else:
            logger.info(f"  ❌ ERROR: {result['error'][:60]}")

        # Save checkpoint every query
        save_checkpoint(results_list, idx)
        logger.info(f"  💾 Checkpoint saved ({idx}/{len(query_dirs)})")

    conn.close()

    # Final summary
    wins = len([r for r in results_list if r.get("classification") == "WIN"])
    passes = len([r for r in results_list if r.get("classification") == "PASS"])
    regressions = len([r for r in results_list if r.get("classification") == "REGRESSION"])
    errors = len([r for r in results_list if r["status"] != "PASS"])

    logger.info("")
    logger.info("=" * 80)
    logger.info("BENCHMARK COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total Queries: {len(results_list)}")
    logger.info(f"✅ Wins (≥1.3x):       {wins} ({wins*100//len(results_list) if results_list else 0}%)")
    logger.info(f"⚪ Passes (0.95-1.3x): {passes} ({passes*100//len(results_list) if results_list else 0}%)")
    logger.info(f"❌ Regressions (<0.95x): {regressions} ({regressions*100//len(results_list) if results_list else 0}%)")
    logger.info(f"⚠️  Errors:            {errors} ({errors*100//len(results_list) if results_list else 0}%)")

    # Calculate average speedup
    valid_results = [r for r in results_list if r["status"] == "PASS"]
    if valid_results:
        avg_speedup = statistics.mean([r["speedup"] for r in valid_results])
        logger.info(f"📊 Average Speedup: {avg_speedup:.2f}x")

    # Final results
    final_results = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "validation_method": "3-run (discard warmup, avg last 2)",
        "total_queries": len(query_dirs),
        "validated_queries": len(results_list),
        "results": results_list,
        "summary": {
            "wins": wins,
            "passes": passes,
            "regressions": regressions,
            "errors": errors,
            "average_speedup": round(avg_speedup, 2) if valid_results else 0
        }
    }

    with open(RESULTS_FILE, "w") as f:
        json.dump(final_results, f, indent=2)

    logger.info("")
    logger.info(f"📁 Results saved to: {RESULTS_FILE}")
    logger.info("=" * 80)

    # Clean up checkpoint on success
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

if __name__ == "__main__":
    main()
