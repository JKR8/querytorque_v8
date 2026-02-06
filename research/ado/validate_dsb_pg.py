#!/usr/bin/env python3
"""
Validate DSB optimizations on PostgreSQL SF10.

Uses 3x runs (discard 1st warmup, average last 2) for reliable timing.
Checks semantic equivalence via row counts and value comparison.

Usage:
    python validate_dsb_pg.py --round round_01
    python validate_dsb_pg.py --round round_01 --query query001_multi
    python validate_dsb_pg.py --round round_01 --runs 5  # 5x trimmed mean
"""

import argparse
import csv
import hashlib
import json
import logging
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Any

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-sql"))

from qt_sql.execution.postgres_executor import PostgresExecutor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# PostgreSQL DSB SF10
DSB_DSN = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"


@dataclass
class TimingResult:
    """Timing from multiple runs."""
    runs: List[float] = field(default_factory=list)
    warmup_ms: float = 0.0
    measured_ms: float = 0.0  # avg of last 2 (3x) or trimmed mean (5x)


@dataclass
class ValidationResult:
    """Result from validating a single query."""
    query_id: str
    status: str  # "PASS", "FAIL", "ERROR"
    speedup: float = 0.0
    original_ms: float = 0.0
    optimized_ms: float = 0.0
    original_rows: int = 0
    optimized_rows: int = 0
    rows_match: bool = False
    checksum_match: bool = False
    transforms: List[str] = field(default_factory=list)
    examples_used: List[str] = field(default_factory=list)
    error: str = ""
    elapsed_s: float = 0.0
    timestamp: str = ""


def parse_dsn(dsn: str) -> dict:
    """Parse PostgreSQL DSN into connection params."""
    # postgres://user:password@host:port/database
    import re
    pattern = r"postgres(?:ql)?://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)"
    match = re.match(pattern, dsn)
    if match:
        return {
            "user": match.group(1),
            "password": match.group(2),
            "host": match.group(3),
            "port": int(match.group(4)),
            "database": match.group(5),
        }
    raise ValueError(f"Invalid DSN: {dsn}")


def time_query(executor: PostgresExecutor, sql: str, runs: int = 3) -> TimingResult:
    """Time a query with multiple runs.

    For 3 runs: discard 1st (warmup), average last 2
    For 5 runs: discard min/max, average middle 3
    """
    result = TimingResult()

    for i in range(runs):
        start = time.perf_counter()
        try:
            executor.execute(sql)
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e}")
        elapsed_ms = (time.perf_counter() - start) * 1000
        result.runs.append(elapsed_ms)

    if runs == 3:
        # Discard 1st (warmup), average last 2
        result.warmup_ms = result.runs[0]
        result.measured_ms = sum(result.runs[1:]) / 2
    elif runs >= 5:
        # Trimmed mean: discard min/max, average rest
        sorted_runs = sorted(result.runs)
        trimmed = sorted_runs[1:-1]  # Remove min and max
        result.warmup_ms = result.runs[0]
        result.measured_ms = sum(trimmed) / len(trimmed)
    else:
        # Fallback: just average all
        result.measured_ms = sum(result.runs) / len(result.runs)

    return result


def compute_checksum(rows: List[dict]) -> str:
    """Compute checksum of query results."""
    if not rows:
        return "empty"

    # Sort rows for consistent comparison
    try:
        # Convert all values to strings for consistent hashing
        normalized = []
        for row in rows:
            norm_row = {k: str(v) if v is not None else "NULL" for k, v in sorted(row.items())}
            normalized.append(json.dumps(norm_row, sort_keys=True))
        normalized.sort()
        content = "\n".join(normalized)
        return hashlib.md5(content.encode()).hexdigest()[:16]
    except Exception:
        return "error"


def validate_query(
    query_id: str,
    original_sql: str,
    optimized_sql: str,
    metadata: dict,
    dsn: str,
    runs: int = 3,
    timeout: int = 120,
) -> ValidationResult:
    """Validate a single query optimization."""
    start_time = time.time()
    result = ValidationResult(
        query_id=query_id,
        status="ERROR",
        transforms=metadata.get("transforms", []),
        examples_used=metadata.get("examples_used", []),
        timestamp=datetime.now().isoformat(),
    )

    conn_params = parse_dsn(dsn)
    executor = PostgresExecutor(**conn_params)

    try:
        executor.connect()
        # Set statement timeout
        executor.execute(f"SET statement_timeout = '{timeout}s'")

        # Execute original for results + timing
        logger.debug(f"{query_id}: Running original ({runs}x)...")
        try:
            orig_rows = executor.execute(original_sql)
            orig_timing = time_query(executor, original_sql, runs)
        except Exception as e:
            result.error = f"Original failed: {str(e)[:80]}"
            return result

        # Execute optimized for results + timing
        logger.debug(f"{query_id}: Running optimized ({runs}x)...")
        try:
            opt_rows = executor.execute(optimized_sql)
            opt_timing = time_query(executor, optimized_sql, runs)
        except Exception as e:
            result.error = f"Optimized failed: {str(e)[:80]}"
            return result

        # Compare results
        result.original_rows = len(orig_rows)
        result.optimized_rows = len(opt_rows)
        result.rows_match = result.original_rows == result.optimized_rows

        # Checksum comparison
        orig_checksum = compute_checksum(orig_rows)
        opt_checksum = compute_checksum(opt_rows)
        result.checksum_match = orig_checksum == opt_checksum

        # Timing
        result.original_ms = round(orig_timing.measured_ms, 2)
        result.optimized_ms = round(opt_timing.measured_ms, 2)

        if result.optimized_ms > 0:
            result.speedup = round(result.original_ms / result.optimized_ms, 2)
        else:
            result.speedup = 1.0

        # Determine status
        if result.rows_match and result.checksum_match:
            result.status = "PASS"
        else:
            result.status = "FAIL"
            if not result.rows_match:
                result.error = f"Row count: {result.original_rows} vs {result.optimized_rows}"
            else:
                result.error = "Checksum mismatch"

    except Exception as e:
        result.error = str(e)
    finally:
        executor.close()

    result.elapsed_s = round(time.time() - start_time, 2)
    return result


def load_round_queries(round_dir: Path) -> List[dict]:
    """Load all queries from a round directory."""
    queries = []

    for query_dir in sorted(round_dir.iterdir()):
        if not query_dir.is_dir():
            continue

        original_path = query_dir / "original.sql"
        optimized_path = query_dir / "optimized.sql"
        metadata_path = query_dir / "metadata.json"

        if not original_path.exists() or not optimized_path.exists():
            continue

        metadata = {}
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text())

        queries.append({
            "query_id": query_dir.name,
            "original_sql": original_path.read_text(),
            "optimized_sql": optimized_path.read_text(),
            "metadata": metadata,
        })

    return queries


def save_results(results: List[ValidationResult], output_dir: Path):
    """Save validation results to CSV and JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save individual JSON files
    for r in results:
        json_path = output_dir / f"{r.query_id}_result.json"
        json_path.write_text(json.dumps(asdict(r), indent=2))

    # Save summary CSV
    csv_path = output_dir / "summary.csv"
    fieldnames = [
        "query_id", "status", "speedup", "original_ms", "optimized_ms",
        "original_rows", "optimized_rows", "rows_match", "checksum_match",
        "transforms", "error", "elapsed_s", "timestamp"
    ]

    # Sort by speedup descending
    sorted_results = sorted(results, key=lambda r: r.speedup, reverse=True)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted_results:
            row = asdict(r)
            row["transforms"] = ";".join(row["transforms"])
            del row["examples_used"]  # Too verbose for CSV
            writer.writerow(row)

    logger.info(f"Results saved to {csv_path}")

    # Save full summary JSON
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_queries": len(results),
        "passed": len([r for r in results if r.status == "PASS"]),
        "failed": len([r for r in results if r.status == "FAIL"]),
        "errors": len([r for r in results if r.status == "ERROR"]),
        "results": [asdict(r) for r in sorted_results],
    }
    summary_path = output_dir / "full_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))


def print_summary(results: List[ValidationResult]):
    """Print validation summary."""
    total = len(results)
    passed = [r for r in results if r.status == "PASS"]
    failed = [r for r in results if r.status == "FAIL"]
    errors = [r for r in results if r.status == "ERROR"]

    # Speedup categories
    wins = [r for r in passed if r.speedup > 1.10]
    regressions = [r for r in passed if r.speedup < 0.90]

    print("\n" + "=" * 70)
    print("DSB VALIDATION SUMMARY (PostgreSQL SF10)")
    print("=" * 70)
    print(f"Total queries:     {total}")
    print(f"PASS:              {len(passed)} ({len(passed)/total*100:.1f}%)" if total else "")
    print(f"FAIL:              {len(failed)} ({len(failed)/total*100:.1f}%)" if total else "")
    print(f"ERROR:             {len(errors)} ({len(errors)/total*100:.1f}%)" if total else "")
    print("-" * 70)
    print(f"Wins (>1.10x):     {len(wins)}")
    print(f"Regressions (<0.90x): {len(regressions)}")
    print("=" * 70)

    if passed:
        avg_speedup = sum(r.speedup for r in passed) / len(passed)
        max_speedup = max(r.speedup for r in passed)
        max_q = max(passed, key=lambda r: r.speedup)
        print(f"Avg speedup:       {avg_speedup:.2f}x")
        print(f"Max speedup:       {max_speedup:.2f}x ({max_q.query_id})")
        print("=" * 70)

    # Top 10 by speedup
    if passed:
        print("\nTop 10 by Speedup:")
        top_10 = sorted(passed, key=lambda r: r.speedup, reverse=True)[:10]
        for r in top_10:
            transforms = ",".join(r.transforms) if r.transforms else "-"
            print(f"  {r.query_id:20s}: {r.speedup:5.2f}x  ({r.original_ms:8.1f}ms -> {r.optimized_ms:8.1f}ms)  [{transforms}]")

    # Regressions
    if regressions:
        print("\nRegressions (slower):")
        for r in sorted(regressions, key=lambda r: r.speedup):
            print(f"  {r.query_id:20s}: {r.speedup:5.2f}x  ({r.original_ms:8.1f}ms -> {r.optimized_ms:8.1f}ms)")

    # Failures
    if failed:
        print("\nFailed (semantic mismatch):")
        for r in sorted(failed, key=lambda r: r.query_id):
            print(f"  {r.query_id:20s}: {r.error[:40]}")

    # Errors
    if errors:
        print("\nErrors:")
        for r in sorted(errors, key=lambda r: r.query_id):
            print(f"  {r.query_id:20s}: {r.error[:50]}")


def main():
    parser = argparse.ArgumentParser(
        description="Validate DSB optimizations on PostgreSQL SF10"
    )
    parser.add_argument("--round", "-r", required=True, help="Round directory name (e.g., round_01)")
    parser.add_argument("--query", "-q", help="Specific query to validate (e.g., query001_multi)")
    parser.add_argument("--dsn", default=DSB_DSN, help="PostgreSQL DSN")
    parser.add_argument("--runs", type=int, default=3, help="Timing runs (3 or 5)")
    parser.add_argument("--timeout", "-t", type=int, default=120, help="Query timeout in seconds (default: 120)")
    parser.add_argument("--output", "-o", help="Output directory (default: rounds/<round>/validation)")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Find round directory
    round_dir = SCRIPT_DIR / "rounds" / args.round
    if not round_dir.exists():
        logger.error(f"Round directory not found: {round_dir}")
        sys.exit(1)

    # Output directory
    output_dir = Path(args.output) if args.output else round_dir / "validation"

    # Load queries
    queries = load_round_queries(round_dir)
    if args.query:
        queries = [q for q in queries if q["query_id"] == args.query]

    if not queries:
        logger.error("No queries found to validate")
        sys.exit(1)

    total_queries = len(queries)
    logger.info(f"=" * 60)
    logger.info(f"DSB Validation - {args.round}")
    logger.info(f"=" * 60)
    logger.info(f"Queries: {total_queries}")
    logger.info(f"Database: {args.dsn}")
    logger.info(f"Timing: {args.runs}x runs (drop first, avg rest)")
    logger.info(f"Timeout: {args.timeout}s per query")
    logger.info(f"Output: {output_dir}")
    logger.info(f"=" * 60)

    # Validate each query
    results: List[ValidationResult] = []
    pass_count = 0
    fail_count = 0
    error_count = 0
    start_time = time.time()

    for idx, q in enumerate(queries, 1):
        query_start = time.time()
        logger.info(f"")
        logger.info(f"[{idx}/{total_queries}] {q['query_id']}")
        logger.info(f"  Transforms: {q['metadata'].get('transforms', [])}")

        result = validate_query(
            query_id=q["query_id"],
            original_sql=q["original_sql"],
            optimized_sql=q["optimized_sql"],
            metadata=q["metadata"],
            dsn=args.dsn,
            runs=args.runs,
            timeout=args.timeout,
        )
        results.append(result)

        # Update counts
        if result.status == "PASS":
            pass_count += 1
            status_icon = "✓"
        elif result.status == "FAIL":
            fail_count += 1
            status_icon = "✗"
        else:
            error_count += 1
            status_icon = "!"

        # Log result details
        query_elapsed = time.time() - query_start
        logger.info(f"  Original:  {result.original_rows} rows, {result.original_ms:.1f}ms")
        logger.info(f"  Optimized: {result.optimized_rows} rows, {result.optimized_ms:.1f}ms")
        logger.info(f"  Rows match: {result.rows_match}, Checksum match: {result.checksum_match}")

        if result.error:
            logger.info(f"  Error: {result.error[:60]}")

        logger.info(f"  {status_icon} {result.status} | Speedup: {result.speedup:.2f}x | Took: {query_elapsed:.1f}s")

        # Progress summary
        total_elapsed = time.time() - start_time
        avg_per_query = total_elapsed / idx
        remaining = (total_queries - idx) * avg_per_query
        logger.info(f"  Progress: {pass_count} PASS, {fail_count} FAIL, {error_count} ERROR | ETA: {remaining/60:.1f}min")

        # Save incrementally
        save_results(results, output_dir)

    # Final summary
    print_summary(results)


if __name__ == "__main__":
    main()
