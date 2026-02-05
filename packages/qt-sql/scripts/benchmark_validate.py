#!/usr/bin/env python3
"""
Stage 2: Benchmark Validation - Runtime Comparison.

Runs 5x trimmed mean benchmark on all queries with valid syntax from Stage 1.
Compares original vs optimized timing and validates semantic equivalence.

Features:
- 5-run trimmed mean timing (discard min/max, average middle 3)
- Checksum-based equivalence checking
- Row count validation
- Parallel validation (optional)
- Resume from checkpoint

Usage:
    python benchmark_validate.py \
        --input-dir scripts/benchmark_output_v2 \
        --output-dir scripts/validation_results \
        --full-db /mnt/d/TPC-DS/tpcds_sf100.duckdb
"""

import argparse
import csv
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from qt_sql.execution import DuckDBExecutor
from qt_sql.validation.benchmarker import QueryBenchmarker
from qt_sql.validation.equivalence_checker import EquivalenceChecker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Default paths
FULL_DB_DEFAULT = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
QUERIES_DIR_DEFAULT = "/mnt/d/TPC-DS/queries_duckdb_converted"


@dataclass
class ValidationResult:
    """Result from validating a single query."""
    query_num: int
    status: str  # "PASS", "FAIL", "ERROR"
    speedup: float = 0.0
    original_ms: float = 0.0
    optimized_ms: float = 0.0
    original_cost: float = 0.0
    optimized_cost: float = 0.0
    cost_reduction_pct: float = 0.0
    row_count: int = 0
    rows_match: bool = False
    checksum_match: bool = False
    error: str = ""
    elapsed_s: float = 0.0
    timestamp: str = ""


def load_query(query_num: int, queries_dir: Path) -> str:
    """Load SQL query by number."""
    patterns = [
        f"query_{query_num}.sql",
        f"query{query_num:02d}.sql",
        f"query{query_num}.sql",
    ]
    for pattern in patterns:
        path = queries_dir / pattern
        if path.exists():
            return path.read_text()
    raise FileNotFoundError(f"Query {query_num} not found in {queries_dir}")


def validate_single_query(
    query_num: int,
    input_dir: Path,
    queries_dir: Path,
    full_db: str,
    runs: int = 5,
) -> ValidationResult:
    """Validate a single query with 5x trimmed mean benchmark."""
    start_time = time.time()
    result = ValidationResult(
        query_num=query_num,
        status="ERROR",
        timestamp=datetime.now().isoformat(),
    )

    query_dir = input_dir / f"q{query_num}"
    final_sql_path = query_dir / "final_optimized.sql"

    if not final_sql_path.exists():
        result.error = "No final_optimized.sql found"
        result.elapsed_s = round(time.time() - start_time, 2)
        return result

    try:
        # Load original and optimized SQL
        original_sql = load_query(query_num, queries_dir)
        optimized_sql = final_sql_path.read_text()

        # Connect to full database
        executor = DuckDBExecutor(full_db, read_only=True)
        executor.connect()

        try:
            benchmarker = QueryBenchmarker(executor)

            # Run 5x trimmed mean benchmark
            logger.info(f"Q{query_num}: running {runs}x trimmed mean benchmark...")
            benchmark_result = benchmarker.benchmark_pair_trimmed_mean(
                original_sql,
                optimized_sql,
                runs=runs,
                capture_results=True,
            )

            # Check for execution errors
            if benchmark_result.original.error:
                result.error = f"Original query error: {benchmark_result.original.error}"
                result.status = "ERROR"
                return result

            if benchmark_result.optimized.error:
                result.error = f"Optimized query error: {benchmark_result.optimized.error}"
                result.status = "ERROR"
                return result

            # Extract metrics
            result.original_ms = round(benchmark_result.original.timing.measured_time_ms, 2)
            result.optimized_ms = round(benchmark_result.optimized.timing.measured_time_ms, 2)
            result.speedup = round(benchmark_result.speedup, 2)
            result.row_count = benchmark_result.original.row_count

            # Extract cost estimates
            result.original_cost = round(benchmark_result.original.cost.estimated_cost, 2)
            result.optimized_cost = round(benchmark_result.optimized.cost.estimated_cost, 2)
            if result.original_cost > 0 and result.original_cost != float('inf'):
                result.cost_reduction_pct = round(
                    ((result.original_cost - result.optimized_cost) / result.original_cost) * 100, 1
                )
            else:
                result.cost_reduction_pct = 0.0

            # Check row count match
            result.rows_match = (
                benchmark_result.original.row_count == benchmark_result.optimized.row_count
            )

            # Check checksum match
            checker = EquivalenceChecker()
            if benchmark_result.original.rows and benchmark_result.optimized.rows:
                orig_checksum = checker.compute_checksum(benchmark_result.original.rows)
                opt_checksum = checker.compute_checksum(benchmark_result.optimized.rows)
                result.checksum_match = (orig_checksum == opt_checksum)
            else:
                # If no rows, consider it a match if row counts match
                result.checksum_match = result.rows_match

            # Determine status
            if result.rows_match and result.checksum_match:
                result.status = "PASS"
            else:
                result.status = "FAIL"
                if not result.rows_match:
                    result.error = f"Row count mismatch: {benchmark_result.original.row_count} vs {benchmark_result.optimized.row_count}"
                elif not result.checksum_match:
                    result.error = "Checksum mismatch"

        finally:
            executor.close()

    except FileNotFoundError as e:
        result.error = str(e)
    except Exception as e:
        result.error = str(e)
        logger.error(f"Q{query_num}: {e}")

    result.elapsed_s = round(time.time() - start_time, 2)
    return result


def load_completed_validations(output_dir: Path) -> dict:
    """Load already-validated results."""
    completed = {}
    summary_path = output_dir / "summary.csv"
    if summary_path.exists():
        with open(summary_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                qnum = int(row["query_num"])
                completed[qnum] = ValidationResult(
                    query_num=qnum,
                    status=row["status"],
                    speedup=float(row.get("speedup", 0)),
                    original_ms=float(row.get("original_ms", 0)),
                    optimized_ms=float(row.get("optimized_ms", 0)),
                    original_cost=float(row.get("original_cost", 0)),
                    optimized_cost=float(row.get("optimized_cost", 0)),
                    cost_reduction_pct=float(row.get("cost_reduction_pct", 0)),
                    row_count=int(row.get("row_count", 0)),
                    rows_match=row.get("rows_match", "").lower() == "true",
                    checksum_match=row.get("checksum_match", "").lower() == "true",
                    error=row.get("error", ""),
                    elapsed_s=float(row.get("elapsed_s", 0)),
                    timestamp=row.get("timestamp", ""),
                )
    return completed


def save_results(results: List[ValidationResult], output_dir: Path):
    """Save validation results to CSV and individual JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save individual JSON files
    for r in results:
        json_path = output_dir / f"q{r.query_num}_result.json"
        json_path.write_text(json.dumps(asdict(r), indent=2))

    # Save summary CSV (sorted by speedup descending)
    csv_path = output_dir / "summary.csv"
    fieldnames = [
        "query_num", "status", "speedup", "original_ms", "optimized_ms",
        "original_cost", "optimized_cost", "cost_reduction_pct",
        "row_count", "rows_match", "checksum_match", "error", "elapsed_s", "timestamp"
    ]

    sorted_results = sorted(results, key=lambda r: r.speedup, reverse=True)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted_results:
            writer.writerow(asdict(r))

    logger.info(f"Results saved to {csv_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Stage 2: Validate rewrites with 5x trimmed mean benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--input-dir", "-i", required=True, help="Input directory from Stage 1")
    parser.add_argument("--output-dir", "-o", required=True, help="Output directory for results")
    parser.add_argument("--full-db", default=FULL_DB_DEFAULT, help="Full database for benchmarking")
    parser.add_argument("--queries-dir", default=QUERIES_DIR_DEFAULT, help="Original queries directory")

    # Query selection
    parser.add_argument("--queries", help="Specific queries (comma-separated)")
    parser.add_argument("--test-query", type=int, help="Test with a single query first")

    # Execution options
    parser.add_argument("--runs", type=int, default=5, help="Number of benchmark runs per query")
    parser.add_argument("--parallel", type=int, default=1, help="Parallel validations (use with caution)")
    parser.add_argument("--no-resume", action="store_true", help="Don't skip already-validated queries")

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    queries_dir = Path(args.queries_dir)
    if not queries_dir.exists():
        logger.error(f"Queries directory not found: {queries_dir}")
        sys.exit(1)

    # Find all queries with final_optimized.sql
    if args.test_query:
        query_nums = [args.test_query]
    elif args.queries:
        query_nums = [int(q.strip()) for q in args.queries.split(",")]
    else:
        query_nums = []
        for query_dir in sorted(input_dir.glob("q*")):
            final_sql = query_dir / "final_optimized.sql"
            if final_sql.exists():
                try:
                    qnum = int(query_dir.name[1:])
                    query_nums.append(qnum)
                except ValueError:
                    pass
        query_nums.sort()

    # Skip already-validated unless --no-resume
    completed = {}
    if not args.no_resume:
        completed = load_completed_validations(output_dir)
        original_count = len(query_nums)
        query_nums = [q for q in query_nums if q not in completed]
        if completed:
            logger.info(f"Skipping {original_count - len(query_nums)} already-validated queries")

    if not query_nums:
        logger.info("All queries already validated. Use --no-resume to re-run.")
        # Print summary from existing results
        if completed:
            results = list(completed.values())
            print_summary(results)
        return

    logger.info(f"Validating {len(query_nums)} queries")
    logger.info(f"Benchmark runs: {args.runs}")
    logger.info(f"Full DB: {args.full_db}")

    results: List[ValidationResult] = list(completed.values())

    if args.parallel > 1:
        # Parallel execution (use with caution - may cause DB contention)
        logger.warning("Parallel validation may cause database contention")
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            futures = {
                pool.submit(
                    validate_single_query,
                    q, input_dir, queries_dir, args.full_db, args.runs
                ): q for q in query_nums
            }

            for future in as_completed(futures):
                query_num = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    status_icon = "✓" if result.status == "PASS" else "✗"
                    cost_str = f", cost: {result.cost_reduction_pct:+.1f}%" if result.original_cost > 0 else ""
                    logger.info(f"Q{query_num} {status_icon} {result.status}: {result.speedup}x ({result.original_ms}ms -> {result.optimized_ms}ms{cost_str})")
                except Exception as e:
                    logger.error(f"Q{query_num} failed: {e}")
                    results.append(ValidationResult(
                        query_num=query_num,
                        status="ERROR",
                        error=str(e),
                        timestamp=datetime.now().isoformat(),
                    ))
    else:
        # Sequential execution (recommended for accuracy)
        for query_num in query_nums:
            result = validate_single_query(
                query_num, input_dir, queries_dir, args.full_db, args.runs
            )
            results.append(result)

            status_icon = "✓" if result.status == "PASS" else "✗"
            cost_str = f", cost: {result.cost_reduction_pct:+.1f}%" if result.original_cost > 0 else ""
            logger.info(f"Q{query_num} {status_icon} {result.status}: {result.speedup}x ({result.original_ms}ms -> {result.optimized_ms}ms{cost_str})")

            # Save incrementally
            save_results(results, output_dir)

    # Final save
    save_results(results, output_dir)
    print_summary(results)


def print_summary(results: List[ValidationResult]):
    """Print validation summary."""
    total = len(results)
    passed = [r for r in results if r.status == "PASS"]
    failed = [r for r in results if r.status == "FAIL"]
    errors = [r for r in results if r.status == "ERROR"]

    # Speedup categories (only for PASS)
    speedup_gt_1 = [r for r in passed if r.speedup > 1.05]
    speedup_gt_1_2 = [r for r in passed if r.speedup > 1.20]
    speedup_gt_2 = [r for r in passed if r.speedup > 2.00]

    # Regressions (slower than original)
    regressions = [r for r in passed if r.speedup < 0.95]

    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total queries:     {total}")
    print(f"PASS:              {len(passed)} ({len(passed)/total*100:.1f}%)" if total else "")
    print(f"FAIL:              {len(failed)} ({len(failed)/total*100:.1f}%)" if total else "")
    print(f"ERROR:             {len(errors)} ({len(errors)/total*100:.1f}%)" if total else "")
    print("-" * 60)
    print(f"Speedup > 1.05x:   {len(speedup_gt_1)} ({len(speedup_gt_1)/total*100:.1f}%)" if total else "")
    print(f"Speedup > 1.20x:   {len(speedup_gt_1_2)} ({len(speedup_gt_1_2)/total*100:.1f}%)" if total else "")
    print(f"Speedup > 2.00x:   {len(speedup_gt_2)} ({len(speedup_gt_2)/total*100:.1f}%)" if total else "")
    print(f"Regressions:       {len(regressions)} ({len(regressions)/total*100:.1f}%)" if total else "")
    print("=" * 60)

    if passed:
        avg_speedup = sum(r.speedup for r in passed) / len(passed)
        max_speedup = max(r.speedup for r in passed)
        min_speedup = min(r.speedup for r in passed)
        print(f"Avg speedup:       {avg_speedup:.2f}x")
        print(f"Max speedup:       {max_speedup:.2f}x")
        print(f"Min speedup:       {min_speedup:.2f}x")
        print("=" * 60)

    # Top 10 by speedup
    if passed:
        print("\nTop 10 by Speedup:")
        top_10 = sorted(passed, key=lambda r: r.speedup, reverse=True)[:10]
        for r in top_10:
            cost_str = f"cost: {r.cost_reduction_pct:+.1f}%" if r.original_cost > 0 else ""
            print(f"  Q{r.query_num:2d}: {r.speedup:5.2f}x  ({r.original_ms:8.1f}ms -> {r.optimized_ms:8.1f}ms)  {cost_str}")

    # Regressions
    if regressions:
        print("\nRegressions (slower than original):")
        for r in sorted(regressions, key=lambda r: r.speedup):
            print(f"  Q{r.query_num:2d}: {r.speedup:5.2f}x  ({r.original_ms:8.1f}ms -> {r.optimized_ms:8.1f}ms)")

    # Failures
    if failed:
        print("\nFailed (semantic mismatch):")
        for r in sorted(failed, key=lambda r: r.query_num):
            print(f"  Q{r.query_num:2d}: {r.error[:50]}")


if __name__ == "__main__":
    main()
