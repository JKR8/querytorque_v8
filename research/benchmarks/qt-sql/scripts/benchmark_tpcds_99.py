#!/usr/bin/env python3
"""
TPC-DS 99 Query Benchmark for Adaptive Rewriter V5.

Benchmarks all 99 TPC-DS queries across optimization modes:
- Mode 2 (parallel): 5 workers compete on sample DB, winners validated on full DB
- Mode 1 (retry): Single worker with error feedback (optional)
- Mode 3 (evolutionary): Iterative improvement (optional)

Features:
- Resume from checkpoint (skips already-processed queries)
- Detailed CSV output with per-query metrics
- Real-time progress display
- Configurable provider/model
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-sql"))
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-shared"))
DEFAULT_OUTPUT_DIR = REPO_ROOT / "research/benchmarks/qt-sql/runs/benchmark_output"

from qt_sql.optimization import optimize_v5_json_queue
from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_retry, optimize_v5_evolutionary
from qt_sql.validation.schemas import ValidationStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# Default paths
SAMPLE_DB_DEFAULT = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
FULL_DB_DEFAULT = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
QUERIES_DIR_DEFAULT = "/mnt/d/TPC-DS/queries_duckdb_converted"


@dataclass
class QueryResult:
    """Result for a single query optimization."""
    query_num: int
    mode: str  # "parallel", "retry", "evolutionary"
    status: str = "pending"  # "success", "below_target", "no_winner", "failed", "skipped"

    # Sample DB results
    valid_sample_count: int = 0
    sample_workers: str = ""
    sample_speedups: str = ""
    sample_best_speedup: float = 0.0

    # Full DB results
    full_workers: str = ""
    full_speedups: str = ""

    # Winner info
    winner_found: bool = False
    winner_worker: Optional[int] = None
    winner_sample_speedup: float = 0.0
    winner_full_speedup: float = 0.0

    # Timing
    elapsed_s: float = 0.0
    timestamp: str = ""

    # Error info
    error: str = ""


def load_query(query_num: int, queries_dir: Path) -> str:
    """Load SQL query by number, trying multiple naming patterns."""
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


def load_checkpoint(checkpoint_path: Path) -> dict[int, QueryResult]:
    """Load existing results from checkpoint file."""
    results = {}
    if checkpoint_path.exists():
        with open(checkpoint_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                qnum = int(row["query_num"])
                results[qnum] = QueryResult(
                    query_num=qnum,
                    mode=row.get("mode", "parallel"),
                    status=row.get("status", "unknown"),
                    valid_sample_count=int(row.get("valid_sample_count", 0)),
                    sample_workers=row.get("sample_workers", ""),
                    sample_speedups=row.get("sample_speedups", ""),
                    sample_best_speedup=float(row.get("sample_best_speedup", 0)),
                    full_workers=row.get("full_workers", ""),
                    full_speedups=row.get("full_speedups", ""),
                    winner_found=row.get("winner_found", "").lower() == "true",
                    winner_worker=int(row["winner_worker"]) if row.get("winner_worker") else None,
                    winner_sample_speedup=float(row.get("winner_sample_speedup", 0)),
                    winner_full_speedup=float(row.get("winner_full_speedup", 0)),
                    elapsed_s=float(row.get("elapsed_s", 0)),
                    timestamp=row.get("timestamp", ""),
                    error=row.get("error", ""),
                )
        logger.info(f"Loaded {len(results)} existing results from checkpoint")
    return results


def save_result(result: QueryResult, csv_path: Path, append: bool = True):
    """Save a single result to CSV file."""
    fieldnames = [
        "query_num", "mode", "status",
        "valid_sample_count", "sample_workers", "sample_speedups", "sample_best_speedup",
        "full_workers", "full_speedups",
        "winner_found", "winner_worker", "winner_sample_speedup", "winner_full_speedup",
        "elapsed_s", "timestamp", "error"
    ]

    mode = "a" if append and csv_path.exists() else "w"
    write_header = not csv_path.exists() or mode == "w"

    with open(csv_path, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(asdict(result))


def run_parallel_mode(
    sql: str,
    query_num: int,
    sample_db: str,
    full_db: str,
    max_workers: int,
    target_speedup: float,
    provider: Optional[str],
    model: Optional[str],
    output_dir: Optional[Path],
) -> QueryResult:
    """Run Mode 2: Parallel worker competition."""
    start = time.time()
    result = QueryResult(
        query_num=query_num,
        mode="parallel",
        timestamp=datetime.now().isoformat(),
    )

    try:
        out_dir = str(output_dir / f"q{query_num}") if output_dir else None

        valid, full_results, winner = optimize_v5_json_queue(
            sql,
            sample_db=sample_db,
            full_db=full_db,
            query_id=f"q{query_num}",
            max_workers=max_workers,
            target_speedup=target_speedup,
            provider=provider,
            model=model,
            output_dir=out_dir,
        )

        result.elapsed_s = round(time.time() - start, 2)
        result.valid_sample_count = len(valid)
        result.sample_workers = ",".join(str(v.worker_id) for v in valid)
        result.sample_speedups = ";".join(f"{v.speedup:.2f}" for v in valid)
        result.sample_best_speedup = max([v.speedup for v in valid], default=0.0)

        result.full_workers = ",".join(str(fr.sample.worker_id) for fr in full_results)
        result.full_speedups = ";".join(f"{fr.full_speedup:.2f}" for fr in full_results)

        if winner:
            result.status = "success"
            result.winner_found = True
            result.winner_worker = winner.sample.worker_id
            result.winner_sample_speedup = winner.sample.speedup
            result.winner_full_speedup = winner.full_speedup
        elif valid:
            result.status = "no_winner"
        else:
            result.status = "failed"
            result.error = "No valid candidates"

    except Exception as e:
        result.elapsed_s = round(time.time() - start, 2)
        result.status = "failed"
        result.error = str(e)[:200]
        logger.error(f"Q{query_num} failed: {e}")

    return result


def run_retry_mode(
    sql: str,
    query_num: int,
    sample_db: str,
    full_db: str,
    max_retries: int,
    target_speedup: float,
    provider: Optional[str],
    model: Optional[str],
    output_dir: Optional[Path],
) -> QueryResult:
    """Run Mode 1: Single worker with error feedback retries."""
    start = time.time()
    result = QueryResult(
        query_num=query_num,
        mode="retry",
        timestamp=datetime.now().isoformat(),
    )

    try:
        out_dir = str(output_dir / f"q{query_num}_retry") if output_dir else None

        candidate, full_result, history = optimize_v5_retry(
            sql,
            sample_db=sample_db,
            full_db=full_db,
            query_id=f"q{query_num}",
            max_retries=max_retries,
            target_speedup=target_speedup,
            provider=provider,
            model=model,
            output_dir=out_dir,
        )

        result.elapsed_s = round(time.time() - start, 2)
        result.valid_sample_count = len([h for h in history if h.get("status") == "PASS"])

        if candidate and full_result:
            result.status = "success" if full_result.full_speedup >= target_speedup else "no_winner"
            result.winner_found = full_result.full_speedup >= target_speedup
            result.winner_worker = candidate.worker_id
            result.winner_sample_speedup = candidate.speedup
            result.winner_full_speedup = full_result.full_speedup
        else:
            result.status = "failed"
            result.error = "All retries exhausted"

    except Exception as e:
        result.elapsed_s = round(time.time() - start, 2)
        result.status = "failed"
        result.error = str(e)[:200]
        logger.error(f"Q{query_num} retry failed: {e}")

    return result


def run_evolutionary_mode(
    sql: str,
    query_num: int,
    full_db: str,
    max_iterations: int,
    target_speedup: float,
    provider: Optional[str],
    model: Optional[str],
    output_dir: Optional[Path],
) -> QueryResult:
    """Run Mode 3: Evolutionary iterative improvement."""
    start = time.time()
    result = QueryResult(
        query_num=query_num,
        mode="evolutionary",
        timestamp=datetime.now().isoformat(),
    )

    # Always create output directory for evolutionary mode
    out_dir = output_dir / f"q{query_num}" if output_dir else DEFAULT_OUTPUT_DIR / f"q{query_num}"
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        candidate, full_result, history = optimize_v5_evolutionary(
            sql,
            full_db=full_db,
            query_id=f"q{query_num}",
            max_iterations=max_iterations,
            target_speedup=target_speedup,
            provider=provider,
            model=model,
            output_dir=str(out_dir),
        )

        result.elapsed_s = round(time.time() - start, 2)
        successful = [h for h in history if h.get("status") == "success"]
        result.valid_sample_count = len(successful)

        # Record all speedups from successful iterations
        if successful:
            result.sample_speedups = ";".join(f"{h.get('speedup', 0):.2f}" for h in successful)
            result.sample_best_speedup = max(h.get('speedup', 0) for h in successful)

        if candidate and full_result:
            # Record result even if under target
            result.winner_worker = candidate.worker_id
            result.winner_full_speedup = full_result.full_speedup
            result.winner_sample_speedup = candidate.speedup

            if full_result.full_speedup >= target_speedup:
                result.status = "success"
                result.winner_found = True
            else:
                result.status = "below_target"
                result.winner_found = False
        else:
            result.status = "failed"
            result.error = "No successful iterations"

        # Save history to JSON
        history_file = out_dir / "iterations_history.json"
        history_file.write_text(json.dumps(history, indent=2))

    except Exception as e:
        result.elapsed_s = round(time.time() - start, 2)
        result.status = "failed"
        result.error = str(e)[:200]
        logger.error(f"Q{query_num} evolutionary failed: {e}")

    return result


def print_progress(completed: int, total: int, winners: int, current_query: int):
    """Print progress bar and stats."""
    pct = completed / total * 100
    bar_len = 40
    filled = int(bar_len * completed / total)
    bar = "█" * filled + "░" * (bar_len - filled)
    print(f"\r[{bar}] {completed}/{total} ({pct:.1f}%) | Winners: {winners} | Current: Q{current_query}   ", end="", flush=True)


def print_summary(results: dict[int, QueryResult]):
    """Print final summary statistics."""
    total = len(results)
    winners = sum(1 for r in results.values() if r.winner_found)
    failed = sum(1 for r in results.values() if r.status == "failed")
    below_target = sum(1 for r in results.values() if r.status == "below_target")
    no_winner = sum(1 for r in results.values() if r.status == "no_winner")

    avg_time = sum(r.elapsed_s for r in results.values()) / total if total else 0
    total_time = sum(r.elapsed_s for r in results.values())

    # Get all speedups (winners and below_target)
    all_speedups = [r.winner_full_speedup for r in results.values()
                    if r.winner_full_speedup and r.winner_full_speedup > 0]
    winner_speedups = [r.winner_full_speedup for r in results.values()
                       if r.winner_found and r.winner_full_speedup > 0]

    avg_speedup = sum(all_speedups) / len(all_speedups) if all_speedups else 0
    max_speedup = max(all_speedups) if all_speedups else 0
    avg_winner_speedup = sum(winner_speedups) / len(winner_speedups) if winner_speedups else 0

    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)
    print(f"Total queries:     {total}")
    print(f"Winners (≥2x):     {winners} ({winners/total*100:.1f}%)" if total else "")
    print(f"Below target:      {below_target} ({below_target/total*100:.1f}%)" if total else "")
    print(f"No valid result:   {no_winner} ({no_winner/total*100:.1f}%)" if total else "")
    print(f"Failed:            {failed} ({failed/total*100:.1f}%)" if total else "")
    print("-" * 60)
    print(f"Avg speedup (all): {avg_speedup:.2f}x")
    print(f"Avg speedup (≥2x): {avg_winner_speedup:.2f}x")
    print(f"Max speedup:       {max_speedup:.2f}x")
    print(f"Avg time/query:    {avg_time:.1f}s")
    print(f"Total time:        {total_time/60:.1f}min")
    print("=" * 60)

    # Top 10 by speedup (including below_target)
    if all_speedups:
        print("\nTop 10 Results:")
        sorted_results = sorted(
            [r for r in results.values() if r.winner_full_speedup and r.winner_full_speedup > 0],
            key=lambda r: r.winner_full_speedup,
            reverse=True
        )[:10]
        for r in sorted_results:
            marker = "✓" if r.winner_found else "○"
            print(f"  {marker} Q{r.query_num}: {r.winner_full_speedup:.2f}x (iter {r.winner_worker})")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark TPC-DS 99 queries with Adaptive Rewriter V5",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all 99 queries with parallel mode (default)
  python benchmark_tpcds_99.py --output results.csv

  # Run specific range with resume
  python benchmark_tpcds_99.py --output results.csv --start 1 --end 20

  # Run with retry mode
  python benchmark_tpcds_99.py --output results.csv --mode retry

  # Run with custom provider/model
  python benchmark_tpcds_99.py --output results.csv --provider anthropic --model claude-sonnet-4-20250514
        """
    )

    # Required
    parser.add_argument("--output", "-o", required=True, help="Output CSV file path")

    # Database paths
    parser.add_argument("--sample-db", default=SAMPLE_DB_DEFAULT, help="Sample database path")
    parser.add_argument("--full-db", default=FULL_DB_DEFAULT, help="Full database path")
    parser.add_argument("--queries-dir", default=QUERIES_DIR_DEFAULT, help="TPC-DS queries directory")

    # Query range
    parser.add_argument("--start", type=int, default=1, help="Start query number (1-99)")
    parser.add_argument("--end", type=int, default=99, help="End query number (1-99)")
    parser.add_argument("--queries", help="Specific queries (comma-separated, e.g., '1,5,15,23')")
    parser.add_argument("--exclude", help="Queries to skip (comma-separated)")

    # Mode selection
    parser.add_argument("--mode", choices=["parallel", "retry", "evolutionary"], default="evolutionary",
                        help="Optimization mode (default: evolutionary)")

    # Mode parameters
    parser.add_argument("--max-workers", type=int, default=5, help="Max parallel workers (parallel mode)")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries (retry mode)")
    parser.add_argument("--max-iterations", type=int, default=5, help="Max iterations (evolutionary mode)")
    parser.add_argument("--target-speedup", type=float, default=2.0, help="Target speedup threshold")

    # LLM config
    parser.add_argument("--provider", help="LLM provider (deepseek, anthropic, etc.)")
    parser.add_argument("--model", help="LLM model name")

    # Output options
    parser.add_argument("--output-dir", help="Directory to save intermediate SQL files")
    parser.add_argument("--no-resume", action="store_true", help="Don't resume from checkpoint")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate paths
    queries_dir = Path(args.queries_dir)
    if not queries_dir.exists():
        logger.error(f"Queries directory not found: {queries_dir}")
        sys.exit(1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_dir = Path(args.output_dir) if args.output_dir else None
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)

    # Determine which queries to run
    if args.queries:
        query_nums = [int(q.strip()) for q in args.queries.split(",")]
    else:
        query_nums = list(range(args.start, args.end + 1))

    exclude = set()
    if args.exclude:
        exclude = {int(q.strip()) for q in args.exclude.split(",")}
    query_nums = [q for q in query_nums if q not in exclude]

    # Load checkpoint
    existing_results = {}
    if not args.no_resume:
        existing_results = load_checkpoint(output_path)

    # Filter out already completed queries
    pending_queries = [q for q in query_nums if q not in existing_results]

    logger.info(f"Mode: {args.mode}")
    logger.info(f"Provider: {args.provider or 'default'}, Model: {args.model or 'default'}")
    logger.info(f"Queries: {len(query_nums)} total, {len(pending_queries)} pending")
    logger.info(f"Target speedup: {args.target_speedup}x")

    if not pending_queries:
        logger.info("All queries already processed. Use --no-resume to re-run.")
        print_summary(existing_results)
        return

    # Run benchmark
    all_results = dict(existing_results)
    winners = sum(1 for r in all_results.values() if r.winner_found)

    print(f"\nStarting benchmark ({len(pending_queries)} queries)...\n")

    for i, query_num in enumerate(pending_queries):
        print_progress(
            len(all_results) - len(existing_results) + i,
            len(pending_queries),
            winners,
            query_num
        )

        try:
            sql = load_query(query_num, queries_dir)
        except FileNotFoundError as e:
            logger.warning(f"Skipping Q{query_num}: {e}")
            continue

        # Run appropriate mode
        if args.mode == "parallel":
            result = run_parallel_mode(
                sql, query_num,
                args.sample_db, args.full_db,
                args.max_workers, args.target_speedup,
                args.provider, args.model, output_dir
            )
        elif args.mode == "retry":
            result = run_retry_mode(
                sql, query_num,
                args.sample_db, args.full_db,
                args.max_retries, args.target_speedup,
                args.provider, args.model, output_dir
            )
        else:  # evolutionary
            result = run_evolutionary_mode(
                sql, query_num,
                args.full_db,
                args.max_iterations, args.target_speedup,
                args.provider, args.model, output_dir
            )

        # Save immediately for resume capability
        save_result(result, output_path, append=bool(all_results))
        all_results[query_num] = result

        if result.winner_found:
            winners += 1

        # Log result
        status_icon = "✓" if result.winner_found else "○" if result.status == "no_winner" else "✗"
        speedup_str = f"{result.winner_full_speedup:.2f}x" if result.winner_found else ""
        logger.info(f"Q{query_num} {status_icon} {result.status} {speedup_str} ({result.elapsed_s:.1f}s)")

    print_progress(len(pending_queries), len(pending_queries), winners, 0)
    print()  # New line after progress bar

    # Final summary
    print_summary(all_results)
    logger.info(f"Results saved to: {output_path}")


if __name__ == "__main__":
    main()
