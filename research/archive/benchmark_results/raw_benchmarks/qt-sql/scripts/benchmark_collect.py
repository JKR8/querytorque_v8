#!/usr/bin/env python3
"""
Stage 1: Benchmark Collection - API Calls + Syntax Validation Only.

Collects LLM rewrites for TPC-DS queries with syntax-only validation.
Saves ALL intermediate artifacts (prompts, responses, SQL) for analysis.

Features:
- Parallel API calls (configurable concurrency)
- Retry with error feedback (up to 3 retries)
- Syntax-only validation (no database execution)
- Checkpointing for resume
- Saves EVERYTHING for later analysis

Usage:
    python benchmark_collect.py --output-dir research/benchmarks/qt-sql/runs/benchmark_output_v2
    python benchmark_collect.py --output-dir research/benchmarks/qt-sql/runs/benchmark_output_v2 --test-query 1
    python benchmark_collect.py --output-dir research/benchmarks/qt-sql/runs/benchmark_output_v2 --parallel 10
"""

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-sql"))
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-shared"))

from qt_sql.optimization.dag_v2 import DagV2Pipeline
from qt_sql.optimization.dag_v3 import build_prompt_with_examples, get_matching_examples
from qt_sql.optimization.adaptive_rewriter_v5 import (
    _get_plan_context,
    _build_history_section,
    _create_llm_client,
)
from qt_sql.validation.sql_validator import SQLValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Default paths
SAMPLE_DB_DEFAULT = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
QUERIES_DIR_DEFAULT = "/mnt/d/TPC-DS/queries_duckdb_converted"

# Queries to run (failed + remaining)
QUERIES_TO_RUN = [2, 7, 10, 13, 16] + list(range(18, 100))  # 87 queries
MAX_RETRIES = 3  # Total 4 attempts per query


@dataclass
class CollectResult:
    """Result from collecting rewrites for a single query."""
    query_num: int
    attempts: int
    syntax_valid: bool
    final_status: str  # "success", "syntax_error", "assembly_error", "api_error"
    error_message: str = ""
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


def collect_single_query(
    query_num: int,
    queries_dir: Path,
    sample_db: str,
    output_dir: Path,
    provider: Optional[str],
    model: Optional[str],
    max_retries: int = MAX_RETRIES,
) -> CollectResult:
    """Collect rewrites for a single query.

    Makes API calls, validates syntax only, saves all artifacts.
    """
    start_time = time.time()
    result = CollectResult(
        query_num=query_num,
        attempts=0,
        syntax_valid=False,
        final_status="pending",
        timestamp=datetime.now().isoformat(),
    )

    # Create output directory for this query
    query_dir = output_dir / f"q{query_num}"
    query_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Load query
        sql = load_query(query_num, queries_dir)
        (query_dir / "original.sql").write_text(sql)

        # Get plan context and examples
        plan_summary, plan_text, plan_context = _get_plan_context(sample_db, sql)
        examples = get_matching_examples(sql)[:3]

        # Build base prompt
        pipeline = DagV2Pipeline(sql, plan_context=plan_context)
        base_prompt = pipeline.get_prompt()

        # Create LLM client
        llm_client = _create_llm_client(provider, model)

        # Create syntax validator (no database execution)
        validator = SQLValidator(database=":memory:")  # Won't actually execute

        history_section = ""

        for attempt in range(1, max_retries + 2):  # 4 total attempts
            result.attempts = attempt

            # Build full prompt
            full_prompt = build_prompt_with_examples(
                base_prompt, examples, plan_summary, history_section
            )

            # Save prompt BEFORE API call
            (query_dir / f"attempt_{attempt}_prompt.txt").write_text(full_prompt)

            try:
                # Call LLM
                logger.info(f"Q{query_num} attempt {attempt}: calling LLM...")
                response = llm_client.analyze(full_prompt)

                # ALWAYS save raw response
                (query_dir / f"attempt_{attempt}_response.json").write_text(response)

            except Exception as e:
                error_msg = f"API error: {e}"
                (query_dir / f"attempt_{attempt}_error.txt").write_text(error_msg)
                logger.warning(f"Q{query_num} attempt {attempt}: {error_msg}")

                if attempt <= max_retries:
                    history_section = _build_history_section("", error_msg)
                    continue
                else:
                    result.final_status = "api_error"
                    result.error_message = error_msg
                    break

            # Try to assemble SQL
            try:
                optimized_sql = pipeline.apply_response(response)
                # ALWAYS save assembled SQL
                (query_dir / f"attempt_{attempt}_optimized.sql").write_text(optimized_sql)

            except Exception as e:
                error_msg = f"Assembly error: {e}"
                (query_dir / f"attempt_{attempt}_error.txt").write_text(error_msg)
                logger.warning(f"Q{query_num} attempt {attempt}: {error_msg}")

                if attempt <= max_retries:
                    history_section = _build_history_section(response, error_msg)
                    continue
                else:
                    result.final_status = "assembly_error"
                    result.error_message = error_msg
                    break

            # Syntax-only validation (NO execution)
            is_valid, errors = validator.validate_syntax(optimized_sql)

            if is_valid:
                # Success! Save final version
                (query_dir / "final_optimized.sql").write_text(optimized_sql)
                result.syntax_valid = True
                result.final_status = "success"
                logger.info(f"Q{query_num} attempt {attempt}: syntax valid!")
                break
            else:
                error_msg = f"Syntax error: {errors[0] if errors else 'Unknown'}"
                (query_dir / f"attempt_{attempt}_error.txt").write_text(error_msg)
                logger.warning(f"Q{query_num} attempt {attempt}: {error_msg}")

                if attempt <= max_retries:
                    history_section = _build_history_section(response, error_msg)
                    continue
                else:
                    result.final_status = "syntax_error"
                    result.error_message = error_msg
                    break

    except FileNotFoundError as e:
        result.final_status = "file_not_found"
        result.error_message = str(e)
        logger.error(f"Q{query_num}: {e}")
    except Exception as e:
        result.final_status = "unexpected_error"
        result.error_message = str(e)
        logger.error(f"Q{query_num}: unexpected error: {e}")

    result.elapsed_s = round(time.time() - start_time, 2)

    # Save status
    (query_dir / "status.json").write_text(json.dumps(asdict(result), indent=2))

    return result


def load_completed_queries(output_dir: Path) -> set:
    """Load set of already-completed query numbers."""
    completed = set()
    if output_dir.exists():
        for query_dir in output_dir.glob("q*"):
            status_file = query_dir / "status.json"
            if status_file.exists():
                try:
                    status = json.loads(status_file.read_text())
                    completed.add(status["query_num"])
                except Exception:
                    pass
    return completed


def save_summary(results: List[CollectResult], output_dir: Path):
    """Save summary CSV."""
    import csv

    csv_path = output_dir / "collect_summary.csv"
    fieldnames = ["query_num", "attempts", "syntax_valid", "final_status", "error_message", "elapsed_s", "timestamp"]

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted(results, key=lambda x: x.query_num):
            writer.writerow(asdict(r))

    logger.info(f"Summary saved to {csv_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Stage 1: Collect LLM rewrites with syntax validation only",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--output-dir", "-o", required=True, help="Output directory for artifacts")
    parser.add_argument("--queries-dir", default=QUERIES_DIR_DEFAULT, help="TPC-DS queries directory")
    parser.add_argument("--sample-db", default=SAMPLE_DB_DEFAULT, help="Sample database for EXPLAIN")

    # Query selection
    parser.add_argument("--test-query", type=int, help="Test with a single query first")
    parser.add_argument("--queries", help="Specific queries (comma-separated)")
    parser.add_argument("--start", type=int, help="Start query number")
    parser.add_argument("--end", type=int, help="End query number")

    # Execution options
    parser.add_argument("--parallel", type=int, default=1, help="Number of parallel API calls")
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES, help="Max retry attempts")
    parser.add_argument("--no-resume", action="store_true", help="Don't skip completed queries")

    # LLM config
    parser.add_argument("--provider", help="LLM provider")
    parser.add_argument("--model", help="LLM model")

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    queries_dir = Path(args.queries_dir)
    if not queries_dir.exists():
        logger.error(f"Queries directory not found: {queries_dir}")
        sys.exit(1)

    # Determine which queries to run
    if args.test_query:
        query_nums = [args.test_query]
    elif args.queries:
        query_nums = [int(q.strip()) for q in args.queries.split(",")]
    elif args.start and args.end:
        query_nums = list(range(args.start, args.end + 1))
    else:
        query_nums = QUERIES_TO_RUN

    # Skip completed queries unless --no-resume
    if not args.no_resume:
        completed = load_completed_queries(output_dir)
        original_count = len(query_nums)
        query_nums = [q for q in query_nums if q not in completed]
        if completed:
            logger.info(f"Skipping {original_count - len(query_nums)} completed queries")

    if not query_nums:
        logger.info("All queries already completed. Use --no-resume to re-run.")
        return

    logger.info(f"Processing {len(query_nums)} queries")
    logger.info(f"Parallel workers: {args.parallel}")
    logger.info(f"Max retries: {args.max_retries}")
    logger.info(f"Provider: {args.provider or 'default'}")

    results: List[CollectResult] = []

    if args.parallel > 1:
        # Parallel execution
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            futures = {
                pool.submit(
                    collect_single_query,
                    q, queries_dir, args.sample_db, output_dir,
                    args.provider, args.model, args.max_retries
                ): q for q in query_nums
            }

            for future in as_completed(futures):
                query_num = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    status_icon = "✓" if result.syntax_valid else "✗"
                    logger.info(f"Q{query_num} {status_icon} {result.final_status} ({result.attempts} attempts, {result.elapsed_s}s)")
                except Exception as e:
                    logger.error(f"Q{query_num} failed: {e}")
                    results.append(CollectResult(
                        query_num=query_num,
                        attempts=0,
                        syntax_valid=False,
                        final_status="thread_error",
                        error_message=str(e),
                        timestamp=datetime.now().isoformat(),
                    ))
    else:
        # Sequential execution
        for query_num in query_nums:
            result = collect_single_query(
                query_num, queries_dir, args.sample_db, output_dir,
                args.provider, args.model, args.max_retries
            )
            results.append(result)
            status_icon = "✓" if result.syntax_valid else "✗"
            logger.info(f"Q{query_num} {status_icon} {result.final_status} ({result.attempts} attempts, {result.elapsed_s}s)")

    # Load any previously completed results for summary
    completed = load_completed_queries(output_dir)
    for q in completed:
        if q not in [r.query_num for r in results]:
            status_file = output_dir / f"q{q}" / "status.json"
            if status_file.exists():
                try:
                    data = json.loads(status_file.read_text())
                    results.append(CollectResult(**data))
                except Exception:
                    pass

    # Summary
    save_summary(results, output_dir)

    valid_count = sum(1 for r in results if r.syntax_valid)
    total_count = len(results)

    print("\n" + "=" * 50)
    print("COLLECTION SUMMARY")
    print("=" * 50)
    print(f"Total queries:     {total_count}")
    print(f"Syntax valid:      {valid_count} ({valid_count/total_count*100:.1f}%)")
    print(f"Failed:            {total_count - valid_count}")
    print("=" * 50)

    # Show failures
    failures = [r for r in results if not r.syntax_valid]
    if failures:
        print("\nFailed queries:")
        for r in sorted(failures, key=lambda x: x.query_num):
            print(f"  Q{r.query_num}: {r.final_status} - {r.error_message[:60]}")

    print(f"\nResults saved to: {output_dir}")
    print(f"Valid SQL files: {output_dir}/q*/final_optimized.sql")


if __name__ == "__main__":
    main()
