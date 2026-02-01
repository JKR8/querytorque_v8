#!/usr/bin/env python3
"""Test MCTS SQL optimizer on TPC-DS queries.

This script tests the MCTS-based SQL optimizer on TPC-DS benchmark queries.
It can run on specific queries (e.g., the 10 that currently fail validation)
or on all 99 queries.

Usage:
    # Test specific queries (the failing ones)
    python research/scripts/test_mcts.py --queries q16,q23,q51,q58,q64,q65,q75,q79,q83,q85

    # Test all queries
    python research/scripts/test_mcts.py --all --max-iterations 30

    # Test a single query
    python research/scripts/test_mcts.py --queries q1 --verbose

    # Save results to file
    python research/scripts/test_mcts.py --all --output results.json
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-sql"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-shared"))


@dataclass
class QueryResult:
    """Result for a single query optimization."""

    query_id: str
    status: str  # "pass", "fail", "error"
    original_time_ms: float = 0.0
    optimized_time_ms: float = 0.0
    speedup: float = 1.0
    transforms_applied: list[str] = field(default_factory=list)
    method: str = ""
    iterations: int = 0
    elapsed_time: float = 0.0
    error: Optional[str] = None
    tree_stats: dict = field(default_factory=dict)


@dataclass
class BenchmarkResults:
    """Aggregate benchmark results."""

    total_queries: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    total_speedup: float = 0.0
    avg_speedup: float = 1.0
    wins_count: int = 0  # queries with speedup >= 1.1
    losses_count: int = 0  # queries with speedup < 0.9
    total_time: float = 0.0
    results: list[QueryResult] = field(default_factory=list)


def find_query_files(queries_dir: Path, query_ids: list[str]) -> dict[str, Path]:
    """Find query files for given query IDs."""
    query_files = {}

    for qid in query_ids:
        # Normalize query ID (q1, q01, 1 -> q1.sql)
        qid_clean = qid.lower().strip()
        if not qid_clean.startswith("q"):
            qid_clean = f"q{qid_clean}"

        # Try different file patterns
        patterns = [
            f"{qid_clean}.sql",
            f"query{qid_clean[1:]}.sql",
            f"{qid_clean.upper()}.sql",
        ]

        for pattern in patterns:
            path = queries_dir / pattern
            if path.exists():
                query_files[qid_clean] = path
                break
        else:
            # Try globbing
            matches = list(queries_dir.glob(f"*{qid_clean[1:]}*.sql"))
            if matches:
                query_files[qid_clean] = matches[0]

    return query_files


def get_all_query_ids() -> list[str]:
    """Get all TPC-DS query IDs (1-99)."""
    return [f"q{i}" for i in range(1, 100)]


def get_failing_query_ids() -> list[str]:
    """Get the 10 queries that currently fail validation."""
    return ["q16", "q23", "q51", "q58", "q64", "q65", "q75", "q79", "q83", "q85"]


def run_mcts_optimization(
    query_sql: str,
    database: str,
    provider: str,
    model: Optional[str],
    max_iterations: int,
    verbose: bool,
    num_parallel: int = 4,
) -> QueryResult:
    """Run MCTS optimization on a single query."""
    from qt_sql.optimization.mcts import MCTSSQLOptimizer

    try:
        with MCTSSQLOptimizer(
            database=database,
            provider=provider,
            model=model,
        ) as optimizer:
            # Use parallel optimization if num_parallel > 1
            if num_parallel > 1:
                result = optimizer.optimize_parallel(
                    query=query_sql,
                    max_iterations=max_iterations,
                    num_parallel=num_parallel,
                )
            else:
                result = optimizer.optimize(
                    query=query_sql,
                    max_iterations=max_iterations,
                )

        # Get timing from validation result if available
        original_time = 0.0
        optimized_time = 0.0
        if result.validation_result:
            original_time = getattr(result.validation_result, "original_timing_ms", 0.0)
            optimized_time = getattr(result.validation_result, "optimized_timing_ms", 0.0)

        return QueryResult(
            query_id="",  # Will be set by caller
            status="pass" if result.valid else "fail",
            original_time_ms=original_time,
            optimized_time_ms=optimized_time,
            speedup=result.speedup,
            transforms_applied=result.transforms_applied,
            method=result.method,
            iterations=result.iterations,
            elapsed_time=result.elapsed_time,
            tree_stats=result.tree_stats,
        )

    except Exception as e:
        return QueryResult(
            query_id="",
            status="error",
            error=str(e),
        )


def run_benchmark(
    queries_dir: Path,
    database: str,
    query_ids: list[str],
    provider: str,
    model: Optional[str],
    max_iterations: int,
    verbose: bool,
    output_dir: Optional[Path],
    num_parallel: int = 4,
) -> BenchmarkResults:
    """Run benchmark on multiple queries."""
    # Setup logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(__name__)

    # Find query files
    query_files = find_query_files(queries_dir, query_ids)

    if not query_files:
        logger.error(f"No query files found in {queries_dir}")
        return BenchmarkResults()

    logger.info(f"Found {len(query_files)} query files")

    # Run optimizations
    results = BenchmarkResults(total_queries=len(query_files))
    start_time = time.time()

    for qid, qpath in sorted(query_files.items()):
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {qid}: {qpath.name}")
        logger.info(f"{'='*60}")

        query_sql = qpath.read_text(encoding="utf-8")

        qresult = run_mcts_optimization(
            query_sql=query_sql,
            database=database,
            provider=provider,
            model=model,
            max_iterations=max_iterations,
            verbose=verbose,
            num_parallel=num_parallel,
        )
        qresult.query_id = qid

        # Update aggregate stats
        if qresult.status == "pass":
            results.passed += 1
            results.total_speedup += qresult.speedup

            if qresult.speedup >= 1.1:
                results.wins_count += 1
            elif qresult.speedup < 0.9:
                results.losses_count += 1

        elif qresult.status == "fail":
            results.failed += 1
        else:
            results.errors += 1

        results.results.append(qresult)

        # Log result
        if qresult.status == "pass":
            logger.info(
                f"  {qid}: PASS - {qresult.speedup:.2f}x speedup "
                f"(transforms: {qresult.transforms_applied})"
            )
        elif qresult.status == "fail":
            logger.warning(f"  {qid}: FAIL - validation failed")
        else:
            logger.error(f"  {qid}: ERROR - {qresult.error}")

        # Save individual result if output dir specified
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            result_file = output_dir / f"{qid}_result.json"
            with open(result_file, "w") as f:
                json.dump(asdict(qresult), f, indent=2)

    results.total_time = time.time() - start_time

    # Calculate averages
    if results.passed > 0:
        results.avg_speedup = results.total_speedup / results.passed

    return results


def print_summary(results: BenchmarkResults) -> None:
    """Print benchmark summary."""
    print("\n" + "=" * 60)
    print("MCTS BENCHMARK SUMMARY")
    print("=" * 60)

    print(f"\nTotal queries: {results.total_queries}")
    print(f"  Passed: {results.passed} ({100*results.passed/results.total_queries:.1f}%)")
    print(f"  Failed: {results.failed} ({100*results.failed/results.total_queries:.1f}%)")
    print(f"  Errors: {results.errors} ({100*results.errors/results.total_queries:.1f}%)")

    print(f"\nPerformance:")
    print(f"  Average speedup: {results.avg_speedup:.2f}x")
    print(f"  Wins (>=1.1x): {results.wins_count}")
    print(f"  Losses (<0.9x): {results.losses_count}")

    print(f"\nTotal benchmark time: {results.total_time:.1f}s")

    # Show top improvements
    if results.results:
        valid_results = [r for r in results.results if r.status == "pass" and r.speedup > 1.0]
        if valid_results:
            print("\nTop improvements:")
            for r in sorted(valid_results, key=lambda x: x.speedup, reverse=True)[:5]:
                print(f"  {r.query_id}: {r.speedup:.2f}x ({' -> '.join(r.transforms_applied)})")

        # Show failures
        failures = [r for r in results.results if r.status != "pass"]
        if failures:
            print("\nFailed/Error queries:")
            for r in failures:
                status = r.status.upper()
                error = f" - {r.error}" if r.error else ""
                print(f"  {r.query_id}: {status}{error}")


def main():
    parser = argparse.ArgumentParser(
        description="Test MCTS SQL optimizer on TPC-DS queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Query selection
    parser.add_argument(
        "--queries",
        type=str,
        help="Comma-separated list of query IDs (e.g., q1,q2,q3)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run on all 99 TPC-DS queries",
    )
    parser.add_argument(
        "--failures",
        action="store_true",
        help="Run on the 10 queries that currently fail validation",
    )

    # Paths
    parser.add_argument(
        "--queries-dir",
        type=str,
        default="D:/TPC-DS/queries_duckdb_converted",
        help="Directory containing TPC-DS query files",
    )
    parser.add_argument(
        "--database",
        "-d",
        type=str,
        default="D:/TPC-DS/tpcds_sf100.duckdb",
        help="Path to TPC-DS DuckDB database",
    )

    # LLM options
    parser.add_argument(
        "--provider",
        type=str,
        default="deepseek",
        help="LLM provider (default: deepseek)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="LLM model name (optional)",
    )

    # MCTS options
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=30,
        help="Maximum MCTS iterations per query (default: 30)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=4,
        help="Number of parallel LLM calls per iteration (default: 4)",
    )

    # Output options
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for per-query results",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Determine which queries to run
    if args.all:
        query_ids = get_all_query_ids()
    elif args.failures:
        query_ids = get_failing_query_ids()
    elif args.queries:
        query_ids = [q.strip() for q in args.queries.split(",")]
    else:
        # Default to failing queries
        query_ids = get_failing_query_ids()
        print("No queries specified, using failing queries by default.")
        print(f"Query IDs: {query_ids}")

    # Validate paths
    queries_dir = Path(args.queries_dir)
    if not queries_dir.exists():
        print(f"Error: Queries directory not found: {queries_dir}")
        sys.exit(1)

    database = args.database
    if not Path(database).exists():
        print(f"Error: Database not found: {database}")
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else None

    # Run benchmark
    print(f"Running MCTS benchmark on {len(query_ids)} queries...")
    print(f"Database: {database}")
    print(f"Provider: {args.provider}")
    print(f"Max iterations: {args.max_iterations}")
    print(f"Parallel LLM calls: {args.parallel}")

    results = run_benchmark(
        queries_dir=queries_dir,
        database=database,
        query_ids=query_ids,
        provider=args.provider,
        model=args.model,
        max_iterations=args.max_iterations,
        verbose=args.verbose,
        output_dir=output_dir,
        num_parallel=args.parallel,
    )

    # Print summary
    print_summary(results)

    # Save results
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to JSON-serializable format
        results_dict = {
            "total_queries": results.total_queries,
            "passed": results.passed,
            "failed": results.failed,
            "errors": results.errors,
            "avg_speedup": results.avg_speedup,
            "wins_count": results.wins_count,
            "losses_count": results.losses_count,
            "total_time": results.total_time,
            "results": [asdict(r) for r in results.results],
        }

        with open(output_path, "w") as f:
            json.dump(results_dict, f, indent=2)

        print(f"\nResults saved to: {output_path}")

    # Exit code based on results
    if results.errors > 0:
        sys.exit(2)
    elif results.failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
