#!/usr/bin/env python3
"""Run MCTS benchmark on failed queries in parallel.

Each query runs in its own process with sequential MCTS (proper learning).
All queries run simultaneously for maximum throughput.

Captures full details:
- Every LLM response (full SQL for each transform attempt)
- UCT selection decisions
- Validation results
- Tree structure

Usage:
    python scripts/run_mcts_benchmark.py --queries q2,q7,q16 --iterations 30
    python scripts/run_mcts_benchmark.py --all-failed --iterations 30
"""

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# Load .env file from project root
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
if ENV_FILE.exists():
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Paths (WSL paths)
TPCDS_QUERIES = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
TPCDS_DATABASE = Path("/mnt/d/TPC-DS/tpcds_sf100.duckdb")
OUTPUT_BASE = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/experiments/benchmarks")

# Failed queries from previous run
FAILED_QUERIES = ["q2", "q7", "q16", "q26", "q30", "q35", "q44", "q51", "q59", "q65", "q67", "q81"]


@dataclass
class QueryResult:
    """Result for a single query optimization."""
    query: str
    status: str  # success, failed, error
    speedup: float = 0.0
    original_time_ms: float = 0.0
    optimized_time_ms: float = 0.0
    iterations: int = 0
    transforms: list = None
    elapsed_seconds: float = 0.0
    error: Optional[str] = None

    def __post_init__(self):
        if self.transforms is None:
            self.transforms = []

    def to_dict(self):
        return asdict(self)


def get_query_file(query_name: str) -> Path:
    """Get the SQL file path for a query name like 'q7'."""
    num = int(query_name[1:])
    # Files are named query_1.sql, query_2.sql, etc. (no zero-padding)
    filename = f"query_{num}.sql"
    return TPCDS_QUERIES / filename


def run_single_query(
    query_name: str,
    database: str,
    max_iterations: int,
    output_dir: Path,
    provider: str,
    model: Optional[str],
    env_file: str,
) -> QueryResult:
    """Run MCTS optimization on a single query (runs in subprocess)."""
    # Import inside function for multiprocessing
    import logging
    import os
    from pathlib import Path

    # Load .env file in subprocess
    env_path = Path(env_file)
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s - {query_name} - %(levelname)s - %(message)s"
    )

    from qt_sql.optimization.mcts.optimizer import MCTSSQLOptimizer

    query_file = get_query_file(query_name)
    if not query_file.exists():
        return QueryResult(
            query=query_name,
            status="error",
            error=f"Query file not found: {query_file}"
        )

    sql = query_file.read_text(encoding="utf-8")
    query_output_dir = output_dir / query_name
    query_output_dir.mkdir(parents=True, exist_ok=True)

    # Save original
    (query_output_dir / "original.sql").write_text(sql, encoding="utf-8")

    start_time = time.perf_counter()

    try:
        with MCTSSQLOptimizer(
            database=database,
            provider=provider,
            model=model,
        ) as optimizer:
            # Use sequential MCTS with full SQL logging
            result = optimizer.optimize(
                query=sql,
                max_iterations=max_iterations,
                early_stop_speedup=3.0,
                convergence_patience=10,
                log_full_sql=True,  # Capture full SQL for every transform
            )

        elapsed = time.perf_counter() - start_time

        # Save optimized SQL
        (query_output_dir / "optimized.sql").write_text(result.optimized_sql, encoding="utf-8")

        # Save full result (summary)
        (query_output_dir / "result.json").write_text(
            json.dumps(result.to_dict(), indent=2, default=str),
            encoding="utf-8"
        )

        # Save detailed log with full SQL for every transform attempt
        if result.detailed_log:
            (query_output_dir / "detailed_log.json").write_text(
                json.dumps(result.detailed_log, indent=2, default=str),
                encoding="utf-8"
            )

            # Also save each transform attempt as a separate file for easy inspection
            attempts_dir = query_output_dir / "attempts"
            attempts_dir.mkdir(exist_ok=True)

            for i, attempt in enumerate(result.detailed_log.get("attempts", [])):
                attempt_file = attempts_dir / f"{i:03d}_{attempt['transform_id']}.json"
                attempt_file.write_text(json.dumps(attempt, indent=2, default=str), encoding="utf-8")

                # Also save the SQL separately if it exists
                if attempt.get("output_sql"):
                    sql_file = attempts_dir / f"{i:03d}_{attempt['transform_id']}.sql"
                    sql_file.write_text(attempt["output_sql"], encoding="utf-8")

        # Save attempt summary (by transform type)
        if result.attempt_summary:
            (query_output_dir / "attempt_summary.json").write_text(
                json.dumps(result.attempt_summary, indent=2),
                encoding="utf-8"
            )

        # Save tree structure
        if result.detailed_log and "tree" in result.detailed_log:
            (query_output_dir / "tree.json").write_text(
                json.dumps(result.detailed_log["tree"], indent=2),
                encoding="utf-8"
            )

        # Save selection log (UCT decisions)
        if result.detailed_log and "selections" in result.detailed_log:
            (query_output_dir / "selections.json").write_text(
                json.dumps(result.detailed_log["selections"], indent=2),
                encoding="utf-8"
            )

        if result.valid:
            return QueryResult(
                query=query_name,
                status="success",
                speedup=result.speedup,
                original_time_ms=result.validation_result.original_timing_ms if result.validation_result else 0,
                optimized_time_ms=result.validation_result.optimized_timing_ms if result.validation_result else 0,
                iterations=result.iterations,
                transforms=result.transforms_applied,
                elapsed_seconds=elapsed,
            )
        else:
            return QueryResult(
                query=query_name,
                status="failed",
                speedup=result.speedup,
                iterations=result.iterations,
                transforms=result.transforms_applied,
                elapsed_seconds=elapsed,
                error="No valid optimization found",
            )

    except Exception as e:
        elapsed = time.perf_counter() - start_time
        import traceback
        error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"

        # Save error log
        (query_output_dir / "error.log").write_text(error_msg, encoding="utf-8")

        return QueryResult(
            query=query_name,
            status="error",
            elapsed_seconds=elapsed,
            error=str(e),
        )


def main():
    parser = argparse.ArgumentParser(description="Run MCTS benchmark on TPC-DS queries")
    parser.add_argument("--queries", type=str, help="Comma-separated query names (e.g., q2,q7,q16)")
    parser.add_argument("--all-failed", action="store_true", help="Run all previously failed queries")
    parser.add_argument("--iterations", type=int, default=30, help="Max MCTS iterations per query")
    parser.add_argument("--database", type=str, default=str(TPCDS_DATABASE), help="Database path")
    parser.add_argument("--provider", type=str, default=None, help="LLM provider (default: from .env)")
    parser.add_argument("--model", type=str, default=None, help="LLM model (default: from .env)")
    parser.add_argument("--max-workers", type=int, default=12, help="Max parallel query processes")
    parser.add_argument("--output-name", type=str, default=None, help="Custom output folder name")
    args = parser.parse_args()

    # Determine queries to run
    if args.all_failed:
        queries = FAILED_QUERIES
    elif args.queries:
        queries = [q.strip() for q in args.queries.split(",")]
    else:
        print("Error: Specify --queries or --all-failed")
        sys.exit(1)

    # Setup output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_name = args.output_name or f"mcts_pushdown_{timestamp}"
    output_dir = OUTPUT_BASE / output_name
    output_dir.mkdir(parents=True, exist_ok=True)

    # Resolve provider from env if not specified
    effective_provider = args.provider or os.environ.get("QT_LLM_PROVIDER", "groq")
    effective_model = args.model or os.environ.get("QT_LLM_MODEL", "default")

    # Log configuration
    config = {
        "timestamp": timestamp,
        "queries": queries,
        "database": args.database,
        "max_iterations": args.iterations,
        "provider": effective_provider,
        "model": effective_model,
        "max_workers": args.max_workers,
        "mcts_mode": "sequential (parallel=1, true MCTS learning)",
        "log_full_sql": True,
        "transforms": [
            "push_pred", "flatten_subq", "reorder_join", "cte_inline",
            "eliminate_distinct", "opt_groupby", "opt_window", "batch_agg",
            "multi_push_pred"  # New predicate pushdown transform
        ],
    }
    (output_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"MCTS Benchmark - {len(queries)} queries")
    print(f"{'='*60}")
    print(f"Database: {args.database}")
    print(f"Iterations: {args.iterations}")
    print(f"Provider: {effective_provider}")
    print(f"Model: {effective_model}")
    print(f"Max workers: {args.max_workers}")
    print(f"Output: {output_dir}")
    print(f"Queries: {', '.join(queries)}")
    print(f"Full SQL logging: ENABLED")
    print(f"{'='*60}\n")

    # Run queries in parallel
    results = []
    start_time = time.perf_counter()

    with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                run_single_query,
                query_name=q,
                database=args.database,
                max_iterations=args.iterations,
                output_dir=output_dir,
                provider=args.provider,
                model=args.model,
                env_file=str(ENV_FILE),
            ): q
            for q in queries
        }

        for future in as_completed(futures):
            query_name = futures[future]
            try:
                result = future.result()
                results.append(result)

                if result.status == "success":
                    print(f"  {result.query}: ✓ {result.speedup:.2f}x ({result.elapsed_seconds:.1f}s, {result.iterations} iters)")
                elif result.status == "failed":
                    print(f"  {result.query}: ✗ no valid optimization ({result.elapsed_seconds:.1f}s)")
                else:
                    print(f"  {result.query}: ERROR - {result.error[:50]}...")

            except Exception as e:
                print(f"  {query_name}: EXCEPTION - {e}")
                results.append(QueryResult(
                    query=query_name,
                    status="error",
                    error=str(e),
                ))

    total_elapsed = time.perf_counter() - start_time

    # Sort results by query name
    results.sort(key=lambda r: r.query)

    # Generate summary
    success = [r for r in results if r.status == "success"]
    failed = [r for r in results if r.status == "failed"]
    errors = [r for r in results if r.status == "error"]

    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    print(f"Total: {len(results)}")
    print(f"Success: {len(success)}")
    print(f"Failed: {len(failed)}")
    print(f"Errors: {len(errors)}")
    print(f"Total time: {total_elapsed:.1f}s")

    if success:
        speedups = [r.speedup for r in success]
        avg_speedup = sum(speedups) / len(speedups)
        wins = [r for r in success if r.speedup >= 1.1]
        regressions = [r for r in success if r.speedup < 1.0]

        print(f"\nAvg speedup: {avg_speedup:.2f}x")
        print(f"Wins (>=1.1x): {len(wins)}")
        print(f"Regressions (<1.0x): {len(regressions)}")

        if wins:
            print("\nTop wins:")
            for r in sorted(wins, key=lambda x: x.speedup, reverse=True)[:5]:
                print(f"  {r.query}: {r.speedup:.2f}x ({', '.join(r.transforms)})")

    # Save final report
    report = {
        **config,
        "total_elapsed_seconds": total_elapsed,
        "summary": {
            "total": len(results),
            "success": len(success),
            "failed": len(failed),
            "errors": len(errors),
            "avg_speedup": sum(r.speedup for r in success) / len(success) if success else 0,
        },
        "results": [r.to_dict() for r in results],
    }

    (output_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    # Also create summary.txt for easy reading
    summary_lines = [
        f"MCTS Benchmark Report",
        f"=" * 40,
        f"Date: {timestamp}",
        f"Model: {args.model}",
        f"Iterations: {args.iterations}",
        f"MCTS Mode: Sequential (true MCTS learning)",
        f"Full SQL Logging: Enabled",
        "",
        f"Results: {len(success)}/{len(results)} passed",
        f"Avg speedup: {report['summary']['avg_speedup']:.2f}x",
        "",
        "Per-query results:",
    ]

    for r in results:
        if r.status == "success":
            summary_lines.append(f"  {r.query}: {r.speedup:.2f}x ✓ ({r.iterations} iters, {', '.join(r.transforms)})")
        elif r.status == "failed":
            summary_lines.append(f"  {r.query}: FAILED ({r.iterations} iters)")
        else:
            summary_lines.append(f"  {r.query}: ERROR - {r.error}")

    summary_lines.extend([
        "",
        "Output files per query:",
        "  original.sql       - Original query",
        "  optimized.sql      - Best optimized query",
        "  result.json        - Summary result",
        "  detailed_log.json  - Full MCTS log with all attempts",
        "  attempt_summary.json - Summary by transform type",
        "  tree.json          - MCTS tree structure",
        "  selections.json    - UCT selection decisions",
        "  attempts/          - Individual transform attempts with full SQL",
    ])

    (output_dir / "summary.txt").write_text("\n".join(summary_lines), encoding="utf-8")

    print(f"\nReport saved: {output_dir / 'report.json'}")
    print(f"Summary saved: {output_dir / 'summary.txt'}")
    print(f"\nPer-query detailed logs: {output_dir}/<query>/detailed_log.json")


if __name__ == "__main__":
    main()
