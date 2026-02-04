#!/usr/bin/env python3
"""
Benchmark script for AdaptiveRewriter.

Runs the DAG-based LLM optimizer with history tracking on TPC-DS queries.

Features:
- DAG-based node rewrites (not full SQL replacement)
- Full history of attempts with compact summaries
- Rich error categorization
- Best-so-far tracking
- DSPy signatures for structured LLM output

Usage:
    # Single query
    python scripts/run_adaptive_benchmark.py --query 1

    # Multiple queries
    python scripts/run_adaptive_benchmark.py --queries 1,15,92

    # All queries
    python scripts/run_adaptive_benchmark.py --all

    # With custom settings
    python scripts/run_adaptive_benchmark.py --query 1 --iterations 10 --target 3.0
"""

import argparse
import json
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# Add package to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from qt_sql.optimization.adaptive_rewriter import (
    DSPY_AVAILABLE,
    AdaptiveRewriter,
    RewriteResult,
)

if not DSPY_AVAILABLE:
    print("ERROR: dspy-ai is required. Install with: pip install dspy-ai")
    sys.exit(1)


# Default paths
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
FULL_DB = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
QUERIES_DIR = "/mnt/d/TPC-DS/queries_duckdb_converted"


def load_query(query_num: int) -> str:
    """Load a TPC-DS query."""
    # Try different naming conventions
    patterns = [
        f"query_{query_num}.sql",      # query_1.sql
        f"query{query_num:02d}.sql",   # query01.sql
        f"query{query_num}.sql",       # query1.sql
    ]
    for pattern in patterns:
        query_path = Path(QUERIES_DIR) / pattern
        if query_path.exists():
            return query_path.read_text()
    raise FileNotFoundError(f"Query {query_num} not found in {QUERIES_DIR}")


def run_query_optimization(
    query_num: int,
    sample_db: str,
    full_db: str,
    provider: str,
    max_iterations: int,
    target_speedup: float,
    output_dir: Path,
) -> dict:
    """Run optimization on a single query."""
    import time
    import duckdb

    try:
        # Load query
        sql = load_query(query_num)

        # Phase 1: Optimize on sample DB
        query_log_dir = output_dir / f"q{query_num}"
        rewriter = AdaptiveRewriter(
            db_path=sample_db,
            provider=provider,
            max_iterations=max_iterations,
            target_speedup=target_speedup,
            benchmark_runs=5,
            log_dir=str(query_log_dir),
        )

        result = rewriter.optimize(sql)

        # Phase 2: Validate on full DB
        if result.valid and result.optimized_sql != sql:
            try:
                conn = duckdb.connect(full_db, read_only=True)

                # Check semantic correctness
                orig_result = conn.execute(sql).fetchall()
                opt_result = conn.execute(result.optimized_sql).fetchall()

                if set(tuple(r) for r in orig_result) != set(tuple(r) for r in opt_result):
                    return {
                        "query": query_num,
                        "status": "validation_failed",
                        "error": f"Row mismatch on full DB: {len(orig_result)} vs {len(opt_result)}",
                        "sample_speedup": result.speedup,
                        "full_speedup": 0.0,
                        "attempts": result.total_attempts,
                        "elapsed_s": result.elapsed_time,
                    }

                # Measure full DB timing (5 runs, drop min/max)
                orig_times = []
                opt_times = []
                for _ in range(5):
                    start = time.perf_counter()
                    conn.execute(sql).fetchall()
                    orig_times.append((time.perf_counter() - start) * 1000)

                    start = time.perf_counter()
                    conn.execute(result.optimized_sql).fetchall()
                    opt_times.append((time.perf_counter() - start) * 1000)

                conn.close()

                orig_times.sort()
                opt_times.sort()
                orig_avg = sum(orig_times[1:-1]) / 3
                opt_avg = sum(opt_times[1:-1]) / 3
                full_speedup = orig_avg / opt_avg if opt_avg > 0 else 1.0

                return {
                    "query": query_num,
                    "status": "pass" if full_speedup >= 1.0 else "regression",
                    "sample_speedup": result.speedup,
                    "full_speedup": full_speedup,
                    "original_ms": orig_avg,
                    "optimized_ms": opt_avg,
                    "attempts": result.total_attempts,
                    "successful_attempts": result.successful_attempts,
                    "elapsed_s": result.elapsed_time,
                    "rewrites": result.rewrites,  # DAG nodes that were rewritten
                    "attempt_history": [
                        {
                            "num": a.attempt_num,
                            "strategy": a.strategy,
                            "changes": a.changes,
                            "nodes_rewritten": a.nodes_rewritten,
                            "result": a.result,
                            "speedup": a.speedup,
                        }
                        for a in result.history.attempts
                    ],
                }

            except Exception as e:
                return {
                    "query": query_num,
                    "status": "error",
                    "error": f"Full DB validation failed: {e}",
                    "sample_speedup": result.speedup,
                    "attempts": result.total_attempts,
                }

        else:
            return {
                "query": query_num,
                "status": "no_improvement",
                "sample_speedup": result.speedup,
                "attempts": result.total_attempts,
                "elapsed_s": result.elapsed_time,
                "attempt_history": [
                    {
                        "num": a.attempt_num,
                        "strategy": a.strategy,
                        "changes": a.changes,
                        "nodes_rewritten": a.nodes_rewritten,
                        "result": a.result,
                        "speedup": a.speedup,
                        "error": a.error.message if a.error else None,
                    }
                    for a in result.history.attempts
                ],
            }

    except Exception as e:
        return {
            "query": query_num,
            "status": "error",
            "error": str(e),
        }


def main():
    # Optional logging control via LOGLEVEL env (e.g., INFO, DEBUG)
    loglevel = os.getenv("LOGLEVEL")
    if loglevel:
        level = getattr(logging, loglevel.upper(), logging.INFO)
        logging.basicConfig(level=level, format="%(message)s")

    parser = argparse.ArgumentParser(description="Benchmark AdaptiveRewriter")
    parser.add_argument("--query", type=int, help="Single query number")
    parser.add_argument("--queries", type=str, help="Comma-separated query numbers")
    parser.add_argument("--all", action="store_true", help="Run all 99 queries")
    parser.add_argument("--provider", default="deepseek", help="LLM provider")
    parser.add_argument("--iterations", type=int, default=5, help="Max iterations per query")
    parser.add_argument("--target", type=float, default=2.0, help="Target speedup")
    parser.add_argument("--workers", type=int, default=1, help="Parallel workers")
    parser.add_argument("--sample-db", default=SAMPLE_DB, help="Sample DB path")
    parser.add_argument("--full-db", default=FULL_DB, help="Full DB path")
    parser.add_argument("--output-dir", default=None, help="Output directory")

    args = parser.parse_args()

    # Determine queries to run
    if args.query:
        queries = [args.query]
    elif args.queries:
        queries = [int(q.strip()) for q in args.queries.split(",")]
    elif args.all:
        queries = list(range(1, 100))
    else:
        print("Must specify --query, --queries, or --all")
        sys.exit(1)

    # Create output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent.parent.parent.parent / "research" / "experiments" / "adaptive_runs" / f"{args.provider}_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"AdaptiveRewriter Benchmark")
    print(f"=" * 60)
    print(f"Provider: {args.provider}")
    print(f"Max iterations: {args.iterations}")
    print(f"Target speedup: {args.target}x")
    print(f"Queries: {len(queries)}")
    print(f"Workers: {args.workers}")
    print(f"Output: {output_dir}")
    print()

    results = []
    start_time = datetime.now()

    if args.workers == 1:
        # Sequential execution
        for i, q in enumerate(queries, 1):
            print(f"[{i}/{len(queries)}] Query {q}...")
            result = run_query_optimization(
                q, args.sample_db, args.full_db,
                args.provider, args.iterations, args.target,
                output_dir
            )
            results.append(result)

            status = result["status"]
            if status == "pass":
                print(f"  PASS: {result.get('full_speedup', 0):.2f}x (sample: {result.get('sample_speedup', 0):.2f}x)")
            elif status == "regression":
                print(f"  REGRESSION: {result.get('full_speedup', 0):.2f}x")
            elif status == "no_improvement":
                print(f"  NO IMPROVEMENT after {result.get('attempts', 0)} attempts")
            else:
                print(f"  {status.upper()}: {result.get('error', 'unknown')}")
    else:
        # Parallel execution
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    run_query_optimization,
                    q, args.sample_db, args.full_db,
                    args.provider, args.iterations, args.target,
                    output_dir
                ): q
                for q in queries
            }

            for i, future in enumerate(as_completed(futures), 1):
                q = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"[{i}/{len(queries)}] Q{q}: {result['status']} ({result.get('full_speedup', result.get('sample_speedup', 0)):.2f}x)")
                except Exception as e:
                    results.append({"query": q, "status": "error", "error": str(e)})
                    print(f"[{i}/{len(queries)}] Q{q}: ERROR - {e}")

    elapsed = (datetime.now() - start_time).total_seconds() / 60

    # Calculate summary
    passed = [r for r in results if r["status"] == "pass"]
    failed = [r for r in results if r["status"] in ("validation_failed", "regression")]
    errors = [r for r in results if r["status"] == "error"]
    no_improvement = [r for r in results if r["status"] == "no_improvement"]

    avg_speedup = sum(r.get("full_speedup", 0) for r in passed) / len(passed) if passed else 0

    # Save summary
    summary = {
        "timestamp": timestamp,
        "provider": args.provider,
        "model": "adaptive_rewriter",
        "max_iterations": args.iterations,
        "target_speedup": args.target,
        "sample_db": args.sample_db,
        "full_db": args.full_db,
        "queries": queries,
        "total_time_minutes": elapsed,
        "passed": len(passed),
        "failed": len(failed),
        "errors": len(errors),
        "no_improvement": len(no_improvement),
        "avg_speedup": avg_speedup,
        "results": results,
    }

    summary_path = output_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    # Print summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total time: {elapsed:.1f} minutes")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")
    print(f"Errors: {len(errors)}")
    print(f"No improvement: {len(no_improvement)}")
    print(f"Average speedup (passed): {avg_speedup:.2f}x")
    print()

    if passed:
        print("Top speedups:")
        top = sorted(passed, key=lambda r: r.get("full_speedup", 0), reverse=True)[:10]
        for r in top:
            print(f"  Q{r['query']}: {r.get('full_speedup', 0):.2f}x")

    print(f"\nResults saved to: {summary_path}")


if __name__ == "__main__":
    main()
