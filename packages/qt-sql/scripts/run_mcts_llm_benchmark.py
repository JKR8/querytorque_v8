#!/usr/bin/env python3
"""Run MCTS + DAG + LLM ranking benchmark on all TPC-DS queries.

Benchmark methodology:
- MCTS uses SAMPLE DB during optimization (fast iteration)
- 5-run timing: discard min & max, average middle 3 (robust)
- Full DB validation done sequentially after all MCTS complete
- Clear separation of sample vs full DB results

Usage:
    python scripts/run_mcts_llm_benchmark.py --all
    python scripts/run_mcts_llm_benchmark.py --queries 1,15,92
    python scripts/run_mcts_llm_benchmark.py --range 1-20
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

# Add package paths
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-sql"))
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-shared"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Paths
TPCDS_QUERIES = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
SAMPLE_DATABASE = Path("/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb")
FULL_DATABASE = Path("/mnt/d/TPC-DS/tpcds_sf100.duckdb")
OUTPUT_BASE = PROJECT_ROOT / "research" / "experiments" / "mcts_llm_runs"


@dataclass
class QueryResult:
    """Result for a single query optimization."""
    query: int
    status: str  # pass, fail, error
    # Sample DB results (from MCTS)
    sample_original_ms: float = 0.0
    sample_optimized_ms: float = 0.0
    sample_speedup: float = 1.0
    # Full DB results (from validation)
    full_original_ms: float = 0.0
    full_optimized_ms: float = 0.0
    full_speedup: float = 1.0
    # Metadata
    transforms: list = None
    iterations: int = 0
    mcts_elapsed_s: float = 0.0
    error: Optional[str] = None
    optimized_sql: Optional[str] = None

    def __post_init__(self):
        if self.transforms is None:
            self.transforms = []

    def to_dict(self):
        d = asdict(self)
        # Don't include full SQL in summary
        d.pop('optimized_sql', None)
        return d


def time_query_5runs(con, sql: str) -> float:
    """Run query 5 times, discard min & max, average middle 3.

    This is the robust "pro" method that eliminates outliers.

    Args:
        con: DuckDB connection
        sql: SQL query to time

    Returns:
        Average time in milliseconds of middle 3 runs
    """
    times = []
    for i in range(5):
        start = time.perf_counter()
        con.execute(sql).fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000
        times.append(elapsed_ms)

    # Sort and discard min & max
    times.sort()
    middle_3 = times[1:4]  # indices 1, 2, 3 (discard 0 and 4)
    return sum(middle_3) / 3


def validate_equivalence(con, original_sql: str, optimized_sql: str) -> tuple[bool, str]:
    """Check if queries return equivalent results.

    Returns:
        (is_equivalent, error_message)
    """
    try:
        orig_result = con.execute(original_sql).fetchall()
        opt_result = con.execute(optimized_sql).fetchall()

        if len(orig_result) != len(opt_result):
            return False, f"Row count mismatch: {len(orig_result)} vs {len(opt_result)}"

        # Compare checksums (simple value comparison)
        if orig_result != opt_result:
            return False, "Value mismatch detected"

        return True, ""
    except Exception as e:
        return False, f"Execution error: {str(e)}"


def run_single_query_mcts(
    query_num: int,
    output_dir: Path,
    max_iterations: int = 20,
) -> QueryResult:
    """Run MCTS optimization on a single query using SAMPLE DB.

    This runs in a subprocess. Full DB validation happens later sequentially.
    """
    import json
    import os
    import sys
    import time
    from pathlib import Path

    # Re-setup environment in subprocess
    PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
    ENV_FILE = PROJECT_ROOT / ".env"
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())

    sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-sql"))
    sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-shared"))

    import duckdb
    from qt_sql.optimization.mcts import MCTSSQLOptimizer
    from qt_sql.optimization.mcts.priors import PriorConfig

    # Paths in subprocess
    TPCDS_QUERIES = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
    SAMPLE_DATABASE = Path("/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb")

    query_file = TPCDS_QUERIES / f"query_{query_num}.sql"
    if not query_file.exists():
        return QueryResult(query=query_num, status="error", error=f"File not found: {query_file}")

    sql = query_file.read_text(encoding="utf-8")
    # Remove comments
    sql = '\n'.join(l for l in sql.split('\n') if not l.strip().startswith('--')).strip()

    # Handle multi-statement queries (like q39) - take first statement
    if ';' in sql:
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        if statements:
            sql = statements[0]

    query_dir = output_dir / f"q{query_num}"
    query_dir.mkdir(parents=True, exist_ok=True)
    (query_dir / "original.sql").write_text(sql, encoding="utf-8")

    print(f"Q{query_num}: Starting MCTS on SAMPLE DB...")

    # MCTS config with LLM ranking
    config = PriorConfig(
        use_puct=True,
        use_llm_ranking=True,
        c_puct=2.0,
    )

    mcts_start = time.perf_counter()

    try:
        optimizer = MCTSSQLOptimizer(
            database=str(SAMPLE_DATABASE),  # SAMPLE DB for fast MCTS
            provider='deepseek',
            model='deepseek-chat',
            prior_config=config,
            use_dag_mode=True,
            max_depth=3,
        )

        result = optimizer.optimize(
            query=sql,
            max_iterations=max_iterations,
            early_stop_speedup=2.5,
            convergence_patience=8,
            log_full_sql=True,
        )

        optimizer.close()
        mcts_elapsed = time.perf_counter() - mcts_start

        # Save MCTS result
        (query_dir / "mcts_result.json").write_text(
            json.dumps(result.to_dict(), indent=2, default=str),
            encoding="utf-8"
        )

        if result.optimized_sql and result.optimized_sql != sql:
            (query_dir / "optimized.sql").write_text(result.optimized_sql, encoding="utf-8")

        # Save detailed logs
        if result.detailed_log:
            (query_dir / "detailed_log.json").write_text(
                json.dumps(result.detailed_log, indent=2, default=str),
                encoding="utf-8"
            )

        # Save attempt summary
        if result.attempt_summary:
            (query_dir / "attempt_summary.json").write_text(
                json.dumps(result.attempt_summary, indent=2, default=str),
                encoding="utf-8"
            )

        # Get sample DB timing with 5-run method
        con = duckdb.connect(str(SAMPLE_DATABASE), read_only=True)
        sample_original_ms = time_query_5runs(con, sql)

        if result.optimized_sql and result.optimized_sql != sql:
            sample_optimized_ms = time_query_5runs(con, result.optimized_sql)
        else:
            sample_optimized_ms = sample_original_ms
        con.close()

        sample_speedup = sample_original_ms / sample_optimized_ms if sample_optimized_ms > 0 else 1.0

        # Save sample results
        sample_result = {
            "database": "sample_1pct",
            "timing_method": "5 runs, discard min/max, avg middle 3",
            "original_ms": round(sample_original_ms, 2),
            "optimized_ms": round(sample_optimized_ms, 2),
            "speedup": round(sample_speedup, 2),
            "transforms": result.transforms_applied,
            "iterations": result.iterations,
            "mcts_elapsed_s": round(mcts_elapsed, 1),
        }
        (query_dir / "sample_result.json").write_text(
            json.dumps(sample_result, indent=2),
            encoding="utf-8"
        )

        print(f"Q{query_num}: SAMPLE {sample_speedup:.2f}x ({result.transforms_applied}) in {result.iterations} iters")

        return QueryResult(
            query=query_num,
            status="mcts_complete",  # Not final - needs full DB validation
            sample_original_ms=round(sample_original_ms, 2),
            sample_optimized_ms=round(sample_optimized_ms, 2),
            sample_speedup=round(sample_speedup, 2),
            transforms=result.transforms_applied,
            iterations=result.iterations,
            mcts_elapsed_s=round(mcts_elapsed, 1),
            optimized_sql=result.optimized_sql,
        )

    except Exception as e:
        import traceback
        error_msg = f"{type(e).__name__}: {str(e)}"
        (query_dir / "error.log").write_text(traceback.format_exc(), encoding="utf-8")
        print(f"Q{query_num}: ERROR - {error_msg[:50]}...")

        return QueryResult(
            query=query_num,
            status="error",
            error=error_msg,
        )


def validate_on_full_db(result: QueryResult, output_dir: Path) -> QueryResult:
    """Validate a single query result on FULL DB.

    This runs sequentially after all MCTS completes.
    """
    import duckdb

    if result.status == "error" or not result.optimized_sql:
        return result

    query_dir = output_dir / f"q{result.query}"
    original_sql_file = query_dir / "original.sql"

    if not original_sql_file.exists():
        result.status = "error"
        result.error = "Original SQL file not found"
        return result

    original_sql = original_sql_file.read_text(encoding="utf-8")
    optimized_sql = result.optimized_sql

    print(f"Q{result.query}: Validating on FULL DB...")

    try:
        con = duckdb.connect(str(FULL_DATABASE), read_only=True)

        # Check equivalence
        is_equiv, equiv_error = validate_equivalence(con, original_sql, optimized_sql)

        if not is_equiv:
            con.close()
            result.status = "fail"
            result.error = equiv_error

            # Save validation failure
            (query_dir / "full_validation.json").write_text(
                json.dumps({"status": "fail", "error": equiv_error}, indent=2),
                encoding="utf-8"
            )
            print(f"Q{result.query}: FULL DB FAIL - {equiv_error}")
            return result

        # Time with 5-run method
        full_original_ms = time_query_5runs(con, original_sql)
        full_optimized_ms = time_query_5runs(con, optimized_sql)
        con.close()

        full_speedup = full_original_ms / full_optimized_ms if full_optimized_ms > 0 else 1.0

        result.full_original_ms = round(full_original_ms, 2)
        result.full_optimized_ms = round(full_optimized_ms, 2)
        result.full_speedup = round(full_speedup, 2)
        result.status = "pass"

        # Save full validation result
        validation_result = {
            "status": "pass",
            "database": "full_sf100",
            "timing_method": "5 runs, discard min/max, avg middle 3",
            "original_ms": round(full_original_ms, 2),
            "optimized_ms": round(full_optimized_ms, 2),
            "speedup": round(full_speedup, 2),
            "sample_speedup": result.sample_speedup,
            "sample_vs_full_diff": round(result.sample_speedup - full_speedup, 2),
        }
        (query_dir / "full_validation.json").write_text(
            json.dumps(validation_result, indent=2),
            encoding="utf-8"
        )

        print(f"Q{result.query}: FULL DB {full_speedup:.2f}x (sample was {result.sample_speedup:.2f}x)")
        return result

    except Exception as e:
        result.status = "error"
        result.error = f"Full DB validation error: {str(e)}"
        print(f"Q{result.query}: FULL DB ERROR - {str(e)[:50]}...")
        return result


def main():
    parser = argparse.ArgumentParser(description="Run MCTS+LLM benchmark on TPC-DS")
    parser.add_argument("--all", action="store_true", help="Run all 99 queries")
    parser.add_argument("--queries", type=str, help="Comma-separated query numbers (e.g., 1,15,92)")
    parser.add_argument("--range", type=str, help="Query range (e.g., 1-20)")
    parser.add_argument("--iterations", type=int, default=20, help="Max MCTS iterations")
    parser.add_argument("--workers", type=int, default=20, help="Max parallel workers for MCTS")
    parser.add_argument("--output-name", type=str, help="Custom output folder name")
    parser.add_argument("--skip-full-validation", action="store_true", help="Skip full DB validation")
    args = parser.parse_args()

    # Determine queries
    if args.all:
        queries = list(range(1, 100))
    elif args.queries:
        queries = [int(q.strip()) for q in args.queries.split(",")]
    elif args.range:
        start, end = map(int, args.range.split("-"))
        queries = list(range(start, end + 1))
    else:
        print("Error: Specify --all, --queries, or --range")
        sys.exit(1)

    # Setup output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_name = args.output_name or f"deepseek_mcts_llm_{timestamp}"
    output_dir = OUTPUT_BASE / output_name
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save config
    config = {
        "timestamp": timestamp,
        "provider": "deepseek",
        "model": "deepseek-chat",
        "mode": "MCTS + DAG + LLM ranking",
        "sample_db": str(SAMPLE_DATABASE),
        "full_db": str(FULL_DATABASE),
        "max_iterations": args.iterations,
        "max_workers": args.workers,
        "timing_method": "5 runs, discard min/max, avg middle 3",
        "queries": queries,
    }
    (output_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")

    print("=" * 70)
    print("MCTS + DAG + LLM PUCT Benchmark")
    print("=" * 70)
    print(f"Provider: DeepSeek (deepseek-chat)")
    print(f"MCTS on: SAMPLE DB (1%)")
    print(f"Validation on: FULL DB (sequential queue)")
    print(f"Timing: 5 runs, discard min/max, avg middle 3")
    print(f"Iterations: {args.iterations}")
    print(f"Workers: {args.workers}")
    print(f"Queries: {len(queries)} ({min(queries)}-{max(queries)})")
    print(f"Output: {output_dir}")
    print("=" * 70)
    print()

    # Phase 1: Run MCTS on SAMPLE DB (parallel)
    print("=" * 70)
    print("PHASE 1: MCTS Optimization (SAMPLE DB, parallel)")
    print("=" * 70)

    results = []
    mcts_start = time.perf_counter()

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(run_single_query_mcts, q, output_dir, args.iterations): q
            for q in queries
        }

        for future in as_completed(futures):
            query_num = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Q{query_num}: EXCEPTION - {e}")
                results.append(QueryResult(query=query_num, status="error", error=str(e)))

            # Save intermediate results
            (output_dir / "mcts_results.json").write_text(
                json.dumps([r.to_dict() for r in sorted(results, key=lambda x: x.query)], indent=2),
                encoding="utf-8"
            )

    mcts_elapsed = time.perf_counter() - mcts_start

    # Phase 1 Summary
    print()
    print("=" * 70)
    print("PHASE 1 COMPLETE: MCTS on SAMPLE DB")
    print("=" * 70)
    mcts_complete = [r for r in results if r.status == "mcts_complete"]
    mcts_errors = [r for r in results if r.status == "error"]
    print(f"Completed: {len(mcts_complete)}")
    print(f"Errors: {len(mcts_errors)}")
    print(f"Time: {mcts_elapsed/60:.1f} minutes")

    if mcts_complete:
        avg_sample_speedup = sum(r.sample_speedup for r in mcts_complete) / len(mcts_complete)
        sample_wins = [r for r in mcts_complete if r.sample_speedup >= 1.2]
        print(f"Avg sample speedup: {avg_sample_speedup:.2f}x")
        print(f"Sample wins (>=1.2x): {len(sample_wins)}")

        print("\nTop sample speedups:")
        for r in sorted(mcts_complete, key=lambda x: x.sample_speedup, reverse=True)[:10]:
            print(f"  Q{r.query}: {r.sample_speedup:.2f}x ({r.transforms})")
    print()

    # Phase 2: Validate on FULL DB (sequential)
    if not args.skip_full_validation:
        print("=" * 70)
        print("PHASE 2: Full DB Validation (sequential)")
        print("=" * 70)

        validation_start = time.perf_counter()
        validated_results = []

        for i, result in enumerate(sorted(results, key=lambda x: x.query)):
            if result.status == "mcts_complete":
                validated = validate_on_full_db(result, output_dir)
                validated_results.append(validated)
            else:
                validated_results.append(result)

            # Save progress
            (output_dir / "results.json").write_text(
                json.dumps([r.to_dict() for r in sorted(validated_results, key=lambda x: x.query)], indent=2),
                encoding="utf-8"
            )

        results = validated_results
        validation_elapsed = time.perf_counter() - validation_start

        print()
        print(f"Validation time: {validation_elapsed/60:.1f} minutes")

    # Final Summary
    print()
    print("=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)

    results.sort(key=lambda x: x.query)

    passed = [r for r in results if r.status == "pass"]
    failed = [r for r in results if r.status == "fail"]
    errors = [r for r in results if r.status == "error"]

    print(f"Total: {len(results)}")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")
    print(f"Errors: {len(errors)}")

    if passed:
        avg_sample = sum(r.sample_speedup for r in passed) / len(passed)
        avg_full = sum(r.full_speedup for r in passed) / len(passed)
        wins = [r for r in passed if r.full_speedup >= 1.2]

        print(f"\nAvg sample speedup: {avg_sample:.2f}x")
        print(f"Avg FULL speedup: {avg_full:.2f}x")
        print(f"Wins on FULL (>=1.2x): {len(wins)}")

        if wins:
            print("\nTop FULL DB wins:")
            for r in sorted(wins, key=lambda x: x.full_speedup, reverse=True)[:10]:
                diff = r.sample_speedup - r.full_speedup
                print(f"  Q{r.query}: {r.full_speedup:.2f}x (sample: {r.sample_speedup:.2f}x, diff: {diff:+.2f})")

        # Show sample vs full discrepancies
        print("\nSample vs Full discrepancies (>0.3x diff):")
        discrepancies = [(r, r.sample_speedup - r.full_speedup) for r in passed if abs(r.sample_speedup - r.full_speedup) > 0.3]
        for r, diff in sorted(discrepancies, key=lambda x: abs(x[1]), reverse=True)[:10]:
            print(f"  Q{r.query}: sample {r.sample_speedup:.2f}x vs full {r.full_speedup:.2f}x (diff: {diff:+.2f})")

    # Save final summary
    summary = {
        **config,
        "mcts_time_minutes": round(mcts_elapsed / 60, 1),
        "validation_time_minutes": round((time.perf_counter() - mcts_start - mcts_elapsed) / 60, 1) if not args.skip_full_validation else 0,
        "passed": len(passed),
        "failed": len(failed),
        "errors": len(errors),
        "avg_sample_speedup": round(sum(r.sample_speedup for r in passed) / len(passed), 2) if passed else 0,
        "avg_full_speedup": round(sum(r.full_speedup for r in passed) / len(passed), 2) if passed else 0,
        "results": [r.to_dict() for r in results],
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"\nResults saved: {output_dir}/summary.json")


if __name__ == "__main__":
    main()
