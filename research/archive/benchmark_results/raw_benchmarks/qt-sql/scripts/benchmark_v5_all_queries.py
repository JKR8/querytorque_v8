#!/usr/bin/env python3
"""
V5 Benchmark - All 99 TPC-DS queries with 20 workers each

- Runs 20 concurrent workers per query
- Validates on 1% sample DB only
- Saves all generations for all queries
- Creates CSV summary of results
- Handles interruptions gracefully

Usage:
    export DEEPSEEK_API_KEY=your_key_here
    python3 research/benchmarks/qt-sql/scripts/benchmark_v5_all_queries.py [--output-csv results.csv] [--start-from 1]
"""

import sys
import csv
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add packages to path
project_root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))


def create_benchmark_dir() -> Path:
    """Create timestamped benchmark directory."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    benchmark_dir = project_root / "research" / "experiments" / "v5_benchmark_20workers" / f"run_{timestamp}"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    return benchmark_dir


def create_query_dir(benchmark_dir: Path, query_num: int) -> Path:
    """Create directory for specific query."""
    query_dir = benchmark_dir / f"q{query_num}"
    query_dir.mkdir(exist_ok=True)
    return query_dir


def worker_call(
    worker_id: int,
    sql: str,
    base_prompt: str,
    plan_summary: str,
    examples: list,
    sample_db: str,
) -> dict:
    """Single worker: make LLM call + validate on sample DB (with 1 retry on failure)."""
    from qt_sql.optimization.dag_v3 import build_prompt_with_examples
    from qt_sql.optimization.dag_v2 import DagV2Pipeline
    from qt_sql.validation.sql_validator import SQLValidator
    import dspy

    try:
        # Build prompt with examples
        full_prompt = build_prompt_with_examples(base_prompt, examples, plan_summary, "")

        # Call LLM
        lm = dspy.settings.lm
        response = lm(full_prompt)

        if isinstance(response, list) and len(response) > 0:
            response_text = str(response[0])
        elif hasattr(response, "text"):
            response_text = response.text
        else:
            response_text = str(response)

        # Apply rewrites
        pipeline = DagV2Pipeline(sql)
        optimized_sql = pipeline.apply_response(response_text)

        # Validate on sample DB
        validator = SQLValidator(database=sample_db)
        val_result = validator.validate(sql, optimized_sql)

        # RETRY LOGIC: If validation failed, try once more with error feedback
        if val_result.status.value != "pass":
            error = val_result.errors[0] if val_result.errors else "Validation failed"

            # Build retry prompt with error feedback
            history = (
                "## Previous Attempt (FAILED)\n\n"
                f"Failure reason: {error}\n\n"
                "Previous rewrites:\n"
                f"```\n{response_text}\n```\n\n"
                "Try a DIFFERENT approach."
            )

            retry_prompt = build_prompt_with_examples(base_prompt, examples, plan_summary, history)

            # Retry LLM call
            retry_response = lm(retry_prompt)

            if isinstance(retry_response, list) and len(retry_response) > 0:
                retry_response_text = str(retry_response[0])
            elif hasattr(retry_response, "text"):
                retry_response_text = retry_response.text
            else:
                retry_response_text = str(retry_response)

            # Apply retry rewrites
            retry_optimized_sql = pipeline.apply_response(retry_response_text)

            # Validate retry
            retry_val_result = validator.validate(sql, retry_optimized_sql)

            # Use retry result
            return {
                "worker_id": worker_id,
                "optimized_sql": retry_optimized_sql,
                "status": retry_val_result.status.value,
                "speedup": retry_val_result.speedup,
                "error": retry_val_result.errors[0] if retry_val_result.errors else None,
                "original_time": retry_val_result.original_time,
                "optimized_time": retry_val_result.optimized_time,
                "prompt": retry_prompt,
                "response": retry_response_text,
                "retried": True,
                "first_error": error,
            }

        # First attempt succeeded
        return {
            "worker_id": worker_id,
            "optimized_sql": optimized_sql,
            "status": val_result.status.value,
            "speedup": val_result.speedup,
            "error": val_result.errors[0] if val_result.errors else None,
            "original_time": val_result.original_time,
            "optimized_time": val_result.optimized_time,
            "prompt": full_prompt,
            "response": response_text,
            "retried": False,
        }
    except Exception as e:
        return {
            "worker_id": worker_id,
            "optimized_sql": "",
            "status": "error",
            "speedup": 0.0,
            "error": str(e),
            "original_time": None,
            "optimized_time": None,
            "prompt": "",
            "response": "",
            "retried": False,
        }


def save_generation(query_dir: Path, worker_id: int, result: dict):
    """Save individual generation."""
    gen_dir = query_dir / f"gen_{worker_id:02d}"
    gen_dir.mkdir(exist_ok=True)

    # Save SQL
    (gen_dir / "optimized.sql").write_text(result["optimized_sql"])

    # Save validation
    validation = {
        "worker_id": worker_id,
        "status": result["status"],
        "speedup": result["speedup"],
        "error": result["error"],
        "original_time": result.get("original_time"),
        "optimized_time": result.get("optimized_time"),
        "retried": result.get("retried", False),
        "first_error": result.get("first_error"),
    }
    (gen_dir / "validation.json").write_text(json.dumps(validation, indent=2))

    # Save prompt and response
    (gen_dir / "prompt.txt").write_text(result["prompt"])
    (gen_dir / "response.txt").write_text(result["response"])


def load_query(query_num: int) -> str:
    """Load TPC-DS query."""
    queries_dir = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
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


def process_query(query_num: int, benchmark_dir: Path, sample_db: str) -> dict:
    """Process single query with 20 workers."""
    print(f"\n{'='*70}")
    print(f"Query {query_num}")
    print(f"{'='*70}")

    query_dir = create_query_dir(benchmark_dir, query_num)
    start_time = time.time()

    try:
        # Load query
        sql = load_query(query_num)
        (query_dir / "original.sql").write_text(sql)

        # Import utilities
        from qt_sql.optimization.adaptive_rewriter_v5 import (
            _get_plan_context,
            _build_base_prompt,
            _split_example_batches,
        )
        from qt_sql.optimization.dag_v3 import get_matching_examples
        import dspy

        # Configure LM if needed
        if dspy.settings.lm is None:
            from qt_sql.optimization.dspy_optimizer import configure_lm
            configure_lm(provider="deepseek")

        # Get plan
        plan_summary, plan_text, plan_json = _get_plan_context(sample_db, sql)
        (query_dir / "plan_summary.txt").write_text(plan_summary)

        # Build prompt
        base_prompt = _build_base_prompt(sql, plan_json)

        # Get examples
        all_examples = get_matching_examples(sql)
        batches = _split_example_batches(all_examples, batch_size=3)

        # Create 20 example sets
        example_sets = []
        for i in range(20):
            if i < len(batches):
                example_sets.append(batches[i])
            else:
                batch_idx = i % len(batches) if batches else 0
                example_sets.append(batches[batch_idx] if batches else [])

        print(f"Running 20 workers...")

        # Run workers
        results = []
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = []
            for i in range(20):
                futures.append(pool.submit(
                    worker_call,
                    i + 1,
                    sql,
                    base_prompt,
                    plan_summary,
                    example_sets[i],
                    sample_db,
                ))

            # Collect and save
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    save_generation(query_dir, result["worker_id"], result)

                    status = "✅" if result["status"] == "pass" else "❌"
                    retry_mark = " (retry)" if result.get("retried") else ""
                    print(f"  {status} Gen {result['worker_id']:02d}: {result['status']}, {result['speedup']:.2f}x{retry_mark}")
                except Exception as e:
                    print(f"  ❌ Worker failed: {e}")

        elapsed = time.time() - start_time

        # Count valid
        valid = [r for r in results if r["status"] == "pass"]
        failed = [r for r in results if r["status"] != "pass"]

        # Save summary
        summary = {
            "query_number": query_num,
            "elapsed_seconds": round(elapsed, 2),
            "total_workers": 20,
            "completed": len(results),
            "valid_count": len(valid),
            "failed_count": len(failed),
            "valid_workers": [r["worker_id"] for r in valid],
            "speedups": {r["worker_id"]: round(r["speedup"], 2) for r in valid},
            "best_speedup": round(max([r["speedup"] for r in valid], default=0.0), 2),
            "best_worker": max(valid, key=lambda x: x["speedup"])["worker_id"] if valid else None,
            "avg_speedup": round(sum(r["speedup"] for r in valid) / len(valid), 2) if valid else 0.0,
        }
        (query_dir / "summary.json").write_text(json.dumps(summary, indent=2))

        print(f"\n✅ Q{query_num}: {len(valid)}/20 valid, best={summary['best_speedup']:.2f}x, elapsed={elapsed:.1f}s")

        return {
            "query": query_num,
            "status": "success",
            "elapsed": round(elapsed, 2),
            "completed": len(results),
            "valid": len(valid),
            "failed": len(failed),
            "best_speedup": summary["best_speedup"],
            "best_worker": summary["best_worker"],
            "avg_speedup": summary["avg_speedup"],
        }

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ Q{query_num} failed: {e}")

        return {
            "query": query_num,
            "status": "error",
            "elapsed": round(elapsed, 2),
            "completed": 0,
            "valid": 0,
            "failed": 0,
            "best_speedup": 0.0,
            "best_worker": None,
            "avg_speedup": 0.0,
            "error": str(e),
        }


def main():
    parser = argparse.ArgumentParser(description="Benchmark all 99 TPC-DS queries with 20 workers each")
    parser.add_argument("--output-csv", help="Output CSV file path")
    parser.add_argument("--start-from", type=int, default=1, help="Start from query number")
    parser.add_argument("--end-at", type=int, default=99, help="End at query number")
    parser.add_argument("--sample-db", default="/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb")

    args = parser.parse_args()

    print("="*70)
    print("V5 Benchmark - All Queries - 20 Workers Each")
    print("="*70)
    print()

    # Create benchmark directory
    benchmark_dir = create_benchmark_dir()
    print(f"Benchmark directory: {benchmark_dir}")
    print(f"Sample DB: {args.sample_db}")
    print(f"Queries: {args.start_from}-{args.end_at}")
    print()

    # Determine CSV output
    if args.output_csv:
        csv_path = Path(args.output_csv)
    else:
        csv_path = benchmark_dir / "results.csv"

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Save config
    config = {
        "timestamp": datetime.now().isoformat(),
        "sample_db": args.sample_db,
        "num_workers": 20,
        "start_query": args.start_from,
        "end_query": args.end_at,
    }
    (benchmark_dir / "config.json").write_text(json.dumps(config, indent=2))

    # CSV setup
    fieldnames = [
        "query",
        "status",
        "elapsed",
        "completed",
        "valid",
        "failed",
        "best_speedup",
        "best_worker",
        "avg_speedup",
    ]

    all_results = []
    start_benchmark = time.time()

    try:
        with csv_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()

            for q in range(args.start_from, args.end_at + 1):
                result = process_query(q, benchmark_dir, args.sample_db)
                all_results.append(result)

                # Write immediately
                writer.writerow(result)
                f.flush()

        elapsed_benchmark = time.time() - start_benchmark

        # Final summary
        print("\n" + "="*70)
        print("BENCHMARK COMPLETE")
        print("="*70)
        print()

        total_queries = len(all_results)
        successful = [r for r in all_results if r["status"] == "success"]
        errored = [r for r in all_results if r["status"] == "error"]

        total_valid = sum(r["valid"] for r in successful)
        total_workers = total_queries * 20

        print(f"Total queries: {total_queries}")
        print(f"Successful: {len(successful)}")
        print(f"Errors: {len(errored)}")
        print()
        print(f"Total workers: {total_workers}")
        print(f"Valid generations: {total_valid}/{total_workers} ({total_valid/total_workers*100:.1f}%)")
        print()

        if successful:
            best_overall = max(successful, key=lambda x: x["best_speedup"])
            avg_best = sum(r["best_speedup"] for r in successful) / len(successful)

            print(f"Best speedup overall: {best_overall['best_speedup']:.2f}x (Q{best_overall['query']})")
            print(f"Average best speedup: {avg_best:.2f}x")
            print()

        print(f"Total time: {elapsed_benchmark/3600:.1f}h")
        print()
        print(f"Results CSV: {csv_path}")
        print(f"Full outputs: {benchmark_dir}")
        print()

        # Save final summary
        final_summary = {
            "total_queries": total_queries,
            "successful": len(successful),
            "errored": len(errored),
            "total_workers": total_workers,
            "total_valid": total_valid,
            "valid_rate": round(total_valid/total_workers*100, 1) if total_workers > 0 else 0,
            "best_overall": best_overall if successful else None,
            "avg_best_speedup": round(avg_best, 2) if successful else 0,
            "elapsed_hours": round(elapsed_benchmark/3600, 2),
        }
        (benchmark_dir / "final_summary.json").write_text(json.dumps(final_summary, indent=2))

    except KeyboardInterrupt:
        print("\n" + "="*70)
        print("INTERRUPTED")
        print("="*70)
        print()
        print(f"Completed queries: {len(all_results)}")
        print(f"Partial results saved to: {csv_path}")
        print(f"Full outputs: {benchmark_dir}")
        print()
        return 130

    except Exception as e:
        print("\n" + "="*70)
        print("ERROR")
        print("="*70)
        print()
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        print(f"Partial results saved to: {csv_path}")
        print()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
