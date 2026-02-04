#!/usr/bin/env python3
"""
V5 Parallel Test - 20 concurrent API calls on one query

- 20 workers making concurrent LLM calls
- Uses v5 JSON (not DSPy)
- Validates on 1% sample DB only (no full DB)
- Saves all 20 generations with validation results
- Incremental saving (nothing lost if interrupted)

Usage:
    export DEEPSEEK_API_KEY=your_key_here
    python3 scripts/test_v5_parallel_20.py [query_number]
"""

import sys
import json
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add packages to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))


def create_output_dir(query_num: int) -> Path:
    """Create timestamped output directory."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = project_root / "research" / "experiments" / "v5_parallel_20" / f"q{query_num}_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def save_generation(output_dir: Path, worker_id: int, result: dict):
    """Save individual generation immediately."""
    gen_dir = output_dir / f"gen_{worker_id:02d}"
    gen_dir.mkdir(exist_ok=True)

    # Save optimized SQL
    (gen_dir / "optimized.sql").write_text(result["optimized_sql"])

    # Save validation result
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

    status_symbol = "✅" if result["status"] == "pass" else "❌"
    retry_mark = " (retry)" if result.get("retried") else ""
    print(f"  {status_symbol} Gen {worker_id:02d}: {result['status']}, {result['speedup']:.2f}x{retry_mark}")


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


def main():
    query_num = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    print("=" * 70)
    print(f"V5 Parallel Test - 20 Workers - Query {query_num}")
    print("=" * 70)
    print()

    # Create output directory
    output_dir = create_output_dir(query_num)
    print(f"Output directory: {output_dir}")
    print()

    # Load query
    print(f"Loading query {query_num}...")
    try:
        sql = load_query(query_num)
        print(f"✅ Query loaded ({len(sql)} chars)")
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return 1
    print()

    # Database path
    sample_db = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
    print(f"Sample DB: {sample_db}")
    print()

    # Save config
    config = {
        "query_number": query_num,
        "sample_db": sample_db,
        "num_workers": 20,
        "timestamp": datetime.now().isoformat(),
        "version": "v5_json",
    }
    (output_dir / "config.json").write_text(json.dumps(config, indent=2))
    (output_dir / "original.sql").write_text(sql)

    # Import v5 utilities
    from qt_sql.optimization.adaptive_rewriter_v5 import (
        _get_plan_context,
        _build_base_prompt,
        _split_example_batches,
    )
    from qt_sql.optimization.dag_v3 import get_matching_examples
    import dspy

    # Configure LM
    if dspy.settings.lm is None:
        from qt_sql.optimization.dspy_optimizer import configure_lm
        configure_lm(provider="deepseek")

    # Get plan context
    print("Analyzing execution plan...")
    plan_summary, plan_text, plan_json = _get_plan_context(sample_db, sql)
    (output_dir / "plan_summary.txt").write_text(plan_summary)
    (output_dir / "plan_full.txt").write_text(plan_text)
    if plan_json:
        (output_dir / "plan.json").write_text(json.dumps(plan_json, indent=2))
    print("✅ Plan analyzed")
    print()

    # Build base prompt
    base_prompt = _build_base_prompt(sql, plan_json)
    (output_dir / "base_prompt.txt").write_text(base_prompt)

    # Get examples
    all_examples = get_matching_examples(sql)
    batches = _split_example_batches(all_examples, batch_size=3)

    # Extend to 20 batches (cycle through examples)
    example_sets = []
    for i in range(20):
        if i < len(batches):
            example_sets.append(batches[i])
        else:
            # Cycle through batches or use empty
            batch_idx = i % len(batches) if batches else 0
            example_sets.append(batches[batch_idx] if batches else [])

    print(f"Running 20 workers in parallel...")
    print(f"Making 20 concurrent LLM API calls...")
    print()

    start_time = time.time()
    results = []

    try:
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

            # Collect results as they complete and save immediately
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                    save_generation(output_dir, result["worker_id"], result)
                except Exception as e:
                    print(f"  ❌ Worker failed: {e}")
                    import traceback
                    traceback.print_exc()

        elapsed = time.time() - start_time

        print()
        print("=" * 70)
        print("RESULTS")
        print("=" * 70)
        print()
        print(f"Elapsed: {elapsed:.1f}s")
        print(f"Completed: {len(results)}/20 workers")
        print()

        # Count valid
        valid = [r for r in results if r["status"] == "pass"]
        failed = [r for r in results if r["status"] != "pass"]

        print(f"✅ Valid: {len(valid)}/20")
        print(f"❌ Failed: {len(failed)}/20")
        print()

        if valid:
            # Sort by speedup
            valid_sorted = sorted(valid, key=lambda x: x["speedup"], reverse=True)

            print("Top 10 Speedups:")
            print("-" * 70)
            for r in valid_sorted[:10]:
                print(f"  Gen {r['worker_id']:02d}: {r['speedup']:.2f}x")
            print()

            best = valid_sorted[0]
            print(f"🏆 Best Generation: #{best['worker_id']} with {best['speedup']:.2f}x speedup")
            print()

            avg_speedup = sum(r["speedup"] for r in valid) / len(valid)
            print(f"Average speedup (valid only): {avg_speedup:.2f}x")
            print()

        # Save summary
        summary = {
            "query_number": query_num,
            "timestamp": datetime.now().isoformat(),
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
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))

        # Human-readable summary
        lines = []
        lines.append("=" * 70)
        lines.append(f"V5 Parallel Test - Query {query_num}")
        lines.append("=" * 70)
        lines.append("")
        lines.append(f"Timestamp: {summary['timestamp']}")
        lines.append(f"Elapsed: {summary['elapsed_seconds']}s")
        lines.append(f"Workers: 20")
        lines.append(f"Completed: {len(results)}/20")
        lines.append("")
        lines.append(f"✅ Valid: {len(valid)}/20 ({len(valid)/20*100:.1f}%)")
        lines.append(f"❌ Failed: {len(failed)}/20 ({len(failed)/20*100:.1f}%)")
        lines.append("")

        if valid:
            lines.append(f"Best speedup: {summary['best_speedup']:.2f}x (gen {summary['best_worker']})")
            lines.append(f"Average speedup: {summary['avg_speedup']:.2f}x")
            lines.append("")
            lines.append("Top 10 Speedups:")
            for r in valid_sorted[:10]:
                lines.append(f"  Gen {r['worker_id']:02d}: {r['speedup']:.2f}x")

        lines.append("")
        lines.append("=" * 70)

        (output_dir / "summary.txt").write_text("\n".join(lines))

        print("=" * 70)
        print(f"✅ All outputs saved to: {output_dir}")
        print("=" * 70)
        print()

        return 0

    except KeyboardInterrupt:
        print()
        print("=" * 70)
        print("⚠️  INTERRUPTED")
        print("=" * 70)
        print()
        elapsed = time.time() - start_time
        print(f"Elapsed before interrupt: {elapsed:.1f}s")
        print(f"Completed: {len(results)}/20 workers")
        print(f"Partial results saved to: {output_dir}")
        print()
        return 130

    except Exception as e:
        print()
        print("=" * 70)
        print("❌ ERROR")
        print("=" * 70)
        print()
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        print(f"Partial results saved to: {output_dir}")
        print()
        return 1


if __name__ == "__main__":
    sys.exit(main())
