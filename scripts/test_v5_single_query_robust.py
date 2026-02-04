#!/usr/bin/env python3
"""
Robust single-query v5 test with incremental output saving.

Saves all worker outputs as they complete, so nothing is lost if interrupted.

Usage:
    export DEEPSEEK_API_KEY=your_key_here
    python3 scripts/test_v5_single_query_robust.py [query_number]

Output directory: research/experiments/v5_test_runs/q{N}_YYYYMMDD_HHMMSS/
"""

import sys
import json
import time
from pathlib import Path
from datetime import datetime
from dataclasses import asdict

# Add packages to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))

from qt_sql.optimization.adaptive_rewriter_v5 import (
    optimize_v5_json_queue,
    CandidateResult,
    FullRunResult,
)


def create_output_dir(query_num: int) -> Path:
    """Create timestamped output directory."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = project_root / "research" / "experiments" / "v5_test_runs" / f"q{query_num}_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def save_candidate(output_dir: Path, candidate: CandidateResult, phase: str = "sample"):
    """Save individual candidate result immediately."""
    worker_dir = output_dir / f"worker_{candidate.worker_id}"
    worker_dir.mkdir(exist_ok=True)

    # Save SQL
    (worker_dir / f"{phase}_optimized.sql").write_text(candidate.optimized_sql)

    # Save metadata
    metadata = {
        "worker_id": candidate.worker_id,
        "phase": phase,
        "status": candidate.status.value,
        "speedup": candidate.speedup,
        "error": candidate.error,
    }
    (worker_dir / f"{phase}_metadata.json").write_text(json.dumps(metadata, indent=2))

    # Save prompt and response
    (worker_dir / f"{phase}_prompt.txt").write_text(candidate.prompt)
    (worker_dir / f"{phase}_response.txt").write_text(candidate.response)

    print(f"  ✅ Saved worker {candidate.worker_id} ({phase}): {candidate.status.value}, {candidate.speedup:.2f}x")


def save_full_result(output_dir: Path, full_result: FullRunResult):
    """Save full DB validation result."""
    worker_dir = output_dir / f"worker_{full_result.sample.worker_id}"

    full_metadata = {
        "worker_id": full_result.sample.worker_id,
        "phase": "full",
        "status": full_result.full_status.value,
        "speedup": full_result.full_speedup,
        "error": full_result.full_error,
        "sample_speedup": full_result.sample.speedup,
    }
    (worker_dir / "full_metadata.json").write_text(json.dumps(full_metadata, indent=2))

    print(f"  ✅ Saved worker {full_result.sample.worker_id} (full): {full_result.full_status.value}, {full_result.full_speedup:.2f}x")


def save_summary(output_dir: Path, valid: list, full_results: list, winner, elapsed: float, query_num: int, sql: str):
    """Save comprehensive summary."""
    summary = {
        "query_number": query_num,
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "original_sql_length": len(sql),
        "sample_phase": {
            "total_workers": 5,
            "valid_count": len(valid),
            "valid_workers": [v.worker_id for v in valid],
            "speedups": {v.worker_id: round(v.speedup, 2) for v in valid},
            "best_speedup": round(max([v.speedup for v in valid], default=0.0), 2),
        },
        "full_phase": {
            "validated_count": len(full_results),
            "validated_workers": [fr.sample.worker_id for fr in full_results],
            "speedups": {
                fr.sample.worker_id: round(fr.full_speedup, 2)
                for fr in full_results
                if fr.full_status.value == "pass"
            },
            "best_speedup": round(
                max([fr.full_speedup for fr in full_results if fr.full_status.value == "pass"], default=0.0),
                2
            ),
        },
        "winner": {
            "found": bool(winner),
            "worker_id": winner.sample.worker_id if winner else None,
            "sample_speedup": round(winner.sample.speedup, 2) if winner else None,
            "full_speedup": round(winner.full_speedup, 2) if winner else None,
        } if winner else {"found": False},
    }

    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    # Human-readable summary
    lines = []
    lines.append("=" * 70)
    lines.append(f"V5 Test - Query {query_num}")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"Timestamp: {summary['timestamp']}")
    lines.append(f"Elapsed: {summary['elapsed_seconds']}s")
    lines.append("")
    lines.append("Sample Phase (1% DB)")
    lines.append("-" * 70)
    lines.append(f"Valid workers: {len(valid)}/5")
    if valid:
        lines.append(f"Best speedup: {summary['sample_phase']['best_speedup']:.2f}x")
        lines.append("Per-worker speedups:")
        for v in sorted(valid, key=lambda x: x.speedup, reverse=True):
            lines.append(f"  Worker {v.worker_id}: {v.speedup:.2f}x ({v.status.value})")
    lines.append("")

    lines.append("Full Phase (SF100)")
    lines.append("-" * 70)
    lines.append(f"Validated: {len(full_results)}/{len(valid)}")
    if full_results:
        lines.append(f"Best speedup: {summary['full_phase']['best_speedup']:.2f}x")
        lines.append("Per-worker speedups:")
        for fr in sorted(full_results, key=lambda x: x.full_speedup, reverse=True):
            if fr.full_status.value == "pass":
                lines.append(f"  Worker {fr.sample.worker_id}: {fr.full_speedup:.2f}x ({fr.full_status.value})")
            else:
                lines.append(f"  Worker {fr.sample.worker_id}: FAILED ({fr.full_status.value})")
    lines.append("")

    if winner:
        lines.append("🏆 Winner Found")
        lines.append("-" * 70)
        lines.append(f"Worker: {winner.sample.worker_id}")
        lines.append(f"Sample speedup: {winner.sample.speedup:.2f}x")
        lines.append(f"Full speedup: {winner.full_speedup:.2f}x")
    else:
        lines.append("❌ No Winner (speedup <2.0x)")

    lines.append("")
    lines.append("=" * 70)

    (output_dir / "summary.txt").write_text("\n".join(lines))

    # Save original SQL
    (output_dir / "original.sql").write_text(sql)


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
    print(f"V5 Robust Test - Query {query_num}")
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

    # Database paths
    sample_db = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
    full_db = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"

    print(f"Sample DB: {sample_db}")
    print(f"Full DB: {full_db}")
    print()

    # Save configuration
    config = {
        "query_number": query_num,
        "sample_db": sample_db,
        "full_db": full_db,
        "max_workers": 5,
        "target_speedup": 2.0,
        "timestamp": datetime.now().isoformat(),
    }
    (output_dir / "config.json").write_text(json.dumps(config, indent=2))

    # Monkey-patch to save results incrementally
    print("Running v5 optimization with incremental saving...")
    print()

    start_time = time.time()

    try:
        # Import the internal worker to intercept results
        from qt_sql.optimization.adaptive_rewriter_v5 import (
            _get_plan_context,
            _build_base_prompt,
            _split_example_batches,
            _worker_json,
            get_matching_examples,
        )
        from qt_sql.validation.sql_validator import SQLValidator
        from concurrent.futures import ThreadPoolExecutor, as_completed
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

        # Build prompt
        base_prompt = _build_base_prompt(sql, plan_json)
        (output_dir / "base_prompt.txt").write_text(base_prompt)

        # Get examples
        examples = get_matching_examples(sql)
        batches = _split_example_batches(examples, batch_size=3)
        coverage_batches = batches[:4]
        while len(coverage_batches) < 4:
            coverage_batches.append([])

        # Run workers with incremental saving
        print("Running 5 workers in parallel on sample DB...")
        tasks = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            # Coverage workers
            for i, batch in enumerate(coverage_batches):
                tasks.append(pool.submit(
                    _worker_json,
                    i + 1,
                    sql,
                    base_prompt,
                    plan_summary,
                    batch,
                    sample_db,
                    True,
                    False,
                    None,
                ))

            # Explore worker
            tasks.append(pool.submit(
                _worker_json,
                5,
                sql,
                base_prompt,
                plan_summary,
                [],
                sample_db,
                True,
                True,
                plan_text,
            ))

            # Collect results as they complete and save immediately
            results = []
            for future in as_completed(tasks):
                try:
                    result = future.result()
                    results.append(result)
                    save_candidate(output_dir, result, "sample")
                except Exception as e:
                    print(f"  ❌ Worker failed: {e}")
                    import traceback
                    traceback.print_exc()

        print()
        print(f"✅ Sample phase complete: {len(results)}/5 workers finished")
        print()

        # Filter valid candidates
        from qt_sql.validation.schemas import ValidationStatus
        valid = [r for r in results if r.status == ValidationStatus.PASS]

        if not valid:
            print("❌ No valid candidates from sample DB")
            elapsed = time.time() - start_time
            save_summary(output_dir, valid, [], None, elapsed, query_num, sql)
            return 0

        print(f"Running full DB validation on {len(valid)} valid candidates...")
        print()

        # Validate on full DB sequentially with incremental saving
        full_validator = SQLValidator(database=full_db)
        full_results = []
        winner = None

        for cand in valid:
            print(f"Validating worker {cand.worker_id} on full DB...")
            try:
                full = full_validator.validate(sql, cand.optimized_sql)
                full_err = full.errors[0] if full.errors else None

                from qt_sql.optimization.adaptive_rewriter_v5 import FullRunResult
                full_result = FullRunResult(
                    sample=cand,
                    full_status=full.status,
                    full_speedup=full.speedup,
                    full_error=full_err,
                )
                full_results.append(full_result)
                save_full_result(output_dir, full_result)

                # Check for winner
                if full.status == ValidationStatus.PASS and full.speedup >= 2.0:
                    winner = full_result
                    print(f"  🏆 Winner found! Breaking early.")
                    break

            except Exception as e:
                print(f"  ❌ Validation failed: {e}")
                import traceback
                traceback.print_exc()

        print()
        elapsed = time.time() - start_time

        # Save final summary
        save_summary(output_dir, valid, full_results, winner, elapsed, query_num, sql)

        # Print results
        print("=" * 70)
        print("RESULTS")
        print("=" * 70)
        print()
        print(f"Elapsed: {elapsed:.1f}s")
        print(f"Output: {output_dir}")
        print()
        print(f"Sample valid: {len(valid)}/5")
        if valid:
            print(f"  Best speedup: {max([v.speedup for v in valid]):.2f}x")
        print()
        print(f"Full validated: {len(full_results)}/{len(valid)}")
        if full_results:
            best_full = max([fr.full_speedup for fr in full_results if fr.full_status == ValidationStatus.PASS], default=0.0)
            print(f"  Best speedup: {best_full:.2f}x")
        print()

        if winner:
            print("🏆 WINNER FOUND")
            print(f"   Worker: {winner.sample.worker_id}")
            print(f"   Sample: {winner.sample.speedup:.2f}x")
            print(f"   Full: {winner.full_speedup:.2f}x")
        else:
            print("❌ No winner (speedup <2.0x)")

        print()
        print("=" * 70)
        print("✅ Test completed successfully")
        print("=" * 70)
        print()
        print(f"All outputs saved to: {output_dir}")
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
