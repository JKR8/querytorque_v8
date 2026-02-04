"""Candidate benchmarking workflow for JSON v5 optimizer.

This module handles:
1. Saving candidates to disk for analysis
2. Benchmarking all candidates on main DB with 5 runs + trimmed mean
3. Selecting the winner
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass, asdict

from qt_sql.optimization.adaptive_rewriter_v5 import CandidateResult
from qt_sql.optimization.mcts.benchmark import BenchmarkRunner, BenchmarkResult
from qt_sql.validation.schemas import ValidationStatus

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkedCandidate:
    """A candidate with full benchmark results."""
    worker_id: int
    optimized_sql: str
    sample_db_status: str  # PASS/FAIL from sample DB validation
    sample_db_speedup: float  # Reference only, not used for selection
    main_db_latency_s: float  # Actual latency on main DB (trimmed mean of 5 runs)
    main_db_speedup: float  # Actual speedup on main DB
    error: Optional[str] = None


def save_candidates_to_disk(
    candidates: List[CandidateResult],
    original_sql: str,
    output_dir: Path,
) -> Path:
    """Save all candidates to disk for analysis.

    Creates a timestamped folder with:
    - original.sql - The original query
    - summary.json - Overview of all candidates
    - worker_{N}/ - Folder for each worker with:
        - optimized.sql - The optimized query
        - metadata.json - Worker info, status, sample DB speedup
        - llm_response.txt - Raw LLM response

    Args:
        candidates: List of candidates from optimize_v5_json_queue
        original_sql: The original SQL query
        output_dir: Base output directory

    Returns:
        Path to the created run directory
    """
    # Create timestamped run directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Saving {len(candidates)} candidates to {run_dir}")

    # Save original query
    (run_dir / "original.sql").write_text(original_sql)

    # Build summary
    summary = {
        "timestamp": timestamp,
        "total_candidates": len(candidates),
        "valid_candidates": sum(1 for c in candidates if c.status == ValidationStatus.PASS),
        "workers": []
    }

    # Save each candidate
    for candidate in candidates:
        worker_dir = run_dir / f"worker_{candidate.worker_id}"
        worker_dir.mkdir(exist_ok=True)

        # Save optimized SQL
        (worker_dir / "optimized.sql").write_text(candidate.optimized_sql)

        # Save LLM response
        (worker_dir / "llm_response.txt").write_text(candidate.response)

        # Save metadata
        metadata = {
            "worker_id": candidate.worker_id,
            "status": candidate.status.value,
            "sample_db_speedup": candidate.speedup,
            "error": candidate.error,
            "prompt": candidate.prompt,
        }
        (worker_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

        # Add to summary
        summary["workers"].append({
            "worker_id": candidate.worker_id,
            "status": candidate.status.value,
            "sample_db_speedup": candidate.speedup,
            "has_error": candidate.error is not None,
        })

    # Save summary
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    logger.info(f"✅ Saved candidates to {run_dir}")
    return run_dir


def benchmark_candidates(
    candidates: List[CandidateResult],
    original_sql: str,
    main_db: str,
    runs: int = 5,
) -> List[BenchmarkedCandidate]:
    """Benchmark all valid candidates on main DB with trimmed mean.

    For each candidate:
    1. Run original query 5 times (discard first, average rest 4)
    2. Run optimized query 5 times (discard first, average rest 4)
    3. Compute speedup = original_latency / optimized_latency

    Args:
        candidates: List of candidates from optimize_v5_json_queue
        original_sql: The original SQL query
        main_db: Path to main database
        runs: Number of runs per query (default 5)

    Returns:
        List of BenchmarkedCandidate with actual speedups
    """
    logger.info(f"Benchmarking {len(candidates)} candidates on main DB with {runs} runs each")

    benchmarker = BenchmarkRunner(
        database=main_db,
        runs=runs,
        dialect="duckdb",
        use_cache=False  # Don't cache, we want fresh timings
    )

    # Benchmark original query once (reused for all candidates)
    logger.info("Benchmarking original query...")
    original_result = benchmarker.run_query_robust(original_sql)
    original_latency = original_result.latency_s
    logger.info(f"Original latency: {original_latency:.3f}s (trimmed mean of {runs} runs)")

    # Benchmark each candidate
    benchmarked = []
    for i, candidate in enumerate(candidates, 1):
        logger.info(f"Benchmarking candidate {i}/{len(candidates)} (worker #{candidate.worker_id})...")

        try:
            # Run optimized query
            optimized_result = benchmarker.run_query_robust(candidate.optimized_sql)
            optimized_latency = optimized_result.latency_s

            # Compute speedup
            if optimized_latency > 0:
                speedup = original_latency / optimized_latency
            else:
                speedup = float('inf') if original_latency > 0 else 1.0

            logger.info(f"  Worker #{candidate.worker_id}: {optimized_latency:.3f}s → {speedup:.2f}x speedup")

            benchmarked.append(BenchmarkedCandidate(
                worker_id=candidate.worker_id,
                optimized_sql=candidate.optimized_sql,
                sample_db_status=candidate.status.value,
                sample_db_speedup=candidate.speedup,
                main_db_latency_s=optimized_latency,
                main_db_speedup=speedup,
                error=None,
            ))

        except Exception as e:
            logger.error(f"  Worker #{candidate.worker_id} failed: {e}")
            benchmarked.append(BenchmarkedCandidate(
                worker_id=candidate.worker_id,
                optimized_sql=candidate.optimized_sql,
                sample_db_status=candidate.status.value,
                sample_db_speedup=candidate.speedup,
                main_db_latency_s=0.0,
                main_db_speedup=1.0,
                error=str(e),
            ))

    return benchmarked


def save_benchmark_results(
    benchmarked: List[BenchmarkedCandidate],
    original_latency_s: float,
    run_dir: Path,
) -> Path:
    """Save benchmark results to the run directory.

    Args:
        benchmarked: List of benchmarked candidates
        original_latency_s: Original query latency
        run_dir: Run directory from save_candidates_to_disk

    Returns:
        Path to benchmark_results.json
    """
    results = {
        "original_latency_s": original_latency_s,
        "candidates": [asdict(b) for b in benchmarked],
        "winner": None,
    }

    # Find winner (highest speedup with no errors)
    valid = [b for b in benchmarked if b.error is None]
    if valid:
        winner = max(valid, key=lambda b: b.main_db_speedup)
        results["winner"] = {
            "worker_id": winner.worker_id,
            "speedup": winner.main_db_speedup,
            "latency_s": winner.main_db_latency_s,
        }

    # Save to run directory
    results_file = run_dir / "benchmark_results.json"
    results_file.write_text(json.dumps(results, indent=2))

    logger.info(f"✅ Saved benchmark results to {results_file}")
    return results_file


def optimize_v5_complete(
    sql: str,
    sample_db: str,
    main_db: str,
    output_dir: Path,
    provider: str = "deepseek",
    runs: int = 5,
) -> tuple[List[BenchmarkedCandidate], Optional[BenchmarkedCandidate], Path]:
    """Complete v5 optimization workflow.

    This is the end-to-end function that:
    1. Generates 5 candidates with different example batches (JSON v5)
    2. Validates all candidates on sample DB (syntax + row count)
    3. Saves all candidates to disk for analysis
    4. Benchmarks all valid candidates on main DB (5 runs with trimmed mean)
    5. Selects winner with best actual speedup
    6. Saves benchmark results

    Args:
        sql: Original SQL query to optimize
        sample_db: Path to sample database (for validation only)
        main_db: Path to main database (for benchmarking)
        output_dir: Base output directory for results
        provider: LLM provider (default: deepseek)
        runs: Number of benchmark runs per query (default: 5)

    Returns:
        Tuple of (all_benchmarked_candidates, winner, run_directory)
    """
    from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_json_queue

    logger.info("=" * 80)
    logger.info("Starting JSON v5 Complete Optimization Workflow")
    logger.info("=" * 80)
    logger.info(f"Provider: {provider}")
    logger.info(f"Sample DB: {sample_db}")
    logger.info(f"Main DB: {main_db}")
    logger.info(f"Benchmark runs: {runs}")
    logger.info("")

    # Step 1: Generate & validate candidates
    logger.info("Step 1: Generating 5 candidates with JSON v5...")
    candidates, _full_results, _winner = optimize_v5_json_queue(
        sql=sql,
        sample_db=sample_db,
        full_db=main_db,
        max_workers=5,
        provider=provider,
    )
    logger.info(f"✅ Generated {len(candidates)} candidates")
    logger.info(f"   Valid: {sum(1 for c in candidates if c.status == ValidationStatus.PASS)}")
    logger.info("")

    # Step 2: Save to disk
    logger.info("Step 2: Saving candidates to disk...")
    run_dir = save_candidates_to_disk(candidates, sql, output_dir)
    logger.info("")

    # Step 3: Benchmark on main DB
    logger.info("Step 3: Benchmarking candidates on main DB...")
    benchmarked = benchmark_candidates(candidates, sql, main_db, runs=runs)
    logger.info("")

    # Step 4: Save benchmark results
    logger.info("Step 4: Saving benchmark results...")

    # Get original latency
    benchmarker = BenchmarkRunner(main_db, runs=runs, use_cache=False)
    original_result = benchmarker.run_query_robust(sql)
    original_latency = original_result.latency_s

    save_benchmark_results(benchmarked, original_latency, run_dir)
    logger.info("")

    # Step 5: Select winner
    valid_benchmarked = [b for b in benchmarked if b.error is None]
    winner = max(valid_benchmarked, key=lambda b: b.main_db_speedup) if valid_benchmarked else None

    if winner:
        logger.info("=" * 80)
        logger.info("WINNER")
        logger.info("=" * 80)
        logger.info(f"Worker: #{winner.worker_id}")
        logger.info(f"Main DB Speedup: {winner.main_db_speedup:.2f}x")
        logger.info(f"Main DB Latency: {winner.main_db_latency_s:.3f}s")
        logger.info(f"Sample DB Speedup: {winner.sample_db_speedup:.2f}x (reference only)")
        logger.info("")
        logger.info(f"Results saved to: {run_dir}")
        logger.info("=" * 80)
    else:
        logger.warning("No valid candidates produced")

    return benchmarked, winner, run_dir
