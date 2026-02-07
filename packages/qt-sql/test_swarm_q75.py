#!/usr/bin/env python3
"""Swarm mode integration test on TPC-DS Q75.

Runs the full SwarmSession pipeline with complete logging of every step:
- Fan-out: analyst distributes 12 FAISS examples across 4 workers
- Parallel LLM generation (4 workers, different strategies)
- Sequential validation (timing isolation)
- Snipe phase if target not reached

All inputs/outputs saved to: ado/benchmarks/duckdb_tpcds/swarm_sessions/query_75/

Usage:
    cd packages/qt-sql
    python3 test_swarm_q75.py
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure qt-shared is importable (provides qt_shared.llm for LLM calls)
_qt_shared_path = str(Path(__file__).resolve().parent.parent / "qt-shared")
if _qt_shared_path not in sys.path:
    sys.path.insert(0, _qt_shared_path)

# Load env from project root
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

# Setup comprehensive logging
LOG_DIR = Path("ado/benchmarks/duckdb_tpcds/swarm_sessions/query_75")
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# File handler: DEBUG level (everything)
file_handler = logging.FileHandler(log_file, mode="w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(name)-30s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
))

# Console handler: INFO level
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
))

# Root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

# Also capture ADO subsystem logs
for module in ["ado", "ado.pipeline", "ado.analyst_session", "ado.generate",
               "ado.validate", "ado.knowledge", "ado.node_prompter",
               "ado.sessions.swarm_session", "ado.sessions.standard_session",
               "ado.sessions.expert_session", "ado.prompts"]:
    logging.getLogger(module).setLevel(logging.DEBUG)

logger = logging.getLogger("test_swarm_q75")


def intercept_llm_calls(pipeline):
    """Wrap the CandidateGenerator._analyze to log all LLM I/O."""
    from ado.generate import CandidateGenerator
    original_analyze = CandidateGenerator._analyze
    call_counter = [0]

    def logged_analyze(self, prompt):
        call_counter[0] += 1
        call_num = call_counter[0]

        # Save prompt
        prompt_file = LOG_DIR / f"llm_call_{call_num:02d}_prompt.txt"
        prompt_file.write_text(prompt)
        logger.info(f"LLM call #{call_num}: prompt saved ({len(prompt)} chars) -> {prompt_file.name}")

        # Time the call
        t0 = time.time()
        response = original_analyze(self, prompt)
        elapsed = time.time() - t0

        # Save response
        response_file = LOG_DIR / f"llm_call_{call_num:02d}_response.txt"
        response_file.write_text(response)
        logger.info(f"LLM call #{call_num}: response saved ({len(response)} chars, {elapsed:.1f}s) -> {response_file.name}")

        # Save R1 reasoning chain if available
        try:
            from qt_shared.llm.deepseek import DeepSeekClient
            reasoning = DeepSeekClient.last_reasoning
            if reasoning:
                reasoning_file = LOG_DIR / f"llm_call_{call_num:02d}_reasoning.txt"
                reasoning_file.write_text(reasoning)
                logger.info(f"LLM call #{call_num}: reasoning saved ({len(reasoning)} chars) -> {reasoning_file.name}")
        except Exception:
            pass

        return response

    CandidateGenerator._analyze = logged_analyze
    return call_counter


def intercept_validation(pipeline):
    """Wrap Pipeline._validate to log all validation I/O."""
    original_validate = pipeline._validate
    val_counter = [0]

    def logged_validate(original_sql, optimized_sql):
        val_counter[0] += 1
        val_num = val_counter[0]

        # Save both SQLs
        val_dir = LOG_DIR / f"validation_{val_num:02d}"
        val_dir.mkdir(exist_ok=True)
        (val_dir / "original.sql").write_text(original_sql)
        (val_dir / "optimized.sql").write_text(optimized_sql)

        logger.info(f"Validation #{val_num}: starting...")
        t0 = time.time()
        status, speedup = original_validate(original_sql, optimized_sql)
        elapsed = time.time() - t0

        result = {"status": status, "speedup": speedup, "elapsed_seconds": elapsed}
        (val_dir / "result.json").write_text(json.dumps(result, indent=2))
        logger.info(f"Validation #{val_num}: {status} {speedup:.2f}x ({elapsed:.1f}s)")

        return status, speedup

    pipeline._validate = logged_validate
    return val_counter


def intercept_faiss(pipeline):
    """Wrap _find_examples to log FAISS retrieval."""
    original_find = pipeline._find_examples

    def logged_find(sql, engine="duckdb", k=3):
        logger.info(f"FAISS: requesting k={k} examples for engine={engine}")
        examples = original_find(sql, engine=engine, k=k)
        ids = [e.get("id", "?") for e in examples]
        logger.info(f"FAISS: returned {len(examples)} examples: {ids}")

        # Save
        faiss_file = LOG_DIR / f"faiss_k{k}_examples.json"
        faiss_file.write_text(json.dumps(
            [{"id": e.get("id"), "speedup": e.get("verified_speedup"), "description": e.get("description", "")[:100]}
             for e in examples],
            indent=2,
        ))
        return examples

    pipeline._find_examples = logged_find


def main():
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("SWARM MODE TEST: TPC-DS Q75 on DuckDB SF10")
    logger.info("=" * 70)

    # Verify env
    provider = os.environ.get("QT_LLM_PROVIDER", "")
    model = os.environ.get("QT_LLM_MODEL", "")
    logger.info(f"LLM provider: {provider}, model: {model}")
    if not provider:
        logger.error("No QT_LLM_PROVIDER set. Cannot run.")
        sys.exit(1)

    # Import
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from ado.pipeline import Pipeline
    from ado.schemas import OptimizationMode

    # Load pipeline
    benchmark_dir = "ado/benchmarks/duckdb_tpcds"
    logger.info(f"Loading pipeline from: {benchmark_dir}")
    pipeline = Pipeline(benchmark_dir=benchmark_dir)
    logger.info(f"Config: engine={pipeline.config.engine}, db={pipeline.config.db_path_or_dsn}")

    # Load Q75
    sql_path = Path(benchmark_dir) / "queries" / "query_75.sql"
    sql = sql_path.read_text()
    logger.info(f"Query SQL loaded: {len(sql)} chars, {sql.count(chr(10))} lines")

    # Save input SQL
    (LOG_DIR / "input_query.sql").write_text(sql)

    # Install interceptors
    logger.info("Installing LLM/validation/FAISS interceptors...")
    llm_counter = intercept_llm_calls(pipeline)
    val_counter = intercept_validation(pipeline)
    intercept_faiss(pipeline)

    # Save gold example catalog
    gold = pipeline._list_gold_examples("duckdb")
    (LOG_DIR / "gold_example_catalog.json").write_text(json.dumps(gold, indent=2))
    logger.info(f"Gold examples available: {len(gold)}")

    # Run swarm
    logger.info("")
    logger.info("=" * 70)
    logger.info("STARTING SWARM SESSION")
    logger.info(f"  target_speedup: 2.0x")
    logger.info(f"  max_iterations: 3 (1 fan-out + 2 snipe)")
    logger.info("=" * 70)
    logger.info("")

    try:
        result = pipeline.run_optimization_session(
            query_id="query_75",
            sql=sql,
            max_iterations=3,
            target_speedup=2.0,
            mode=OptimizationMode.SWARM,
        )
    except Exception as e:
        logger.exception(f"SwarmSession FAILED: {e}")
        sys.exit(1)

    elapsed = time.time() - start_time

    # Save final result
    final_result = {
        "query_id": result.query_id,
        "mode": result.mode,
        "status": result.status,
        "best_speedup": result.best_speedup,
        "best_transforms": result.best_transforms,
        "n_iterations": result.n_iterations,
        "n_api_calls": result.n_api_calls,
        "total_llm_calls": llm_counter[0],
        "total_validations": val_counter[0],
        "elapsed_seconds": elapsed,
    }
    (LOG_DIR / "final_result.json").write_text(json.dumps(final_result, indent=2))

    # Save best SQL
    (LOG_DIR / "best_optimized.sql").write_text(result.best_sql)

    # Save full iterations data
    (LOG_DIR / "iterations_data.json").write_text(
        json.dumps(result.iterations, indent=2, default=str)
    )

    # Summary
    logger.info("")
    logger.info("=" * 70)
    logger.info("SWARM SESSION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"  Status:          {result.status}")
    logger.info(f"  Best speedup:    {result.best_speedup:.2f}x")
    logger.info(f"  Best transforms: {result.best_transforms}")
    logger.info(f"  Iterations:      {result.n_iterations}")
    logger.info(f"  LLM calls:       {llm_counter[0]}")
    logger.info(f"  Validations:     {val_counter[0]}")
    logger.info(f"  Total time:      {elapsed:.1f}s")
    logger.info(f"  Log file:        {log_file}")
    logger.info(f"  Artifacts:       {LOG_DIR}")
    logger.info("")

    # Print per-iteration summary
    for i, it_data in enumerate(result.iterations):
        phase = it_data.get("phase", "?")
        best_sp = it_data.get("best_speedup", 0)
        n_workers = len(it_data.get("worker_results", []))
        logger.info(f"  Iteration {i}: phase={phase}, workers={n_workers}, best={best_sp:.2f}x")

        for wr in it_data.get("worker_results", []):
            wid = wr.get("worker_id", "?")
            strategy = wr.get("strategy", "?")
            status = wr.get("status", "?")
            speedup = wr.get("speedup", 0)
            transforms = wr.get("transforms", [])
            logger.info(f"    W{wid} ({strategy}): {status} {speedup:.2f}x {transforms}")

    logger.info("")
    logger.info(f"All artifacts saved to: {LOG_DIR}")


if __name__ == "__main__":
    main()
