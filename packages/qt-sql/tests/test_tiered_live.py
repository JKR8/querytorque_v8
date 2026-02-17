#!/usr/bin/env python3
"""Live E2E test: tiered patch mode on DuckDB Q21 via OpenRouter.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 test_tiered_live.py
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Logging ───────────────────────────────────────────────────────────────
LOG_DIR = Path("test_patch_logs")
LOG_DIR.mkdir(exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"tiered_live_{ts}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-30s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("tiered_live")

# ── Config ────────────────────────────────────────────────────────────────
QUERY_ID = "query_21"
BENCH_DIR = Path("packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds")
MAX_ITERATIONS = 2  # 1 initial + 1 snipe

logger.info(f"{'='*60}")
logger.info(f"  DuckDB Tiered Patch — LIVE E2E ({QUERY_ID})")
logger.info(f"{'='*60}")

# ── Load query ────────────────────────────────────────────────────────────
sql_path = BENCH_DIR / "queries" / f"{QUERY_ID}.sql"
original_sql = sql_path.read_text().strip()
logger.info(f"Query: {QUERY_ID} ({len(original_sql)} chars)")

# ── Build Pipeline + Session ──────────────────────────────────────────────
from qt_sql.schemas import BenchmarkConfig
from qt_sql.pipeline import Pipeline
from qt_sql.sessions.oneshot_patch_session import OneshotPatchSession

# Build pipeline from benchmark dir
pipeline = Pipeline(
    benchmark_dir=BENCH_DIR,
    provider="openrouter",
    model="deepseek/deepseek-r1",
)

# Patch config for tiered mode
pipeline.config.tiered_patch_enabled = True
pipeline.config.target_speedup = 10.0
pipeline.config.benchmark_dsn = pipeline.config.db_path_or_dsn

config = pipeline.config
logger.info(f"Config: engine={config.engine}, tiered={config.tiered_patch_enabled}")
logger.info(f"  analyst_model={config.analyst_model}")
logger.info(f"  worker_model={config.worker_model}")
logger.info(f"  target_speedup={config.target_speedup}")
logger.info(f"  semantic_validation={config.semantic_validation_enabled}")

# Create session
session = OneshotPatchSession(
    pipeline=pipeline,
    query_id=QUERY_ID,
    original_sql=original_sql,
    target_speedup=config.target_speedup,
    max_iterations=MAX_ITERATIONS,
    patch=True,
)

# ── Run ───────────────────────────────────────────────────────────────────
logger.info(f"Starting tiered session...")
t0 = time.time()

try:
    result = session.run()
except Exception as e:
    logger.error(f"Session failed: {e}", exc_info=True)
    sys.exit(1)

elapsed = time.time() - t0

# ── Results ───────────────────────────────────────────────────────────────
logger.info(f"\n{'='*60}")
logger.info(f"  RESULTS")
logger.info(f"{'='*60}")
logger.info(f"  Query:      {result.query_id}")
logger.info(f"  Status:     {result.status}")
logger.info(f"  Speedup:    {result.best_speedup:.2f}x")
logger.info(f"  Transforms: {result.best_transforms}")
logger.info(f"  Iterations: {result.n_iterations}")
logger.info(f"  API calls:  {result.n_api_calls}")
logger.info(f"  Elapsed:    {elapsed:.1f}s")

# Save result JSON
result_path = LOG_DIR / f"tiered_result_{QUERY_ID}_{ts}.json"
result_dict = {
    "query_id": result.query_id,
    "status": result.status,
    "best_speedup": round(result.best_speedup, 2),
    "best_transforms": result.best_transforms,
    "n_iterations": result.n_iterations,
    "n_api_calls": result.n_api_calls,
    "elapsed_s": round(elapsed, 1),
    "iterations": result.iterations,
}
result_path.write_text(json.dumps(result_dict, indent=2, default=str))
logger.info(f"  Result saved: {result_path}")

# Save best SQL
if result.best_sql and result.best_sql != original_sql:
    sql_path = LOG_DIR / f"best_sql_{QUERY_ID}_{ts}.sql"
    sql_path.write_text(result.best_sql)
    logger.info(f"  Best SQL saved: {sql_path}")

logger.info(f"  Log file: {log_file}")
