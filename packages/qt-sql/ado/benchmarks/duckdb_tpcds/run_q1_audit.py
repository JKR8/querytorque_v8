#!/usr/bin/env python3
"""Run Q1 optimization loop with full artifact trail.

Up to 3 attempts (6 API calls). Stop early if speedup > 2x.
Validates on SF10. Saves every intermediate artifact.
"""

import json
import os
import sys
import time
from pathlib import Path

# Load .env — find repo root by walking up to find .env
from dotenv import load_dotenv
_p = Path(__file__).resolve()
while _p.parent != _p:
    if (_p / ".env").exists():
        load_dotenv(_p / ".env")
        break
    _p = _p.parent

# Ensure packages on path
QT_SQL = Path(__file__).resolve().parents[3]  # packages/qt-sql
REPO = QT_SQL.parents[1]                       # QueryTorque_V8
sys.path.insert(0, str(QT_SQL))
sys.path.insert(0, str(REPO / "packages" / "qt-shared"))
os.chdir(QT_SQL)  # So relative imports in ado/ work

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("q1_audit")

# ── Config ──────────────────────────────────────────────────────
QUERY_ID = "query_1"
DIALECT = "duckdb"
DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"
BENCHMARK_DIR = Path(__file__).parent
AUDIT_DIR = BENCHMARK_DIR / "audit_q1_analyze"
MAX_ATTEMPTS = 3
STOP_THRESHOLD = 2.0  # stop if speedup >= this
VALIDATION_RUNS = 3   # 3x: discard 1st, average last 2

# ── Load shared artifacts ───────────────────────────────────────
logger.info("Loading shared artifacts...")
sql = (BENCHMARK_DIR / "queries" / f"{QUERY_ID}.sql").read_text()
explain = json.loads((BENCHMARK_DIR / "explains" / f"{QUERY_ID}.json").read_text())

# Parse DAG with real costs
from qt_sql.optimization.dag_v2 import DagBuilder, CostAnalyzer
from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization

dag = DagBuilder(sql, dialect=DIALECT).build()
ctx = analyze_plan_for_optimization(explain["plan_json"], sql)
costs = CostAnalyzer(dag, plan_context=ctx).analyze()

# FAISS examples
from ado.pipeline import Pipeline
pipeline = Pipeline(benchmark_dir=str(BENCHMARK_DIR))
faiss_examples = pipeline._find_examples(sql, DIALECT)
faiss_picks = [ex["id"] for ex in faiss_examples]

# Available gold examples (for analyst)
available_examples = pipeline._list_gold_examples("duckdb")

# LLM client
from qt_shared.llm import create_llm_client
llm = create_llm_client()
if not llm:
    raise RuntimeError("No LLM client configured. Check QT_LLM_PROVIDER / QT_DEEPSEEK_API_KEY in .env")
logger.info(f"LLM: {llm.__class__.__name__}")

# ── Helpers ─────────────────────────────────────────────────────

def save_artifact(attempt: int, name: str, content: str):
    """Save artifact to attempt directory."""
    attempt_dir = AUDIT_DIR / f"attempt_{attempt}"
    attempt_dir.mkdir(parents=True, exist_ok=True)
    path = attempt_dir / name
    path.write_text(content)
    size = len(content)
    logger.info(f"  Saved {name} ({size:,} bytes)")
    return path


def validate_sql(original_sql: str, optimized_sql: str) -> dict:
    """Validate optimized SQL against original on SF10.

    3x runs: discard 1st (warmup), average last 2.
    """
    import duckdb

    def run_timed(conn, query, runs=3):
        times = []
        for i in range(runs):
            start = time.time()
            try:
                result = conn.execute(query).fetchall()
                elapsed = time.time() - start
                times.append(elapsed)
                row_count = len(result)
            except Exception as e:
                return {"error": str(e), "times": times}
        # Discard 1st, average last 2
        avg = sum(times[1:]) / len(times[1:]) if len(times) > 1 else times[0]
        return {"avg_ms": avg * 1000, "times_ms": [t * 1000 for t in times], "row_count": row_count}

    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        logger.info("  Validating original SQL...")
        orig = run_timed(conn, original_sql)
        if "error" in orig:
            return {"status": "ERROR", "error": f"Original failed: {orig['error']}"}

        logger.info(f"  Original: {orig['avg_ms']:.1f}ms (runs: {orig['times_ms']})")

        logger.info("  Validating optimized SQL...")
        opt = run_timed(conn, optimized_sql)
        if "error" in opt:
            return {"status": "ERROR", "error": f"Optimized failed: {opt['error']}",
                    "original_ms": orig["avg_ms"]}

        logger.info(f"  Optimized: {opt['avg_ms']:.1f}ms (runs: {opt['times_ms']})")

        # Row count check
        rows_match = orig["row_count"] == opt["row_count"]
        if not rows_match:
            logger.warning(f"  ROW COUNT MISMATCH: {orig['row_count']} vs {opt['row_count']}")

        speedup = orig["avg_ms"] / opt["avg_ms"] if opt["avg_ms"] > 0 else 0
        if speedup >= 1.10:
            status = "WIN"
        elif speedup >= 0.95:
            status = "PASS"
        else:
            status = "REGRESSION"

        return {
            "status": status,
            "speedup": round(speedup, 2),
            "original_ms": round(orig["avg_ms"], 1),
            "optimized_ms": round(opt["avg_ms"], 1),
            "original_runs_ms": [round(t, 1) for t in orig["times_ms"]],
            "optimized_runs_ms": [round(t, 1) for t in opt["times_ms"]],
            "original_rows": orig["row_count"],
            "optimized_rows": opt["row_count"],
            "rows_match": rows_match,
        }
    finally:
        conn.close()


def extract_sql(response: str) -> str:
    """Extract SQL from LLM response."""
    import re
    # Find ```sql ... ``` blocks
    blocks = re.findall(r'```sql\s*(.*?)\s*```', response, re.DOTALL)
    if blocks:
        # Return the longest block (usually the full rewrite)
        return max(blocks, key=len).strip()
    # Fallback: look for SELECT/WITH at start of line
    lines = response.split("\n")
    sql_lines = []
    capturing = False
    for line in lines:
        stripped = line.strip().upper()
        if stripped.startswith(("SELECT", "WITH")):
            capturing = True
        if capturing:
            sql_lines.append(line)
            if stripped.endswith(";"):
                break
    return "\n".join(sql_lines).strip().rstrip(";")


# ── Main loop ───────────────────────────────────────────────────

attempt_history = []
best_result = None
best_speedup = 0
best_sql = None

for attempt in range(1, MAX_ATTEMPTS + 1):
    logger.info(f"\n{'='*60}")
    logger.info(f"ATTEMPT {attempt}/{MAX_ATTEMPTS}")
    logger.info(f"{'='*60}")

    # ── Step 1: Build analyst prompt ────────────────────────────
    from ado.analyst import (
        build_analysis_prompt, parse_analysis_response,
        parse_example_overrides, format_analysis_for_prompt,
    )

    # Build history for attempts > 1
    history = None
    if attempt_history:
        history = {"attempts": attempt_history}

    analyst_prompt = build_analysis_prompt(
        query_id=QUERY_ID,
        sql=sql,
        dag=dag,
        costs=costs,
        history=history,
        faiss_picks=faiss_picks,
        available_examples=available_examples,
        dialect=DIALECT,
    )
    save_artifact(attempt, "01_analyst_prompt.txt", analyst_prompt)

    # ── Step 2: Call analyst LLM ────────────────────────────────
    logger.info(f"  Calling LLM (analyst)... [{len(analyst_prompt)} chars]")
    t0 = time.time()
    analyst_response = llm.analyze(analyst_prompt)
    t1 = time.time()
    logger.info(f"  Analyst response: {len(analyst_response)} chars in {t1-t0:.1f}s")
    save_artifact(attempt, "02_analyst_response.txt", analyst_response)

    # ── Step 3: Parse analysis + build rewrite prompt ───────────
    analysis = parse_analysis_response(analyst_response)
    formatted_analysis = format_analysis_for_prompt(analysis)
    save_artifact(attempt, "03_analysis_formatted.txt", formatted_analysis)

    # Check for example overrides
    overrides = parse_example_overrides(analyst_response)
    if overrides:
        logger.info(f"  Analyst overrode examples: {overrides}")
        examples_to_use = pipeline._load_examples_by_id(overrides, "duckdb")
        if not examples_to_use:
            examples_to_use = faiss_examples
    else:
        examples_to_use = faiss_examples

    from ado.node_prompter import Prompter
    prompter = Prompter()
    rewrite_prompt = prompter.build_prompt(
        query_id=QUERY_ID,
        full_sql=sql,
        dag=dag,
        costs=costs,
        history=history,
        examples=examples_to_use,
        expert_analysis=formatted_analysis,
        dialect=DIALECT,
    )
    save_artifact(attempt, "04_rewrite_prompt.txt", rewrite_prompt)

    # ── Step 4: Call rewrite LLM ────────────────────────────────
    logger.info(f"  Calling LLM (rewrite)... [{len(rewrite_prompt)} chars]")
    t0 = time.time()
    rewrite_response = llm.analyze(rewrite_prompt)
    t1 = time.time()
    logger.info(f"  Rewrite response: {len(rewrite_response)} chars in {t1-t0:.1f}s")
    save_artifact(attempt, "05_rewrite_response.txt", rewrite_response)

    # ── Step 5: Extract SQL ─────────────────────────────────────
    optimized_sql = extract_sql(rewrite_response)
    if not optimized_sql:
        logger.error("  Failed to extract SQL from response!")
        save_artifact(attempt, "06_optimized.sql", "-- EXTRACTION FAILED --\n" + rewrite_response)
        attempt_history.append({
            "attempt": attempt,
            "status": "ERROR",
            "error": "SQL extraction failed",
            "speedup": 0,
            "transforms": [],
        })
        continue
    save_artifact(attempt, "06_optimized.sql", optimized_sql)

    # ── Step 6: Validate ────────────────────────────────────────
    logger.info("  Running validation (3x runs on SF10)...")
    validation = validate_sql(sql, optimized_sql)
    save_artifact(attempt, "07_validation.json", json.dumps(validation, indent=2))

    speedup = validation.get("speedup", 0)
    status = validation.get("status", "ERROR")
    logger.info(f"  Result: {status} — {speedup}x speedup")

    # Track attempt
    attempt_record = {
        "attempt": attempt,
        "status": status,
        "speedup": speedup,
        "error": validation.get("error", ""),
        "optimized_sql": optimized_sql,
        "transforms": [],  # Could be extracted from analysis
    }
    attempt_history.append(attempt_record)

    # Track best
    if speedup > best_speedup and status != "ERROR":
        best_speedup = speedup
        best_result = validation
        best_sql = optimized_sql
        best_attempt = attempt

    # ── Check stop condition ────────────────────────────────────
    if speedup >= STOP_THRESHOLD:
        logger.info(f"  STOP: {speedup}x >= {STOP_THRESHOLD}x threshold!")
        break

    if attempt < MAX_ATTEMPTS:
        logger.info(f"  Speedup {speedup}x < {STOP_THRESHOLD}x, continuing...")


# ── Summary ─────────────────────────────────────────────────────
logger.info(f"\n{'='*60}")
logger.info("FINAL SUMMARY")
logger.info(f"{'='*60}")
logger.info(f"Attempts: {len(attempt_history)}")
for rec in attempt_history:
    logger.info(f"  #{rec['attempt']}: {rec['status']} — {rec['speedup']}x")

if best_sql:
    logger.info(f"\nBest: attempt #{best_attempt} — {best_speedup}x")
    save_artifact(0, "BEST_optimized.sql", best_sql)
    save_artifact(0, "BEST_validation.json", json.dumps(best_result, indent=2))

    # Save summary
    summary = {
        "query_id": QUERY_ID,
        "total_attempts": len(attempt_history),
        "best_attempt": best_attempt,
        "best_speedup": best_speedup,
        "best_status": best_result["status"],
        "all_attempts": attempt_history,
    }
    save_artifact(0, "SUMMARY.json", json.dumps(summary, indent=2))
else:
    logger.warning("No successful optimization found!")

logger.info("Done.")
