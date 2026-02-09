#!/usr/bin/env python3
"""Test iterative optimization: feed the 1.85x winner back in as input.

Runs up to prompt generation only. Review prompts before proceeding.
"""

import json
import os
import sys
import time
from pathlib import Path

# Load .env
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
os.chdir(QT_SQL)

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("q1_iterative")

# ── Config ──────────────────────────────────────────────────────
QUERY_ID = "query_1"
DIALECT = "duckdb"
BENCHMARK_DIR = Path(__file__).parent
AUDIT_DIR = BENCHMARK_DIR / "audit_q1_iterative"

# ── Load SQL ────────────────────────────────────────────────────
true_original_sql = (BENCHMARK_DIR / "queries" / f"{QUERY_ID}.sql").read_text()
optimized_input_sql = (
    BENCHMARK_DIR / "audit_q1_analyze" / "attempt_0" / "BEST_optimized.sql"
).read_text()

logger.info("="*60)
logger.info("ITERATIVE TEST: Feeding 1.85x winner back as input")
logger.info("Stops at prompt — review before continuing")
logger.info("="*60)

# ── Parse DAG from OPTIMIZED input ─────────────────────────────
explain = json.loads((BENCHMARK_DIR / "explains" / f"{QUERY_ID}.json").read_text())

from qt_sql.optimization.dag_v2 import DagBuilder, CostAnalyzer
from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization

dag = DagBuilder(optimized_input_sql, dialect=DIALECT).build()
try:
    ctx = analyze_plan_for_optimization(explain["plan_json"], optimized_input_sql)
    costs = CostAnalyzer(dag, plan_context=ctx).analyze()
except Exception as e:
    logger.warning(f"Cost analysis on optimized SQL failed ({e}), using DAG-only costs")
    costs = CostAnalyzer(dag).analyze()

logger.info(f"DAG nodes: {len(dag.nodes) if hasattr(dag, 'nodes') else '?'}")
logger.info(f"Costs: {str(costs)[:500]}")

# ── FAISS examples ─────────────────────────────────────────────
from ado.pipeline import Pipeline
pipeline = Pipeline(benchmark_dir=str(BENCHMARK_DIR))
faiss_examples = pipeline._find_examples(optimized_input_sql, DIALECT)
faiss_picks = [ex["id"] for ex in faiss_examples]
logger.info(f"FAISS matched examples: {faiss_picks}")

available_examples = pipeline._list_gold_examples("duckdb")

# ── Save artifacts ─────────────────────────────────────────────
def save_artifact(name: str, content: str):
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    path = AUDIT_DIR / name
    path.write_text(content)
    logger.info(f"Saved {name} ({len(content):,} bytes)")
    return path

save_artifact("00_input_sql.sql", optimized_input_sql)
save_artifact("00_true_original.sql", true_original_sql)
save_artifact("01_dag.json", json.dumps({"nodes": str(dag)}, indent=2))
from dataclasses import asdict
try:
    costs_serializable = {k: asdict(v) if hasattr(v, '__dataclass_fields__') else v for k, v in costs.items()} if isinstance(costs, dict) else str(costs)
    save_artifact("02_costs.json", json.dumps(costs_serializable, indent=2, default=str))
except Exception:
    save_artifact("02_costs.json", str(costs))
save_artifact("03_faiss_examples.json", json.dumps(faiss_examples, indent=2))

# ── Build analyst prompt ───────────────────────────────────────
from ado.analyst import build_analysis_prompt

analyst_prompt = build_analysis_prompt(
    query_id=QUERY_ID,
    sql=optimized_input_sql,
    dag=dag,
    costs=costs,
    history=None,
    faiss_picks=faiss_picks,
    available_examples=available_examples,
    dialect=DIALECT,
)
save_artifact("04_analyst_prompt.txt", analyst_prompt)

# ── Call analyst LLM ───────────────────────────────────────────
from qt_shared.llm import create_llm_client
llm = create_llm_client()
if not llm:
    raise RuntimeError("No LLM client configured.")
logger.info(f"LLM: {llm.__class__.__name__}")

logger.info(f"Calling LLM (analyst)... [{len(analyst_prompt)} chars]")
t0 = time.time()
analyst_response = llm.analyze(analyst_prompt)
t1 = time.time()
logger.info(f"Analyst response: {len(analyst_response)} chars in {t1-t0:.1f}s")
save_artifact("05_analyst_response.txt", analyst_response)

# ── Parse analysis + build rewrite prompt ──────────────────────
from ado.analyst import parse_analysis_response, parse_example_overrides, format_analysis_for_prompt

analysis = parse_analysis_response(analyst_response)
formatted_analysis = format_analysis_for_prompt(analysis)
save_artifact("06_analysis_formatted.txt", formatted_analysis)

overrides = parse_example_overrides(analyst_response)
if overrides:
    logger.info(f"Analyst overrode examples: {overrides}")
    examples_to_use = pipeline._load_examples_by_id(overrides, "duckdb")
    if not examples_to_use:
        examples_to_use = faiss_examples
else:
    examples_to_use = faiss_examples

from ado.node_prompter import Prompter
prompter = Prompter()
rewrite_prompt = prompter.build_prompt(
    query_id=QUERY_ID,
    full_sql=optimized_input_sql,
    dag=dag,
    costs=costs,
    history=None,
    examples=examples_to_use,
    expert_analysis=formatted_analysis,
    dialect=DIALECT,
)
save_artifact("07_rewrite_prompt.txt", rewrite_prompt)

# ── Call rewrite LLM ──────────────────────────────────────────
logger.info(f"Calling LLM (rewrite)... [{len(rewrite_prompt)} chars]")
t0 = time.time()
rewrite_response = llm.analyze(rewrite_prompt)
t1 = time.time()
logger.info(f"Rewrite response: {len(rewrite_response)} chars in {t1-t0:.1f}s")
save_artifact("08_rewrite_response.txt", rewrite_response)

# ── Extract SQL ───────────────────────────────────────────────
import re
def extract_sql(response: str) -> str:
    blocks = re.findall(r'```sql\s*(.*?)\s*```', response, re.DOTALL)
    if blocks:
        return max(blocks, key=len).strip().rstrip(";")
    return ""

optimized_sql = extract_sql(rewrite_response)
if not optimized_sql:
    logger.error("Failed to extract SQL from rewrite response!")
    save_artifact("09_optimized.sql", "-- EXTRACTION FAILED --")
    sys.exit(1)
save_artifact("09_optimized.sql", optimized_sql)

# ── Validate against TRUE ORIGINAL ────────────────────────────
DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"

def validate_sql(baseline_sql: str, optimized_sql: str) -> dict:
    """3x runs: discard 1st (warmup), average last 2."""
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
        avg = sum(times[1:]) / len(times[1:]) if len(times) > 1 else times[0]
        return {"avg_ms": avg * 1000, "times_ms": [t * 1000 for t in times], "row_count": row_count}

    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        logger.info("  Validating baseline (true original)...")
        orig = run_timed(conn, baseline_sql)
        if "error" in orig:
            return {"status": "ERROR", "error": f"Baseline failed: {orig['error']}"}
        logger.info(f"  Baseline: {orig['avg_ms']:.1f}ms (runs: {orig['times_ms']})")

        logger.info("  Validating re-optimized SQL...")
        opt = run_timed(conn, optimized_sql)
        if "error" in opt:
            return {"status": "ERROR", "error": f"Optimized failed: {opt['error']}",
                    "baseline_ms": orig["avg_ms"]}
        logger.info(f"  Re-optimized: {opt['avg_ms']:.1f}ms (runs: {opt['times_ms']})")

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
            "baseline_ms": round(orig["avg_ms"], 1),
            "optimized_ms": round(opt["avg_ms"], 1),
            "baseline_runs_ms": [round(t, 1) for t in orig["times_ms"]],
            "optimized_runs_ms": [round(t, 1) for t in opt["times_ms"]],
            "baseline_rows": orig["row_count"],
            "optimized_rows": opt["row_count"],
            "rows_match": rows_match,
        }
    finally:
        conn.close()

logger.info("\nRunning validation (3x runs on SF10, vs true original)...")
validation = validate_sql(true_original_sql, optimized_sql)
save_artifact("10_validation.json", json.dumps(validation, indent=2))

speedup = validation.get("speedup", 0)
status = validation.get("status", "ERROR")

# Also validate the 1.85x input for comparison
logger.info("\nBaseline check (1.85x winner vs true original)...")
baseline_check = validate_sql(true_original_sql, optimized_input_sql)
save_artifact("10_baseline_check.json", json.dumps(baseline_check, indent=2))

logger.info("")
logger.info("="*60)
logger.info("RESULTS")
logger.info("="*60)
logger.info(f"Previous best (1.85x winner): {baseline_check.get('speedup', 'ERROR')}x")
logger.info(f"New re-optimized:             {speedup}x ({status})")
if speedup > baseline_check.get("speedup", 0):
    logger.info(f"COMPOUNDED! {baseline_check.get('speedup',0)}x -> {speedup}x")
else:
    logger.info(f"No improvement over previous best")
logger.info("="*60)
