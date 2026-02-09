#!/usr/bin/env python3
"""Run analyst mode up to prompt stage for a single query.

Usage:
    python run_analyst_prompt_only.py query_4
    python run_analyst_prompt_only.py query_23a
"""

import json
import os
import sys
import time
from pathlib import Path

query_id = sys.argv[1] if len(sys.argv) > 1 else None
if not query_id:
    print("Usage: python run_analyst_prompt_only.py <query_id>")
    sys.exit(1)

# Load .env
from dotenv import load_dotenv
_p = Path(__file__).resolve()
while _p.parent != _p:
    if (_p / ".env").exists():
        load_dotenv(_p / ".env")
        break
    _p = _p.parent

QT_SQL = Path(__file__).resolve().parents[3]
REPO = QT_SQL.parents[1]
sys.path.insert(0, str(QT_SQL))
sys.path.insert(0, str(REPO / "packages" / "qt-shared"))
os.chdir(QT_SQL)

import logging
logging.basicConfig(
    level=logging.INFO,
    format=f"%(levelname)s [{query_id}] %(name)s: %(message)s",
)
logger = logging.getLogger("analyst")

from qt_sql.benchmarks.artifact_checks import (
    check_input_sql, check_faiss_examples, check_analyst_prompt,
    check_analyst_response, check_formatted_analysis, check_rewrite_prompt,
)

DIALECT = "duckdb"
BENCHMARK_DIR = Path(__file__).parent
AUDIT_DIR = BENCHMARK_DIR / f"analyst_{query_id}"

# ── Load SQL ────────────────────────────────────────────────────
sql_path = BENCHMARK_DIR / "queries" / f"{query_id}.sql"
if not sql_path.exists():
    logger.error(f"Query file not found: {sql_path}")
    sys.exit(1)
sql = sql_path.read_text()
# Strip comments and trailing semicolons
lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
sql = "\n".join(lines).strip().rstrip(";")

logger.info(f"Query: {query_id} ({len(sql)} chars)")

# ── Load explain plan ──────────────────────────────────────────
explain_path = BENCHMARK_DIR / "explains" / f"{query_id}.json"
if explain_path.exists():
    explain = json.loads(explain_path.read_text())
    logger.info("Loaded explain plan")
else:
    explain = {}
    logger.warning("No explain plan found — using DAG-only costs")

# ── Parse DAG ──────────────────────────────────────────────────
from qt_sql.dag import DagBuilder, CostAnalyzer
from qt_sql.plan_analyzer import analyze_plan_for_optimization

dag = DagBuilder(sql, dialect=DIALECT).build()
try:
    if explain.get("plan_json"):
        ctx = analyze_plan_for_optimization(explain["plan_json"], sql)
        costs = CostAnalyzer(dag, plan_context=ctx).analyze()
    else:
        costs = CostAnalyzer(dag).analyze()
except Exception as e:
    logger.warning(f"Cost analysis failed ({e}), using DAG-only")
    costs = CostAnalyzer(dag).analyze()

logger.info(f"DAG nodes: {len(dag.nodes) if hasattr(dag, 'nodes') else '?'}")

# ── FAISS ──────────────────────────────────────────────────────
from qt_sql.pipeline import Pipeline
pipeline = Pipeline(benchmark_dir=str(BENCHMARK_DIR))
faiss_examples = pipeline._find_examples(sql, DIALECT)
faiss_picks = [ex["id"] for ex in faiss_examples]
logger.info(f"FAISS: {faiss_picks}")

available_examples = pipeline._list_gold_examples("duckdb")

# ── Save helper ────────────────────────────────────────────────
def save(name, content):
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    (AUDIT_DIR / name).write_text(content)
    logger.info(f"Saved {name} ({len(content):,} bytes)")

save("00_input.sql", sql)
check_input_sql(sql, query_id)
save("01_faiss_examples.json", json.dumps(faiss_examples, indent=2))
check_faiss_examples(json.dumps(faiss_examples, indent=2), query_id)

# ── Load history (what was already tried for this query) ──────
def load_query_history(query_id: str) -> dict:
    """Load prior attempts for this query from all sources."""
    attempts = []

    # Source 1: state_N validation directories
    for state_dir in sorted(BENCHMARK_DIR.glob("state_*")):
        if not state_dir.is_dir():
            continue
        state_num = state_dir.name.split("_")[-1]
        # Try both naming conventions: q51 and query_51
        for variant in [query_id, query_id.replace("query_", "q")]:
            val_path = state_dir / "validation" / f"{variant}.json"
            if val_path.exists():
                data = json.loads(val_path.read_text())
                attempts.append({
                    "state": int(state_num),
                    "source": f"state_{state_num}",
                    "status": data.get("status", "unknown"),
                    "speedup": data.get("speedup", 0),
                    "transforms": data.get("transforms_applied", []),
                    "error": data.get("error", ""),
                })
                break

    # Source 2: leaderboard (may have analyst_mode results)
    lb_path = BENCHMARK_DIR / "leaderboard.json"
    if lb_path.exists():
        lb = json.loads(lb_path.read_text())
        queries = lb.get("queries", lb) if isinstance(lb, dict) else lb
        if isinstance(queries, list):
            for q in queries:
                qid = q.get("query_id", "")
                if qid in (query_id, query_id.replace("query_", "q")):
                    source = q.get("source", "state_0")
                    # Only add if not already captured from state validation
                    if source != "state_0" or not attempts:
                        existing_sources = {a.get("source") for a in attempts}
                        if source not in existing_sources:
                            attempts.append({
                                "source": source,
                                "status": q.get("status", "unknown"),
                                "speedup": q.get("speedup", 0),
                                "transforms": q.get("transforms", []),
                            })

    if not attempts:
        return None

    logger.info(f"Loaded history: {len(attempts)} prior attempts")
    for a in attempts:
        logger.info(f"  {a.get('source','?')}: {a.get('status','?')} "
                     f"{a.get('speedup',0):.2f}x [{', '.join(a.get('transforms',[]))}]")

    return {"attempts": attempts, "promotion": None}

history = load_query_history(query_id)

# ── Analyst prompt ─────────────────────────────────────────────
from qt_sql.analyst import build_analysis_prompt

analyst_prompt = build_analysis_prompt(
    query_id=query_id,
    sql=sql,
    dag=dag,
    costs=costs,
    history=history,
    faiss_picks=faiss_picks,
    available_examples=available_examples,
    dialect=DIALECT,
)
save("02_analyst_prompt.txt", analyst_prompt)
check_analyst_prompt(analyst_prompt, query_id, history)

# ── Call analyst LLM ───────────────────────────────────────────
from qt_shared.llm import create_llm_client
llm = create_llm_client()
if not llm:
    raise RuntimeError("No LLM client configured.")

logger.info(f"Calling analyst LLM... [{len(analyst_prompt)} chars]")
t0 = time.time()
analyst_response = llm.analyze(analyst_prompt)
t1 = time.time()
logger.info(f"Analyst: {len(analyst_response)} chars in {t1-t0:.1f}s")
save("03_analyst_response.txt", analyst_response)
check_analyst_response(analyst_response, query_id, history)

# ── Parse + build rewrite prompt ───────────────────────────────
from qt_sql.analyst import parse_analysis_response, parse_example_overrides, format_analysis_for_prompt

analysis = parse_analysis_response(analyst_response)
formatted = format_analysis_for_prompt(analysis)
save("04_analysis_formatted.txt", formatted)
check_formatted_analysis(formatted, query_id)

overrides = parse_example_overrides(analyst_response)
if overrides:
    logger.info(f"Analyst overrode examples: {overrides}")
    examples_to_use = pipeline._load_examples_by_id(overrides, "duckdb")
    if not examples_to_use:
        examples_to_use = faiss_examples
else:
    examples_to_use = faiss_examples

from qt_sql.node_prompter import Prompter
prompter = Prompter()
rewrite_prompt = prompter.build_prompt(
    query_id=query_id,
    full_sql=sql,
    dag=dag,
    costs=costs,
    history=history,
    examples=examples_to_use,
    expert_analysis=formatted,
    dialect=DIALECT,
)
save("05_rewrite_prompt.txt", rewrite_prompt)
check_rewrite_prompt(rewrite_prompt, query_id, history)

logger.info(f"DONE — prompts saved to {AUDIT_DIR}/")
