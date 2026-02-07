#!/usr/bin/env python3
"""Run rewrite LLM + validation for a query that already has prompts generated.

Usage:
    python run_analyst_rewrite_validate.py query_4
"""

import json
import os
import re
import sys
import time
from pathlib import Path

query_id = sys.argv[1] if len(sys.argv) > 1 else None
if not query_id:
    print("Usage: python run_analyst_rewrite_validate.py <query_id>")
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
logger = logging.getLogger("rewrite")

from ado.benchmarks.artifact_checks import check_input_sql, check_rewrite_prompt

DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"
BENCHMARK_DIR = Path(__file__).parent
AUDIT_DIR = BENCHMARK_DIR / f"analyst_{query_id}"

# ── Load rewrite prompt ────────────────────────────────────────
prompt_path = AUDIT_DIR / "05_rewrite_prompt.txt"
if not prompt_path.exists():
    logger.error(f"No rewrite prompt found at {prompt_path}")
    sys.exit(1)
rewrite_prompt = prompt_path.read_text()

# ── Load original SQL ─────────────────────────────────────────
sql_path = AUDIT_DIR / "00_input.sql"
original_sql = sql_path.read_text()

# ── In-flight checks on loaded artifacts ──────────────────────
check_input_sql(original_sql, query_id)
check_rewrite_prompt(rewrite_prompt, query_id)

# ── LLM ───────────────────────────────────────────────────────
from qt_shared.llm import create_llm_client
llm = create_llm_client()
if not llm:
    raise RuntimeError("No LLM client configured.")

logger.info(f"Calling rewrite LLM... [{len(rewrite_prompt)} chars]")
t0 = time.time()
rewrite_response = llm.analyze(rewrite_prompt)
t1 = time.time()
logger.info(f"Rewrite: {len(rewrite_response)} chars in {t1-t0:.1f}s")
(AUDIT_DIR / "06_rewrite_response.txt").write_text(rewrite_response)

# ── Extract SQL ───────────────────────────────────────────────
blocks = re.findall(r'```sql\s*(.*?)\s*```', rewrite_response, re.DOTALL)
if blocks:
    optimized_sql = max(blocks, key=len).strip().rstrip(";")
else:
    # Fallback: try any ``` block, or the text before "Changes:" / "Expected"
    blocks = re.findall(r'```\s*(.*?)\s*```', rewrite_response, re.DOTALL)
    if blocks:
        optimized_sql = max(blocks, key=len).strip().rstrip(";")
        logger.warning("Extracted SQL from unfenced code block (no ```sql tag)")
    else:
        # Last resort: take everything up to "Changes:" or "Expected speedup:"
        parts = re.split(r'\n(?:Changes|Expected speedup):', rewrite_response, maxsplit=1)
        candidate = parts[0].strip().rstrip(";")
        if "SELECT" in candidate.upper():
            optimized_sql = candidate
            logger.warning("Extracted SQL from raw response (no code fences)")
        else:
            logger.error("Failed to extract SQL!")
            (AUDIT_DIR / "07_optimized.sql").write_text("-- EXTRACTION FAILED --")
            sys.exit(1)

(AUDIT_DIR / "07_optimized.sql").write_text(optimized_sql)
logger.info(f"Extracted SQL: {len(optimized_sql)} chars")

# ── Validate (3x runs) ───────────────────────────────────────
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
            return {"error": str(e), "times": [t * 1000 for t in times]}
    avg = sum(times[1:]) / len(times[1:]) if len(times) > 1 else times[0]
    return {"avg_ms": avg * 1000, "times_ms": [t * 1000 for t in times], "row_count": row_count}

conn = duckdb.connect(DB_PATH, read_only=True)
try:
    logger.info("Validating original...")
    orig = run_timed(conn, original_sql)
    if "error" in orig:
        validation = {"status": "ERROR", "error": f"Original failed: {orig['error']}"}
    else:
        logger.info(f"Original: {orig['avg_ms']:.1f}ms")

        logger.info("Validating optimized...")
        opt = run_timed(conn, optimized_sql)
        if "error" in opt:
            validation = {"status": "ERROR", "error": f"Optimized failed: {opt['error']}",
                         "original_ms": round(orig["avg_ms"], 1)}
        else:
            logger.info(f"Optimized: {opt['avg_ms']:.1f}ms")

            rows_match = orig["row_count"] == opt["row_count"]
            speedup = orig["avg_ms"] / opt["avg_ms"] if opt["avg_ms"] > 0 else 0

            if not rows_match:
                status = "WRONG_RESULTS"
            elif speedup >= 1.10:
                status = "WIN"
            elif speedup >= 0.95:
                status = "PASS"
            else:
                status = "REGRESSION"

            validation = {
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

(AUDIT_DIR / "08_validation.json").write_text(json.dumps(validation, indent=2))

status = validation.get("status", "ERROR")
speedup = validation.get("speedup", 0)
logger.info(f"RESULT: {status} — {speedup}x")
logger.info(f"  Original: {validation.get('original_ms', '?')}ms")
logger.info(f"  Optimized: {validation.get('optimized_ms', '?')}ms")
if validation.get("error"):
    logger.info(f"  Error: {validation['error']}")
