#!/usr/bin/env python3
"""End-to-end patch mode test — sends a real query through the full pipeline.

Logs every step: IR build, prompt generation, LLM call, response parsing,
patch application, final SQL output.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 test_patch_e2e.py
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Setup logging ──────────────────────────────────────────────────────────
LOG_DIR = Path("test_patch_logs")
LOG_DIR.mkdir(exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"patch_e2e_{ts}.log"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("patch_e2e")

# ── Configuration ──────────────────────────────────────────────────────────
BENCH_DIR = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76")
QUERY_ID = "query001_multi_i1"
DIALECT = "postgres"

def banner(msg: str):
    print(f"\n{'='*70}")
    print(f"  {msg}")
    print(f"{'='*70}\n")

def section(msg: str):
    print(f"\n{'─'*50}")
    print(f"  {msg}")
    print(f"{'─'*50}")

# ── Step 1: Load query SQL ─────────────────────────────────────────────────
banner("STEP 1: Load Query SQL")

sql_path = BENCH_DIR / "queries" / f"{QUERY_ID}.sql"
if not sql_path.exists():
    logger.error(f"Query file not found: {sql_path}")
    sys.exit(1)

original_sql = sql_path.read_text().strip()
logger.info(f"Loaded {QUERY_ID}: {len(original_sql)} chars, {original_sql.count(chr(10))+1} lines")
print(f"  Query: {QUERY_ID}")
print(f"  Length: {len(original_sql)} chars")

# ── Step 2: Build IR ──────────────────────────────────────────────────────
banner("STEP 2: Build Script IR")

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect

t0 = time.time()
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
ir_build_ms = (time.time() - t0) * 1000

logger.info(f"IR built in {ir_build_ms:.1f}ms: {len(script_ir.statements)} statements")
print(f"  Statements: {len(script_ir.statements)}")
print(f"  Build time: {ir_build_ms:.1f}ms")

# ── Step 3: Render IR Node Map ─────────────────────────────────────────────
banner("STEP 3: Render IR Node Map")

ir_node_map = render_ir_node_map(script_ir)
logger.info(f"IR node map:\n{ir_node_map}")
print(ir_node_map)

# Save IR node map
(LOG_DIR / f"ir_node_map_{ts}.txt").write_text(ir_node_map)

# ── Step 4: Build Worker Prompt (patch mode) ───────────────────────────────
banner("STEP 4: Build Worker Prompt (patch mode)")

from qt_sql.prompts.worker import build_worker_prompt

# We need a minimal BriefingWorker/BriefingShared to test
# Load from the prepared prompt context if available, or build mock
prepared_dir = sorted((BENCH_DIR / "prepared").iterdir())[-1] if (BENCH_DIR / "prepared").exists() else None

# Build a minimal worker prompt with mock briefing data
class MockBriefing:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

# Extract output columns from the query (approximate)
import sqlglot
try:
    parsed = sqlglot.parse_one(original_sql, dialect="postgres")
    output_columns = []
    for expr in parsed.find_all(sqlglot.exp.Column):
        if expr.parent and isinstance(expr.parent, sqlglot.exp.Select):
            alias = expr.alias or expr.name
            if alias and alias not in output_columns:
                output_columns.append(alias)
    if not output_columns:
        # Fallback: just get first SELECT's column names
        for sel in parsed.find_all(sqlglot.exp.Select):
            for expr in sel.expressions:
                name = expr.alias or (expr.name if hasattr(expr, 'name') else str(expr))
                if name and name not in output_columns:
                    output_columns.append(name)
            break  # Only first (outer) SELECT
except Exception as e:
    logger.warning(f"Column extraction failed: {e}")
    output_columns = ["*"]

logger.info(f"Output columns: {output_columns}")

shared = MockBriefing(
    semantic_contract="Preserve exact row count, column names, ordering, and all aggregate values.",
    current_plan_gap="",
    goal_violations="",
    active_constraints="",
    regression_warnings="",
)

worker = MockBriefing(
    approach="Pre-compute per-store average return amounts in a CTE to avoid correlated subquery re-execution.",
    strategy="decorrelate_subquery",
    target_query_map="",
    target_logical_tree="",
    node_contracts="",
    hazard_flags="Do not change column aliases in the final SELECT.",
    example_adaptation="",
    examples=[],
)

worker_prompt = build_worker_prompt(
    worker_briefing=worker,
    shared_briefing=shared,
    examples=[],
    original_sql=original_sql,
    output_columns=output_columns,
    dialect=DIALECT,
    engine_version="14.3",
    patch=True,
    ir_node_map=ir_node_map,
)

prompt_lines = worker_prompt.count('\n') + 1
logger.info(f"Worker prompt: {len(worker_prompt)} chars, {prompt_lines} lines")
print(f"  Prompt length: {len(worker_prompt)} chars, {prompt_lines} lines")

# Save prompt
prompt_path = LOG_DIR / f"worker_prompt_{ts}.txt"
prompt_path.write_text(worker_prompt)
print(f"  Saved to: {prompt_path}")

# ── Step 5: Send to LLM ───────────────────────────────────────────────────
banner("STEP 5: Send to LLM")

from qt_shared.llm import create_llm_client

llm_client = create_llm_client()
logger.info(f"LLM client: provider={os.environ.get('QT_LLM_PROVIDER', '?')}, model={os.environ.get('QT_LLM_MODEL', '?')}")

print(f"  Provider: {os.environ.get('QT_LLM_PROVIDER', '?')}")
print(f"  Model: {os.environ.get('QT_LLM_MODEL', '?')}")
print(f"  Sending prompt ({len(worker_prompt)} chars)...")

# Use cached response if available from previous run
cached_response = LOG_DIR / "llm_response_latest.txt"
if cached_response.exists() and "--no-cache" not in sys.argv:
    llm_response = cached_response.read_text()
    llm_elapsed = 0.0
    logger.info(f"Using cached LLM response: {len(llm_response)} chars")
    print(f"  CACHED response: {len(llm_response)} chars")
else:
    t_llm = time.time()
    try:
        llm_response = llm_client.analyze(worker_prompt)
        llm_elapsed = time.time() - t_llm
        logger.info(f"LLM response: {len(llm_response)} chars in {llm_elapsed:.1f}s")
        print(f"  Response: {len(llm_response)} chars in {llm_elapsed:.1f}s")
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        print(f"  ERROR: {e}")
        sys.exit(1)
    # Cache for reruns
    cached_response.write_text(llm_response)

# Save raw response
response_path = LOG_DIR / f"llm_response_{ts}.txt"
response_path.write_text(llm_response)
print(f"  Saved to: {response_path}")

# Print response (truncated for display)
section("Raw LLM Response (first 2000 chars)")
print(llm_response[:2000])
if len(llm_response) > 2000:
    print(f"\n  ... ({len(llm_response) - 2000} more chars)")

# ── Step 6: Parse Response ─────────────────────────────────────────────────
banner("STEP 6: Parse Response")

from qt_sql.sql_rewriter import SQLRewriter

rewriter = SQLRewriter(original_sql, dialect=DIALECT, script_ir=script_ir)

# Step 6a: Extract JSON
json_str = rewriter.parser.extract_json(llm_response)
if json_str:
    logger.info(f"Extracted JSON: {len(json_str)} chars")
    print(f"  JSON extracted: {len(json_str)} chars")

    # Save extracted JSON
    json_path = LOG_DIR / f"extracted_json_{ts}.json"
    json_path.write_text(json_str)

    # Step 6b: Detect format
    fmt = rewriter.parser.detect_format(json_str)
    logger.info(f"Detected format: {fmt}")
    print(f"  Format detected: {fmt}")

    if fmt == "patch":
        # Parse and display the patch plan
        try:
            patch_data = json.loads(json_str)
            plan_id = patch_data.get("plan_id", "?")
            steps = patch_data.get("steps", [])
            logger.info(f"Patch plan: {plan_id}, {len(steps)} steps")
            print(f"  Plan ID: {plan_id}")
            print(f"  Steps: {len(steps)}")

            for i, step in enumerate(steps):
                op = step.get("op", "?")
                target = step.get("target", {})
                desc = step.get("description", "")
                print(f"    [{i+1}] op={op} target={json.dumps(target)} — {desc}")
                logger.info(f"  Step {i+1}: op={op} target={json.dumps(target)} desc={desc}")
                # Log payload (truncated)
                payload = step.get("payload", {})
                for k, v in payload.items():
                    v_str = str(v)[:200]
                    logger.info(f"    payload.{k}: {v_str}")
                    print(f"        {k}: {v_str}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {e}")
            print(f"  JSON parse ERROR: {e}")
    else:
        print(f"  WARNING: Expected 'patch' format, got '{fmt}'")
        logger.warning(f"Non-patch format: {fmt}")
else:
    logger.error("No JSON found in response")
    print("  ERROR: No JSON found in LLM response")

# ── Step 7: Apply Rewrite ─────────────────────────────────────────────────
banner("STEP 7: Apply Rewrite (full pipeline)")

t_apply = time.time()
result = rewriter.apply_response(llm_response)
apply_ms = (time.time() - t_apply) * 1000

logger.info(f"Rewrite result: success={result.success}, transform={result.transform}")
print(f"  Success: {result.success}")
print(f"  Transform: {result.transform}")
print(f"  Apply time: {apply_ms:.1f}ms")

if result.error:
    logger.error(f"Rewrite error: {result.error}")
    print(f"  Error: {result.error}")

if result.warnings:
    for w in result.warnings:
        logger.warning(f"Warning: {w}")
        print(f"  Warning: {w}")

if result.success:
    opt_sql = result.optimized_sql
    logger.info(f"Optimized SQL: {len(opt_sql)} chars")

    # Save optimized SQL
    opt_path = LOG_DIR / f"optimized_sql_{ts}.sql"
    opt_path.write_text(opt_sql)
    print(f"  Optimized SQL: {len(opt_sql)} chars")
    print(f"  Saved to: {opt_path}")

    section("Optimized SQL")
    print(opt_sql[:3000])
    if len(opt_sql) > 3000:
        print(f"\n  ... ({len(opt_sql) - 3000} more chars)")

    # ── Step 8: Diff ───────────────────────────────────────────────────
    banner("STEP 8: SQL Diff")

    if opt_sql.strip() == original_sql.strip():
        print("  WARNING: Optimized SQL is identical to original!")
        logger.warning("No changes detected in optimized SQL")
    else:
        # Line-by-line diff summary
        orig_lines = original_sql.strip().splitlines()
        opt_lines = opt_sql.strip().splitlines()
        print(f"  Original: {len(orig_lines)} lines")
        print(f"  Optimized: {len(opt_lines)} lines")
        print(f"  Delta: {len(opt_lines) - len(orig_lines):+d} lines")

        # Show actual unified diff
        import difflib
        diff = list(difflib.unified_diff(
            orig_lines, opt_lines,
            fromfile="original.sql", tofile="optimized.sql",
            lineterm="",
        ))
        if diff:
            diff_text = "\n".join(diff)
            (LOG_DIR / f"sql_diff_{ts}.diff").write_text(diff_text)
            section("Unified Diff")
            print(diff_text[:3000])
            if len(diff_text) > 3000:
                print(f"\n  ... ({len(diff_text) - 3000} more chars)")
else:
    section("Fallback: checking if DAP/legacy format was used")
    # The response may have used DAP instead of patch format
    if json_str:
        fmt = rewriter.parser.detect_format(json_str)
        print(f"  Detected format: {fmt}")
        if fmt == "dap":
            print("  LLM produced DAP format instead of patch — graceful fallback working")
        elif fmt == "rewrite_sets":
            print("  LLM produced legacy rewrite_sets format — graceful fallback working")

    # Try raw SQL extraction
    import re
    sql_match = re.search(r'```sql\s*\n(.*?)```', llm_response, re.DOTALL)
    if sql_match:
        print(f"  Raw SQL block found: {len(sql_match.group(1))} chars")

# ── Summary ────────────────────────────────────────────────────────────────
banner("SUMMARY")

print(f"  Query:        {QUERY_ID}")
print(f"  IR build:     {ir_build_ms:.1f}ms")
print(f"  Prompt:       {len(worker_prompt)} chars")
print(f"  LLM time:     {llm_elapsed:.1f}s")
print(f"  Response:     {len(llm_response)} chars")
print(f"  Format:       {fmt if json_str else 'no JSON'}")
print(f"  Patch apply:  {'SUCCESS' if result.success else 'FAILED'}")
print(f"  Transform:    {result.transform or 'n/a'}")
print(f"  Log file:     {log_file}")
print(f"  All artifacts: {LOG_DIR}/")
print()
