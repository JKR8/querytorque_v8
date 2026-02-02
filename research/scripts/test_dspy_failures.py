#!/usr/bin/env python3
"""
Re-test failed queries with tuned DeepSeek constraints.
Tests if model-specific tuning fixes the 18 failures.
Runs queries in PARALLEL for speed.
"""

import os
import sys
import re
import json
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

if not os.getenv("DEEPSEEK_API_KEY"):
    print("ERROR: Set DEEPSEEK_API_KEY")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import dspy
import duckdb

# ============================================================
# Config
# ============================================================
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
PROMPTS_DIR = Path("research/prompts/batch")
MAX_RETRIES = 2
MODEL_NAME = "deepseek"
DB_NAME = "duckdb"

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = Path(f"research/experiments/dspy_runs/failures_{timestamp}")
output_dir.mkdir(parents=True, exist_ok=True)

# The 18 failed queries from baseline run
FAILED_QUERIES = [
    "q2", "q11", "q13", "q14", "q30", "q33", "q35", "q38",
    "q48", "q49", "q53", "q56", "q59", "q70", "q77", "q81",
    "q93", "q94"
]

print(f"DSPy Failed Queries Re-test - With Tuned Constraints")
print(f"=" * 60)
print(f"Output: {output_dir}")
print(f"Queries: {len(FAILED_QUERIES)} failed queries")
print(f"Model: {MODEL_NAME} (with constraints)")
print(f"Database: {DB_NAME}")
print(f"Max retries: {MAX_RETRIES}")

# ============================================================
# Setup
# ============================================================
print(f"\nConfiguring DeepSeek with tuned constraints...")
lm = dspy.LM(
    "openai/deepseek-chat",
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    api_base="https://api.deepseek.com"
)
dspy.configure(lm=lm)

from qt_sql.optimization.dspy_optimizer import (
    ValidatedOptimizationPipeline,
    create_duckdb_validator,
    load_model_config,
    build_system_prompt,
)

# Show loaded constraints
model_config = load_model_config(MODEL_NAME)
if model_config.get("constraints"):
    print(f"\nLoaded {len(model_config['constraints'])} constraints:")
    for c in model_config['constraints']:
        print(f"  - {c['id']}: {c['text'][:60]}...")

# Show the full prompt suffix
prompt = build_system_prompt(MODEL_NAME, DB_NAME)
print(f"\nPrompt suffix ({len(prompt)} chars):")
print("-" * 40)
print(prompt[:500])
if len(prompt) > 500:
    print("...")
print("-" * 40)

validator = create_duckdb_validator(SAMPLE_DB)
pipeline = ValidatedOptimizationPipeline(
    validator_fn=validator,
    max_retries=MAX_RETRIES,
    model_name=MODEL_NAME,
    db_name=DB_NAME
)

print(f"\nConnecting to database...")
conn = duckdb.connect(SAMPLE_DB, read_only=True)

# ============================================================
# Helpers
# ============================================================
def extract_from_prompt(prompt_text):
    sql_match = re.search(r'```sql\n(.*?)```', prompt_text, re.DOTALL)
    sql = sql_match.group(1).strip() if sql_match else ""
    plan_match = re.search(r'\*\*Operators by cost:\*\*(.*?)(?=\*\*Table scans|\n---)', prompt_text, re.DOTALL)
    plan = plan_match.group(1).strip() if plan_match else ""
    scans_match = re.search(r'\*\*Table scans:\*\*(.*?)(?=\n---)', prompt_text, re.DOTALL)
    scans = scans_match.group(1).strip() if scans_match else ""
    return sql, plan, scans

def benchmark(sql, runs=3):
    """3 runs, discard first, average 2-3."""
    times = []
    rows = 0
    for _ in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        times.append(time.perf_counter() - start)
        rows = len(result)
    return sum(times[1:]) / 2, rows

# ============================================================
# Run failed queries IN PARALLEL
# ============================================================
PARALLEL_WORKERS = 6  # DeepSeek allows decent parallelism

def process_query(qname):
    """Process a single query - returns result dict."""
    prompt_file = PROMPTS_DIR / f"{qname}_prompt.txt"

    if not prompt_file.exists():
        return {"query": qname, "status": "skip", "reason": "no prompt"}

    sql, plan, scans = extract_from_prompt(prompt_file.read_text())
    if not sql:
        return {"query": qname, "status": "skip", "reason": "no SQL"}

    # Each thread needs its own DB connection
    thread_conn = duckdb.connect(SAMPLE_DB, read_only=True)

    def thread_benchmark(sql_str, runs=3):
        times = []
        rows = 0
        for _ in range(runs):
            start = time.perf_counter()
            result = thread_conn.execute(sql_str).fetchall()
            times.append(time.perf_counter() - start)
            rows = len(result)
        return sum(times[1:]) / 2, rows

    # Benchmark original
    try:
        orig_time, orig_rows = thread_benchmark(sql)
    except Exception as e:
        thread_conn.close()
        return {"query": qname, "status": "orig_error", "error": str(e)}

    # Run DSPy with tuned constraints
    try:
        result = pipeline(query=sql, plan=plan, rows=scans)
    except Exception as e:
        thread_conn.close()
        return {"query": qname, "status": "dspy_error", "error": str(e)}

    # Check result
    if result.correct:
        try:
            opt_time, _ = thread_benchmark(result.optimized_sql)
            speedup = orig_time / opt_time if opt_time > 0 else 1.0
            thread_conn.close()

            # Save files
            qdir = output_dir / qname
            qdir.mkdir(exist_ok=True)
            (qdir / "original.sql").write_text(sql)
            (qdir / "optimized.sql").write_text(result.optimized_sql)
            (qdir / "rationale.txt").write_text(result.rationale)

            return {
                "query": qname,
                "status": "fixed",
                "original_time": round(orig_time, 4),
                "optimized_time": round(opt_time, 4),
                "speedup": round(speedup, 2),
                "attempts": result.attempts,
                "rows": orig_rows
            }
        except Exception as e:
            thread_conn.close()
            return {"query": qname, "status": "bench_error", "error": str(e)}
    else:
        thread_conn.close()
        # Save failed attempt
        qdir = output_dir / qname
        qdir.mkdir(exist_ok=True)
        (qdir / "original.sql").write_text(sql)
        (qdir / "failed.sql").write_text(result.optimized_sql)
        (qdir / "error.txt").write_text(result.error or "Unknown")

        return {
            "query": qname,
            "status": "still_failed",
            "attempts": result.attempts,
            "error": result.error
        }


results = []
stats = {"fixed": 0, "still_failed": 0, "error": 0, "skip": 0}

print(f"\n{'='*60}")
print(f"Re-testing {len(FAILED_QUERIES)} failed queries ({PARALLEL_WORKERS} parallel workers)...")
print(f"{'='*60}\n")

with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
    futures = {executor.submit(process_query, q): q for q in FAILED_QUERIES}

    for future in as_completed(futures):
        qname = futures[future]
        try:
            result = future.result()
            results.append(result)

            status = result["status"]
            if status == "fixed":
                stats["fixed"] += 1
                print(f"[{qname.upper()}] FIXED! {result['original_time']:.3f}s -> {result['optimized_time']:.3f}s ({result['speedup']:.2f}x)")
            elif status == "still_failed":
                stats["still_failed"] += 1
                print(f"[{qname.upper()}] STILL FAILED after {result['attempts']} attempts")
            elif status == "skip":
                stats["skip"] += 1
            else:
                stats["error"] += 1
                print(f"[{qname.upper()}] ERROR: {result.get('error', 'Unknown')[:50]}")
        except Exception as e:
            stats["error"] += 1
            print(f"[{qname.upper()}] EXCEPTION: {e}")

conn.close()

# ============================================================
# Summary
# ============================================================
print(f"\n{'='*60}")
print(f"RESULTS - Tuned Constraints Test")
print(f"{'='*60}")

print(f"\nOf {len(FAILED_QUERIES)} previously failed queries:")
print(f"  FIXED:        {stats['fixed']}")
print(f"  Still failed: {stats['still_failed']}")
print(f"  Errors:       {stats['error']}")

if stats['fixed'] > 0:
    print(f"\nFixed queries:")
    for r in results:
        if r.get("status") == "fixed":
            print(f"  {r['query']}: {r.get('speedup', 'N/A')}x speedup")

if stats['still_failed'] > 0:
    print(f"\nStill failing:")
    for r in results:
        if r.get("status") == "still_failed":
            err = r.get('error', 'Unknown')[:60]
            print(f"  {r['query']}: {err}...")

# Save results
with open(output_dir / "results.json", "w") as f:
    json.dump(results, f, indent=2)

with open(output_dir / "summary.txt", "w") as f:
    f.write(f"DSPy Failed Queries Re-test - With Tuned Constraints\n")
    f.write(f"Date: {timestamp}\n")
    f.write(f"Model: {MODEL_NAME}\n")
    f.write(f"Database: {DB_NAME}\n\n")
    f.write(f"Results:\n")
    f.write(f"  Fixed: {stats['fixed']}\n")
    f.write(f"  Still failed: {stats['still_failed']}\n")
    f.write(f"  Errors: {stats['error']}\n")

print(f"\nResults saved to: {output_dir}")
