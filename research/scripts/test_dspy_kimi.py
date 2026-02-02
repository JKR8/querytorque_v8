#!/usr/bin/env python3
"""
DSPy Benchmark - Kimi K2.5 via OpenRouter
Uses DAG-based optimization (node-level rewrites)

Usage:
    .venv/bin/python research/scripts/test_dspy_kimi.py --queries q1
    .venv/bin/python research/scripts/test_dspy_kimi.py --queries q1,q15,q39
    .venv/bin/python research/scripts/test_dspy_kimi.py  # all 99 queries
"""

import argparse
import os
import sys
import re
import json
import time
from datetime import datetime
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(description="DSPy Kimi K2.5 Benchmark (DAG mode)")
    parser.add_argument("--queries", type=str, default=None,
                        help="Comma-separated query IDs (e.g., q1,q15)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging")
    return parser.parse_args()

args = parse_args()

# ============================================================
# API Configuration
# ============================================================
API_KEY = os.getenv("OPENROUTER_API_KEY")
if not API_KEY:
    key_file = Path("openrouter.txt")
    if key_file.exists():
        API_KEY = key_file.read_text().strip()
    else:
        print("ERROR: Set OPENROUTER_API_KEY or create openrouter.txt")
        sys.exit(1)

API_BASE = "https://openrouter.ai/api/v1"
MODEL_NAME = "moonshotai/kimi-k2.5"
PROVIDER_NAME = "kimi-k2.5"

print("=" * 60)
print(f"DSPy DAG Benchmark - {PROVIDER_NAME}")
print("=" * 60)

# ============================================================
# Setup
# ============================================================
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import dspy
import duckdb

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
PROMPTS_DIR = Path("research/prompts/batch")
MAX_RETRIES = 2

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = Path(f"research/experiments/dspy_runs/kimi_dag_{timestamp}")
output_dir.mkdir(parents=True, exist_ok=True)

# Query selection
if args.queries:
    QUERIES = [q.strip() for q in args.queries.split(",")]
else:
    QUERIES = [f"q{i}" for i in range(1, 100)]

print(f"Output: {output_dir}")
print(f"Queries: {len(QUERIES)} queries")
print(f"Model: {MODEL_NAME}")
print(f"Mode: DAG (node-level rewrites)")

# ============================================================
# Configure DSPy with Kimi K2.5 via OpenRouter
# ============================================================
print(f"\nConfiguring DSPy with {PROVIDER_NAME}...")

lm = dspy.LM(
    f"openai/{MODEL_NAME}",
    api_key=API_KEY,
    api_base=API_BASE,
    extra_headers={
        "HTTP-Referer": "https://querytorque.com",
        "X-Title": "QueryTorque Benchmark"
    }
)
dspy.configure(lm=lm)

# ============================================================
# Import DAG Pipeline
# ============================================================
from qt_sql.optimization.dspy_optimizer import (
    DagOptimizationPipeline,
    create_duckdb_validator,
)

validator = create_duckdb_validator(SAMPLE_DB)
pipeline = DagOptimizationPipeline(
    validator_fn=validator,
    max_retries=MAX_RETRIES,
    model_name=PROVIDER_NAME,
    db_name="duckdb"
)

# ============================================================
# Database Connection
# ============================================================
print(f"Connecting to database...")
conn = duckdb.connect(SAMPLE_DB, read_only=True)
conn.execute("SELECT 1").fetchall()
print(f"Connected!")

# ============================================================
# Helpers
# ============================================================
def extract_from_prompt(prompt_text):
    sql_match = re.search(r'```sql\n(.*?)```', prompt_text, re.DOTALL)
    sql = sql_match.group(1).strip() if sql_match else ""
    plan_match = re.search(r'\*\*Operators by cost:\*\*(.*?)(?=\*\*Table scans|\n---)', prompt_text, re.DOTALL)
    plan = plan_match.group(1).strip() if plan_match else ""
    return sql, plan

def benchmark(sql, runs=3):
    """3 runs, discard first, average 2-3."""
    times = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        times.append(time.perf_counter() - start)
    return sum(times[1:]) / 2, len(result), result

def validate_results(orig_result, opt_result):
    """Check semantic equivalence."""
    orig_set = set(tuple(r) for r in orig_result)
    opt_set = set(tuple(r) for r in opt_result)
    return orig_set == opt_set

# ============================================================
# Run Benchmark
# ============================================================
results = []
stats = {"success": 0, "failed": 0, "error": 0, "skipped": 0}
total_llm_time = 0.0

print(f"\n{'='*60}")
print(f"Running {len(QUERIES)} queries (DAG mode)...")
print(f"{'='*60}\n")

for idx, qname in enumerate(QUERIES, 1):
    prompt_file = PROMPTS_DIR / f"{qname}_prompt.txt"

    if not prompt_file.exists():
        print(f"[{idx:02d}/{len(QUERIES)}] {qname.upper()} SKIP - no prompt")
        stats["skipped"] += 1
        continue

    sql, plan = extract_from_prompt(prompt_file.read_text())
    if not sql:
        print(f"[{idx:02d}/{len(QUERIES)}] {qname.upper()} SKIP - no SQL")
        stats["skipped"] += 1
        continue

    print(f"[{idx:02d}/{len(QUERIES)}] {qname.upper()} ", end="", flush=True)

    # Benchmark original
    try:
        orig_time, orig_rows, orig_result = benchmark(sql)
    except Exception as e:
        print(f"ORIG ERROR: {str(e)[:50]}")
        stats["error"] += 1
        results.append({"query": qname, "status": "orig_error", "error": str(e)})
        continue

    # Run DAG optimization
    try:
        llm_start = time.time()
        dag_result = pipeline(sql=sql, plan=plan)
        llm_time = time.time() - llm_start
        total_llm_time += llm_time

        opt_sql = dag_result.optimized_sql
        explanation = dag_result.explanation
        rewrites = dag_result.rewrites
    except Exception as e:
        print(f"LLM ERROR: {str(e)[:50]}")
        stats["error"] += 1
        results.append({"query": qname, "status": "llm_error", "error": str(e)})
        continue

    # Check if DAG validation already passed
    if dag_result.correct:
        try:
            opt_time, opt_rows, opt_result = benchmark(opt_sql)
            speedup = orig_time / opt_time if opt_time > 0 else 1.0

            status = "pass"
            stats["success"] += 1
            icon = "✓" if speedup >= 1.2 else "~"
            print(f"{icon} {orig_time*1000:.1f}ms → {opt_time*1000:.1f}ms ({speedup:.2f}x) LLM={llm_time:.1f}s rewrites={len(rewrites)}")

            results.append({
                "query": qname,
                "status": status,
                "original_time_ms": round(orig_time * 1000, 2),
                "optimized_time_ms": round(opt_time * 1000, 2),
                "speedup": round(speedup, 2),
                "correct": True,
                "llm_time_s": round(llm_time, 2),
                "rewrites": list(rewrites.keys()) if rewrites else [],
                "attempts": dag_result.attempts
            })

            # Save outputs
            qdir = output_dir / qname
            qdir.mkdir(exist_ok=True)
            (qdir / "original.sql").write_text(sql)
            (qdir / "optimized.sql").write_text(opt_sql)
            (qdir / "explanation.txt").write_text(explanation)
            (qdir / "rewrites.json").write_text(json.dumps(rewrites, indent=2))

        except Exception as e:
            print(f"BENCH ERROR: {str(e)[:50]}")
            stats["error"] += 1
            results.append({"query": qname, "status": "bench_error", "error": str(e)})
    else:
        # Validation failed
        print(f"✗ FAILED after {dag_result.attempts} attempts - {dag_result.error[:50] if dag_result.error else 'unknown'}")
        stats["failed"] += 1
        results.append({
            "query": qname,
            "status": "validation_failed",
            "correct": False,
            "llm_time_s": round(llm_time, 2),
            "attempts": dag_result.attempts,
            "error": dag_result.error
        })

        # Save failed attempt
        qdir = output_dir / qname
        qdir.mkdir(exist_ok=True)
        (qdir / "original.sql").write_text(sql)
        (qdir / "failed.sql").write_text(opt_sql)
        (qdir / "error.txt").write_text(dag_result.error or "Unknown")

conn.close()

# ============================================================
# Summary
# ============================================================
print(f"\n{'='*60}")
print(f"SUMMARY - {PROVIDER_NAME} (DAG mode)")
print(f"{'='*60}")

print(f"\nStats:")
print(f"  Success (validated): {stats['success']}")
print(f"  Failed validation:   {stats['failed']}")
print(f"  Errors:              {stats['error']}")
print(f"  Skipped:             {stats['skipped']}")

total_processed = stats['success'] + stats['failed'] + stats['error']
if total_processed > 0:
    print(f"\nTotal LLM time: {total_llm_time:.1f}s ({total_llm_time/60:.1f}min)")
    print(f"Avg LLM time per query: {total_llm_time/total_processed:.1f}s")

# Top speedups
successful = [r for r in results if r.get("correct") and r.get("speedup", 0) >= 1.1]
successful.sort(key=lambda x: x["speedup"], reverse=True)

if successful:
    print(f"\nTop speedups (validated, ≥1.1x):")
    print(f"{'Query':<8} {'Original':<12} {'Optimized':<12} {'Speedup':<10} {'LLM Time':<10} {'Rewrites'}")
    print("-" * 70)
    for r in successful[:15]:
        rewrites_str = ",".join(r.get("rewrites", []))[:15]
        print(f"{r['query']:<8} {r['original_time_ms']:<12.1f} {r['optimized_time_ms']:<12.1f} {r['speedup']:<10.2f}x {r['llm_time_s']:<10.1f}s {rewrites_str}")

# Wins count
wins = len([r for r in results if r.get("correct") and r.get("speedup", 0) >= 1.2])
regressions = len([r for r in results if r.get("correct") and r.get("speedup", 0) < 1.0])

print(f"\nWins (≥1.2x): {wins}")
print(f"Regressions (<1.0x): {regressions}")

# Calculate averages
validated = [r for r in results if r.get("correct")]
if validated:
    avg_speedup = sum(r["speedup"] for r in validated) / len(validated)
    print(f"Average speedup (validated): {avg_speedup:.2f}x")

# Save results
with open(output_dir / "results.json", "w") as f:
    json.dump(results, f, indent=2)

with open(output_dir / "summary.txt", "w") as f:
    f.write(f"DSPy DAG Benchmark - {PROVIDER_NAME}\n")
    f.write(f"Date: {timestamp}\n")
    f.write(f"Model: {MODEL_NAME}\n")
    f.write(f"Mode: DAG (node-level rewrites)\n")
    f.write(f"API: {API_BASE}\n\n")
    f.write(f"Success: {stats['success']}\n")
    f.write(f"Failed: {stats['failed']}\n")
    f.write(f"Errors: {stats['error']}\n")
    f.write(f"Skipped: {stats['skipped']}\n\n")
    if total_processed > 0:
        f.write(f"Total LLM time: {total_llm_time:.1f}s\n")
    if validated:
        f.write(f"Average speedup: {avg_speedup:.2f}x\n")
    f.write(f"Wins (≥1.2x): {wins}\n")
    f.write(f"Regressions (<1.0x): {regressions}\n")

print(f"\nResults saved to: {output_dir}")
