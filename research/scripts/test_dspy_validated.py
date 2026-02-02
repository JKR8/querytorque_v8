#!/usr/bin/env python3
"""
Test DSPy with Validation and Retries

Tests on Q2 (which broke before) to see if retries fix it.
"""

import os
import sys
import re
import time
from pathlib import Path

if not os.getenv("DEEPSEEK_API_KEY"):
    print("ERROR: Set DEEPSEEK_API_KEY")
    sys.exit(1)

# Add package to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import dspy
import duckdb

# ============================================================
# Setup
# ============================================================
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
PROMPTS_DIR = Path("research/prompts/batch")

print("DSPy Validated Optimization Test")
print("=" * 50)

# Configure LLM
print("\nConfiguring DeepSeek...")
lm = dspy.LM(
    "openai/deepseek-chat",
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    api_base="https://api.deepseek.com"
)
dspy.configure(lm=lm)

# ============================================================
# Import our validated pipeline
# ============================================================
from qt_sql.optimization.dspy_optimizer import (
    ValidatedOptimizationPipeline,
    create_duckdb_validator,
    OptimizationResult
)

# ============================================================
# Helper
# ============================================================
def extract_from_prompt(prompt_text):
    sql_match = re.search(r'```sql\n(.*?)```', prompt_text, re.DOTALL)
    sql = sql_match.group(1).strip() if sql_match else ""
    plan_match = re.search(r'\*\*Operators by cost:\*\*(.*?)(?=\*\*Table scans|\n---)', prompt_text, re.DOTALL)
    plan = plan_match.group(1).strip() if plan_match else ""
    scans_match = re.search(r'\*\*Table scans:\*\*(.*?)(?=\n---)', prompt_text, re.DOTALL)
    scans = scans_match.group(1).strip() if scans_match else ""
    return sql, plan, scans

def benchmark(conn, sql, runs=3):
    """3 runs, discard first, average 2."""
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        times.append(time.perf_counter() - start)
    return sum(times[1:]) / 2, len(result)

# ============================================================
# Test Q2 with validation + retries
# ============================================================
QUERIES_TO_TEST = ["q2", "q3", "q15"]

print(f"\nTesting: {QUERIES_TO_TEST}")
print(f"Database: {SAMPLE_DB}")
print(f"Max retries: 2")

# Create validator
validator = create_duckdb_validator(SAMPLE_DB)

# Create pipeline with validation
pipeline = ValidatedOptimizationPipeline(
    validator_fn=validator,
    max_retries=2
)

# Connect for benchmarking
conn = duckdb.connect(SAMPLE_DB, read_only=True)

for qname in QUERIES_TO_TEST:
    print(f"\n{'='*50}")
    print(f"{qname.upper()} - With Validation + Retries")
    print('='*50)

    # Load prompt
    prompt_file = PROMPTS_DIR / f"{qname}_prompt.txt"
    if not prompt_file.exists():
        print(f"  Prompt not found")
        continue

    sql, plan, scans = extract_from_prompt(prompt_file.read_text())

    # Benchmark original
    orig_time, orig_rows = benchmark(conn, sql)
    print(f"  Original: {orig_time:.4f}s ({orig_rows} rows)")

    # Run validated optimization
    print(f"  Running DSPy with validation...")
    result: OptimizationResult = pipeline(query=sql, plan=plan, rows=scans)

    print(f"  Attempts: {result.attempts}")
    print(f"  Correct: {result.correct}")

    if result.error:
        print(f"  Error: {result.error}")

    if result.correct:
        # Benchmark optimized
        opt_time, opt_rows = benchmark(conn, result.optimized_sql)
        speedup = orig_time / opt_time if opt_time > 0 else 0
        print(f"  Optimized: {opt_time:.4f}s ({opt_rows} rows)")
        print(f"  Speedup: {speedup:.2f}x")
        print(f"\n  Rationale: {result.rationale[:200]}...")
    else:
        print(f"  FAILED after {result.attempts} attempts")
        print(f"  Last error: {result.error}")

conn.close()

print("\n" + "="*50)
print("DONE")
print("="*50)
