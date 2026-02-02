#!/usr/bin/env python3
"""
DSPy Batch Test - Run multiple TPC-DS queries

Tests Q2, Q3, Q7, Q15, Q23 with DSPy optimization
"""

import os
import sys
import json
import time
import re
from datetime import datetime
from pathlib import Path

if not os.getenv("DEEPSEEK_API_KEY"):
    print("ERROR: Set DEEPSEEK_API_KEY")
    sys.exit(1)

import dspy
import duckdb

# ============================================================
# Setup
# ============================================================
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
QUERIES_DIR = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
PROMPTS_DIR = Path("research/prompts/batch")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = Path(f"research/experiments/dspy_runs/batch_{timestamp}")
output_dir.mkdir(parents=True, exist_ok=True)

# Queries to test
QUERIES = ["q2", "q3", "q7", "q15", "q23"]

print(f"Output folder: {output_dir}")
print(f"Testing queries: {QUERIES}")

# ============================================================
# DSPy Setup
# ============================================================
class SQLOptimizer(dspy.Signature):
    """Optimize SQL query for better execution performance."""
    original_query: str = dspy.InputField(desc="The original SQL query to optimize")
    execution_plan: str = dspy.InputField(desc="Execution plan showing operator costs and row counts")
    table_scans: str = dspy.InputField(desc="Table scan info: table name, rows, filter status")
    optimized_query: str = dspy.OutputField(desc="The optimized SQL query with identical semantics")
    rationale: str = dspy.OutputField(desc="Why this optimization improves performance")

class OptimizationPipeline(dspy.Module):
    def __init__(self):
        super().__init__()
        self.optimizer = dspy.ChainOfThought(SQLOptimizer)

    def forward(self, query, plan, scans):
        return self.optimizer(original_query=query, execution_plan=plan, table_scans=scans)

print("\nConfiguring DeepSeek...")
lm = dspy.LM(
    "openai/deepseek-chat",
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    api_base="https://api.deepseek.com"
)
dspy.configure(lm=lm)

pipeline = OptimizationPipeline()

# ============================================================
# Database connection
# ============================================================
print(f"Connecting to {SAMPLE_DB}...")
conn = duckdb.connect(SAMPLE_DB, read_only=True)
conn.execute("SELECT 1").fetchall()  # Warm up

# ============================================================
# Helper functions
# ============================================================
def extract_from_prompt(prompt_text):
    """Extract SQL, plan, and scans from the batch prompt."""
    # Extract SQL
    sql_match = re.search(r'```sql\n(.*?)```', prompt_text, re.DOTALL)
    sql = sql_match.group(1).strip() if sql_match else ""

    # Extract execution plan
    plan_match = re.search(r'\*\*Operators by cost:\*\*(.*?)(?=\*\*Table scans|\n---)', prompt_text, re.DOTALL)
    plan = plan_match.group(1).strip() if plan_match else ""

    # Extract table scans
    scans_match = re.search(r'\*\*Table scans:\*\*(.*?)(?=\n---)', prompt_text, re.DOTALL)
    scans = scans_match.group(1).strip() if scans_match else ""

    return sql, plan, scans

def benchmark_query(sql, runs=3):
    """Run query and return avg time and row count.

    Methodology: 3 runs, discard first (warmup), average remaining 2.
    """
    times = []
    rows = None
    result = None
    for i in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        rows = len(result)
    # Discard first run (cache warmup), average remaining
    avg_time = sum(times[1:]) / len(times[1:]) if len(times) > 1 else times[0]
    return avg_time, rows, result

def compare_results(orig_result, opt_result):
    """Check if results match."""
    return sorted([tuple(r) for r in orig_result]) == sorted([tuple(r) for r in opt_result])

# ============================================================
# Run tests
# ============================================================
results = []

for qname in QUERIES:
    print(f"\n{'='*60}")
    print(f"Testing {qname.upper()}")
    print('='*60)

    # Read prompt
    prompt_file = PROMPTS_DIR / f"{qname}_prompt.txt"
    if not prompt_file.exists():
        print(f"  Prompt file not found: {prompt_file}")
        continue

    prompt_text = prompt_file.read_text()
    sql, plan, scans = extract_from_prompt(prompt_text)

    if not sql:
        print(f"  Could not extract SQL from prompt")
        continue

    print(f"  Original SQL: {len(sql)} chars")

    # Benchmark original
    print("  Running original...")
    try:
        orig_time, orig_rows, orig_result = benchmark_query(sql)
        print(f"    Time: {orig_time:.3f}s, Rows: {orig_rows}")
    except Exception as e:
        print(f"    ERROR: {e}")
        continue

    # Run DSPy optimization
    print("  Running DSPy optimization...")
    try:
        dspy_result = pipeline(query=sql, plan=plan, scans=scans)
        opt_sql = dspy_result.optimized_query
        rationale = dspy_result.rationale
        print(f"    Got optimized query: {len(opt_sql)} chars")
    except Exception as e:
        print(f"    DSPy ERROR: {e}")
        results.append({
            "query": qname,
            "status": "dspy_error",
            "error": str(e)
        })
        continue

    # Benchmark optimized
    print("  Running optimized...")
    try:
        opt_time, opt_rows, opt_result = benchmark_query(opt_sql)
        print(f"    Time: {opt_time:.3f}s, Rows: {opt_rows}")

        # Compare
        correct = compare_results(orig_result, opt_result)
        speedup = orig_time / opt_time if opt_time > 0 else 0

        print(f"\n  RESULT: {speedup:.2f}x speedup, Correct: {'YES' if correct else 'NO'}")

        results.append({
            "query": qname,
            "status": "success",
            "original_time": round(orig_time, 4),
            "optimized_time": round(opt_time, 4),
            "speedup": round(speedup, 2),
            "correct": correct,
            "original_rows": orig_rows,
            "optimized_rows": opt_rows,
            "rationale": rationale[:200] + "..." if len(rationale) > 200 else rationale
        })

        # Save query files
        qdir = output_dir / qname
        qdir.mkdir(exist_ok=True)
        (qdir / "original.sql").write_text(sql)
        (qdir / "optimized.sql").write_text(opt_sql)
        (qdir / "rationale.txt").write_text(rationale)

    except Exception as e:
        print(f"    Execution ERROR: {e}")
        results.append({
            "query": qname,
            "status": "execution_error",
            "error": str(e),
            "optimized_sql": opt_sql[:500]
        })
        # Still save the query
        qdir = output_dir / qname
        qdir.mkdir(exist_ok=True)
        (qdir / "original.sql").write_text(sql)
        (qdir / "optimized_failed.sql").write_text(opt_sql)
        (qdir / "error.txt").write_text(str(e))

conn.close()

# ============================================================
# Summary
# ============================================================
print("\n" + "="*60)
print("SUMMARY")
print("="*60)

summary_lines = []
summary_lines.append(f"{'Query':<8} {'Status':<12} {'Original':<10} {'Optimized':<10} {'Speedup':<8} {'Correct':<8}")
summary_lines.append("-" * 60)

for r in results:
    if r["status"] == "success":
        line = f"{r['query']:<8} {'OK':<12} {r['original_time']:<10.3f} {r['optimized_time']:<10.3f} {r['speedup']:<8.2f}x {'YES' if r['correct'] else 'NO':<8}"
    else:
        line = f"{r['query']:<8} {r['status']:<12} {'--':<10} {'--':<10} {'--':<8} {'--':<8}"
    summary_lines.append(line)
    print(line)

# Save results
with open(output_dir / "results.json", "w") as f:
    json.dump(results, f, indent=2)

with open(output_dir / "summary.txt", "w") as f:
    f.write("\n".join(summary_lines))

print(f"\nResults saved to: {output_dir}")
