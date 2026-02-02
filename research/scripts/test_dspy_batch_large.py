#!/usr/bin/env python3
"""
DSPy Large Batch Test - Q1-Q20
With correct benchmark methodology and validation
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
PROMPTS_DIR = Path("research/prompts/batch")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = Path(f"research/experiments/dspy_runs/batch_{timestamp}")
output_dir.mkdir(parents=True, exist_ok=True)

# Test Q1-Q20
QUERIES = [f"q{i}" for i in range(1, 21)]

print(f"Output: {output_dir}")
print(f"Queries: Q1-Q20 ({len(QUERIES)} queries)")

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
# Database
# ============================================================
print(f"Connecting to {SAMPLE_DB}...")
conn = duckdb.connect(SAMPLE_DB, read_only=True)
conn.execute("SELECT 1").fetchall()

# ============================================================
# Helpers
# ============================================================
def extract_from_prompt(prompt_text):
    """Extract SQL, plan, and scans from batch prompt."""
    sql_match = re.search(r'```sql\n(.*?)```', prompt_text, re.DOTALL)
    sql = sql_match.group(1).strip() if sql_match else ""

    plan_match = re.search(r'\*\*Operators by cost:\*\*(.*?)(?=\*\*Table scans|\n---)', prompt_text, re.DOTALL)
    plan = plan_match.group(1).strip() if plan_match else ""

    scans_match = re.search(r'\*\*Table scans:\*\*(.*?)(?=\n---)', prompt_text, re.DOTALL)
    scans = scans_match.group(1).strip() if scans_match else ""

    return sql, plan, scans

def benchmark_query(sql, runs=3):
    """3 runs, discard first, average remaining 2."""
    times = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        times.append(time.perf_counter() - start)
    avg = sum(times[1:]) / len(times[1:]) if len(times) > 1 else times[0]
    return avg, len(result), result

def compare_results(orig, opt):
    """Check semantic equivalence."""
    return sorted([tuple(r) for r in orig]) == sorted([tuple(r) for r in opt])

# ============================================================
# Run tests
# ============================================================
results = []
successes = 0
failures = 0

for qname in QUERIES:
    print(f"\n[{qname.upper()}] ", end="", flush=True)

    prompt_file = PROMPTS_DIR / f"{qname}_prompt.txt"
    if not prompt_file.exists():
        print("SKIP (no prompt)")
        continue

    prompt_text = prompt_file.read_text()
    sql, plan, scans = extract_from_prompt(prompt_text)

    if not sql:
        print("SKIP (no SQL)")
        continue

    # Benchmark original
    try:
        orig_time, orig_rows, orig_result = benchmark_query(sql)
    except Exception as e:
        print(f"ORIG ERROR: {e}")
        continue

    # DSPy optimization
    try:
        dspy_result = pipeline(query=sql, plan=plan, scans=scans)
        opt_sql = dspy_result.optimized_query
        rationale = dspy_result.rationale
    except Exception as e:
        print(f"DSPY ERROR: {e}")
        results.append({"query": qname, "status": "dspy_error", "error": str(e)})
        failures += 1
        continue

    # Benchmark optimized
    try:
        opt_time, opt_rows, opt_result = benchmark_query(opt_sql)
        correct = compare_results(orig_result, opt_result)
        speedup = orig_time / opt_time if opt_time > 0 else 0

        status = "✓" if correct and speedup > 1.1 else ("✗" if not correct else "~")
        print(f"{status} {orig_time:.3f}s → {opt_time:.3f}s ({speedup:.2f}x) {'CORRECT' if correct else 'WRONG'}")

        results.append({
            "query": qname,
            "status": "success" if correct else "wrong_results",
            "original_time": round(orig_time, 4),
            "optimized_time": round(opt_time, 4),
            "speedup": round(speedup, 2),
            "correct": correct,
            "original_rows": orig_rows,
            "optimized_rows": opt_rows,
        })

        if correct:
            successes += 1
        else:
            failures += 1

        # Save queries
        qdir = output_dir / qname
        qdir.mkdir(exist_ok=True)
        (qdir / "original.sql").write_text(sql)
        (qdir / "optimized.sql").write_text(opt_sql)
        (qdir / "rationale.txt").write_text(rationale)

    except Exception as e:
        print(f"EXEC ERROR: {e}")
        results.append({"query": qname, "status": "exec_error", "error": str(e)})
        failures += 1
        # Save failed query
        qdir = output_dir / qname
        qdir.mkdir(exist_ok=True)
        (qdir / "original.sql").write_text(sql)
        (qdir / "optimized_failed.sql").write_text(opt_sql)
        (qdir / "error.txt").write_text(str(e))

conn.close()

# ============================================================
# Summary
# ============================================================
print("\n" + "="*70)
print("SUMMARY")
print("="*70)

# Sort by speedup
correct_results = [r for r in results if r.get("correct")]
correct_results.sort(key=lambda x: x.get("speedup", 0), reverse=True)

print(f"\n{'Query':<8} {'Original':<10} {'Optimized':<10} {'Speedup':<10} {'Status'}")
print("-"*50)

for r in results:
    if r["status"] == "success":
        print(f"{r['query']:<8} {r['original_time']:<10.4f} {r['optimized_time']:<10.4f} {r['speedup']:<10.2f}x ✓")
    elif r["status"] == "wrong_results":
        print(f"{r['query']:<8} {r['original_time']:<10.4f} {r['optimized_time']:<10.4f} {r['speedup']:<10.2f}x ✗ WRONG")
    else:
        print(f"{r['query']:<8} {'--':<10} {'--':<10} {'--':<10} ERROR")

print("-"*50)
print(f"Correct: {successes}/{len(results)} ({100*successes/len(results):.0f}%)")
print(f"Speedup >1.5x (correct): {len([r for r in results if r.get('correct') and r.get('speedup', 0) > 1.5])}")

# Save results
with open(output_dir / "results.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"\nResults saved to: {output_dir}")
