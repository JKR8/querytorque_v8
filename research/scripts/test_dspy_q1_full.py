#!/usr/bin/env python3
"""
DSPy Q1 Full Test - Logs everything and tests on database

Creates a folder with:
- input.txt (what we sent)
- prompt.txt (what DSPy generated)
- response.txt (raw LLM response)
- output.txt (parsed result)
- benchmark.txt (timing comparison)
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Check for API key
if not os.getenv("DEEPSEEK_API_KEY"):
    print("ERROR: Set DEEPSEEK_API_KEY environment variable")
    sys.exit(1)

import dspy
import duckdb

# Create output folder
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = Path(f"research/experiments/dspy_runs/q1_{timestamp}")
output_dir.mkdir(parents=True, exist_ok=True)
print(f"Output folder: {output_dir}")

# ============================================================
# Q1 Data
# ============================================================
Q1_SQL = """
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
 LIMIT 100;
""".strip()

Q1_PLAN = """
Operators by cost:
- SEQ_SCAN (customer): 73.4% cost, 1,999,335 rows
- HASH_JOIN: 11.0% cost, 7,986 rows
- SEQ_SCAN (store_returns): 5.2% cost, 56,138 rows
- HASH_GROUP_BY: 2.9% cost, 55,341 rows
"""

Q1_SCANS = """
- store_returns: 56,138 rows (NO FILTER)
- date_dim: 366 rows ← FILTERED by d_year=2000
- customer: 1,999,335 rows (NO FILTER)
- store: 53 rows ← FILTERED by s_state='SD'

Key insight: store has only 53 rows after s_state='SD' filter,
but this filter is applied AFTER aggregating store_returns.
"""

# Save inputs
with open(output_dir / "1_input.txt", "w") as f:
    f.write("=" * 60 + "\n")
    f.write("ORIGINAL SQL (Q1)\n")
    f.write("=" * 60 + "\n\n")
    f.write(Q1_SQL)
    f.write("\n\n")
    f.write("=" * 60 + "\n")
    f.write("EXECUTION PLAN\n")
    f.write("=" * 60 + "\n")
    f.write(Q1_PLAN)
    f.write("\n")
    f.write("=" * 60 + "\n")
    f.write("TABLE SCANS\n")
    f.write("=" * 60 + "\n")
    f.write(Q1_SCANS)

print("✓ Saved inputs to 1_input.txt")

# ============================================================
# DSPy Signature
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
        return self.optimizer(
            original_query=query,
            execution_plan=plan,
            table_scans=scans
        )

# ============================================================
# Configure LLM with logging
# ============================================================
print("\nConfiguring DeepSeek LLM...")

lm = dspy.LM(
    "openai/deepseek-chat",
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    api_base="https://api.deepseek.com"
)
dspy.configure(lm=lm)

# ============================================================
# Run optimization
# ============================================================
print("Running DSPy optimization...")

pipeline = OptimizationPipeline()
result = pipeline(query=Q1_SQL, plan=Q1_PLAN, scans=Q1_SCANS)

print("✓ Optimization complete!")

# ============================================================
# Extract and save the prompt/response
# ============================================================

# Get the history from litellm (DSPy uses litellm under the hood)
try:
    # DSPy 2.5+ stores history differently
    history = lm.history
    if history:
        with open(output_dir / "2_prompt.txt", "w") as f:
            f.write("=" * 60 + "\n")
            f.write("PROMPT SENT TO LLM\n")
            f.write("=" * 60 + "\n\n")
            for i, call in enumerate(history):
                f.write(f"--- Call {i+1} ---\n")
                if isinstance(call, dict):
                    if 'messages' in call:
                        for msg in call['messages']:
                            f.write(f"\n[{msg.get('role', 'unknown')}]\n")
                            f.write(msg.get('content', str(msg)))
                            f.write("\n")
                    else:
                        f.write(json.dumps(call, indent=2, default=str))
                else:
                    f.write(str(call))
                f.write("\n\n")
        print("✓ Saved prompt to 2_prompt.txt")
except Exception as e:
    print(f"  (Could not extract prompt history: {e})")
    # Save what we can from the result
    with open(output_dir / "2_prompt.txt", "w") as f:
        f.write("=" * 60 + "\n")
        f.write("DSPY SIGNATURE (prompt template)\n")
        f.write("=" * 60 + "\n\n")
        f.write("DSPy auto-generates prompts from this signature:\n\n")
        f.write("class SQLOptimizer(dspy.Signature):\n")
        f.write('    """Optimize SQL query for better execution performance."""\n')
        f.write('    original_query: str = InputField(desc="The original SQL query to optimize")\n')
        f.write('    execution_plan: str = InputField(desc="Execution plan showing operator costs")\n')
        f.write('    table_scans: str = InputField(desc="Table scan info")\n')
        f.write('    optimized_query: str = OutputField(desc="The optimized SQL query")\n')
        f.write('    rationale: str = OutputField(desc="Why this improves performance")\n')
        f.write("\n\nWith ChainOfThought, DSPy adds 'reasoning' before the outputs.\n")
    print("✓ Saved signature info to 2_prompt.txt")

# Save raw response (the reasoning trace)
with open(output_dir / "3_response.txt", "w") as f:
    f.write("=" * 60 + "\n")
    f.write("RAW DSPY RESPONSE\n")
    f.write("=" * 60 + "\n\n")
    # result is a dspy.Prediction object
    f.write(f"Type: {type(result)}\n\n")
    f.write("Fields:\n")
    for key in dir(result):
        if not key.startswith('_'):
            try:
                val = getattr(result, key)
                if not callable(val):
                    f.write(f"\n--- {key} ---\n")
                    f.write(str(val)[:2000])
                    f.write("\n")
            except:
                pass
print("✓ Saved response to 3_response.txt")

# Save parsed output
with open(output_dir / "4_output.txt", "w") as f:
    f.write("=" * 60 + "\n")
    f.write("PARSED OUTPUT\n")
    f.write("=" * 60 + "\n\n")
    f.write("OPTIMIZED QUERY:\n")
    f.write("-" * 40 + "\n")
    f.write(result.optimized_query)
    f.write("\n\n")
    f.write("RATIONALE:\n")
    f.write("-" * 40 + "\n")
    f.write(result.rationale)
print("✓ Saved output to 4_output.txt")

# ============================================================
# Test on sample database
# ============================================================
print("\n" + "=" * 60)
print("BENCHMARKING ON SAMPLE DATABASE")
print("=" * 60)

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"

if not os.path.exists(SAMPLE_DB):
    print(f"Sample database not found: {SAMPLE_DB}")
    print("Skipping benchmark...")
    benchmark_result = "Database not found"
else:
    try:
        conn = duckdb.connect(SAMPLE_DB, read_only=True)

        # Warm up
        print("Warming up...")
        conn.execute("SELECT 1").fetchall()

        # Run original
        print("Running original query...")
        import time

        times_original = []
        for i in range(3):
            start = time.perf_counter()
            original_result = conn.execute(Q1_SQL).fetchall()
            elapsed = time.perf_counter() - start
            times_original.append(elapsed)
            print(f"  Run {i+1}: {elapsed:.3f}s ({len(original_result)} rows)")

        avg_original = sum(times_original) / len(times_original)
        print(f"  Average: {avg_original:.3f}s")

        # Run optimized
        print("\nRunning optimized query...")
        optimized_sql = result.optimized_query

        times_optimized = []
        try:
            for i in range(3):
                start = time.perf_counter()
                optimized_result = conn.execute(optimized_sql).fetchall()
                elapsed = time.perf_counter() - start
                times_optimized.append(elapsed)
                print(f"  Run {i+1}: {elapsed:.3f}s ({len(optimized_result)} rows)")

            avg_optimized = sum(times_optimized) / len(times_optimized)
            print(f"  Average: {avg_optimized:.3f}s")

            # Compare results
            speedup = avg_original / avg_optimized if avg_optimized > 0 else 0
            correct = sorted(original_result) == sorted(optimized_result)

            print(f"\n{'=' * 60}")
            print("RESULTS")
            print(f"{'=' * 60}")
            print(f"Original:  {avg_original:.3f}s")
            print(f"Optimized: {avg_optimized:.3f}s")
            print(f"Speedup:   {speedup:.2f}x")
            print(f"Correct:   {'✓ YES' if correct else '✗ NO'}")

            if not correct:
                print(f"\nOriginal rows: {len(original_result)}")
                print(f"Optimized rows: {len(optimized_result)}")

            benchmark_result = f"""
Original:  {avg_original:.3f}s ({len(original_result)} rows)
Optimized: {avg_optimized:.3f}s ({len(optimized_result)} rows)
Speedup:   {speedup:.2f}x
Correct:   {'YES' if correct else 'NO - RESULTS DIFFER'}
"""
        except Exception as e:
            print(f"  ERROR running optimized query: {e}")
            benchmark_result = f"ERROR: {e}"

        conn.close()

    except Exception as e:
        print(f"Database error: {e}")
        benchmark_result = f"Database error: {e}"

# Save benchmark
with open(output_dir / "5_benchmark.txt", "w") as f:
    f.write("=" * 60 + "\n")
    f.write("BENCHMARK RESULTS\n")
    f.write("=" * 60 + "\n\n")
    f.write(f"Database: {SAMPLE_DB}\n")
    f.write(f"Query: TPC-DS Q1\n\n")
    f.write(benchmark_result)
print(f"✓ Saved benchmark to 5_benchmark.txt")

print(f"\n✓ All files saved to: {output_dir}")
print("\nFiles created:")
for f in sorted(output_dir.glob("*")):
    print(f"  - {f.name}")
