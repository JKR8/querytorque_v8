#!/usr/bin/env python3
"""
DSPy Q1 Test - Understanding How DSPy Works

DSPy is a framework for programming with LLMs using:
1. Signatures - Define inputs/outputs like function signatures
2. Modules - Combine signatures with reasoning strategies
3. Optimizers - Auto-tune prompts using training data

Instead of writing prompts manually, you declare WHAT you want
and DSPy figures out HOW to prompt the LLM.

Run: python test_dspy_q1.py
"""

import os
import sys

# Check for API key
if not os.getenv("DEEPSEEK_API_KEY"):
    print("ERROR: Set DEEPSEEK_API_KEY environment variable")
    print("  export DEEPSEEK_API_KEY=your_key_here")
    sys.exit(1)

import dspy

# ============================================================
# STEP 1: Define a Signature (like a function signature)
# ============================================================
# This tells DSPy what inputs the LLM receives and what outputs
# it should produce. DSPy auto-generates the prompt from this.

class SQLOptimizer(dspy.Signature):
    """Optimize SQL query for better execution performance."""

    # Inputs - what the LLM receives
    original_query: str = dspy.InputField(
        desc="The original SQL query to optimize"
    )
    execution_plan: str = dspy.InputField(
        desc="Execution plan showing operator costs and row counts"
    )
    table_scans: str = dspy.InputField(
        desc="Table scan info: table name, rows, filter status"
    )

    # Outputs - what the LLM should return
    optimized_query: str = dspy.OutputField(
        desc="The optimized SQL query with identical semantics"
    )
    rationale: str = dspy.OutputField(
        desc="Why this optimization improves performance"
    )


# ============================================================
# STEP 2: Create a Module (wraps signature with reasoning)
# ============================================================
# ChainOfThought makes the LLM "think step by step" before
# producing the final output.

class OptimizationPipeline(dspy.Module):
    def __init__(self):
        super().__init__()
        # ChainOfThought = think step-by-step before answering
        self.optimizer = dspy.ChainOfThought(SQLOptimizer)

    def forward(self, query, plan, scans):
        return self.optimizer(
            original_query=query,
            execution_plan=plan,
            table_scans=scans
        )


# ============================================================
# STEP 3: Configure the LLM
# ============================================================
print("Configuring DeepSeek LLM...")

lm = dspy.LM(
    "openai/deepseek-chat",
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    api_base="https://api.deepseek.com"
)
dspy.configure(lm=lm)


# ============================================================
# STEP 4: Run on Q1
# ============================================================
print("\n" + "="*60)
print("TPC-DS Q1 Optimization Test")
print("="*60)

# Q1 data from the batch prompt
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

print("\nOriginal Query:")
print("-" * 40)
print(Q1_SQL[:200] + "...")

print("\nExecution Plan:")
print("-" * 40)
print(Q1_PLAN)

print("\nTable Scans:")
print("-" * 40)
print(Q1_SCANS)

print("\n" + "="*60)
print("Running DSPy Optimization...")
print("="*60)

# Create and run the pipeline
pipeline = OptimizationPipeline()
result = pipeline(query=Q1_SQL, plan=Q1_PLAN, scans=Q1_SCANS)

print("\n✓ Optimization complete!")
print("\n" + "="*60)
print("OPTIMIZED QUERY")
print("="*60)
print(result.optimized_query)

print("\n" + "="*60)
print("RATIONALE")
print("="*60)
print(result.rationale)

# ============================================================
# STEP 5: Show what DSPy did behind the scenes
# ============================================================
print("\n" + "="*60)
print("WHAT DSPy DID (Behind the Scenes)")
print("="*60)
print("""
1. Took your Signature (SQLOptimizer) and auto-generated a prompt
2. Added "Let's think step by step" (ChainOfThought)
3. Sent to DeepSeek with structured output parsing
4. Extracted optimized_query and rationale from response

Key benefit: You defined WHAT (inputs/outputs), DSPy handled HOW (prompting).

To see the actual prompt DSPy generated, you can inspect:
  lm.history[-1]['prompt']
""")

# Show the generated prompt if available
try:
    if hasattr(lm, 'history') and lm.history:
        print("\nActual prompt sent to LLM:")
        print("-" * 40)
        prompt = lm.history[-1].get('prompt', str(lm.history[-1]))
        # Truncate if too long
        if len(prompt) > 1500:
            print(prompt[:1500] + "\n... [truncated]")
        else:
            print(prompt)
except Exception as e:
    print(f"(Could not retrieve prompt history: {e})")
