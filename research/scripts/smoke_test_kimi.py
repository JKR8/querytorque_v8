#!/usr/bin/env python3
"""
Smoke Test - Kimi K2 E2E Verification (via OpenRouter)

Tests:
1. OpenRouter API connection (accessing Kimi K2)
2. DSPy configuration with Kimi K2
3. DuckDB SF100 connection
4. Explain plan generation
5. Full optimization pipeline on Q1
6. Semantic validation

Run: python3 research/scripts/smoke_test_kimi.py
"""

import os
import sys
import time
from pathlib import Path

# ============================================================
# Test 1: Environment & API Key
# ============================================================
print("=" * 60)
print("SMOKE TEST: Kimi K2 (via OpenRouter) + DSPy + DuckDB")
print("=" * 60)

# Check for OpenRouter API key (Kimi K2 access via OpenRouter)
API_KEY = os.getenv("OPENROUTER_API_KEY")
if not API_KEY:
    # Try reading from file
    key_file = Path("openrouter.txt")
    if key_file.exists():
        API_KEY = key_file.read_text().strip()
        os.environ["OPENROUTER_API_KEY"] = API_KEY
        print("[1] API Key: Loaded from openrouter.txt")
    else:
        print("[1] ERROR: Set OPENROUTER_API_KEY or create openrouter.txt")
        sys.exit(1)
else:
    print("[1] API Key: From environment")

print(f"    Key prefix: {API_KEY[:15]}...")

# OpenRouter settings for Kimi K2.5
API_BASE = "https://openrouter.ai/api/v1"
MODEL_NAME = "moonshotai/kimi-k2.5"  # Latest Kimi K2.5 model

# ============================================================
# Test 2: Raw API Connection (no DSPy)
# ============================================================
print("\n[2] Testing raw OpenRouter -> Kimi K2 connection...")

try:
    from openai import OpenAI

    client = OpenAI(
        api_key=API_KEY,
        base_url=API_BASE,
        default_headers={
            "HTTP-Referer": "https://querytorque.com",
            "X-Title": "QueryTorque Benchmark"
        }
    )

    start = time.time()
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": "Reply with just: OK"}],
        max_tokens=10
    )
    elapsed = time.time() - start

    reply = response.choices[0].message.content
    print(f"    Model: {MODEL_NAME}")
    print(f"    Response: {reply}")
    print(f"    Latency: {elapsed:.2f}s")
    print("    [PASS] Kimi K2 API working")

except Exception as e:
    print(f"    [FAIL] API error: {e}")
    sys.exit(1)

# ============================================================
# Test 3: DuckDB Connection
# ============================================================
print("\n[3] Testing DuckDB SF100 connection...")

DB_PATH = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"

try:
    import duckdb

    # Try sample DB first (faster), fall back to full
    db_to_use = SAMPLE_DB if Path(SAMPLE_DB).exists() else DB_PATH
    print(f"    Using: {db_to_use}")

    conn = duckdb.connect(db_to_use, read_only=True)

    # Quick table check
    result = conn.execute("SELECT COUNT(*) FROM store_returns").fetchone()
    print(f"    store_returns rows: {result[0]:,}")

    result = conn.execute("SELECT COUNT(*) FROM date_dim").fetchone()
    print(f"    date_dim rows: {result[0]:,}")

    print("    [PASS] DuckDB connected")

except Exception as e:
    print(f"    [FAIL] DuckDB error: {e}")
    sys.exit(1)

# ============================================================
# Test 4: Explain Plan Generation
# ============================================================
print("\n[4] Testing explain plan generation...")

Q1_SQL = """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk,
           sr_store_sk AS ctr_store_sk,
           SUM(SR_FEE) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk
      AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
)
AND s_store_sk = ctr1.ctr_store_sk
AND s_state = 'SD'
AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100;
"""

try:
    # Get explain plan
    explain_result = conn.execute(f"EXPLAIN {Q1_SQL}").fetchall()
    plan_text = "\n".join([row[1] for row in explain_result])
    print(f"    Plan lines: {len(explain_result)}")
    print(f"    Plan preview: {plan_text[:200]}...")

    # Get analyze plan (with actual costs)
    analyze_result = conn.execute(f"EXPLAIN ANALYZE {Q1_SQL}").fetchall()
    print(f"    Analyze complete")
    print("    [PASS] Explain plans working")

except Exception as e:
    print(f"    [FAIL] Explain error: {e}")
    sys.exit(1)

# ============================================================
# Test 5: DSPy + Kimi Configuration
# ============================================================
print("\n[5] Testing DSPy with Kimi K2 (via OpenRouter)...")

try:
    import dspy

    # Configure DSPy with OpenRouter -> Kimi K2
    lm = dspy.LM(
        f"openai/{MODEL_NAME}",  # OpenRouter model path
        api_key=API_KEY,
        api_base=API_BASE,
        extra_headers={
            "HTTP-Referer": "https://querytorque.com",
            "X-Title": "QueryTorque Benchmark"
        }
    )
    dspy.configure(lm=lm)

    # Simple test
    class SimpleTest(dspy.Signature):
        """Test signature."""
        input_text: str = dspy.InputField()
        output_text: str = dspy.OutputField()

    predictor = dspy.Predict(SimpleTest)
    result = predictor(input_text="Say hello")

    print(f"    DSPy response: {result.output_text[:50]}...")
    print("    [PASS] DSPy + Kimi working")

except Exception as e:
    print(f"    [FAIL] DSPy error: {e}")
    sys.exit(1)

# ============================================================
# Test 6: Full Optimization Pipeline
# ============================================================
print("\n[6] Running full Q1 optimization pipeline...")

try:
    # Define optimizer signature
    class SQLOptimizer(dspy.Signature):
        """Optimize SQL query for better execution performance."""
        original_query: str = dspy.InputField(desc="The original SQL query")
        execution_plan: str = dspy.InputField(desc="Execution plan with costs")
        table_scans: str = dspy.InputField(desc="Table scan info")
        optimized_query: str = dspy.OutputField(desc="Optimized SQL query")
        rationale: str = dspy.OutputField(desc="Optimization reasoning")

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

    pipeline = OptimizationPipeline()

    # Prepare inputs
    Q1_PLAN = """
Operators by cost:
- SEQ_SCAN (customer): 73.4% cost, 1,999,335 rows
- HASH_JOIN: 11.0% cost, 7,986 rows
- SEQ_SCAN (store_returns): 5.2% cost, 56,138 rows
- HASH_GROUP_BY: 2.9% cost, 55,341 rows
"""

    Q1_SCANS = """
- store_returns: 56,138 rows (NO FILTER)
- date_dim: 366 rows - FILTERED by d_year=2000
- customer: 1,999,335 rows (NO FILTER)
- store: 53 rows - FILTERED by s_state='SD'
Key: store has 53 rows after filter but applied AFTER aggregation
"""

    print("    Calling Kimi K2 for optimization...")
    start = time.time()
    result = pipeline(query=Q1_SQL.strip(), plan=Q1_PLAN, scans=Q1_SCANS)
    opt_time = time.time() - start

    opt_sql = result.optimized_query
    rationale = result.rationale

    print(f"    LLM time: {opt_time:.1f}s")
    print(f"    Rationale: {rationale[:100]}...")
    print(f"    Optimized SQL length: {len(opt_sql)} chars")
    print("    [PASS] Optimization complete")

except Exception as e:
    print(f"    [FAIL] Pipeline error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================
# Test 7: Benchmark & Validate
# ============================================================
print("\n[7] Benchmarking original vs optimized...")

def benchmark(sql, warmup=1, runs=3):
    """Benchmark with warmup."""
    for _ in range(warmup):
        conn.execute(sql).fetchall()

    times = []
    result = None
    for _ in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        times.append(time.perf_counter() - start)

    return sum(times) / len(times), result

try:
    # Benchmark original
    print("    Running original query...")
    orig_time, orig_result = benchmark(Q1_SQL)
    print(f"    Original: {orig_time*1000:.1f}ms, {len(orig_result)} rows")

    # Benchmark optimized
    print("    Running optimized query...")
    opt_exec_time, opt_result = benchmark(opt_sql)
    print(f"    Optimized: {opt_exec_time*1000:.1f}ms, {len(opt_result)} rows")

    # Speedup
    speedup = orig_time / opt_exec_time if opt_exec_time > 0 else 0
    print(f"    Speedup: {speedup:.2f}x")

except Exception as e:
    print(f"    [WARN] Optimized query failed to execute: {e}")
    print("    Saving failed query for inspection...")
    opt_result = None

# ============================================================
# Test 8: Semantic Validation
# ============================================================
print("\n[8] Validating semantic correctness...")

try:
    if opt_result is not None:
        # Compare results
        orig_set = set(tuple(r) for r in orig_result)
        opt_set = set(tuple(r) for r in opt_result)

        if orig_set == opt_set:
            print("    Results: MATCH")
            print("    [PASS] Semantic validation passed")
            validation_status = "PASS"
        else:
            print(f"    Original rows: {len(orig_result)}")
            print(f"    Optimized rows: {len(opt_result)}")
            print(f"    Missing: {len(orig_set - opt_set)}")
            print(f"    Extra: {len(opt_set - orig_set)}")
            print("    [FAIL] Semantic validation failed")
            validation_status = "FAIL"
    else:
        print("    [SKIP] No optimized result to validate")
        validation_status = "SKIP"

except Exception as e:
    print(f"    [FAIL] Validation error: {e}")
    validation_status = "ERROR"

# ============================================================
# Summary
# ============================================================
print("\n" + "=" * 60)
print("SMOKE TEST SUMMARY")
print("=" * 60)

print(f"""
Provider:     Kimi K2 ({MODEL_NAME})
API Base:     {API_BASE}
Database:     {db_to_use}
Test Query:   TPC-DS Q1

Results:
- API Connection:    PASS
- DuckDB Connection: PASS
- Explain Plans:     PASS
- DSPy Pipeline:     PASS
- Optimization:      PASS
- Validation:        {validation_status}

Performance:
- Original:   {orig_time*1000:.1f}ms
- Optimized:  {opt_exec_time*1000:.1f}ms
- Speedup:    {speedup:.2f}x
- LLM Time:   {opt_time:.1f}s
""")

# Save optimized query for inspection
output_dir = Path("research/experiments/smoke_test")
output_dir.mkdir(parents=True, exist_ok=True)

(output_dir / "q1_original.sql").write_text(Q1_SQL)
(output_dir / "q1_optimized.sql").write_text(opt_sql)
(output_dir / "q1_rationale.txt").write_text(rationale)

print(f"Saved to: {output_dir}/")
print("\nReady for full benchmark run!")

conn.close()
