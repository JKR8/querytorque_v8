#!/usr/bin/env python3
"""Test DSPy with Gemini 3 Pro Preview on Q15."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-sql"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-shared"))

import dspy
from qt_sql.optimization.dspy_optimizer import ValidatedOptimizationPipeline
from qt_sql.optimization.iterative_optimizer import test_optimization

Q15_SQL = """select ca_zip
       ,sum(cs_sales_price)
 from catalog_sales
     ,customer
     ,customer_address
     ,date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                  '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 1 and d_year = 2001
 group by ca_zip
 order by ca_zip
 LIMIT 100;"""

Q15_PLAN = """Operators by cost:
- HASH_JOIN: 26.6% cost, 1,432,318 rows
- SEQ_SCAN (catalog_sales): 19.6% cost, 1,439,513 rows
- SEQ_SCAN (customer): 14.6% cost, 1,999,998 rows
- SEQ_SCAN (customer_address): 11.1% cost, 1,000,000 rows"""

Q15_SCANS = """Table scans:
- catalog_sales: 1,439,513 rows (NO FILTER)
- customer: 2M rows (minimal filter)
- customer_address: 1M rows (dynamic filter on ca_zip)
- date_dim: 73K -> 91 rows (FILTERED by d_qoy=1, d_year=2001)"""


def main():
    # Get OpenRouter API key
    openrouter_key = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/openrouter.txt").read_text().strip()

    db_path = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
    output_dir = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/experiments/dspy_gemini")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("="*60)
    print("Testing DSPy + Gemini 3 Pro Preview on Q15")
    print("="*60)

    # Configure DSPy with Gemini via OpenRouter
    print("\nConfiguring DSPy with Gemini 3 Pro Preview...")
    lm = dspy.LM(
        model="openrouter/google/gemini-3-pro-preview",
        api_key=openrouter_key,
        api_base="https://openrouter.ai/api/v1",
        temperature=0.1,
    )
    dspy.configure(lm=lm)

    # Create validator function
    def validator(original: str, optimized: str) -> tuple[bool, str]:
        result = test_optimization(original, optimized, db_path, runs=3)
        if result.error:
            return False, f"Error: {result.error}"
        if not result.semantically_correct:
            return False, "Results differ"
        return True, f"Valid, speedup: {result.speedup:.2f}x"

    # Create pipeline with few-shot examples
    print("Creating ValidatedOptimizationPipeline with few-shot examples...")
    pipeline = ValidatedOptimizationPipeline(
        validator_fn=validator,
        max_retries=2,
        use_few_shot=True,
        num_examples=3,
    )

    # Run optimization
    print("\nRunning optimization...")
    try:
        result = pipeline(
            query=Q15_SQL,
            plan=Q15_PLAN,
            rows=Q15_SCANS,
        )

        print(f"\nOptimized query:\n{'-'*40}")
        print(result.optimized_sql[:600] + "..." if len(result.optimized_sql) > 600 else result.optimized_sql)
        print('-'*40)

        print(f"\nRationale: {result.rationale}")

        # Save results
        (output_dir / "q15_optimized.sql").write_text(result.optimized_sql)
        (output_dir / "q15_rationale.txt").write_text(result.rationale)

        # Benchmark
        print("\nBenchmarking (3 runs, discard 1st)...")
        bench_result = test_optimization(Q15_SQL, result.optimized_sql, db_path, runs=3)

        if bench_result.error:
            print(f"  ERROR: {bench_result.error}")
        elif not bench_result.semantically_correct:
            print(f"  INVALID: Results differ")
        else:
            print(f"\n  RESULTS:")
            print(f"    Original:  {bench_result.original_time:.3f}s")
            print(f"    Optimized: {bench_result.optimized_time:.3f}s")
            print(f"    Speedup:   {bench_result.speedup:.2f}x")

            print(f"\n{'='*60}")
            print("COMPARISON: Q15")
            print('='*60)
            print(f"  DSPy + DeepSeek:  2.98x")
            print(f"  DSPy + Gemini 3:  {bench_result.speedup:.2f}x")

    except Exception as e:
        print(f"Exception: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
