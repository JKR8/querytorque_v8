#!/usr/bin/env python3
"""Test Q1 with prompt-adjusted reasoning model (deepseek-reasoner)."""

import sys
import os
import json
from pathlib import Path

sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_json_queue

# Q1 SQL
Q1_SQL = """
WITH customer_total_return AS (
  SELECT sr_customer_sk AS ctr_customer_sk,
         sr_store_sk AS ctr_store_sk,
         SUM(SR_FEE) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (
    SELECT avg(ctr_total_return)*1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
  )
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'SD'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
""".strip()

def main():
    """Run Q1 optimization test."""
    print("=" * 80)
    print("Q1 OPTIMIZATION TEST - REASONING MODEL")
    print("=" * 80)

    # Configuration
    print("\n[1/5] Configuration")
    print("-" * 80)

    # Check for database paths
    sample_db = os.getenv('QT_SAMPLE_DB', 'D:/TPC-DS/tpcds_sf1_sample.duckdb')
    full_db = os.getenv('QT_FULL_DB', 'D:/TPC-DS/tpcds_sf100.duckdb')

    print(f"Sample DB: {sample_db}")
    print(f"Full DB: {full_db}")

    # Check API key
    api_key = os.getenv('QT_DEEPSEEK_API_KEY') or os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        print("\n❌ ERROR: DeepSeek API key not found!")
        print("Set environment variable:")
        print("  export QT_DEEPSEEK_API_KEY=your_api_key")
        print("  or")
        print("  export DEEPSEEK_API_KEY=your_api_key")
        return 1

    print(f"✓ API key configured (length: {len(api_key)})")

    # Provider configuration
    provider = 'deepseek'
    model = None  # Will use default: deepseek-reasoner

    print(f"✓ Provider: {provider}")
    print(f"✓ Model: {model or 'deepseek-reasoner (default)'}")
    print(f"✓ Prompt: 'autonomous Query Rewrite Engine' (updated)")
    print(f"✓ Workers: 5 (4 DAG JSON + 1 Full SQL)")

    # Check database files
    print("\n[2/5] Database Validation")
    print("-" * 80)

    if not Path(sample_db).exists():
        print(f"⚠️  WARNING: Sample DB not found at {sample_db}")
        print("   Set QT_SAMPLE_DB environment variable")
    else:
        print(f"✓ Sample DB found: {Path(sample_db).stat().st_size / (1024**3):.2f} GB")

    if not Path(full_db).exists():
        print(f"⚠️  WARNING: Full DB not found at {full_db}")
        print("   Set QT_FULL_DB environment variable")
    else:
        print(f"✓ Full DB found: {Path(full_db).stat().st_size / (1024**3):.2f} GB")

    # Run optimization
    print("\n[3/5] Running Optimization")
    print("-" * 80)
    print("Query: TPC-DS Q1 (correlated subquery)")
    print(f"Query ID: q1")
    print(f"Target speedup: 2.0x")
    print("\nThis will:")
    print("  1. Generate 5 worker prompts (ML-guided examples)")
    print("  2. Call deepseek-reasoner for each worker (parallel)")
    print("  3. Validate on sample DB (tick/cross)")
    print("  4. Benchmark valid candidates on full DB (5-run trimmed mean)")
    print("  5. Return first candidate meeting target speedup")
    print("\n⏳ Running... (this may take 2-5 minutes)")

    try:
        valid_candidates, full_results, winner = optimize_v5_json_queue(
            sql=Q1_SQL,
            query_id='q1',
            sample_db=sample_db,
            full_db=full_db,
            provider=provider,
            model=model,
            max_workers=5,
            target_speedup=2.0,
        )

        # Results
        print("\n[4/5] Results")
        print("-" * 80)

        print(f"\n✓ Optimization complete!")
        print(f"  Valid candidates: {len(valid_candidates)}")
        print(f"  Full results: {len(full_results)}")

        # Show valid candidates
        if valid_candidates:
            print("\n** Valid Candidates (Sample DB) **")
            for i, cand in enumerate(valid_candidates, 1):
                print(f"\n  {i}. Worker {cand.worker_id}")
                print(f"     Status: {cand.status}")
                print(f"     Speedup: {cand.speedup:.2f}x")
                if cand.error:
                    print(f"     Error: {cand.error}")

        # Show full results
        if full_results:
            print("\n** Full DB Results **")
            for i, result in enumerate(full_results, 1):
                print(f"\n  {i}. Worker {result.sample.worker_id}")
                print(f"     Sample: {result.sample.speedup:.2f}x")
                print(f"     Full: {result.full_speedup:.2f}x")
                print(f"     Status: {result.full_status}")

        # Winner
        if winner:
            print("\n" + "=" * 80)
            print("🏆 WINNER")
            print("=" * 80)
            print(f"\nWorker {winner.sample.worker_id} achieved target speedup!")
            print(f"\n** Performance **")
            print(f"  Sample DB speedup: {winner.sample.speedup:.2f}x")
            print(f"  Full DB speedup: {winner.full_speedup:.2f}x")
            print(f"  Target: 2.0x")
            print(f"  Status: {'✅ MET' if winner.full_speedup >= 2.0 else '⚠️ BELOW TARGET'}")

            print(f"\n** Optimized SQL **")
            print("-" * 80)
            print(winner.sample.optimized_sql)
            print("-" * 80)

            # Analyze the optimization
            print("\n** Analysis **")
            if 'store_avg_return' in winner.sample.optimized_sql:
                print("✓ Decorrelation detected (store_avg_return CTE)")
            if 'WHERE' in winner.sample.optimized_sql and "s_state = 'SD'" in winner.sample.optimized_sql:
                # Check if filter is in WHERE or JOIN
                if "JOIN store" in winner.sample.optimized_sql and "AND s.s_state = 'SD'" in winner.sample.optimized_sql.split("WHERE")[0]:
                    print("⚠️ Filter in JOIN condition")
                else:
                    print("✓ Filter in WHERE clause (correct placement)")

            # Save results
            output_dir = Path('test_results')
            output_dir.mkdir(exist_ok=True)

            output_file = output_dir / 'q1_reasoning_model_result.json'
            result_data = {
                'query_id': 'q1',
                'provider': provider,
                'model': model or 'deepseek-reasoner',
                'worker_id': winner.sample.worker_id,
                'sample_speedup': winner.sample.speedup,
                'full_speedup': winner.full_speedup,
                'status': str(winner.full_status),
                'optimized_sql': winner.sample.optimized_sql,
                'prompt_length': len(winner.sample.prompt),
                'response_length': len(winner.sample.response),
            }

            with open(output_file, 'w') as f:
                json.dump(result_data, f, indent=2)

            print(f"\n✓ Results saved to: {output_file}")

        else:
            print("\n" + "=" * 80)
            print("❌ NO WINNER")
            print("=" * 80)
            print("\nNo candidate achieved target speedup of 2.0x")
            print("\nPossible reasons:")
            print("  1. All optimizations failed validation")
            print("  2. Speedups below target")
            print("  3. Semantic errors in rewrites")

        # Summary
        print("\n[5/5] Summary")
        print("=" * 80)
        print(f"Provider: {provider}")
        print(f"Model: {model or 'deepseek-reasoner'}")
        print(f"Prompt: autonomous Query Rewrite Engine ✅")
        print(f"Valid candidates: {len(valid_candidates)}/5")
        print(f"Winner: {'✅ Yes' if winner else '❌ No'}")
        if winner:
            print(f"Best speedup: {winner.full_speedup:.2f}x")
        print("=" * 80)

        return 0 if winner else 1

    except Exception as e:
        print(f"\n❌ ERROR during optimization:")
        print(f"  {type(e).__name__}: {e}")

        import traceback
        print("\nFull traceback:")
        traceback.print_exc()

        return 1

if __name__ == '__main__':
    exit(main())
