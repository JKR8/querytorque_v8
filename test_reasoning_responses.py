#!/usr/bin/env python3
"""Test both reasoning responses for Q1."""

import sys
import json
sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

from qt_sql.optimization.dag_v2 import DagV2Pipeline
import sqlglot

# Original Q1 SQL
ORIGINAL_SQL = """
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

# Response 1: Original reasoning (2 rewrite_sets)
RESPONSE_1 = {
  "rewrite_sets": [
    {
      "id": "rs_01",
      "transform": "decorrelate",
      "nodes": {
        "filtered_store_returns": "SELECT sr_customer_sk, sr_store_sk, sr_fee FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk WHERE d_year = 2000",
        "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM filtered_store_returns GROUP BY sr_customer_sk, sr_store_sk",
        "store_avg_return": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return_threshold FROM customer_total_return GROUP BY ctr_store_sk",
        "main_query": "SELECT c.c_customer_id FROM customer_total_return ctr1 JOIN store s ON ctr1.ctr_store_sk = s.s_store_sk AND s.s_state = 'SD' JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk WHERE ctr1.ctr_total_return > sar.avg_return_threshold ORDER BY c.c_customer_id LIMIT 100"
      },
      "invariants_kept": ["same result rows", "same ordering", "same column output"],
      "expected_speedup": "2.90x",
      "risk": "low"
    },
    {
      "id": "rs_02",
      "transform": "pushdown",
      "nodes": {
        "filtered_store_returns": "SELECT sr_customer_sk, sr_store_sk, sr_fee FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk JOIN store s ON sr_store_sk = s.s_store_sk WHERE d_year = 2000 AND s.s_state = 'SD'",
        "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM filtered_store_returns GROUP BY sr_customer_sk, sr_store_sk",
        "store_avg_return": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return_threshold FROM customer_total_return GROUP BY ctr_store_sk",
        "main_query": "SELECT c.c_customer_id FROM customer_total_return ctr1 JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk WHERE ctr1.ctr_total_return > sar.avg_return_threshold ORDER BY c.c_customer_id LIMIT 100"
      },
      "invariants_kept": ["same result rows", "same ordering", "same column output"],
      "expected_speedup": "1.8x",
      "risk": "low"
    }
  ],
  "explanation": "Applied two optimization techniques: (1) Decorrelated the subquery by extracting the store-specific average calculation into a separate CTE, eliminating per-row subquery execution. (2) Pushed the store filter (s_state='SD') down into the store_returns join, reducing data movement before aggregation."
}

# Response 2: Adjusted prompt (1 rewrite_set)
RESPONSE_2 = {
  "rewrite_sets": [
    {
      "id": "rs_01",
      "transform": "decorrelate",
      "nodes": {
        "filtered_store_returns": "SELECT sr_customer_sk, sr_store_sk, sr_fee FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk WHERE d_year = 2000",
        "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM filtered_store_returns GROUP BY sr_customer_sk, sr_store_sk",
        "store_avg_return": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return_threshold FROM customer_total_return GROUP BY ctr_store_sk",
        "main_query": "SELECT c_customer_id FROM customer_total_return ctr1 JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk JOIN store s ON ctr1.ctr_store_sk = s.s_store_sk JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk WHERE ctr1.ctr_total_return > sar.avg_return_threshold AND s.s_state = 'SD' ORDER BY c_customer_id LIMIT 100"
      },
      "invariants_kept": ["same result rows", "same ordering", "same column output", "same grouping and aggregation"],
      "expected_speedup": "2.90x",
      "risk": "low"
    }
  ],
  "explanation": "The correlated subquery computing average ctr_total_return per store was extracted into a separate CTE (store_avg_return) with GROUP BY ctr_store_sk. This allows computing the average once per store instead of per row. The store filter (s_state='SD') is kept in the main query as it doesn't affect the aggregate calculation."
}

def test_response(name, response):
    """Test a single response."""
    print(f"\n{'=' * 80}")
    print(f"TESTING: {name}")
    print(f"Rewrite sets: {len(response['rewrite_sets'])}")
    print("=" * 80)

    pipeline = DagV2Pipeline(ORIGINAL_SQL)

    for i, rs in enumerate(response['rewrite_sets'], 1):
        print(f"\n--- Rewrite Set {i}: {rs['id']} ({rs['transform']}) ---")
        print(f"Expected speedup: {rs['expected_speedup']}")

        # Test this individual rewrite_set
        single_rs_response = json.dumps({"rewrite_sets": [rs]})

        try:
            full_sql = pipeline.apply_response(single_rs_response)
            print(f"✓ Assembly successful ({len(full_sql)} chars)")

            # Validate syntax
            parsed = sqlglot.parse_one(full_sql, dialect='duckdb')
            ctes = list(parsed.find_all(sqlglot.exp.CTE))
            print(f"✓ Syntax valid - {len(ctes)} CTEs: {[str(c.alias) for c in ctes]}")

            # Show assembled SQL (first 500 chars)
            print(f"\nAssembled SQL preview:")
            print(full_sql[:500] + "..." if len(full_sql) > 500 else full_sql)

        except Exception as e:
            print(f"✗ Failed: {e}")
            import traceback
            traceback.print_exc()

    print(f"\nExplanation: {response['explanation'][:200]}...")

def compare_responses():
    """Compare the two responses."""
    print("\n" + "=" * 80)
    print("COMPARISON")
    print("=" * 80)

    print("\n**Response 1 (Original Reasoning):**")
    print(f"  • Rewrite sets: 2")
    print(f"  • Transforms: decorrelate (2.90x) + pushdown (1.8x)")
    print(f"  • Strategy: Two separate optimizations")
    print(f"  • Store filter location: IN JOIN (rs_01) and EARLY (rs_02)")

    print("\n**Response 2 (Adjusted Prompt):**")
    print(f"  • Rewrite sets: 1")
    print(f"  • Transforms: decorrelate (2.90x)")
    print(f"  • Strategy: Single comprehensive optimization")
    print(f"  • Store filter location: IN WHERE clause")

    print("\n**Key Differences:**")
    print("  1. Response 1 has 2 rewrite_sets (decorrelate + pushdown)")
    print("  2. Response 2 has 1 rewrite_set (decorrelate only)")
    print("  3. Response 1 rs_01: Store filter in JOIN condition")
    print("  4. Response 2 rs_01: Store filter in WHERE clause")
    print("  5. Response 2 explanation notes filter doesn't affect aggregates")

    print("\n**Which is better?**")
    print("  • Response 2 is more correct: store filter should stay in WHERE")
    print("    because pushing it early would change the average calculation")
    print("  • Response 1 rs_02 pushes filter TOO early (before aggregation)")
    print("  • Response 2 correctly keeps filter after decorrelation")

def main():
    """Test both responses."""
    test_response("Response 1 (Original Reasoning)", RESPONSE_1)
    test_response("Response 2 (Adjusted Prompt)", RESPONSE_2)
    compare_responses()

if __name__ == '__main__':
    main()
