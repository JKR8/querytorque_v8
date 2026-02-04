#!/usr/bin/env python3
"""Test DeepSeek response for Q1."""

import sys
import json
sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

from qt_sql.optimization.dag_v2 import DagV2Pipeline

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

# DeepSeek response
DEEPSEEK_RESPONSE = {
  "rewrite_sets": [
    {
      "id": "rs_01",
      "transform": "decorrelate",
      "nodes": {
        "filtered_dates": "SELECT d_date_sk FROM date_dim WHERE d_year = 2000",
        "store_filtered_returns": "SELECT sr_customer_sk, sr_store_sk, sr_fee FROM store_returns JOIN filtered_dates ON sr_returned_date_sk = d_date_sk",
        "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM store_filtered_returns GROUP BY sr_customer_sk, sr_store_sk",
        "store_avg_return": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_threshold FROM customer_total_return GROUP BY ctr_store_sk",
        "main_query": "SELECT c_customer_id FROM customer_total_return ctr1 JOIN store s ON ctr1.ctr_store_sk = s.s_store_sk JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk WHERE s.s_state = 'SD' AND ctr1.ctr_total_return > sar.avg_threshold ORDER BY c_customer_id LIMIT 100"
      },
      "invariants_kept": [
        "same result rows",
        "same ordering",
        "same column output",
        "same grouping and aggregation"
      ],
      "expected_speedup": "2.8x",
      "risk": "low"
    },
    {
      "id": "rs_02",
      "transform": "pushdown",
      "nodes": {
        "store_filtered_returns": "SELECT sr_customer_sk, sr_store_sk, sr_fee FROM store_returns JOIN (SELECT d_date_sk FROM date_dim WHERE d_year = 2000) AS filtered_dates ON sr_returned_date_sk = d_date_sk JOIN store s ON sr_store_sk = s.s_store_sk WHERE s.s_state = 'SD'",
        "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM store_filtered_returns GROUP BY sr_customer_sk, sr_store_sk",
        "store_avg_return": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_threshold FROM customer_total_return GROUP BY ctr_store_sk",
        "main_query": "SELECT c_customer_id FROM customer_total_return ctr1 JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk WHERE ctr1.ctr_total_return > sar.avg_threshold ORDER BY c_customer_id LIMIT 100"
      },
      "invariants_kept": [
        "same result rows",
        "same ordering",
        "same column output",
        "same grouping and aggregation"
      ],
      "expected_speedup": "1.5x",
      "risk": "low"
    }
  ],
  "explanation": "Applied two complementary optimizations: (1) DECORRELATE: eliminated the correlated subquery by pre-computing store averages in a CTE and using JOIN instead of correlated comparison. This allows the database to compute averages once per store rather than per row. (2) PUSHDOWN: moved the store.s_state='SD' filter earlier into the returns processing to reduce the data volume before aggregation. Both transforms work together to reduce redundant computation and early filter fact data."
}

def main():
    """Test DeepSeek response."""
    print("=" * 80)
    print("TESTING DEEPSEEK Q1 RESPONSE")
    print("=" * 80)

    # Test each rewrite_set
    for i, rewrite_set in enumerate(DEEPSEEK_RESPONSE['rewrite_sets'], 1):
        print(f"\n{'=' * 80}")
        print(f"REWRITE SET {i}: {rewrite_set['id']} ({rewrite_set['transform']})")
        print(f"Expected speedup: {rewrite_set['expected_speedup']}")
        print("=" * 80)

        # Assemble to full SQL
        print("\n[1/3] Assembling nodes to full SQL...")
        try:
            # Create pipeline and apply the rewrite set
            pipeline = DagV2Pipeline(ORIGINAL_SQL)

            # Convert single rewrite_set to JSON response format
            response_json = json.dumps({"rewrite_sets": [rewrite_set]})

            full_sql = pipeline.apply_response(response_json)
            print("✓ Assembly successful")

            # Print the assembled SQL
            print(f"\n[2/3] Assembled SQL ({len(full_sql)} chars):")
            print("-" * 80)
            print(full_sql)
            print("-" * 80)

        except Exception as e:
            print(f"✗ Assembly failed: {e}")
            import traceback
            traceback.print_exc()
            continue

        # Validate syntax
        print("\n[3/3] Validating SQL syntax...")
        try:
            import sqlglot
            parsed = sqlglot.parse_one(full_sql, dialect='duckdb')
            print("✓ SQL syntax valid")

            # Check for CTEs
            ctes = list(parsed.find_all(sqlglot.exp.CTE))
            print(f"✓ Found {len(ctes)} CTEs: {[str(cte.alias) for cte in ctes]}")

        except Exception as e:
            print(f"✗ SQL syntax invalid: {e}")
            continue

        print(f"\n✅ Rewrite set {i} validated successfully!")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total rewrite sets: {len(DEEPSEEK_RESPONSE['rewrite_sets'])}")
    print(f"Transforms used: {', '.join(rs['transform'] for rs in DEEPSEEK_RESPONSE['rewrite_sets'])}")
    print(f"\nExplanation:")
    print(DEEPSEEK_RESPONSE['explanation'])

    print("\n" + "=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    print("To benchmark these queries:")
    print("1. Connect to TPC-DS database")
    print("2. Execute original SQL and measure time")
    print("3. Execute each optimized SQL and measure time")
    print("4. Calculate speedup = original_time / optimized_time")
    print("5. Compare with expected speedups (2.8x and 1.5x)")

if __name__ == '__main__':
    main()
