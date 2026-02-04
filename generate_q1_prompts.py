#!/usr/bin/env python3
"""Generate Q1 prompts with updated ALLOWED_TRANSFORMS."""

import sys
sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_json_queue
from qt_sql.optimization.dag_v3 import load_all_examples, GoldExample
from qt_sql.optimization.query_recommender import get_query_recommendations
from pathlib import Path

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

# Dummy EXPLAIN plan for Worker 5
EXPLAIN_PLAN = """
Operators by cost:
- SEQ_SCAN: 69.4% cost, 1,999,335 rows
- HASH_JOIN: 7.7% cost, 7,986 rows
- SEQ_SCAN: 4.7% cost, 56,138 rows
- HASH_GROUP_BY: 4.2% cost, 55,341 rows
- LEFT_DELIM_JOIN: 3.9% cost, 0 rows

Scans:
- store_returns: 865,743 rows (no filter)
- date_dim: 73,049 → 366 rows (filtered)
- customer: 24,000,000 rows (no filter)

Joins:
- HASH_JOIN: store_returns x date_dim -> 56,138 rows
- LEFT_DELIM_JOIN: ctr1 x ctr2 -> 0 rows (correlated)
- HASH_JOIN: customer x ctr1 -> 7,986 rows
""".strip()

def main():
    """Generate Q1 prompts."""
    print("Generating Q1 prompts with updated ALLOWED_TRANSFORMS...")

    # Get ML recommendations for Q1
    ml_recs = get_query_recommendations('q1', top_n=12)
    print(f"Q1 ML recommendations: {ml_recs[:3]}")

    # Load all gold examples
    all_examples = load_all_examples()
    print(f"Loaded {len(all_examples)} gold examples")

    # Pad to 12 examples
    padded_recs = ml_recs[:]
    remaining = [ex.id for ex in all_examples if ex.id not in padded_recs]
    while len(padded_recs) < 12 and remaining:
        padded_recs.append(remaining.pop(0))

    print(f"Padded to {len(padded_recs)} examples")

    # Split into 4 batches for workers 1-4
    batches = [
        padded_recs[0:3],   # Worker 1
        padded_recs[3:6],   # Worker 2
        padded_recs[6:9],   # Worker 3
        padded_recs[9:12],  # Worker 4
    ]

    # Generate Worker 1 prompt
    print("\n=== Generating Worker 1 prompt ===")
    worker1_examples = [ex for ex in all_examples if ex.id in batches[0]]

    from qt_sql.optimization.dag_v2 import DagBuilder, DagV2PromptBuilder

    # Build DAG
    dag_builder = DagBuilder(Q1_SQL)
    dag = dag_builder.build()

    # Build prompt with examples
    prompt_builder = DagV2PromptBuilder(dag)

    # Get ALLOWED_TRANSFORMS
    allowed_transforms = DagBuilder.ALLOWED_TRANSFORMS
    print(f"ALLOWED_TRANSFORMS: {len(allowed_transforms)} transforms")

    # Build base prompt
    base_prompt = prompt_builder.build_prompt()

    # Add examples
    from qt_sql.optimization.dag_v3 import build_prompt_with_examples
    worker1_prompt = build_prompt_with_examples(
        base_prompt=base_prompt,
        examples=worker1_examples
    )

    # Save Worker 1 prompt
    prompt_dir = Path('/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql/prompts')
    prompt_dir.mkdir(exist_ok=True)

    worker1_file = prompt_dir / 'q1_worker1_prompt.txt'
    worker1_file.write_text(worker1_prompt)
    print(f"✓ Saved Worker 1 prompt: {worker1_file}")
    print(f"  Length: {len(worker1_prompt)} chars")
    print(f"  Examples: {[ex.id for ex in worker1_examples]}")

    # Generate Worker 5 prompt (Full SQL)
    print("\n=== Generating Worker 5 prompt ===")

    worker5_prompt = f"""You are a SQL optimizer. Rewrite the ENTIRE query for maximum performance.

## Adversarial Explore Mode
Be creative and aggressive. Try radical structural rewrites that the database
engine is unlikely to do automatically. Don't be constrained by incremental changes.

## Original Query
```sql
{Q1_SQL}
```

## Full Execution Plan (EXPLAIN ANALYZE)
```
{EXPLAIN_PLAN}
```

## Instructions
1. Analyze the execution plan bottlenecks
2. Rewrite the entire query for maximum performance
3. Try transforms like:
   - Decorrelating subqueries
   - Converting OR to UNION ALL
   - Pushing down filters aggressively
   - Materializing CTEs strategically
   - Reordering joins
   - Eliminating redundant operations

## Output Format
Return ONLY the complete optimized SQL query. No JSON. No explanation. Just SQL.

Example output:
WITH cte1 AS (
  SELECT ...
)
SELECT ...
FROM cte1
...
"""

    worker5_file = prompt_dir / 'q1_worker5_prompt.txt'
    worker5_file.write_text(worker5_prompt)
    print(f"✓ Saved Worker 5 prompt: {worker5_file}")
    print(f"  Length: {len(worker5_prompt)} chars")
    print(f"  Format: Full SQL (no DAG JSON)")

    print("\n✅ Done! Prompts saved to:")
    print(f"  - {worker1_file}")
    print(f"  - {worker5_file}")

if __name__ == '__main__':
    main()
