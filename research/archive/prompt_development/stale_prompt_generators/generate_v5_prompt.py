#!/usr/bin/env python3
"""Generate JSON_V5 prompt for Q1 to inspect."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from qt_sql.optimization.adaptive_rewriter_v5 import (
    _get_plan_context,
    _build_base_prompt,
    _build_prompt_with_examples,
)
from qt_sql.optimization.dag_v3 import get_matching_examples, _split_example_batches

# Q1 SQL
Q1_SQL = """WITH customer_total_return AS (
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
        SELECT avg(ctr_total_return) * 1.2
        FROM customer_total_return ctr2
        WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
      )
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'SD'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100"""

# Sample DB path
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"

def main():
    print("Generating JSON_V5 prompt for Q1...")
    print("=" * 60)

    # Get plan context
    print("\n1. Getting execution plan context...")
    plan_summary, plan_text, plan_json = _get_plan_context(SAMPLE_DB, Q1_SQL)
    print(f"   Plan JSON: {'Present' if plan_json else 'MISSING'}")
    print(f"   Plan summary length: {len(plan_summary)} chars")

    # Build base DAG prompt
    print("\n2. Building base DAG prompt...")
    base_prompt = _build_base_prompt(Q1_SQL, plan_json)
    print(f"   Base prompt length: {len(base_prompt)} chars")

    # Get matching examples
    print("\n3. Getting matching examples...")
    examples = get_matching_examples(Q1_SQL)
    print(f"   Total examples: {len(examples)}")
    if examples:
        print(f"   Example IDs: {[ex.id for ex in examples[:5]]}")

    # Split into batches (worker 1 uses first batch)
    batches = _split_example_batches(examples, batch_size=3)
    worker1_examples = batches[0] if batches else []
    print(f"   Worker 1 will use: {[ex.id for ex in worker1_examples]}")

    # Build full prompt (worker 1)
    print("\n4. Building full prompt (worker 1)...")
    full_prompt = _build_prompt_with_examples(
        base_prompt,
        worker1_examples,
        plan_summary,
        history=""
    )
    print(f"   Full prompt length: {len(full_prompt)} chars")
    print(f"   Full prompt lines: {len(full_prompt.splitlines())}")

    # Save prompt
    output_dir = Path(__file__).parent / "packages/qt-sql/prompts"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "v5_q1_prompt_fresh.txt"
    output_file.write_text(full_prompt)
    print(f"\n✓ Prompt saved to: {output_file}")

    # Show key sections
    print("\n" + "=" * 60)
    print("KEY SECTIONS:")
    print("=" * 60)

    lines = full_prompt.splitlines()
    for i, line in enumerate(lines, 1):
        if line.startswith("## "):
            print(f"Line {i}: {line}")

    print("\n" + "=" * 60)
    print("COST ATTRIBUTION SECTION:")
    print("=" * 60)

    in_cost = False
    for line in lines:
        if line.startswith("## Cost Attribution"):
            in_cost = True
        elif in_cost and line.startswith("## "):
            break
        elif in_cost:
            print(line)

if __name__ == "__main__":
    main()
