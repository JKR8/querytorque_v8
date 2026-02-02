#!/usr/bin/env python3
"""
Test DAG v2 on Q1 with Kimi K2.5

Usage:
    .venv/bin/python research/scripts/test_dag_v2.py q1
"""

import os
import sys
import json
import time
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import requests

from qt_sql.optimization.dag_v2 import DagV2Pipeline, get_dag_v2_examples

# ============================================================
# Configuration
# ============================================================
API_KEY = os.getenv("OPENROUTER_API_KEY")
if not API_KEY:
    key_file = Path(__file__).parent.parent.parent / "openrouter.txt"
    if key_file.exists():
        API_KEY = key_file.read_text().strip()
    else:
        print("ERROR: Set OPENROUTER_API_KEY or create openrouter.txt")
        sys.exit(1)

API_BASE = "https://openrouter.ai/api/v1/chat/completions"
MODEL_NAME = "moonshotai/kimi-k2.5"
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
FULL_DB = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"

# ============================================================
# Q1 Original SQL
# ============================================================
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


def build_few_shot_prompt(examples: list) -> str:
    """Build few-shot examples section."""
    lines = ["## Few-Shot Examples (VERIFIED speedups)", ""]
    for i, ex in enumerate(examples, 1):
        lines.append(f"### Example {i}: {ex['opportunity']} ({ex['speedup']})")
        lines.append("")
        lines.append("Input:")
        lines.append("```sql")
        lines.append(ex['input_slice'])
        lines.append("```")
        lines.append("")
        lines.append("Output:")
        lines.append("```json")
        lines.append(json.dumps(ex['output'], indent=2))
        lines.append("```")
        lines.append("")
    return "\n".join(lines)


def main():
    query_id = sys.argv[1] if len(sys.argv) > 1 else "q1"

    print(f"Testing DAG v2 on {query_id}")
    print("=" * 60)

    # Build DAG v2 pipeline
    pipeline = DagV2Pipeline(Q1_SQL)

    # Show DAG summary
    print("\nDAG Structure:")
    print(pipeline.get_dag_summary())

    # Build prompt
    base_prompt = pipeline.get_prompt()

    # Add few-shot examples
    examples = get_dag_v2_examples()
    few_shot = build_few_shot_prompt(examples)

    full_prompt = few_shot + "\n\n---\n\n" + base_prompt

    print(f"\nPrompt length: {len(full_prompt)} chars")

    # Save prompt for inspection
    prompt_file = Path(f"/tmp/dag_v2_{query_id}_prompt.txt")
    prompt_file.write_text(full_prompt)
    print(f"Prompt saved to: {prompt_file}")

    # Call Kimi
    print(f"\nCalling {MODEL_NAME}...")
    llm_start = time.time()

    response = requests.post(
        API_BASE,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://querytorque.com",
            "X-Title": "QueryTorque DAG v2 Test"
        },
        json={
            "model": MODEL_NAME,
            "messages": [
                {"role": "user", "content": full_prompt}
            ],
            "temperature": 0.1,
        },
        timeout=600
    )

    llm_time = time.time() - llm_start

    if response.status_code != 200:
        print(f"ERROR: API returned {response.status_code}")
        print(response.text)
        sys.exit(1)

    result = response.json()
    content = result["choices"][0]["message"]["content"]

    print(f"LLM response in {llm_time:.1f}s")
    print("\n" + "=" * 60)
    print("LLM Response:")
    print("=" * 60)
    print(content)
    print("=" * 60)

    # Save response
    response_file = Path(f"/tmp/dag_v2_{query_id}_response.txt")
    response_file.write_text(content)

    # Apply response to get optimized SQL
    optimized_sql = pipeline.apply_response(content)

    print("\n" + "=" * 60)
    print("Optimized SQL:")
    print("=" * 60)
    print(optimized_sql)
    print("=" * 60)

    # Save optimized SQL
    opt_file = Path(f"/tmp/dag_v2_{query_id}_optimized.sql")
    opt_file.write_text(optimized_sql)
    print(f"\nOptimized SQL saved to: {opt_file}")

    # Save original for validation
    orig_file = Path(f"/tmp/dag_v2_{query_id}_original.sql")
    orig_file.write_text(Q1_SQL)

    print("\n" + "=" * 60)
    print("Validation Commands:")
    print("=" * 60)
    print(f"# Sample DB:")
    print(f"cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql && ../../.venv/bin/python cli/main.py validate {orig_file} {opt_file} --database {SAMPLE_DB}")
    print(f"\n# Full DB:")
    print(f"cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql && ../../.venv/bin/python cli/main.py validate {orig_file} {opt_file} --database {FULL_DB} --mode full")


if __name__ == "__main__":
    main()
