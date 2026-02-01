#!/usr/bin/env python3
"""Test v7 prompt with Gemini on Q15."""

import os
import sys
import re
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-sql"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-shared"))

from qt_sql.optimization.iterative_optimizer import test_optimization

from openai import OpenAI

V7_PROMPT = """Optimize this SQL query for performance.

## Algorithm

1. **FIND BOTTLENECK**: Look at the largest row sources below
2. **REDUCE EARLY**: For each bottleneck, ask "can we filter this earlier?"
   - Push dimension filters INTO CTEs/subqueries before GROUP BY
   - Convert correlated subqueries to window functions or CTEs
   - Join small filtered tables early to reduce rows before aggregation
3. **VERIFY**: Result must return identical data

Principle: The earlier you reduce rows, the faster everything downstream runs.

## Example

WRONG - filters 1M rows AFTER aggregating all 100M:
```sql
WITH sales_agg AS (
    SELECT store_id, SUM(amount) as total
    FROM sales
    GROUP BY store_id  -- aggregates ALL 100M rows
)
SELECT * FROM sales_agg
WHERE store_id IN (SELECT store_id FROM stores WHERE region = 'West')  -- filters AFTER
```

RIGHT - filters to 1M rows BEFORE aggregating:
```sql
WITH sales_agg AS (
    SELECT s.store_id, SUM(amount) as total
    FROM sales
    JOIN stores s ON sales.store_id = s.store_id
    WHERE s.region = 'West'  -- filter BEFORE GROUP BY
    GROUP BY s.store_id  -- aggregates only 1M West rows
)
SELECT * FROM sales_agg
```

The RIGHT version is 100x faster because it aggregates 100x fewer rows.

## Bottlenecks

- HASH_JOIN: 26.6% cost, 1,432,318 rows
- SEQ_SCAN (catalog_sales): 19.6% cost, 1,439,513 rows <-- BOTTLENECK
- SEQ_SCAN (customer): 14.6% cost, 1,999,998 rows
- SEQ_SCAN (customer_address): 11.1% cost, 1,000,000 rows

## Table Scans (with selectivity)

- catalog_sales: 1,439,513 rows (NO FILTER) <-- BOTTLENECK
- customer: 2M rows (minimal filter)
- customer_address: 1M rows (dynamic filter on ca_zip)
- date_dim: 73K -> 91 rows (FILTERED by d_qoy=1, d_year=2001) <-- HIGH SELECTIVITY, JOIN EARLY

## SQL

```sql
select ca_zip
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
 LIMIT 100;
```

## Output

Return:
1. The optimized SQL query
2. One sentence explaining what you changed and why
"""

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


def extract_sql(response: str) -> str:
    """Extract SQL from response."""
    sql_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
    if sql_match:
        return sql_match.group(1).strip()
    code_match = re.search(r'```\s*(.*?)\s*```', response, re.DOTALL)
    if code_match:
        return code_match.group(1).strip()
    select_match = re.search(r'((?:WITH|SELECT)[\s\S]+?(?:LIMIT\s+\d+|;))', response, re.IGNORECASE)
    if select_match:
        return select_match.group(1).strip().rstrip(';') + ';'
    return response.strip()


def call_gemini(prompt: str, api_key: str) -> str:
    """Call Gemini 3 Pro Preview via OpenRouter."""
    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )
    response = client.chat.completions.create(
        model="google/gemini-3-pro-preview",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )
    return response.choices[0].message.content


def main():
    # Get OpenRouter API key
    openrouter_key = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/openrouter.txt").read_text().strip()

    db_path = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
    output_dir = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/experiments/v7_gemini")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("="*60)
    print("Testing v7 + Gemini on Q15")
    print("Benchmark: 3 runs, discard 1st (warmup), avg 2nd+3rd")
    print("="*60)

    # Save prompt
    (output_dir / "q15_v7_prompt.txt").write_text(V7_PROMPT)

    # Call Gemini
    print("\nCalling Gemini 3 Pro Preview via OpenRouter...")
    try:
        response_text = call_gemini(V7_PROMPT, openrouter_key)

        (output_dir / "q15_gemini_response.txt").write_text(response_text)
        print(f"  Response received ({len(response_text)} chars)")

        optimized_sql = extract_sql(response_text)
        (output_dir / "q15_gemini_optimized.sql").write_text(optimized_sql)

        print(f"\n  Optimized SQL:\n{'-'*40}")
        print(optimized_sql[:500] + "..." if len(optimized_sql) > 500 else optimized_sql)
        print('-'*40)

        # Benchmark
        print(f"\n  Benchmarking (3 runs, discard 1st)...")
        result = test_optimization(
            original_sql=Q15_SQL,
            optimized_sql=optimized_sql,
            db_path=db_path,
            runs=3,
        )

        if result.error:
            print(f"  ERROR: {result.error}")
        elif not result.semantically_correct:
            print(f"  INVALID: Results differ")
            print(f"    Original: {len(result.original_result) if result.original_result else 0} rows")
            print(f"    Optimized: {len(result.optimized_result) if result.optimized_result else 0} rows")
        else:
            print(f"\n  RESULTS:")
            print(f"    Original:  {result.original_time:.3f}s")
            print(f"    Optimized: {result.optimized_time:.3f}s")
            print(f"    Speedup:   {result.speedup:.2f}x")
            print(f"    Semantics: CORRECT")

        # Compare
        print(f"\n{'='*60}")
        print("COMPARISON: Q15")
        print('='*60)
        print(f"  DSPy + DeepSeek: 2.98x")
        print(f"  v7 + Kimi:       1.13x")
        print(f"  v7 + Gemini:     {result.speedup:.2f}x" if not result.error and result.semantically_correct else "  v7 + Gemini:     FAILED")

    except Exception as e:
        print(f"  Exception: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
