#!/usr/bin/env python3
"""Test v7 prompt format on queries where DSPy struggled and succeeded.

Uses the existing test_optimization() from iterative_optimizer.py for proper
benchmarking methodology:
- Multiple runs (default 3)
- Discards first run (cache warmup)
- Averages remaining runs (warm cache)
- Validates semantic equivalence
"""

import os
import sys
import re
import json
from pathlib import Path
from openai import OpenAI

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-sql"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "packages" / "qt-shared"))

from qt_sql.optimization.iterative_optimizer import test_optimization

V7_TEMPLATE = """Optimize this SQL query for performance.

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

{bottlenecks}

## Table Scans (with selectivity)

{table_scans}

## SQL

```sql
{query}
```

## Output

Return:
1. The optimized SQL query
2. One sentence explaining what you changed and why
"""

# Test queries with their plan data
QUERIES = {
    "q3": {
        "name": "Q3 (DSPy struggled: 0.61x with Kimi)",
        "sql": """select dt.d_year
       ,item.i_brand_id brand_id
       ,item.i_brand brand
       ,sum(ss_sales_price) sum_agg
 from  date_dim dt
      ,store_sales
      ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 816
   and dt.d_moy=11
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 LIMIT 100;""",
        "bottlenecks": """- SEQ_SCAN (store_sales): 94.2% cost, 2,859,381 rows <-- BOTTLENECK
- SEQ_SCAN (item): 2.5% cost, 51 rows
- HASH_JOIN: 1.6% cost, 841 rows""",
        "table_scans": """- store_sales: 2,859,381 rows (NO FILTER) <-- BOTTLENECK
- date_dim: 73K -> 150 rows (FILTERED by d_moy=11) <-- HIGH SELECTIVITY
- item: 204K -> 51 rows (FILTERED by i_manufact_id=816) <-- HIGH SELECTIVITY, JOIN EARLY"""
    },
    "q27": {
        "name": "Q27 (DSPy struggled: 0.71x with Kimi)",
        "sql": """select i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'D' and
       cd_education_status = 'Secondary' and
       d_year = 1999 and
       s_state in ('MO','AL', 'MI', 'TN', 'LA', 'SC')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
 LIMIT 100;""",
        "bottlenecks": """- SEQ_SCAN (store_sales): 83.9% cost, 550,426 rows <-- BOTTLENECK
- SEQ_SCAN (customer_demographics): 6.9% cost, 27,440 rows
- HASH_JOIN: 4.3% cost, 7,682 rows""",
        "table_scans": """- store_sales: 550,426 rows (NO FILTER) <-- BOTTLENECK
- customer_demographics: 1.9M -> 27,440 rows (FILTERED by cd_gender='F', cd_marital_status='D', cd_education_status='Secondary') <-- HIGH SELECTIVITY, JOIN EARLY
- date_dim: 73K -> 365 rows (FILTERED by d_year=1999)
- store: 402 rows (FILTERED by s_state IN 6 states)
- item: 204K rows (dynamic filter)"""
    },
    "q15": {
        "name": "Q15 (DSPy succeeded: 2.98x with DeepSeek)",
        "sql": """select ca_zip
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
 LIMIT 100;""",
        "bottlenecks": """- HASH_JOIN: 26.6% cost, 1,432,318 rows
- SEQ_SCAN (catalog_sales): 19.6% cost, 1,439,513 rows <-- BOTTLENECK
- SEQ_SCAN (customer): 14.6% cost, 1,999,998 rows
- SEQ_SCAN (customer_address): 11.1% cost, 1,000,000 rows""",
        "table_scans": """- catalog_sales: 1,439,513 rows (NO FILTER) <-- BOTTLENECK
- customer: 2M rows (minimal filter)
- customer_address: 1M rows (dynamic filter on ca_zip)
- date_dim: 73K -> 91 rows (FILTERED by d_qoy=1, d_year=2001) <-- HIGH SELECTIVITY, JOIN EARLY"""
    }
}

def extract_sql(response: str) -> str:
    """Extract SQL from response."""
    # Try to find SQL in code block
    sql_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
    if sql_match:
        return sql_match.group(1).strip()

    # Try plain code block
    code_match = re.search(r'```\s*(.*?)\s*```', response, re.DOTALL)
    if code_match:
        return code_match.group(1).strip()

    # Find SELECT statement
    select_match = re.search(r'((?:WITH|SELECT)[\s\S]+?(?:LIMIT\s+\d+|;))', response, re.IGNORECASE)
    if select_match:
        return select_match.group(1).strip().rstrip(';') + ';'

    return response.strip()


def call_kimi(prompt: str, api_key: str) -> str:
    """Call Kimi K2.5 via OpenRouter."""
    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )

    response = client.chat.completions.create(
        model="moonshotai/kimi-k2.5",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )
    return response.choices[0].message.content


def call_deepseek(prompt: str, api_key: str) -> str:
    """Call DeepSeek V3."""
    client = OpenAI(
        base_url="https://api.deepseek.com",
        api_key=api_key,
    )

    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )
    return response.choices[0].message.content


def main():
    # Load API keys
    openrouter_key = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/openrouter.txt").read_text().strip()
    deepseek_key = os.environ.get("DEEPSEEK_API_KEY", "")

    if not deepseek_key:
        # Try to load from common locations
        for path in [Path.home() / ".deepseek_api_key", Path("/mnt/c/Users/jakc9/.deepseek_api_key")]:
            if path.exists():
                deepseek_key = path.read_text().strip()
                break

    # Database path
    db_path = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
    print(f"Using database: {db_path}")
    print(f"Benchmark: 3 runs, first discarded (warmup), avg of remaining 2")
    print()

    # Output directory
    output_dir = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/experiments/v7_test")
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for query_id, query_data in QUERIES.items():
        print(f"{'='*60}")
        print(f"Testing {query_data['name']}")
        print('='*60)

        # Build v7 prompt
        prompt = V7_TEMPLATE.format(
            bottlenecks=query_data['bottlenecks'],
            table_scans=query_data['table_scans'],
            query=query_data['sql']
        )

        # Save prompt
        (output_dir / f"{query_id}_v7_prompt.txt").write_text(prompt)

        # Test with Kimi
        print(f"\nCalling Kimi K2.5...")
        try:
            kimi_response = call_kimi(prompt, openrouter_key)
            (output_dir / f"{query_id}_kimi_response.txt").write_text(kimi_response)

            kimi_sql = extract_sql(kimi_response)
            (output_dir / f"{query_id}_kimi_optimized.sql").write_text(kimi_sql)

            # Use proper benchmark from iterative_optimizer
            print(f"  Running benchmark (3 runs, discard 1st, avg 2nd+3rd)...")
            kimi_result = test_optimization(
                original_sql=query_data['sql'],
                optimized_sql=kimi_sql,
                db_path=db_path,
                runs=3,
            )

            if kimi_result.error:
                print(f"  Kimi: ERROR - {kimi_result.error}")
                kimi_speedup = 0
            elif not kimi_result.semantically_correct:
                print(f"  Kimi: INVALID - results differ")
                print(f"    Original rows: {len(kimi_result.original_result) if kimi_result.original_result else 0}")
                print(f"    Optimized rows: {len(kimi_result.optimized_result) if kimi_result.optimized_result else 0}")
                kimi_speedup = 0
            else:
                print(f"  Original:  {kimi_result.original_time:.3f}s (warm avg)")
                print(f"  Optimized: {kimi_result.optimized_time:.3f}s (warm avg)")
                print(f"  Speedup:   {kimi_result.speedup:.2f}x")
                print(f"  Semantics: CORRECT")
                kimi_speedup = kimi_result.speedup

        except Exception as e:
            print(f"  Kimi error: {e}")
            kimi_speedup = 0

        # Test with DeepSeek if available
        if deepseek_key:
            print(f"\nCalling DeepSeek V3...")
            try:
                ds_response = call_deepseek(prompt, deepseek_key)
                (output_dir / f"{query_id}_deepseek_response.txt").write_text(ds_response)

                ds_sql = extract_sql(ds_response)
                (output_dir / f"{query_id}_deepseek_optimized.sql").write_text(ds_sql)

                # Use proper benchmark
                print(f"  Running benchmark (3 runs, discard 1st, avg 2nd+3rd)...")
                ds_result = test_optimization(
                    original_sql=query_data['sql'],
                    optimized_sql=ds_sql,
                    db_path=db_path,
                    runs=3,
                )

                if ds_result.error:
                    print(f"  DeepSeek: ERROR - {ds_result.error}")
                    ds_speedup = 0
                elif not ds_result.semantically_correct:
                    print(f"  DeepSeek: INVALID - results differ")
                    ds_speedup = 0
                else:
                    print(f"  Original:  {ds_result.original_time:.3f}s (warm avg)")
                    print(f"  Optimized: {ds_result.optimized_time:.3f}s (warm avg)")
                    print(f"  Speedup:   {ds_result.speedup:.2f}x")
                    print(f"  Semantics: CORRECT")
                    ds_speedup = ds_result.speedup

            except Exception as e:
                print(f"  DeepSeek error: {e}")
                ds_speedup = 0
        else:
            ds_speedup = 0
            print("\nDeepSeek: skipped (no API key)")

        results.append({
            "query": query_id,
            "name": query_data['name'],
            "kimi_speedup": kimi_speedup,
            "deepseek_speedup": ds_speedup
        })

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY: v7 Prompt Results (Proper Benchmark)")
    print("Methodology: 3 runs, discard 1st (warmup), avg 2nd+3rd")
    print('='*60)
    print(f"{'Query':<8} {'Description':<40} {'Kimi':<10} {'DeepSeek':<10}")
    print("-"*68)
    for r in results:
        kimi = f"{r['kimi_speedup']:.2f}x" if r['kimi_speedup'] > 0 else "FAIL"
        ds = f"{r['deepseek_speedup']:.2f}x" if r['deepseek_speedup'] > 0 else "N/A"
        print(f"{r['query']:<8} {r['name'][:40]:<40} {kimi:<10} {ds:<10}")

    # Save results
    (output_dir / "results.json").write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to {output_dir}/results.json")


if __name__ == "__main__":
    main()
