#!/usr/bin/env python3
"""Test v7 prompt on queries where DSPy+DeepSeek achieved big speedups.

Target: Q15 (2.98x), Q23 (2.33x), Q39 (2.44x), Q95 (2.25x)
"""

import os
import sys
import re
import json
from pathlib import Path
from openai import OpenAI

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

QUERIES = {
    "q15": {
        "name": "Q15 (DSPy+DeepSeek: 2.98x)",
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
    },
    "q23": {
        "name": "Q23 (DSPy+DeepSeek: 2.33x)",
        "sql": """with frequent_ss_items as
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk
    and d_year in (2000,2000+1,2000+2,2000+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3)
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select * from max_store_sales))
  select sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales
           ,date_dim
       where d_year = 2000
         and d_moy = 5
         and cs_sold_date_sk = d_date_sk
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from web_sales
           ,date_dim
       where d_year = 2000
         and d_moy = 5
         and ws_sold_date_sk = d_date_sk
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer))
 LIMIT 100;""",
        "bottlenecks": """- HASH_GROUP_BY: 22.9% cost, 1,638,079 rows <-- BOTTLENECK
- HASH_GROUP_BY: 13.9% cost, 1,456,609 rows
- SEQ_SCAN (store_sales): 1,653,986 rows (3 scans!) <-- REPEATED SCANS
- SEQ_SCAN (customer): 1,999,997 rows (3 scans!)""",
        "table_scans": """- store_sales: scanned 3x (NO FILTER in best_ss_customer) <-- SCAN CONSOLIDATION OPPORTUNITY
- customer: scanned 3x (NO FILTER in best_ss_customer)
- date_dim: 73K -> 1,461 rows (FILTERED by d_year 2000-2003) <-- HIGH SELECTIVITY
- date_dim: 73K -> 31 rows (FILTERED by d_year=2000, d_moy=5) <-- VERY HIGH SELECTIVITY

Filter Gap: best_ss_customer scans ALL store_sales without year filter, but max_store_sales has year filter"""
    },
    "q39": {
        "name": "Q39 (DSPy+DeepSeek: 2.44x)",
        "sql": """with inv as
(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
          ,item
          ,warehouse
          ,date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year =1998
      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
 where case mean when 0 then 0 else stdev/mean end > 1)
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv inv1,inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=1
  and inv2.d_moy=1+1
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov;""",
        "bottlenecks": """- HASH_GROUP_BY: 60.6% cost, 796,718 rows <-- BOTTLENECK
- HASH_JOIN: 20.3% cost, 810,665 rows
- SEQ_SCAN (inventory): 13.6% cost, 810,665 rows""",
        "table_scans": """- inventory: 810,665 rows (NO FILTER) <-- BOTTLENECK
- item: 204K rows (NO FILTER)
- date_dim: 73K -> 365 rows (FILTERED by d_year=1998) <-- HIGH SELECTIVITY, JOIN EARLY
- warehouse: 15 rows (NO FILTER)

CTE inv is self-joined (inv1, inv2) with d_moy=1 and d_moy=2 filters. Consider splitting."""
    },
    "q95": {
        "name": "Q95 (DSPy+DeepSeek: 2.25x)",
        "sql": """with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1,web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 select
   count(distinct ws_order_number) as "order count"
  ,sum(ws_ext_ship_cost) as "total shipping cost"
  ,sum(ws_net_profit) as "total net profit"
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '1999-2-01' and
           (cast('1999-2-01' as date) + INTERVAL 60 DAY)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'NC'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
 LIMIT 100;""",
        "bottlenecks": """- SEQ_SCAN (web_sales): 28.9% cost, 60,193 rows
- SEQ_SCAN (customer_address): 24.6% cost, 31,331 rows
- SEQ_SCAN (web_sales): 15.8% cost, 719,845 rows <-- REPEATED SCAN
- HASH_JOIN: 12.1% cost, 77,632 rows""",
        "table_scans": """- web_sales: scanned 2x in CTE ws_wh (self-join) <-- SCAN CONSOLIDATION
- web_sales: 719,845 rows in main query (NO FILTER) <-- BOTTLENECK
- web_returns: 71,681 rows (NO FILTER)
- date_dim: 73K -> 61 rows (FILTERED by date range) <-- HIGH SELECTIVITY, JOIN EARLY
- customer_address: 1M -> 31,331 rows (FILTERED by ca_state='NC') <-- HIGH SELECTIVITY
- web_site: 30 -> 2 rows (FILTERED by web_company_name='pri') <-- VERY HIGH SELECTIVITY"""
    }
}

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


def main():
    openrouter_key = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/openrouter.txt").read_text().strip()
    db_path = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"

    print("="*70)
    print("v7 Prompt Test on High-Speedup Queries")
    print("Benchmark: 3 runs, discard 1st (warmup), avg 2nd+3rd")
    print("="*70)

    output_dir = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/experiments/v7_bigwins")
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for query_id, query_data in QUERIES.items():
        print(f"\n{'='*60}")
        print(f"Testing {query_data['name']}")
        print('='*60)

        prompt = V7_TEMPLATE.format(
            bottlenecks=query_data['bottlenecks'],
            table_scans=query_data['table_scans'],
            query=query_data['sql']
        )
        (output_dir / f"{query_id}_v7_prompt.txt").write_text(prompt)

        print(f"\nCalling Kimi K2.5...")
        try:
            response = call_kimi(prompt, openrouter_key)
            (output_dir / f"{query_id}_kimi_response.txt").write_text(response)

            optimized_sql = extract_sql(response)
            (output_dir / f"{query_id}_kimi_optimized.sql").write_text(optimized_sql)

            print(f"  Benchmarking (3 runs, discard 1st)...")
            result = test_optimization(
                original_sql=query_data['sql'],
                optimized_sql=optimized_sql,
                db_path=db_path,
                runs=3,
            )

            if result.error:
                print(f"  ERROR: {result.error}")
                speedup = 0
            elif not result.semantically_correct:
                print(f"  INVALID: Results differ")
                print(f"    Original: {len(result.original_result) if result.original_result else 0} rows")
                print(f"    Optimized: {len(result.optimized_result) if result.optimized_result else 0} rows")
                speedup = 0
            else:
                print(f"  Original:  {result.original_time:.3f}s")
                print(f"  Optimized: {result.optimized_time:.3f}s")
                print(f"  Speedup:   {result.speedup:.2f}x")
                print(f"  Semantics: CORRECT")
                speedup = result.speedup

        except Exception as e:
            print(f"  Exception: {e}")
            speedup = 0

        results.append({
            "query": query_id,
            "name": query_data['name'],
            "v7_kimi_speedup": speedup
        })

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY: v7 + Kimi vs DSPy + DeepSeek")
    print('='*70)
    print(f"{'Query':<8} {'DSPy+DeepSeek':<15} {'v7+Kimi':<15} {'Match?':<10}")
    print("-"*48)

    dspy_results = {"q15": 2.98, "q23": 2.33, "q39": 2.44, "q95": 2.25}

    for r in results:
        dspy = dspy_results.get(r['query'], 0)
        v7 = r['v7_kimi_speedup']
        match = "YES" if v7 >= dspy * 0.8 else ("CLOSE" if v7 >= dspy * 0.5 else "NO")
        print(f"{r['query']:<8} {dspy:.2f}x{'':<10} {v7:.2f}x{'':<10} {match:<10}")

    (output_dir / "results.json").write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to {output_dir}")


if __name__ == "__main__":
    main()
