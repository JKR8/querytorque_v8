"""Test specific optimizations on sample database."""

import time
import duckdb

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"


def benchmark(sql: str, runs: int = 3) -> tuple[float, list]:
    """Run query and return (avg_time_seconds, results)."""
    conn = duckdb.connect(SAMPLE_DB, read_only=True)
    times = []
    result = None

    for i in range(runs):
        start = time.time()
        try:
            result = conn.execute(sql).fetchall()
        except Exception as e:
            conn.close()
            return -1, str(e)
        elapsed = time.time() - start
        times.append(elapsed)

    conn.close()

    # Discard first run (warmup), average rest
    avg_time = sum(times[1:]) / len(times[1:]) if len(times) > 1 else times[0]
    return avg_time, result


def compare(name: str, original: str, optimized: str):
    """Compare original vs optimized query."""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print('='*60)

    orig_time, orig_result = benchmark(original)
    if orig_time < 0:
        print(f"Original FAILED: {orig_result}")
        return

    print(f"Original:  {orig_time:.3f}s ({len(orig_result)} rows)")

    opt_time, opt_result = benchmark(optimized)
    if opt_time < 0:
        print(f"Optimized FAILED: {opt_result}")
        return

    print(f"Optimized: {opt_time:.3f}s ({len(opt_result)} rows)")

    speedup = orig_time / opt_time if opt_time > 0 else 0

    # Compare results
    orig_set = set(str(r) for r in orig_result)
    opt_set = set(str(r) for r in opt_result)
    correct = (orig_set == opt_set)

    status = "CORRECT" if correct else "WRONG"
    emoji = "" if speedup >= 2 else ("" if speedup >= 1.2 else "")

    print(f"Speedup: {speedup:.2f}x {emoji} | {status}")

    if not correct:
        print(f"  Original rows: {len(orig_set)}, Optimized rows: {len(opt_set)}")
        if len(orig_set) < 20 and len(opt_set) < 20:
            print(f"  Orig: {sorted(orig_result)[:5]}")
            print(f"  Opt:  {sorted(opt_result)[:5]}")

    return speedup, correct


# Q6: Correlated subquery -> Window function
Q6_ORIGINAL = """
select a.ca_state state, count(*) cnt
 from customer_address a
     ,customer c
     ,store_sales s
     ,date_dim d
     ,item i
 where a.ca_address_sk = c.c_current_addr_sk
 	and c.c_customer_sk = s.ss_customer_sk
 	and s.ss_sold_date_sk = d.d_date_sk
 	and s.ss_item_sk = i.i_item_sk
 	and d.d_month_seq =
 	     (select distinct (d_month_seq)
 	      from date_dim
               where d_year = 2002
 	        and d_moy = 3 )
 	and i.i_current_price > 1.2 *
             (select avg(j.i_current_price)
 	     from item j
 	     where j.i_category = i.i_category)
 group by a.ca_state
 having count(*) >= 10
 order by cnt, a.ca_state
 LIMIT 100;
"""

Q6_OPTIMIZED = """
WITH item_with_avg AS (
    SELECT i_item_sk, i_category, i_current_price,
           AVG(i_current_price) OVER (PARTITION BY i_category) AS category_avg
    FROM item
),
target_month AS (
    SELECT DISTINCT d_month_seq
    FROM date_dim
    WHERE d_year = 2002 AND d_moy = 3
)
SELECT a.ca_state state, count(*) cnt
FROM customer_address a
    ,customer c
    ,store_sales s
    ,date_dim d
    ,item_with_avg i
WHERE a.ca_address_sk = c.c_current_addr_sk
    AND c.c_customer_sk = s.ss_customer_sk
    AND s.ss_sold_date_sk = d.d_date_sk
    AND s.ss_item_sk = i.i_item_sk
    AND d.d_month_seq = (SELECT d_month_seq FROM target_month)
    AND i.i_current_price > 1.2 * i.category_avg
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt, a.ca_state
LIMIT 100;
"""


# Q15: Join elimination on customer table
Q15_ORIGINAL = """
select ca_zip, sum(cs_sales_price)
 from catalog_sales, customer, customer_address, date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk
 	and ( substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2000
 group by ca_zip
 order by ca_zip
 LIMIT 100;
"""

# Can't eliminate customer - need c_current_addr_sk to join to address
# But we can try predicate pushdown
Q15_OPTIMIZED = """
WITH filtered_sales AS (
    SELECT cs_bill_customer_sk, cs_sales_price
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
      AND d_qoy = 2 AND d_year = 2000
)
SELECT ca_zip, sum(cs_sales_price)
FROM filtered_sales fs, customer c, customer_address ca
WHERE fs.cs_bill_customer_sk = c.c_customer_sk
    AND c.c_current_addr_sk = ca.ca_address_sk
    AND (substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
         or ca_state in ('CA','WA','GA')
         or cs_sales_price > 500)
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;
"""


# Q19: Similar structure to Q1 - predicate pushdown possible
Q19_ORIGINAL = """
select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id=16
   and d_moy=12
   and d_year=1998
   and ss_customer_sk = c_customer_sk
   and c_current_addr_sk = ca_address_sk
   and substring(ca_zip,1,5) <> substring(s_zip,1,5)
   and ss_store_sk = s_store_sk
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
 LIMIT 100 ;
"""

# Customer is used for zip code comparison - can't eliminate
# But can push date filter into a CTE
Q19_OPTIMIZED = """
WITH date_filtered_sales AS (
    SELECT ss_item_sk, ss_ext_sales_price, ss_customer_sk, ss_store_sk
    FROM store_sales, date_dim
    WHERE d_date_sk = ss_sold_date_sk
      AND d_moy = 12
      AND d_year = 1998
)
SELECT i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
       sum(ss_ext_sales_price) ext_price
FROM date_filtered_sales ss, item, customer, customer_address, store
WHERE ss.ss_item_sk = i_item_sk
  AND i_manager_id = 16
  AND ss.ss_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND substring(ca_zip,1,5) <> substring(s_zip,1,5)
  AND ss.ss_store_sk = s_store_sk
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
LIMIT 100;
"""


if __name__ == "__main__":
    compare("Q6: Correlated subquery -> Window function", Q6_ORIGINAL, Q6_OPTIMIZED)
    compare("Q15: Date filter predicate pushdown", Q15_ORIGINAL, Q15_OPTIMIZED)
    compare("Q19: Date filter predicate pushdown", Q19_ORIGINAL, Q19_OPTIMIZED)
