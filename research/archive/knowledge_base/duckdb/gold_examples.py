"""
Gold Examples for Few-Shot Learning

These are the top-performing TPC-DS query optimizations from DSPy runs.
Used as demonstrations to guide the LLM in optimization patterns.

Source: research/experiments/dspy_runs/all_20260201_205640/
"""

import dspy
from typing import List


# ============================================================
# Q15 - 2.98x speedup
# Key technique: CTE for early date filtering, OR decomposition
# ============================================================

Q15_ORIGINAL = """-- start query 15 in stream 0 using template query15.tpl
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

-- end query 15 in stream 0 using template query15.tpl"""

Q15_OPTIMIZED = """WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_qoy = 1 AND d_year = 2001
),
filtered_catalog_sales AS (
    SELECT cs_bill_customer_sk, cs_sales_price, ca_zip
    FROM catalog_sales
    INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk
    INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE substr(ca_zip,1,5) IN ('85669', '86197','88274','83405','86475',
                                 '85392', '85460', '80348', '81792')
    UNION ALL
    SELECT cs_bill_customer_sk, cs_sales_price, ca_zip
    FROM catalog_sales
    INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk
    INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE ca_state IN ('CA','WA','GA')
    UNION ALL
    SELECT cs_bill_customer_sk, cs_sales_price, ca_zip
    FROM catalog_sales
    INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk
    INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE cs_sales_price > 500
)
SELECT ca_zip, SUM(cs_sales_price) AS total_sales
FROM filtered_catalog_sales
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;"""

Q15_RATIONALE = """1. Early date filtering: Created CTE to filter date_dim first (reducing to 91 rows), then joined early with catalog_sales.
2. OR condition decomposition: Split OR condition into three UNION ALL branches for independent optimization.
3. Reduced join cardinality: Filter catalog_sales through date_dim first to reduce rows in subsequent joins.
4. UNION ALL for parallel execution: Branches can execute in parallel, leveraging database parallelism."""

Q15_PLAN = """HASH_GROUP_BY[ca_zip] cost=35%
HASH_JOIN[cs_bill_customer_sk=c_customer_sk] cost=25%
HASH_JOIN[c_current_addr_sk=ca_address_sk] cost=15%
SEQ_SCAN[catalog_sales] rows=143M
SEQ_SCAN[customer] rows=2M
SEQ_SCAN[customer_address] rows=1M
SEQ_SCAN[date_dim] rows=365 filtered=91"""

Q15_SCANS = """catalog_sales: 143M rows, filter on cs_sold_date_sk
customer: 2M rows
customer_address: 1M rows, filter on ca_zip/ca_state
date_dim: 365 rows -> 91 (d_qoy=1, d_year=2001)"""


# ============================================================
# Q39 - 2.44x speedup
# Key technique: Push d_moy filter into CTE, simplify CASE
# ============================================================

Q39_ORIGINAL = """-- start query 39 in stream 0 using template query39.tpl
with inv as
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
        ,inv2.d_moy,inv2.mean, inv2.cov
;"""

Q39_OPTIMIZED = """WITH inv AS (
    SELECT
        w_warehouse_name,
        w_warehouse_sk,
        i_item_sk,
        d_moy,
        stdev,
        mean,
        CASE mean WHEN 0 THEN NULL ELSE stdev/mean END AS cov
    FROM (
        SELECT
            w_warehouse_name,
            w_warehouse_sk,
            i_item_sk,
            d_moy,
            STDDEV_SAMP(inv_quantity_on_hand) AS stdev,
            AVG(inv_quantity_on_hand) AS mean
        FROM inventory
        JOIN item ON inv_item_sk = i_item_sk
        JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
        JOIN date_dim ON inv_date_sk = d_date_sk
        WHERE d_year = 1998
          AND d_moy IN (1, 2)  -- Push filter early to reduce grouping
        GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
    ) foo
    WHERE mean != 0 AND stdev/mean > 1  -- Simplify CASE expression
)
SELECT
    inv1.w_warehouse_sk,
    inv1.i_item_sk,
    inv1.d_moy,
    inv1.mean,
    inv1.cov,
    inv2.w_warehouse_sk,
    inv2.i_item_sk,
    inv2.d_moy,
    inv2.mean,
    inv2.cov
FROM inv inv1
JOIN inv inv2
    ON inv1.i_item_sk = inv2.i_item_sk
    AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
WHERE inv1.d_moy = 1
    AND inv2.d_moy = 2
ORDER BY
    inv1.w_warehouse_sk,
    inv1.i_item_sk,
    inv1.d_moy,
    inv1.mean,
    inv1.cov,
    inv2.d_moy,
    inv2.mean,
    inv2.cov;"""

Q39_RATIONALE = """1. Pushed d_moy filter into CTE: Original grouped by all months, but main query only uses months 1 and 2. Added d_moy IN (1, 2) early.
2. Simplified CASE to boolean: Changed CASE mean WHEN 0 to mean != 0 AND stdev/mean > 1 for efficiency.
3. Explicit JOIN syntax: Replaced comma joins for better optimizer understanding.
4. Replaced 1+1 with 2: Avoid runtime computation."""

Q39_PLAN = """HASH_GROUP_BY[warehouse,item,d_moy] cost=60.6%
HASH_JOIN[inv_item_sk=i_item_sk] cost=15%
SEQ_SCAN[inventory] rows=400M
SEQ_SCAN[date_dim] rows=365 filtered=365"""

Q39_SCANS = """inventory: 400M rows
item: 200K rows
warehouse: 15 rows
date_dim: 365 rows -> 365 (d_year=1998)"""


# ============================================================
# Q23 - 2.33x speedup
# Key technique: EXISTS instead of IN, MATERIALIZE hints
# ============================================================

Q23_ORIGINAL = """with frequent_ss_items as
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
  select c_last_name,c_first_name,sales
 from (select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
        from catalog_sales
            ,customer
            ,date_dim
        where d_year = 2000
         and d_moy = 5
         and cs_sold_date_sk = d_date_sk
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and cs_bill_customer_sk = c_customer_sk
       group by c_last_name,c_first_name
      union all
      select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
       from web_sales
           ,customer
           ,date_dim
       where d_year = 2000
         and d_moy = 5
         and ws_sold_date_sk = d_date_sk
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and ws_bill_customer_sk = c_customer_sk
       group by c_last_name,c_first_name)
     order by c_last_name,c_first_name,sales
 LIMIT 100;"""

Q23_OPTIMIZED = """WITH frequent_ss_items AS (
  SELECT /*+ MATERIALIZE */
    SUBSTR(i_item_desc, 1, 30) AS itemdesc,
    i_item_sk AS item_sk,
    d_date AS solddate,
    COUNT(*) AS cnt
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE d_year BETWEEN 2000 AND 2003
  GROUP BY SUBSTR(i_item_desc, 1, 30), i_item_sk, d_date
  HAVING COUNT(*) > 4
),
max_store_sales AS (
  SELECT /*+ MATERIALIZE */
    MAX(csales) AS tpcds_cmax
  FROM (
    SELECT
      c_customer_sk,
      SUM(ss_quantity * ss_sales_price) AS csales
    FROM store_sales
    JOIN customer ON ss_customer_sk = c_customer_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 2000 AND 2003
    GROUP BY c_customer_sk
  )
),
best_ss_customer AS (
  SELECT /*+ MATERIALIZE */
    c_customer_sk,
    SUM(ss_quantity * ss_sales_price) AS ssales
  FROM store_sales
  JOIN customer ON ss_customer_sk = c_customer_sk
  GROUP BY c_customer_sk
  HAVING SUM(ss_quantity * ss_sales_price) > (95/100.0) * (SELECT tpcds_cmax FROM max_store_sales)
)
SELECT c_last_name, c_first_name, sales
FROM (
  SELECT
    c_last_name,
    c_first_name,
    SUM(cs_quantity * cs_list_price) AS sales
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  JOIN customer ON cs_bill_customer_sk = c_customer_sk
  WHERE d_year = 2000
    AND d_moy = 5
    AND EXISTS (SELECT 1 FROM frequent_ss_items f WHERE f.item_sk = cs_item_sk)
    AND EXISTS (SELECT 1 FROM best_ss_customer b WHERE b.c_customer_sk = cs_bill_customer_sk)
  GROUP BY c_last_name, c_first_name

  UNION ALL

  SELECT
    c_last_name,
    c_first_name,
    SUM(ws_quantity * ws_list_price) AS sales
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN customer ON ws_bill_customer_sk = c_customer_sk
  WHERE d_year = 2000
    AND d_moy = 5
    AND EXISTS (SELECT 1 FROM frequent_ss_items f WHERE f.item_sk = ws_item_sk)
    AND EXISTS (SELECT 1 FROM best_ss_customer b WHERE b.c_customer_sk = ws_bill_customer_sk)
  GROUP BY c_last_name, c_first_name
)
ORDER BY c_last_name, c_first_name, sales
LIMIT 100;"""

Q23_RATIONALE = """1. MATERIALIZE hints: Force materialization of CTEs that are referenced multiple times.
2. EXISTS instead of IN: More efficient as it stops searching after first match.
3. Explicit JOIN syntax: Better readability and optimizer understanding.
4. BETWEEN for year ranges: Cleaner than multiple OR conditions.
5. Early filtering: Date filters applied immediately in main query branches."""

Q23_PLAN = """HASH_GROUP_BY[c_customer_sk] cost=40%
HASH_JOIN[ss_customer_sk=c_customer_sk] cost=20%
SEQ_SCAN[store_sales] rows=287M
SEQ_SCAN[catalog_sales] rows=143M
SEQ_SCAN[web_sales] rows=72M"""

Q23_SCANS = """store_sales: 287M rows
catalog_sales: 143M rows
web_sales: 72M rows
customer: 2M rows
date_dim: 365 rows filtered by d_year"""


# ============================================================
# Q45 - 1.8x speedup
# Key technique: OR decomposition, early date filtering
# ============================================================

Q45_ORIGINAL = """-- start query 45 in stream 0 using template query45.tpl
select ca_zip, ca_city, sum(ws_sales_price)
 from web_sales, customer, customer_address, date_dim, item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk
 	and ws_item_sk = i_item_sk
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or
 	      i_item_id in (select i_item_id
                            from item
                            where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                            )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2000
 group by ca_zip, ca_city
 order by ca_zip, ca_city
 LIMIT 100;

-- end query 45 in stream 0 using template query45.tpl"""

Q45_OPTIMIZED = """WITH filtered_items AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
),
date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_qoy = 2 AND d_year = 2000
)
SELECT ca_zip, ca_city, SUM(ws_sales_price)
FROM (
    -- First branch: ca_zip condition
    SELECT ws_sales_price, ca_zip, ca_city
    FROM web_sales ws
    JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
    JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN item i ON ws.ws_item_sk = i.i_item_sk
    WHERE SUBSTR(ca.ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475',
                                      '85392', '85460', '80348', '81792')

    UNION ALL

    -- Second branch: item_id condition
    SELECT ws_sales_price, ca_zip, ca_city
    FROM web_sales ws
    JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
    JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN filtered_items fi ON ws.ws_item_sk = fi.i_item_sk
) combined
GROUP BY ca_zip, ca_city
ORDER BY ca_zip, ca_city
LIMIT 100;"""

Q45_RATIONALE = """1. Early date filtering: Created date_filter CTE for highly selective date conditions.
2. OR to UNION ALL: Split OR condition into two separate query branches.
3. Materialized item filter: Created filtered_items CTE for the small item subquery.
4. Eliminated redundant scans: Original scanned item table twice."""

Q45_PLAN = """HASH_GROUP_BY[ca_zip,ca_city] cost=30%
HASH_JOIN[ws_bill_customer_sk=c_customer_sk] cost=25%
SEQ_SCAN[web_sales] rows=72M
SEQ_SCAN[customer] rows=2M
SEQ_SCAN[item] rows=200K"""

Q45_SCANS = """web_sales: 72M rows
customer: 2M rows
customer_address: 1M rows
item: 200K rows, filtered by i_item_sk
date_dim: 365 rows -> 91 (d_qoy=2, d_year=2000)"""


# ============================================================
# Q95 - 1.7x speedup
# Key technique: EXISTS instead of IN, proper joins
# ============================================================

Q95_ORIGINAL = """-- start query 95 in stream 0 using template query95.tpl
with ws_wh as
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
 LIMIT 100;

-- end query 95 in stream 0 using template query95.tpl"""

Q95_OPTIMIZED = """WITH ws_wh AS (
    SELECT DISTINCT ws1.ws_order_number
    FROM web_sales ws1
    JOIN web_sales ws2 ON ws1.ws_order_number = ws2.ws_order_number
    WHERE ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT
    COUNT(DISTINCT ws1.ws_order_number) AS "order count",
    SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
    SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN DATE '1999-02-01' AND (DATE '1999-02-01' + INTERVAL '60' DAY)
    AND ca_state = 'NC'
    AND web_company_name = 'pri'
    AND EXISTS (
        SELECT 1
        FROM ws_wh
        WHERE ws_wh.ws_order_number = ws1.ws_order_number
    )
    AND EXISTS (
        SELECT 1
        FROM web_returns wr
        JOIN ws_wh ON wr.wr_order_number = ws_wh.ws_order_number
        WHERE wr.wr_order_number = ws1.ws_order_number
    )
LIMIT 100;"""

Q95_RATIONALE = """1. CTE with DISTINCT: Compute once and reuse, reducing repeated scans of web_sales.
2. EXISTS instead of IN: Stops searching after first match.
3. Combined web_returns join: Proper join instead of cartesian product.
4. Removed unnecessary ORDER BY: Aggregate query produces single row.
5. Explicit JOIN syntax: Better optimizer understanding."""

Q95_PLAN = """HASH_GROUP_BY cost=35%
HASH_JOIN[ws_order_number=ws_order_number] cost=25%
SEQ_SCAN[web_sales] rows=72M
SEQ_SCAN[web_returns] rows=7M"""

Q95_SCANS = """web_sales: 72M rows (scanned multiple times in original)
web_returns: 7M rows
customer_address: 1M rows, filtered by ca_state
date_dim: 365 rows -> 60 (date range)"""


# ============================================================
# Build DSPy Examples
# ============================================================

def get_gold_examples(num_examples: int = 5) -> List[dspy.Example]:
    """Get gold examples for few-shot learning.

    Args:
        num_examples: Number of examples to return (max 5)

    Returns:
        List of dspy.Example objects with inputs marked
    """
    all_examples = [
        # Q15 - 2.98x (best)
        dspy.Example(
            original_query=Q15_ORIGINAL,
            execution_plan=Q15_PLAN,
            row_estimates=Q15_SCANS,
            optimized_query=Q15_OPTIMIZED,
            optimization_rationale=Q15_RATIONALE
        ).with_inputs("original_query", "execution_plan", "row_estimates"),

        # Q39 - 2.44x
        dspy.Example(
            original_query=Q39_ORIGINAL,
            execution_plan=Q39_PLAN,
            row_estimates=Q39_SCANS,
            optimized_query=Q39_OPTIMIZED,
            optimization_rationale=Q39_RATIONALE
        ).with_inputs("original_query", "execution_plan", "row_estimates"),

        # Q23 - 2.33x
        dspy.Example(
            original_query=Q23_ORIGINAL,
            execution_plan=Q23_PLAN,
            row_estimates=Q23_SCANS,
            optimized_query=Q23_OPTIMIZED,
            optimization_rationale=Q23_RATIONALE
        ).with_inputs("original_query", "execution_plan", "row_estimates"),

        # Q45 - 1.8x
        dspy.Example(
            original_query=Q45_ORIGINAL,
            execution_plan=Q45_PLAN,
            row_estimates=Q45_SCANS,
            optimized_query=Q45_OPTIMIZED,
            optimization_rationale=Q45_RATIONALE
        ).with_inputs("original_query", "execution_plan", "row_estimates"),

        # Q95 - 1.7x
        dspy.Example(
            original_query=Q95_ORIGINAL,
            execution_plan=Q95_PLAN,
            row_estimates=Q95_SCANS,
            optimized_query=Q95_OPTIMIZED,
            optimization_rationale=Q95_RATIONALE
        ).with_inputs("original_query", "execution_plan", "row_estimates"),
    ]

    return all_examples[:num_examples]


def get_example_by_query(query_name: str) -> dspy.Example:
    """Get a specific gold example by query name.

    Args:
        query_name: Query name like 'q15', 'q39', etc.

    Returns:
        dspy.Example or None if not found
    """
    examples_map = {
        'q15': (Q15_ORIGINAL, Q15_PLAN, Q15_SCANS, Q15_OPTIMIZED, Q15_RATIONALE),
        'q39': (Q39_ORIGINAL, Q39_PLAN, Q39_SCANS, Q39_OPTIMIZED, Q39_RATIONALE),
        'q23': (Q23_ORIGINAL, Q23_PLAN, Q23_SCANS, Q23_OPTIMIZED, Q23_RATIONALE),
        'q45': (Q45_ORIGINAL, Q45_PLAN, Q45_SCANS, Q45_OPTIMIZED, Q45_RATIONALE),
        'q95': (Q95_ORIGINAL, Q95_PLAN, Q95_SCANS, Q95_OPTIMIZED, Q95_RATIONALE),
    }

    if query_name.lower() not in examples_map:
        return None

    orig, plan, scans, opt, rationale = examples_map[query_name.lower()]
    return dspy.Example(
        original_query=orig,
        execution_plan=plan,
        row_estimates=scans,
        optimized_query=opt,
        optimization_rationale=rationale
    ).with_inputs("original_query", "execution_plan", "row_estimates")
