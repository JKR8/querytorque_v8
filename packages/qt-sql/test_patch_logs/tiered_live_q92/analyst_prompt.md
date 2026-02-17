## Role

You are a SQL optimization analyst. Your task is to analyze this query and design **up to 4 structural optimization targets**, each targeting a different optimization family.

For each target, describe the STRUCTURAL SHAPE of the optimized query using an IR node map (CTE names, FROM tables, WHERE conditions, GROUP BY, ORDER BY). A separate code-generation worker will convert your targets into executable patch plans.

You will **choose up to 4 of the 6 families** based on relevance to THIS SPECIFIC QUERY.

## Query: query_92

**Dialect**: SNOWFLAKE

```sql
select 
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    web_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 320
and i_item_sk = ws_item_sk 
and d_date between '2002-02-26' and 
        (cast('2002-02-26' as date) + INTERVAL '90 DAY')
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * avg(ws_ext_discount_amt) 
         FROM 
            web_sales 
           ,date_dim
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between '2002-02-26' and
                             (cast('2002-02-26' as date) + INTERVAL '90 DAY')
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
 LIMIT 100
```


## Current Execution Plan

```
GlobalStats: partitionsTotal=7214 bytesAssigned=4.56GB
3:Filter  (WS_EXT_DISCOUNT_AMT > subquery_scalar(probe(WS_ITEM_SK)))
4:InnerJoin (D_DATE_SK = WS_SOLD_DATE_SK)
6:TableScan DATE_DIM  1 partition
7:InnerJoin (I_ITEM_SK = WS_ITEM_SK)
9:TableScan ITEM  1 partition, filter I_MANUFACT_ID=320
10:TableScan WEB_SALES  182 partitions
-- Correlated subquery: AVG(WS_EXT_DISCOUNT_AMT) WHERE WS_ITEM_SK = outer.I_ITEM_SK
```


## IR Structure (for patch targeting)

```
S0 [SELECT]
  MAIN QUERY (via Q_S0)
    FROM: web_sales, item, date_dim
    WHERE [d19a1964890bdea2]: i_manufact_id = 320 AND i_item_sk = ws_item_sk AND d_date BETWEEN '2002-02-26' AND (CAST('2002-02...
    ORDER BY: SUM(ws_ext_discount_amt)

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**Note**: Use `by_node_id` (e.g., "S0") and `by_anchor_hash` (16-char hex) from map above to target patch operations.


## Optimization Families

Review the 6 families below. Each shows a pattern with a gold example patch plan.

Choose up to **4 most relevant families** for this query based on:
- Query structure (CTEs, subqueries, joins, aggregations, set operations)
- Execution plan signals (WHERE placement, repeated scans, correlated subqueries, materializations)
- Problem signature (cardinality estimation errors, loops vs sets, filter ordering)



### Family A: Early Filtering (Predicate Pushback)
**Description**: Push small filters into CTEs early, reduce row count before expensive operations
**Speedup Range**: 1.3–4.0x (~35% of all wins)
**Use When**:
  1. Late WHERE filters on dimension tables
  2. Cascading CTEs with filters applied downstream
  3. Expensive joins after filters could be pushed earlier

**Gold Example**: `sf_inline_decorrelate` (23.17x)

**BEFORE (slow):**
```sql
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (1, 78, 97, 516, 521)
or i_manager_id BETWEEN 25 and 54)
and i_item_sk = cs_item_sk
and d_date between '1999-03-07' and
...
```

**AFTER (fast):**
```sql
WITH filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id IN (1, 78, 97, 516, 521)
       OR i_manager_id BETWEEN 25 AND 54
),
date_filtered_sales AS (
    SELECT cs.cs_item_sk, cs.cs_ext_discount_amt,
           cs.cs_list_price, cs.cs_sales_price
    FROM catalog_sales cs
...
```

**IR BEFORE:**
```
S0 [SELECT]
  MAIN QUERY (via Q_S0)
    FROM: catalog_sales, item, date_dim
    WHERE [09dc78125155e528]: (i_manufact_id IN (1, 78, 97, 516, 521) OR i_manager_id BETWEEN 25 AND 54) AND i_item_sk = cs_ite...
    ORDER BY: SUM(cs_ext_discount_amt)

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**IR TARGET:**
```
S0 [SELECT]
  CTE: filtered_items  (via CTE_Q_S0_filtered_items)
    FROM: item
    WHERE [5779e47e1e6d90f4]: i_manufact_id IN (1, 78, 97, 516, 521) OR i_manager_id BETWEEN 25 AND 54
  CTE: date_filtered_sales  (via CTE_Q_S0_date_filtered_sales)
    FROM: catalog_sales cs, date_dim d
    WHERE [a0208e1ff961ae1e]: d.d_date BETWEEN '1999-03-07' AND CAST('1999-03-07' AS DATE) + INTERVAL '90 DAY'
  CTE: item_avg_discount  (via CTE_Q_S0_item_avg_discount)
    FROM: date_filtered_sales dfs, filtered_items fi
    WHERE [6e96ef577bfa5c2a]: dfs.cs_list_price BETWEEN 16 AND 45 AND dfs.cs_sales_price / dfs.cs_list_price BETWEEN 63 * 0.01 ...
    GROUP BY: dfs.cs_item_sk
  MAIN QUERY (via Q_S0)
    FROM: date_filtered_sales dfs, item_avg_discount iad
    WHERE [61cfeb7bed62437b]: dfs.cs_ext_discount_amt > iad.threshold
    ORDER BY: 1

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**PATCH PLAN:**
```json
{
  "plan_id": "gold_sf_inline_decorrelate",
  "dialect": "snowflake",
  "description": "Decompose correlated scalar subquery into 3 CTEs (dimension filter, date-filtered fact, per-key threshold) and JOIN. Converts O(N*M) correlated scans to single hash join.",
  "preconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "postconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "steps": [
    {
      "step_id": "s1",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "filtered_items",
        "cte_query_sql": "SELECT i_item_sk FROM item WHERE i_manufact_id IN (1, 78, 97, 516, 521) OR i_manager_id BETWEEN 25 AND 54"
      },
      "description": "Extract item dimension filter into CTE"
    },
    {
      "step_id": "s2",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "date_filtered_sales",
        "cte_query_sql": "SELECT cs.cs_item_sk, cs.cs_ext_discount_amt, cs.cs_list_price, cs.cs_sales_price FROM catalog_sales cs JOIN date_dim d ON d.d_date_sk = cs.cs_sold_date_sk WHERE d.d_date BETWEEN '1999-03-07' AND CAST('1999-03-07' AS DATE) + INTERVAL '90 DAY'"
      },
      "description": "Extract date-filtered fact scan into CTE"
    },
    {
      "step_id": "s3",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "item_avg_discount",
        "cte_query_sql": "SELECT dfs.cs_item_sk, 1.3 * AVG(dfs.cs_ext_discount_amt) AS threshold FROM date_filtered_sales dfs JOIN filtered_items fi ON fi.i_item_sk = dfs.cs_item_sk WHERE dfs.cs_list_price BETWEEN 16 AND 45 AND dfs.cs_sales_price / dfs.cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01 GROUP BY dfs.cs_item_sk"
      },
      "description": "Decorrelate scalar subquery into per-key GROUP BY threshold CTE"
    },
    {
      "step_id": "s4",
      "op": "replace_from",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "from_sql": "date_filtered_sales dfs JOIN item_avg_discount iad ON iad.cs_item_sk = dfs.cs_item_sk"
      },
      "description": "Replace comma-join FROM with CTE-based explicit JOINs"
    },
    {
      "step_id": "s5",
      "op": "replace_where_predicate",
      "target": {
        "by_node_id": "S0",
        "by_anchor_hash": "09dc78125155e528"
      },
      "payload": {
        "expr_sql": "dfs.cs_ext_discount_amt > iad.threshold"
      },
      "description": "Replace complex WHERE (with correlated subquery) with simple threshold comparison"
    }
  ]
}
```



### Family B: Decorrelation (Sets Over Loops)
**Description**: Convert correlated subqueries to standalone CTEs with GROUP BY, eliminate per-row re-execution
**Speedup Range**: 2.4–2.9x (~15% of all wins)
**Use When**:
  1. Correlated subqueries in WHERE clause
  2. Scalar aggregates computed per outer row
  3. DELIM_SCAN in execution plan (indicates correlation)

**Gold Example**: `sf_shared_scan_decorrelate` (7.82x)

**BEFORE (slow):**
```sql
select 
   sum(ws_ext_discount_amt)  as "Excess Discount Amount"
from
    web_sales
   ,item
   ,date_dim
where
(i_manufact_id BETWEEN 341 and 540
or i_category IN ('Home', 'Men', 'Music'))
and i_item_sk = ws_item_sk
...
```

**AFTER (fast):**
```sql
WITH common_scan AS (
  SELECT ws_item_sk, ws_ext_discount_amt, ws_sales_price, ws_list_price
  FROM web_sales
  INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk
  WHERE d_date BETWEEN '1998-03-13' AND CAST('1998-03-13' AS DATE) + INTERVAL '90 DAY'
    AND ws_wholesale_cost BETWEEN 26 AND 46
),
threshold_computation AS (
  SELECT ws_item_sk, 1.3 * AVG(ws_ext_discount_amt) AS threshold
  FROM common_scan
...
```

**IR BEFORE:**
```
S0 [SELECT]
  MAIN QUERY (via Q_S0)
    FROM: web_sales, item, date_dim
    WHERE [0ef6ffe2461512ae]: (i_manufact_id BETWEEN 341 AND 540 OR i_category IN ('Home', 'Men', 'Music')) AND i_item_sk = ws_...
    ORDER BY: SUM(ws_ext_discount_amt)

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**IR TARGET:**
```
S0 [SELECT]
  CTE: common_scan  (via CTE_Q_S0_common_scan)
    FROM: web_sales, date_dim
    WHERE [528282f372d6460c]: d_date BETWEEN '1998-03-13' AND CAST('1998-03-13' AS DATE) + INTERVAL '90 DAY' AND ws_wholesale_c...
  CTE: threshold_computation  (via CTE_Q_S0_threshold_computation)
    FROM: common_scan
    WHERE [72f98b50cc17ebb7]: ws_sales_price / ws_list_price BETWEEN 34 * 0.01 AND 49 * 0.01
    GROUP BY: ws_item_sk
  CTE: outer_rows  (via CTE_Q_S0_outer_rows)
    FROM: common_scan cs, item
    WHERE [c12aae6a913f2cad]: i_manufact_id BETWEEN 341 AND 540 OR i_category IN ('Home', 'Men', 'Music')
  CTE: join_filter  (via CTE_Q_S0_join_filter)
    FROM: outer_rows o, threshold_computation t
    WHERE [d0a3b37ae99bcef6]: o.ws_ext_discount_amt > t.threshold
  MAIN QUERY (via Q_S0)
    FROM: join_filter
    ORDER BY: SUM(ws_ext_discount_amt)

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**PATCH PLAN:**
```json
{
  "plan_id": "gold_sf_shared_scan_decorrelate",
  "dialect": "snowflake",
  "description": "Extract shared fact table scan into CTE, derive threshold and filtered rows from it, then JOIN. Converts O(N*M) correlated execution to O(N+M) hash join.",
  "preconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "postconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "steps": [
    {
      "step_id": "s1",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "common_scan",
        "cte_query_sql": "SELECT ws_item_sk, ws_ext_discount_amt, ws_sales_price, ws_list_price FROM web_sales INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk WHERE d_date BETWEEN '1998-03-13' AND CAST('1998-03-13' AS DATE) + INTERVAL '90 DAY' AND ws_wholesale_cost BETWEEN 26 AND 46"
      },
      "description": "Extract shared fact+date scan with common filters into CTE"
    },
    {
      "step_id": "s2",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "threshold_computation",
        "cte_query_sql": "SELECT ws_item_sk, 1.3 * AVG(ws_ext_discount_amt) AS threshold FROM common_scan WHERE ws_sales_price / ws_list_price BETWEEN 34 * 0.01 AND 49 * 0.01 GROUP BY ws_item_sk"
      },
      "description": "Compute per-item discount threshold from shared scan"
    },
    {
      "step_id": "s3",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "outer_rows",
        "cte_query_sql": "SELECT cs.ws_item_sk, cs.ws_ext_discount_amt FROM common_scan cs INNER JOIN item ON i_item_sk = cs.ws_item_sk WHERE i_manufact_id BETWEEN 341 AND 540 OR i_category IN ('Home', 'Men', 'Music')"
      },
      "description": "Filter to item-matching rows from shared scan"
    },
    {
      "step_id": "s4",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "join_filter",
        "cte_query_sql": "SELECT o.ws_ext_discount_amt FROM outer_rows o INNER JOIN threshold_computation t ON o.ws_item_sk = t.ws_item_sk WHERE o.ws_ext_discount_amt > t.threshold"
      },
      "description": "Join outer rows with threshold, apply discount filter"
    },
    {
      "step_id": "s5",
      "op": "replace_from",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "from_sql": "join_filter"
      },
      "description": "Replace original 3-table comma-join with single CTE reference"
    },
    {
      "step_id": "s6",
      "op": "delete_expr_subtree",
      "target": {
        "by_node_id": "S0",
        "by_anchor_hash": "0ef6ffe2461512ae"
      },
      "description": "Remove original WHERE clause (all conditions now in CTEs)"
    }
  ]
}
```



### Family C: Aggregation Pushdown (Minimize Rows Touched)
**Description**: Aggregate before expensive joins when GROUP BY keys ⊇ join keys, reduce intermediate sizes
**Speedup Range**: 1.3–15.3x (~5% of all wins (high variance))
**Use When**:
  1. GROUP BY happens after large joins
  2. GROUP BY keys are subset of join keys
  3. Intermediate result size >> final result size

**Gold Example**: `aggregate_pushdown` (42.90x)

**BEFORE (slow):**
```sql
select i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(inv_quantity_on_hand) qoh
       from inventory
           ,date_dim
           ,item
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
...
```

**AFTER (fast):**
```sql
WITH date_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1188 AND 1188 + 11), inventory_date AS (SELECT inv_item_sk, inv_quantity_on_hand FROM inventory JOIN date_filtered ON inv_date_sk = d_date_sk), inventory_agg AS (SELECT inv_item_sk, SUM(inv_quantity_on_hand) AS sum_qty, COUNT(inv_quantity_on_hand) AS cnt FROM inventory_date GROUP BY inv_item_sk), join_item AS (SELECT i_product_name, i_brand, i_class, i_category, sum_qty, cnt FROM inventory_agg JOIN item ON inv_item_sk = i_item_sk), rollup_aggregate AS (SELECT i_product_name, i_brand, i_class, i_category, CASE WHEN SUM(cnt) > 0 THEN SUM(sum_qty) / SUM(cnt) END AS qoh FROM join_item GROUP BY ROLLUP(i_product_name, i_brand, i_class, i_category)) SELECT i_product_name, i_brand, i_class, i_category, qoh FROM rollup_aggregate ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC LIMIT 100
```

**IR BEFORE:**
```
S0 [SELECT]
  MAIN QUERY (via Q_S0)
    FROM: inventory, date_dim, item
    WHERE [cb0b927b3e0ad199]: inv_date_sk = d_date_sk AND inv_item_sk = i_item_sk AND d_month_seq BETWEEN 1188 AND 1188 + 11
    ORDER BY: qoh, i_product_name, i_brand, i_class, i_category

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**IR TARGET:**
```
S0 [SELECT]
  CTE: date_filtered  (via CTE_Q_S0_date_filtered)
    FROM: date_dim
    WHERE [64df5f706f9f2db0]: d_month_seq BETWEEN 1188 AND 1188 + 11
  CTE: inventory_date  (via CTE_Q_S0_inventory_date)
    FROM: inventory, date_filtered
  CTE: inventory_agg  (via CTE_Q_S0_inventory_agg)
    FROM: inventory_date
    GROUP BY: inv_item_sk
  CTE: join_item  (via CTE_Q_S0_join_item)
    FROM: inventory_agg, item
  CTE: rollup_aggregate  (via CTE_Q_S0_rollup_aggregate)
    FROM: join_item
  MAIN QUERY (via Q_S0)
    FROM: rollup_aggregate
    ORDER BY: qoh, i_product_name, i_brand, i_class, i_category

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**PATCH PLAN:**
```json
{
  "plan_id": "gold_duckdb_aggregate_pushdown",
  "dialect": "duckdb",
  "description": "Pre-aggregate fact table by join key before dimension joins to reduce rows entering the join from millions to thousands",
  "preconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "postconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "steps": [
    {
      "step_id": "s1",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "date_filtered",
        "cte_query_sql": "SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1188 AND 1188 + 11"
      },
      "description": "Insert CTE 'date_filtered' for date dimension filtering"
    },
    {
      "step_id": "s2",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "inventory_date",
        "cte_query_sql": "SELECT inv_item_sk, inv_quantity_on_hand FROM inventory JOIN date_filtered ON inv_date_sk = d_date_sk"
      },
      "description": "Insert CTE 'inventory_date' for date dimension filtering"
    },
    {
      "step_id": "s3",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "inventory_agg",
        "cte_query_sql": "SELECT inv_item_sk, SUM(inv_quantity_on_hand) AS sum_qty, COUNT(inv_quantity_on_hand) AS cnt FROM inventory_date GROUP BY inv_item_sk"
      },
      "description": "Insert CTE 'inventory_agg' for pre-aggregated computation"
    },
    {
      "step_id": "s4",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "join_item",
        "cte_query_sql": "SELECT i_product_name, i_brand, i_class, i_category, sum_qty, cnt FROM inventory_agg JOIN item ON inv_item_sk = i_item_sk"
      },
      "description": "Insert CTE 'join_item' for pre-filtered join"
    },
    {
      "step_id": "s5",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "rollup_aggregate",
        "cte_query_sql": "SELECT i_product_name, i_brand, i_class, i_category, CASE WHEN SUM(cnt) > 0 THEN SUM(sum_qty) / SUM(cnt) END AS qoh FROM join_item GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)"
      },
      "description": "Insert CTE 'rollup_aggregate' for pre-aggregated computation"
    },
    {
      "step_id": "s6",
      "op": "replace_body",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "sql_fragment": "SELECT i_product_name, i_brand, i_class, i_category, qoh FROM rollup_aggregate ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC LIMIT 100"
      },
      "description": "Replace main query body with optimized version"
    }
  ]
}
```



### Family D: Set Operation Optimization (Sets Over Loops)
**Description**: Replace INTERSECT/UNION-based patterns with EXISTS/NOT EXISTS, avoid full materialization
**Speedup Range**: 1.7–2.7x (~8% of all wins)
**Use When**:
  1. INTERSECT patterns between large sets
  2. UNION ALL with duplicate elimination
  3. Set operations materializing full intermediate results

**Gold Example**: `intersect_to_exists` (1.83x)

**BEFORE (slow):**
```sql
with  cross_items as
 (select i_item_sk ss_item_sk
 from item,
 (select iss.i_brand_id brand_id
     ,iss.i_class_id class_id
     ,iss.i_category_id category_id
 from store_sales
     ,item iss
     ,date_dim d1
 where ss_item_sk = iss.i_item_sk
...
```

**AFTER (fast):**
```sql
WITH cross_items AS (SELECT i.i_item_sk AS ss_item_sk FROM item AS i WHERE EXISTS(SELECT 1 FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2000 + 2 AND iss.i_brand_id = i.i_brand_id AND iss.i_class_id = i.i_class_id AND iss.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2000 + 2 AND ics.i_brand_id = i.i_brand_id AND ics.i_class_id = i.i_class_id AND ics.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM web_sales, item AS iws, date_dim AS d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk AND d3.d_year BETWEEN 2000 AND 2000 + 2 AND iws.i_brand_id = i.i_brand_id AND iws.i_class_id = i.i_class_id AND iws.i_category_id = i.i_category_id)), avg_sales AS (SELECT AVG(quantity * list_price) AS average_sales FROM (SELECT ss_quantity AS quantity, ss_list_price AS list_price FROM store_sales, date_dim WHERE ss_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2 UNION ALL SELECT cs_quantity AS quantity, cs_list_price AS list_price FROM catalog_sales, date_dim WHERE cs_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2 UNION ALL SELECT ws_quantity AS quantity, ws_list_price AS list_price FROM web_sales, date_dim WHERE ws_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2) AS x)
SELECT channel, i_brand_id, i_class_id, i_category_id, SUM(sales), SUM(number_sales) FROM (SELECT 'store' AS channel, i_brand_id, i_class_id, i_category_id, SUM(ss_quantity * ss_list_price) AS sales, COUNT(*) AS number_sales FROM store_sales, item, date_dim WHERE ss_item_sk IN (SELECT ss_item_sk FROM cross_items) AND ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND d_year = 2000 + 2 AND d_moy = 11 GROUP BY i_brand_id, i_class_id, i_category_id HAVING SUM(ss_quantity * ss_list_price) > (SELECT average_sales FROM avg_sales) UNION ALL SELECT 'catalog' AS channel, i_brand_id, i_class_id, i_category_id, SUM(cs_quantity * cs_list_price) AS sales, COUNT(*) AS number_sales FROM catalog_sales, item, date_dim WHERE cs_item_sk IN (SELECT ss_item_sk FROM cross_items) AND cs_item_sk = i_item_sk AND cs_sold_date_sk = d_date_sk AND d_year = 2000 + 2 AND d_moy = 11 GROUP BY i_brand_id, i_class_id, i_category_id HAVING SUM(cs_quantity * cs_list_price) > (SELECT average_sales FROM avg_sales) UNION ALL SELECT 'web' AS channel, i_brand_id, i_class_id, i_category_id, SUM(ws_quantity * ws_list_price) AS sales, COUNT(*) AS number_sales FROM web_sales, item, date_dim WHERE ws_item_sk IN (SELECT ss_item_sk FROM cross_items) AND ws_item_sk = i_item_sk AND ws_sold_date_sk = d_date_sk AND d_year = 2000 + 2 AND d_moy = 11 GROUP BY i_brand_id, i_class_id, i_category_id HAVING SUM(ws_quantity * ws_list_price) > (SELECT average_sales FROM avg_sales)) AS y GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id) ORDER BY channel, i_brand_id, i_class_id, i_category_id LIMIT 100;
```

**IR BEFORE:**
```
S0 [SELECT]
  CTE: cross_items  (via CTE_Q_S0_cross_items)
    FROM: item, (subquery) 
    WHERE [3f561bc366ff68bb]: i_brand_id = brand_id AND i_class_id = class_id AND i_category_id = category_id
  CTE: avg_sales  (via CTE_Q_S0_avg_sales)
    FROM: (subquery) x
  MAIN QUERY (via Q_S0)
    FROM: (subquery) y
    ORDER BY: channel, i_brand_id, i_class_id, i_category_id

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**IR TARGET:**
```
S0 [SELECT]
  CTE: cross_items  (via CTE_Q_S0_cross_items)
    FROM: item i
    WHERE [8094f2c0095034d2]: EXISTS(SELECT 1 FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AN...
  CTE: avg_sales  (via CTE_Q_S0_avg_sales)
    FROM: (subquery) x
  MAIN QUERY (via Q_S0)
    FROM: (subquery) y
    ORDER BY: channel, i_brand_id, i_class_id, i_category_id

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**PATCH PLAN:**
```json
{
  "plan_id": "gold_duckdb_intersect_to_exists",
  "dialect": "duckdb",
  "description": "Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join planning",
  "preconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "postconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "steps": [
    {
      "step_id": "s1",
      "op": "replace_block_with_cte_pair",
      "target": {
        "by_node_id": "S0",
        "by_label": "cross_items"
      },
      "payload": {
        "sql_fragment": "cross_items AS (SELECT i.i_item_sk AS ss_item_sk FROM item AS i WHERE EXISTS(SELECT 1 FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2000 + 2 AND iss.i_brand_id = i.i_brand_id AND iss.i_class_id = i.i_class_id AND iss.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2000 + 2 AND ics.i_brand_id = i.i_brand_id AND ics.i_class_id = i.i_class_id AND ics.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM web_sales, item AS iws, date_dim AS d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk AND d3.d_year BETWEEN 2000 AND 2000 + 2 AND iws.i_brand_id = i.i_brand_id AND iws.i_class_id = i.i_class_id AND iws.i_category_id = i.i_category_id))"
      },
      "description": "Replace CTE 'cross_items' body with optimized version"
    }
  ]
}
```



### Family E: Materialization / Prefetch (Don't Repeat Work)
**Description**: Extract repeated scans or pre-compute intermediate results for reuse across multiple consumers
**Speedup Range**: 1.3–6.2x (~18% of all wins)
**Use When**:
  1. Repeated scans of same table with different filters
  2. Dimension filters applied independently multiple times
  3. CTE referenced multiple times with implicit re-evaluation

**Gold Example**: `multi_dimension_prefetch` (2.71x)

**BEFORE (slow):**
```sql
select s_store_name, s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from date_dim, store_sales, store
 where d_date_sk = ss_sold_date_sk and
...
```

**AFTER (fast):**
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_day_name FROM date_dim WHERE d_year = 2000), filtered_stores AS (SELECT s_store_sk, s_store_id, s_store_name FROM store WHERE s_gmt_offset = -5), filtered_sales AS (SELECT ss_sales_price, d_day_name, s_store_id, s_store_name FROM store_sales JOIN filtered_dates ON d_date_sk = ss_sold_date_sk JOIN filtered_stores ON s_store_sk = ss_store_sk)
SELECT s_store_name, s_store_id, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) AS sat_sales FROM filtered_sales GROUP BY s_store_name, s_store_id ORDER BY s_store_name, s_store_id, sun_sales, mon_sales, tue_sales, wed_sales, thu_sales, fri_sales, sat_sales LIMIT 100;
```

**IR BEFORE:**
```
S0 [SELECT]
  MAIN QUERY (via Q_S0)
    FROM: date_dim, store_sales, store
    WHERE [834e9c75d01a8fa3]: d_date_sk = ss_sold_date_sk AND s_store_sk = ss_store_sk AND s_gmt_offset = -5 AND d_year = 2000
    GROUP BY: s_store_name, s_store_id
    ORDER BY: s_store_name, s_store_id, sun_sales, mon_sales, tue_sales, wed_sales, thu_sales, fri_sales, sat_sales

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**IR TARGET:**
```
S0 [SELECT]
  CTE: filtered_dates  (via CTE_Q_S0_filtered_dates)
    FROM: date_dim
    WHERE [48b31a2bf2993b3d]: d_year = 2000
  CTE: filtered_stores  (via CTE_Q_S0_filtered_stores)
    FROM: store
    WHERE [812f7299c6fba51b]: s_gmt_offset = -5
  CTE: filtered_sales  (via CTE_Q_S0_filtered_sales)
    FROM: store_sales, filtered_dates, filtered_stores
  MAIN QUERY (via Q_S0)
    FROM: filtered_sales
    GROUP BY: s_store_name, s_store_id
    ORDER BY: s_store_name, s_store_id, sun_sales, mon_sales, tue_sales, wed_sales, thu_sales, fri_sales, sat_sales

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**PATCH PLAN:**
```json
{
  "plan_id": "gold_duckdb_multi_dimension_prefetch",
  "dialect": "duckdb",
  "description": "Pre-filter multiple dimension tables (date + store) into separate CTEs before joining with fact table",
  "preconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "postconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "steps": [
    {
      "step_id": "s1",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "filtered_dates",
        "cte_query_sql": "SELECT d_date_sk, d_day_name FROM date_dim WHERE d_year = 2000"
      },
      "description": "Insert CTE 'filtered_dates' for date dimension filtering"
    },
    {
      "step_id": "s2",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "filtered_stores",
        "cte_query_sql": "SELECT s_store_sk, s_store_id, s_store_name FROM store WHERE s_gmt_offset = -5"
      },
      "description": "Insert CTE 'filtered_stores'"
    },
    {
      "step_id": "s3",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "filtered_sales",
        "cte_query_sql": "SELECT ss_sales_price, d_day_name, s_store_id, s_store_name FROM store_sales JOIN filtered_dates ON d_date_sk = ss_sold_date_sk JOIN filtered_stores ON s_store_sk = ss_store_sk"
      },
      "description": "Insert CTE 'filtered_sales' for date dimension filtering"
    },
    {
      "step_id": "s4",
      "op": "replace_from",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "from_sql": "filtered_sales"
      },
      "description": "Replace FROM clause with optimized version"
    },
    {
      "step_id": "s5",
      "op": "delete_expr_subtree",
      "target": {
        "by_node_id": "S0",
        "by_anchor_hash": "834e9c75d01a8fa3"
      },
      "description": "Remove WHERE clause (conditions moved to CTEs)"
    }
  ]
}
```



### Family F: Join Transform (Right Shape First)
**Description**: Restructure join topology: convert comma joins to explicit INNER JOIN, optimize join order, eliminate self-joins via single-pass aggregation
**Speedup Range**: 1.8–8.6x (~19% of all wins)
**Use When**:
  1. Comma-separated joins (implicit cross joins) in FROM clause
  2. Self-joins scanning same table multiple times
  3. Dimension-fact join order suboptimal for predicate pushdown

**Gold Example**: `inner_join_conversion` (3.44x)

**BEFORE (slow):**
```sql
select ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from store_sales left outer join store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,reason
...
```

**AFTER (fast):**
```sql
WITH filtered_reason AS (SELECT r_reason_sk, r_reason_desc FROM reason WHERE r_reason_desc = 'duplicate purchase'), joined_returns_sales AS (SELECT ss.ss_customer_sk, ss.ss_quantity, ss.ss_sales_price, sr.sr_return_quantity FROM store_sales ss INNER JOIN store_returns sr ON (ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number) INNER JOIN filtered_reason fr ON sr.sr_reason_sk = fr.r_reason_sk), aggregated AS (SELECT ss_customer_sk, SUM((ss_quantity - sr_return_quantity) * ss_sales_price) AS sumsales FROM joined_returns_sales GROUP BY ss_customer_sk), top_n AS (SELECT ss_customer_sk, sumsales FROM aggregated ORDER BY sumsales ASC, ss_customer_sk ASC LIMIT 100) SELECT ss_customer_sk, sumsales FROM top_n
```

**IR BEFORE:**
```
S0 [SELECT]
  MAIN QUERY (via Q_S0)
    FROM: (subquery) t
    GROUP BY: ss_customer_sk
    ORDER BY: sumsales, ss_customer_sk

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**IR TARGET:**
```
S0 [SELECT]
  CTE: filtered_reason  (via CTE_Q_S0_filtered_reason)
    FROM: reason
    WHERE [a7afe1b89848b69b]: r_reason_desc = 'duplicate purchase'
  CTE: joined_returns_sales  (via CTE_Q_S0_joined_returns_sales)
    FROM: store_sales ss, store_returns sr, filtered_reason fr
  CTE: aggregated  (via CTE_Q_S0_aggregated)
    FROM: joined_returns_sales
    GROUP BY: ss_customer_sk
  CTE: top_n  (via CTE_Q_S0_top_n)
    FROM: aggregated
    ORDER BY: sumsales, ss_customer_sk
  MAIN QUERY (via Q_S0)
    FROM: top_n

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

**PATCH PLAN:**
```json
{
  "plan_id": "gold_duckdb_inner_join_conversion",
  "dialect": "duckdb",
  "description": "Convert LEFT JOIN + right-table WHERE filter to INNER JOIN + early filter CTE when the WHERE eliminates NULL rows",
  "preconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "postconditions": [
    {
      "kind": "parse_ok"
    }
  ],
  "steps": [
    {
      "step_id": "s1",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "filtered_reason",
        "cte_query_sql": "SELECT r_reason_sk, r_reason_desc FROM reason WHERE r_reason_desc = 'duplicate purchase'"
      },
      "description": "Insert CTE 'filtered_reason'"
    },
    {
      "step_id": "s2",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "joined_returns_sales",
        "cte_query_sql": "SELECT ss.ss_customer_sk, ss.ss_quantity, ss.ss_sales_price, sr.sr_return_quantity FROM store_sales AS ss INNER JOIN store_returns AS sr ON (ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number) INNER JOIN filtered_reason AS fr ON sr.sr_reason_sk = fr.r_reason_sk"
      },
      "description": "Insert CTE 'joined_returns_sales' for pre-filtered join"
    },
    {
      "step_id": "s3",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "aggregated",
        "cte_query_sql": "SELECT ss_customer_sk, SUM((ss_quantity - sr_return_quantity) * ss_sales_price) AS sumsales FROM joined_returns_sales GROUP BY ss_customer_sk"
      },
      "description": "Insert CTE 'aggregated' for pre-aggregated computation"
    },
    {
      "step_id": "s4",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "top_n",
        "cte_query_sql": "SELECT ss_customer_sk, sumsales FROM aggregated ORDER BY sumsales ASC, ss_customer_sk ASC LIMIT 100"
      },
      "description": "Insert CTE 'top_n'"
    },
    {
      "step_id": "s5",
      "op": "replace_body",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "sql_fragment": "SELECT ss_customer_sk, sumsales FROM top_n"
      },
      "description": "Replace main query body with optimized version"
    }
  ]
}
```



## Worker Routing

Your targets will be routed to specialized workers:
- **W1 "Reducer"** (Families A, D): Cardinality reduction — early filtering, set operations
- **W2 "Unnester"** (Families B, C): Decorrelation, aggregation pushdown
- **W3 "Builder"** (Families F, E): Join restructuring, materialization/prefetch
- **W4 "Wildcard"** (Dynamic): Deep specialist — your **#1 target** gets maximum effort

The highest-relevance target always goes to W4. Design diverse targets across worker roles for maximum coverage.


## Your Task

Analyze this query against the 6 families above.

Choose up to 4 families that are most relevant. For each chosen family:
1. Describe the bottleneck hypothesis
2. Design a TARGET IR node map showing what the optimized query SHOULD look like
3. Score relevance (0.0–1.0)
4. Recommend which gold example(s) a code-generation worker should use as reference


**Output format**:

```json
[
  {
    "family": "B",
    "transform": "shared_scan_decorrelate",
    "target_id": "t1",
    "relevance_score": 0.95,
    "hypothesis": "Correlated scalar subquery re-scans web_sales per row. Shared-scan variant: inner=outer table with same date filter.",
    "target_ir": "S0 [SELECT]\n  CTE: common_scan  (via Q1)\n    FROM: web_sales, date_dim\n    WHERE: d_date BETWEEN ... AND d_date_sk = ws_sold_date_sk\n  CTE: thresholds  (via Q2)\n    FROM: common_scan\n    GROUP BY: ws_item_sk\n  MAIN QUERY (via Q0)\n    FROM: common_scan cs, item, thresholds t\n    WHERE: i_manufact_id = 320 AND ... AND cs.ws_ext_discount_amt > t.threshold\n    ORDER BY: sum(ws_ext_discount_amt)",
    "recommended_examples": ["sf_shared_scan_decorrelate"]
  }
]
```

**Rules**:
- target_ir must follow the IR node map format (same as Section 4)
- target_ir describes the STRUCTURAL SHAPE of the optimized query (CTE names, FROM tables, WHERE conditions, GROUP BY, ORDER BY)
- recommended_examples: list gold example IDs the worker should use as reference patch template
- Each target should represent a DIFFERENT optimization strategy
- Rank by relevance_score (highest first)
- Output up to 4 targets

After JSON, provide analysis:

## Analysis
For each available family, explain relevance (HIGH / MEDIUM / LOW) in 1-2 sentences.
**Chosen families**: [list]
**Confidence**: High/Medium/Low
