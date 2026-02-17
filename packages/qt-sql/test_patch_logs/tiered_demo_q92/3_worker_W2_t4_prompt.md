## Role

You are **W2 "Unnester"** — Logic simplification — decorrelation, aggregation pushdown. Eliminate per-row re-execution: convert correlated subqueries to GROUP BY CTEs, push aggregation before joins when GROUP BY keys ⊇ join keys.

Transform this SQL query from its CURRENT IR structure to a TARGET IR structure using patch operations. Output a single PatchPlan JSON.

**Family**: E — shared_scan_materialize
**Hypothesis**: web_sales scanned twice (outer + subquery) with overlapping date filter. Materialize the web_sales+date_dim join once, reuse for both outer and threshold.

## Original SQL

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

## Current IR Node Map

```
S0 [SELECT]
  MAIN QUERY (via Q_S0)
    FROM: web_sales, item, date_dim
    WHERE [d19a1964890bdea2]: i_manufact_id = 320 AND i_item_sk = ws_item_sk AND d_date BETWEEN '2002-02-26' AND (CAST('2002-02...
    ORDER BY: SUM(ws_ext_discount_amt)

Patch operations: insert_cte, replace_expr_subtree, replace_where_predicate, replace_from, delete_expr_subtree
Target: by_node_id (statement, e.g. "S0") + by_anchor_hash (expression)
```

## Target IR (what the optimized query should look like)

```
S0 [SELECT]
  CTE: ws_dated  (via CTE_Q_S0_ws_dated)
    FROM: web_sales ws INNER JOIN date_dim d ON d.d_date_sk = ws.ws_sold_date_sk
    WHERE: d.d_date BETWEEN '2002-02-26' AND ...
    SELECT: ws.ws_item_sk, ws.ws_ext_discount_amt
  CTE: item_thresholds  (via CTE_Q_S0_item_thresholds)
    FROM: ws_dated
    GROUP BY: ws_item_sk
    SELECT: ws_item_sk, 1.3 * AVG(ws_ext_discount_amt) AS threshold
  MAIN QUERY (via Q_S0)
    FROM: ws_dated wd, item i, item_thresholds t
    WHERE: i.i_manufact_id = 320 AND i.i_item_sk = wd.ws_item_sk
      AND wd.ws_ext_discount_amt > t.threshold AND t.ws_item_sk = wd.ws_item_sk
    ORDER BY: SUM(ws_ext_discount_amt)
    LIMIT: 100
```

## Patch Operations

| Op | Description | Payload |
|----|-------------|---------|
| insert_cte | Add a new CTE to the WITH clause | cte_name, cte_query_sql |
| replace_from | Replace the FROM clause | from_sql |
| replace_where_predicate | Replace the WHERE clause | expr_sql |
| replace_body | Replace entire query body (SELECT, FROM, WHERE, GROUP BY) | sql_fragment |
| replace_expr_subtree | Replace a specific expression | expr_sql (+ by_anchor_hash) |
| delete_expr_subtree | Remove a specific expression | (target only, no payload) |

## Gold Patch Example (reference pattern)

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

## Instructions

Adapt the gold example pattern to match the ORIGINAL SQL above.
Use the TARGET IR as your structural guide — create CTEs matching the target's CTE names and structure.
Preferred approach: insert_cte (x2-3) + replace_from or replace_body.
All SQL in payloads must be complete, executable fragments (no ellipsis).
Use dialect: "snowflake" in the output.
Target all steps at by_node_id: "S0" (the main statement).

Output ONLY the JSON object (no markdown, no explanation):