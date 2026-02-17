## Role

You are **W3 "Builder"** — Structural optimization — join restructuring, materialization. Restructure join topology and materialize repeated work: convert comma joins to explicit INNER JOIN, extract shared scans into CTEs, prefetch dimension tables.

Transform this SQL query from its CURRENT IR structure to a TARGET IR structure using patch operations. Output a single PatchPlan JSON.

**Family**: E — date_scan_materialize
**Hypothesis**: Reuse date-filtered web_sales scan in main query and subquery via CTE to avoid duplicate work.

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
  CTE: date_filtered_sales  (via CTE_Q_S0_shared)
    FROM: web_sales, date_dim
    WHERE: d_date BETWEEN '2002-02-26' AND (cast('2002-02-26' as date) + INTERVAL '90 DAY') AND d_date_sk = ws_sold_date_sk
  MAIN QUERY (via Q_S0)
    FROM: date_filtered_sales dfs
    INNER JOIN item ON i_item_sk = dfs.ws_item_sk
    WHERE: i_manufact_id = 320 AND dfs.ws_ext_discount_amt > (SELECT 1.3 * AVG(ws_ext_discount_amt) FROM date_filtered_sales WHERE ws_item_sk = i_item_sk)
    ORDER BY: SUM(ws_ext_discount_amt)
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

## Instructions

Adapt the gold example pattern to match the ORIGINAL SQL above.
Use the TARGET IR as your structural guide — create CTEs matching the target's CTE names and structure.
Preferred approach: insert_cte (x2-3) + replace_from or replace_body.
All SQL in payloads must be complete, executable fragments (no ellipsis).
Use dialect: "snowflake" in the output.
Target all steps at by_node_id: "S0" (the main statement).

Output ONLY the JSON object (no markdown, no explanation):