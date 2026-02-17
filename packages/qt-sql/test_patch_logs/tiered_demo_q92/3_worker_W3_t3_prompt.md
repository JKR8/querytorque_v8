## Role

You are **W3 "Builder"** — Structural optimization — join restructuring, materialization. Restructure join topology and materialize repeated work: convert comma joins to explicit INNER JOIN, extract shared scans into CTEs, prefetch dimension tables.

Transform this SQL query from its CURRENT IR structure to a TARGET IR structure using patch operations. Output a single PatchPlan JSON.

**Family**: F — explicit_join_reorder
**Hypothesis**: Three-table comma join (web_sales, item, date_dim) leaves join order to optimizer. Explicit INNER JOIN with item filter first reduces probe input to web_sales.

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
  MAIN QUERY (via Q_S0)
    FROM: item i
      INNER JOIN web_sales ws ON i.i_item_sk = ws.ws_item_sk
      INNER JOIN date_dim d ON d.d_date_sk = ws.ws_sold_date_sk
    WHERE: i.i_manufact_id = 320
      AND d.d_date BETWEEN '2002-02-26' AND ...
      AND ws.ws_ext_discount_amt > (subquery)
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

## Instructions

Adapt the gold example pattern to match the ORIGINAL SQL above.
Use the TARGET IR as your structural guide — create CTEs matching the target's CTE names and structure.
Preferred approach: insert_cte (x2-3) + replace_from or replace_body.
All SQL in payloads must be complete, executable fragments (no ellipsis).
Use dialect: "snowflake" in the output.
Target all steps at by_node_id: "S0" (the main statement).

Output ONLY the JSON object (no markdown, no explanation):