## Role

You are **W1 "Reducer"** — Cardinality reduction — WHERE filters, set operations, early pruning. Reduce row counts early: push predicates into CTEs, convert set operations to EXISTS/NOT EXISTS, apply early filtering before expensive joins.

Transform this SQL query from its CURRENT IR structure to a TARGET IR structure using patch operations. Output a single PatchPlan JSON.

**Family**: A — early_filter_push
**Hypothesis**: Push i_manufact_id=320 and date filters into CTEs before joins. Reduces web_sales rows early.

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
  CTE: filtered_items  (via CTE_Q_S0_items)
    FROM: item
    WHERE: i_manufact_id = 320
  CTE: filtered_dates  (via CTE_Q_S0_dates)
    FROM: date_dim
    WHERE: d_date BETWEEN '2002-02-26' AND (cast('2002-02-26' as date) + INTERVAL '90 DAY')
  MAIN QUERY (via Q_S0)
    FROM: web_sales ws
    INNER JOIN filtered_items i ON i.i_item_sk = ws.ws_item_sk
    INNER JOIN filtered_dates d ON d.d_date_sk = ws.ws_sold_date_sk
    WHERE: ws.ws_ext_discount_amt > (subquery unchanged)
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

## Instructions

Adapt the gold example pattern to match the ORIGINAL SQL above.
Use the TARGET IR as your structural guide — create CTEs matching the target's CTE names and structure.
Preferred approach: insert_cte (x2-3) + replace_from or replace_body.
All SQL in payloads must be complete, executable fragments (no ellipsis).
Use dialect: "snowflake" in the output.
Target all steps at by_node_id: "S0" (the main statement).

Output ONLY the JSON object (no markdown, no explanation):