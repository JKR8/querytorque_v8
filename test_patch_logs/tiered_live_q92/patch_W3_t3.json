{
  "plan_id": "date_scan_materialize",
  "dialect": "snowflake",
  "description": "Materialize date-filtered web_sales scan into CTE to reuse in main query and subquery",
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
        "cte_name": "date_filtered_sales",
        "cte_query_sql": "SELECT ws_item_sk, ws_ext_discount_amt FROM web_sales JOIN date_dim ON d_date_sk = ws_sold_date_sk WHERE d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL '90 DAY')"
      },
      "description": "Insert CTE 'date_filtered_sales' for date-filtered web_sales"
    },
    {
      "step_id": "s2",
      "op": "replace_from",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "from_sql": "date_filtered_sales dfs INNER JOIN item ON i_item_sk = dfs.ws_item_sk"
      },
      "description": "Replace FROM clause with optimized version using CTE"
    },
    {
      "step_id": "s3",
      "op": "replace_where_predicate",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "expr_sql": "i_manufact_id = 320 AND dfs.ws_ext_discount_amt > (SELECT 1.3 * AVG(ws_ext_discount_amt) FROM date_filtered_sales WHERE ws_item_sk = i_item_sk)"
      },
      "description": "Replace WHERE clause with optimized version using CTE in subquery"
    }
  ]
}