{
  "plan_id": "web_sales_excess_discount_snowflake",
  "dialect": "snowflake",
  "description": "Convert comma joins to explicit INNER JOINs and decorrelate subquery using CTEs",
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
        "cte_name": "filtered_date_range",
        "cte_query_sql": "SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL '90 DAY')"
      },
      "description": "Insert CTE for filtering date range"
    },
    {
      "step_id": "s2",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "avg_discount",
        "cte_query_sql": "SELECT 1.3 * AVG(ws_ext_discount_amt) AS threshold FROM web_sales ws INNER JOIN filtered_date_range fdr ON ws.ws_sold_date_sk = fdr.d_date_sk WHERE ws.ws_item_sk = (SELECT i_item_sk FROM item WHERE i_manufact_id = 320 LIMIT 1)"
      },
      "description": "Insert CTE to compute average discount threshold"
    },
    {
      "step_id": "s3",
      "op": "replace_from",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "from_sql": "web_sales INNER JOIN item ON i_item_sk = ws_item_sk INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk"
      },
      "description": "Replace FROM clause with explicit INNER JOINs"
    },
    {
      "step_id": "s4",
      "op": "replace_where_predicate",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "expr_sql": "i_manufact_id = 320 AND d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL '90 DAY') AND ws_ext_discount_amt > (SELECT threshold FROM avg_discount)"
      },
      "description": "Replace WHERE clause with simplified condition referencing CTE"
    }
  ]
}