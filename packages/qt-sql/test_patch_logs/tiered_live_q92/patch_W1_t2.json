{
  "plan_id": "early_filter_push_web_sales",
  "dialect": "snowflake",
  "description": "Push i_manufact_id and date filters into CTEs before joins to reduce row counts early.",
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
        "cte_query_sql": "SELECT i_item_sk FROM item WHERE i_manufact_id = 320"
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
        "cte_name": "filtered_dates",
        "cte_query_sql": "SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-02-26' AND CAST('2002-02-26' AS DATE) + INTERVAL '90 DAY'"
      },
      "description": "Extract date filter into CTE"
    },
    {
      "step_id": "s3",
      "op": "replace_from",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "from_sql": "web_sales ws INNER JOIN filtered_items i ON i.i_item_sk = ws.ws_item_sk INNER JOIN filtered_dates d ON d.d_date_sk = ws.ws_sold_date_sk"
      },
      "description": "Replace FROM clause with explicit JOINs using filtered CTEs"
    },
    {
      "step_id": "s4",
      "op": "replace_where_predicate",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "expr_sql": "ws.ws_ext_discount_amt > (SELECT 1.3 * AVG(ws_ext_discount_amt) FROM web_sales, date_dim WHERE ws_item_sk = i_item_sk AND d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL '90 DAY') AND d_date_sk = ws_sold_date_sk)"
      },
      "description": "Retain original subquery in WHERE clause"
    }
  ]
}