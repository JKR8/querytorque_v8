{
  "plan_id": "wildcard_shared_scan_decorrelate",
  "dialect": "snowflake",
  "description": "Extract shared web_sales and date_dim scan into CTE, precompute per-item thresholds, then join to filter rows. Converts correlated subquery into efficient hash joins.",
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
        "cte_query_sql": "SELECT ws_item_sk, ws_ext_discount_amt FROM web_sales INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk WHERE d_date BETWEEN '2002-02-26' AND CAST('2002-02-26' AS DATE) + INTERVAL '90 DAY'"
      },
      "description": "Extract shared fact+date scan with common date filter into CTE"
    },
    {
      "step_id": "s2",
      "op": "insert_cte",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "cte_name": "thresholds",
        "cte_query_sql": "SELECT ws_item_sk, 1.3 * AVG(ws_ext_discount_amt) AS threshold FROM common_scan GROUP BY ws_item_sk"
      },
      "description": "Compute per-item discount threshold from shared scan"
    },
    {
      "step_id": "s3",
      "op": "replace_from",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "from_sql": "common_scan cs INNER JOIN item ON i_item_sk = cs.ws_item_sk INNER JOIN thresholds t ON t.ws_item_sk = cs.ws_item_sk"
      },
      "description": "Replace original FROM clause with JOINs on CTEs"
    },
    {
      "step_id": "s4",
      "op": "replace_where_predicate",
      "target": {
        "by_node_id": "S0"
      },
      "payload": {
        "expr_sql": "i_manufact_id = 320 AND cs.ws_ext_discount_amt > t.threshold"
      },
      "description": "Replace WHERE clause to use precomputed thresholds and item filter"
    }
  ]
}