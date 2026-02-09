WITH
  filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1193 AND 1193 + 23
  ),
  filtered_warehouse AS (
    SELECT w_warehouse_sk,
           w_warehouse_name,
           w_gmt_offset
    FROM warehouse
    WHERE w_gmt_offset = -5
  ),
  filtered_ship_mode AS (
    SELECT sm_ship_mode_sk,
           sm_type
    FROM ship_mode
    WHERE sm_type = 'REGULAR'
  ),
  filtered_call_center AS (
    SELECT cc_call_center_sk,
           cc_name
    FROM call_center
    WHERE cc_class = 'small'
  ),
  joined AS (
    SELECT
      cs.cs_ship_date_sk - cs.cs_sold_date_sk AS diff_days,
      SUBSTRING(fw.w_warehouse_name FROM 1 FOR 20) AS wh_sub,
      fsm.sm_type,
      fcc.cc_name
    FROM catalog_sales cs
    JOIN filtered_date fd ON cs.cs_ship_date_sk = fd.d_date_sk
    JOIN filtered_warehouse fw ON cs.cs_warehouse_sk = fw.w_warehouse_sk
    JOIN filtered_ship_mode fsm ON cs.cs_ship_mode_sk = fsm.sm_ship_mode_sk
    JOIN filtered_call_center fcc ON cs.cs_call_center_sk = fcc.cc_call_center_sk
    WHERE cs.cs_list_price BETWEEN 271 AND 300
  )
SELECT
  wh_sub,
  sm_type,
  cc_name,
  COUNT(*) FILTER (WHERE diff_days <= 30) AS "30 days",
  COUNT(*) FILTER (WHERE diff_days > 30 AND diff_days <= 60) AS "31-60 days",
  COUNT(*) FILTER (WHERE diff_days > 60 AND diff_days <= 90) AS "61-90 days",
  COUNT(*) FILTER (WHERE diff_days > 90 AND diff_days <= 120) AS "91-120 days",
  COUNT(*) FILTER (WHERE diff_days > 120) AS ">120 days"
FROM joined
GROUP BY wh_sub, sm_type, cc_name
ORDER BY wh_sub, sm_type, cc_name
LIMIT 100;