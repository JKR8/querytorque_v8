SELECT
  MIN(w_warehouse_name),
  MIN(sm_type),
  MIN(cc_name),
  MIN(cs_order_number),
  MIN(cs_item_sk)
FROM (
  SELECT d_date_sk 
  FROM date_dim 
  WHERE d_month_seq BETWEEN 1193 AND 1193 + 23
) date_filter
JOIN (
  SELECT 
    cs_ship_date_sk,
    cs_warehouse_sk,
    cs_ship_mode_sk,
    cs_call_center_sk,
    cs_order_number,
    cs_item_sk
  FROM catalog_sales 
  WHERE cs_list_price BETWEEN 271 AND 300
) cs_filtered ON cs_ship_date_sk = date_filter.d_date_sk
JOIN (
  SELECT w_warehouse_sk, w_warehouse_name 
  FROM warehouse 
  WHERE w_gmt_offset = -5
) warehouse_filtered ON cs_filtered.cs_warehouse_sk = warehouse_filtered.w_warehouse_sk
JOIN (
  SELECT sm_ship_mode_sk, sm_type 
  FROM ship_mode 
  WHERE sm_type = 'REGULAR'
) ship_mode_filtered ON cs_filtered.cs_ship_mode_sk = ship_mode_filtered.sm_ship_mode_sk
JOIN (
  SELECT cc_call_center_sk, cc_name 
  FROM call_center 
  WHERE cc_class = 'small'
) call_center_filtered ON cs_filtered.cs_call_center_sk = call_center_filtered.cc_call_center_sk