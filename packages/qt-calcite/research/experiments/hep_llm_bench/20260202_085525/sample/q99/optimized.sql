SELECT t.W_SUBSTR, ship_mode.sm_type AS SM_TYPE, LOWER(call_center.cc_name) AS CC_NAME_LOWER, SUM(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= 30 THEN 1 ELSE 0 END) AS 30 days, SUM(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > 30 AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= 60 THEN 1 ELSE 0 END) AS 31-60 days, SUM(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > 60 AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= 90 THEN 1 ELSE 0 END) AS 61-90 days, SUM(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > 90 AND catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk <= 120 THEN 1 ELSE 0 END) AS 91-120 days, SUM(CASE WHEN catalog_sales.cs_ship_date_sk - catalog_sales.cs_sold_date_sk > 120 THEN 1 ELSE 0 END) AS >120 days
FROM catalog_sales
INNER JOIN (SELECT SUBSTRING(w_warehouse_name, 1, 20) AS W_SUBSTR, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset
FROM warehouse) AS t ON catalog_sales.cs_warehouse_sk = t.w_warehouse_sk
INNER JOIN ship_mode ON catalog_sales.cs_ship_mode_sk = ship_mode.sm_ship_mode_sk
INNER JOIN call_center ON catalog_sales.cs_call_center_sk = call_center.cc_call_center_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t0 ON catalog_sales.cs_ship_date_sk = t0.d_date_sk
GROUP BY t.W_SUBSTR, ship_mode.sm_type, call_center.cc_name
ORDER BY t.W_SUBSTR NULLS FIRST, ship_mode.sm_type NULLS FIRST, 3 NULLS FIRST
FETCH NEXT 100 ROWS ONLY