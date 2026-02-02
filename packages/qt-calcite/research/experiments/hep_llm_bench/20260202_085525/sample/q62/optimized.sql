SELECT t.W_SUBSTR, ship_mode.sm_type AS SM_TYPE, web_site.web_name AS WEB_NAME, SUM(CASE WHEN web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk <= 30 THEN 1 ELSE 0 END) AS 30 days, SUM(CASE WHEN web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk > 30 AND web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk <= 60 THEN 1 ELSE 0 END) AS 31-60 days, SUM(CASE WHEN web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk > 60 AND web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk <= 90 THEN 1 ELSE 0 END) AS 61-90 days, SUM(CASE WHEN web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk > 90 AND web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk <= 120 THEN 1 ELSE 0 END) AS 91-120 days, SUM(CASE WHEN web_sales.ws_ship_date_sk - web_sales.ws_sold_date_sk > 120 THEN 1 ELSE 0 END) AS >120 days
FROM web_sales
INNER JOIN (SELECT SUBSTRING(w_warehouse_name, 1, 20) AS W_SUBSTR, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset
FROM warehouse) AS t ON web_sales.ws_warehouse_sk = t.w_warehouse_sk
INNER JOIN ship_mode ON web_sales.ws_ship_mode_sk = ship_mode.sm_ship_mode_sk
INNER JOIN web_site ON web_sales.ws_web_site_sk = web_site.web_site_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t0 ON web_sales.ws_ship_date_sk = t0.d_date_sk
GROUP BY t.W_SUBSTR, ship_mode.sm_type, web_site.web_name
ORDER BY t.W_SUBSTR NULLS FIRST, ship_mode.sm_type NULLS FIRST, web_site.web_name NULLS FIRST
FETCH NEXT 100 ROWS ONLY