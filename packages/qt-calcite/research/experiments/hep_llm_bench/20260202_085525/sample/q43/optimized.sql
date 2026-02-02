SELECT t3.s_store_name AS S_STORE_NAME, t3.s_store_id AS S_STORE_ID, t3.SUN_SALES, t3.MON_SALES, t3.TUE_SALES, t3.WED_SALES, t3.THU_SALES, t3.FRI_SALES, t3.SAT_SALES
FROM (SELECT t0.s_store_name, t0.s_store_id, SUM(store_sales.ss_sales_price) FILTER (WHERE t.d_day_name = 'Sunday' IS TRUE) AS SUN_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE t.d_day_name = 'Monday' IS TRUE) AS MON_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE t.d_day_name = 'Tuesday' IS TRUE) AS TUE_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE t.d_day_name = 'Wednesday' IS TRUE) AS WED_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE t.d_day_name = 'Thursday' IS TRUE) AS THU_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE t.d_day_name = 'Friday' IS TRUE) AS FRI_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE t.d_day_name = 'Saturday' IS TRUE) AS SAT_SALES
FROM (SELECT *
FROM date_dim
WHERE d_year = 2000) AS t
INNER JOIN store_sales ON t.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN (SELECT *
FROM store
WHERE s_gmt_offset = -5) AS t0 ON store_sales.ss_store_sk = t0.s_store_sk
GROUP BY t0.s_store_name, t0.s_store_id
ORDER BY t0.s_store_name, t0.s_store_id, 3, 4, 5, 6, 7, 8, 9
FETCH NEXT 100 ROWS ONLY) AS t3