SELECT t2.S_STORE_NAME1, t2.S_STORE_ID1, t2.D_WEEK_SEQ1, t2.SUN_SALES1 / t6.SUN_SALES2 AS SUN_SALES_RATIO, t2.MON_SALES1 / t6.MON_SALES2 AS MON_SALES_RATIO, t2.TUE_SALES1 / t6.TUE_SALES2 AS TUE_SALES_RATIO, t2.WED_SALES1 / t6.WED_SALES2 AS WED_SALES_RATIO, t2.THU_SALES1 / t6.THU_SALES2 AS THU_SALES_RATIO, t2.FRI_SALES1 / t6.FRI_SALES2 AS FRI_SALES_RATIO, t2.SAT_SALES1 / t6.SAT_SALES2 AS SAT_SALES_RATIO
FROM (SELECT store.s_store_name AS S_STORE_NAME1, t0.d_week_seq AS D_WEEK_SEQ1, store.s_store_id AS S_STORE_ID1, t0.SUN_SALES AS SUN_SALES1, t0.MON_SALES AS MON_SALES1, t0.TUE_SALES AS TUE_SALES1, t0.WED_SALES AS WED_SALES1, t0.THU_SALES AS THU_SALES1, t0.FRI_SALES AS FRI_SALES1, t0.SAT_SALES AS SAT_SALES1
FROM (SELECT date_dim.d_week_seq, store_sales.ss_store_sk, SUM(store_sales.ss_sales_price) FILTER (WHERE date_dim.d_day_name = 'Sunday' IS TRUE) AS SUN_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE date_dim.d_day_name = 'Monday' IS TRUE) AS MON_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE date_dim.d_day_name = 'Tuesday' IS TRUE) AS TUE_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE date_dim.d_day_name = 'Wednesday' IS TRUE) AS WED_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE date_dim.d_day_name = 'Thursday' IS TRUE) AS THU_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE date_dim.d_day_name = 'Friday' IS TRUE) AS FRI_SALES, SUM(store_sales.ss_sales_price) FILTER (WHERE date_dim.d_day_name = 'Saturday' IS TRUE) AS SAT_SALES
FROM store_sales
INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
GROUP BY date_dim.d_week_seq, store_sales.ss_store_sk) AS t0
INNER JOIN store ON t0.ss_store_sk = store.s_store_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1212 AND d_month_seq <= 1212 + 11) AS t1 ON t0.d_week_seq = t1.d_week_seq) AS t2
INNER JOIN (SELECT store0.s_store_name AS S_STORE_NAME2, t4.d_week_seq AS D_WEEK_SEQ2, store0.s_store_id AS S_STORE_ID2, t4.SUN_SALES AS SUN_SALES2, t4.MON_SALES AS MON_SALES2, t4.TUE_SALES AS TUE_SALES2, t4.WED_SALES AS WED_SALES2, t4.THU_SALES AS THU_SALES2, t4.FRI_SALES AS FRI_SALES2, t4.SAT_SALES AS SAT_SALES2
FROM (SELECT date_dim1.d_week_seq, store_sales0.ss_store_sk, SUM(store_sales0.ss_sales_price) FILTER (WHERE date_dim1.d_day_name = 'Sunday' IS TRUE) AS SUN_SALES, SUM(store_sales0.ss_sales_price) FILTER (WHERE date_dim1.d_day_name = 'Monday' IS TRUE) AS MON_SALES, SUM(store_sales0.ss_sales_price) FILTER (WHERE date_dim1.d_day_name = 'Tuesday' IS TRUE) AS TUE_SALES, SUM(store_sales0.ss_sales_price) FILTER (WHERE date_dim1.d_day_name = 'Wednesday' IS TRUE) AS WED_SALES, SUM(store_sales0.ss_sales_price) FILTER (WHERE date_dim1.d_day_name = 'Thursday' IS TRUE) AS THU_SALES, SUM(store_sales0.ss_sales_price) FILTER (WHERE date_dim1.d_day_name = 'Friday' IS TRUE) AS FRI_SALES, SUM(store_sales0.ss_sales_price) FILTER (WHERE date_dim1.d_day_name = 'Saturday' IS TRUE) AS SAT_SALES
FROM store_sales AS store_sales0
INNER JOIN date_dim AS date_dim1 ON store_sales0.ss_sold_date_sk = date_dim1.d_date_sk
GROUP BY date_dim1.d_week_seq, store_sales0.ss_store_sk) AS t4
INNER JOIN store AS store0 ON t4.ss_store_sk = store0.s_store_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1212 + 12 AND d_month_seq <= 1212 + 23) AS t5 ON t4.d_week_seq = t5.d_week_seq) AS t6 ON t2.S_STORE_ID1 = t6.S_STORE_ID2 AND t2.D_WEEK_SEQ1 = t6.D_WEEK_SEQ2 - 52
ORDER BY t2.S_STORE_NAME1 NULLS FIRST, t2.S_STORE_ID1 NULLS FIRST, t2.D_WEEK_SEQ1 NULLS FIRST
FETCH NEXT 100 ROWS ONLY