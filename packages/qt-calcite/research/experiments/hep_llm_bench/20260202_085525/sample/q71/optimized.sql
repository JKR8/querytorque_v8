SELECT t11.i_brand_id AS BRAND_ID, t11.i_brand AS BRAND, t11.t_hour AS T_HOUR, t11.t_minute AS T_MINUTE, t11.EXT_PRICE
FROM (SELECT t.i_brand, t.i_brand_id, t8.t_hour, t8.t_minute, SUM(t7.EXT_PRICE) AS EXT_PRICE
FROM (SELECT *
FROM item
WHERE i_manager_id = 1) AS t
INNER JOIN (SELECT *
FROM (SELECT web_sales.ws_ext_sales_price AS EXT_PRICE, web_sales.ws_sold_date_sk AS SOLD_DATE_SK, web_sales.ws_item_sk AS SOLD_ITEM_SK, web_sales.ws_sold_time_sk AS TIME_SK
FROM web_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy = 11 AND d_year = 1999) AS t0 ON web_sales.ws_sold_date_sk = t0.d_date_sk
UNION ALL
SELECT catalog_sales.cs_ext_sales_price AS EXT_PRICE, catalog_sales.cs_sold_date_sk AS SOLD_DATE_SK, catalog_sales.cs_item_sk AS SOLD_ITEM_SK, catalog_sales.cs_sold_time_sk AS TIME_SK
FROM catalog_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy = 11 AND d_year = 1999) AS t2 ON catalog_sales.cs_sold_date_sk = t2.d_date_sk)
UNION ALL
SELECT store_sales.ss_ext_sales_price AS EXT_PRICE, store_sales.ss_sold_date_sk AS SOLD_DATE_SK, store_sales.ss_item_sk AS SOLD_ITEM_SK, store_sales.ss_sold_time_sk AS TIME_SK
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy = 11 AND d_year = 1999) AS t5 ON store_sales.ss_sold_date_sk = t5.d_date_sk) AS t7 ON t.i_item_sk = t7.SOLD_ITEM_SK
INNER JOIN (SELECT *
FROM time_dim
WHERE t_meal_time IN ('breakfast', 'dinner')) AS t8 ON t7.TIME_SK = t8.t_time_sk
GROUP BY t.i_brand, t.i_brand_id, t8.t_hour, t8.t_minute
ORDER BY 5 DESC, t.i_brand_id NULLS FIRST, t8.t_hour NULLS FIRST) AS t11