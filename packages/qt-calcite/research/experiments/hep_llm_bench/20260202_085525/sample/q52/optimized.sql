SELECT t3.d_year AS D_YEAR, t3.i_brand_id AS BRAND_ID, t3.i_brand AS BRAND, t3.EXT_PRICE
FROM (SELECT t.d_year, t0.i_brand, t0.i_brand_id, SUM(store_sales.ss_ext_sales_price) AS EXT_PRICE
FROM (SELECT *
FROM date_dim
WHERE d_moy = 11 AND d_year = 2000) AS t
INNER JOIN store_sales ON t.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN (SELECT *
FROM item
WHERE i_manager_id = 1) AS t0 ON store_sales.ss_item_sk = t0.i_item_sk
GROUP BY t.d_year, t0.i_brand, t0.i_brand_id
ORDER BY t.d_year, 4 DESC, t0.i_brand_id
FETCH NEXT 100 ROWS ONLY) AS t3