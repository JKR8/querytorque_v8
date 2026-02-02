SELECT CHANNEL, COL_NAME, D_YEAR, D_QOY, I_CATEGORY, COUNT(*) AS SALES_CNT, SUM(EXT_SALES_PRICE) AS SALES_AMT
FROM (SELECT *
FROM (SELECT 'store' AS CHANNEL, 'ss_store_sk' AS COL_NAME, date_dim.d_year AS D_YEAR, date_dim.d_qoy AS D_QOY, item.i_category AS I_CATEGORY, t.ss_ext_sales_price AS EXT_SALES_PRICE
FROM (SELECT *
FROM store_sales
WHERE ss_store_sk IS NULL) AS t
INNER JOIN item ON t.ss_item_sk = item.i_item_sk
INNER JOIN date_dim ON t.ss_sold_date_sk = date_dim.d_date_sk
UNION ALL
SELECT 'web' AS CHANNEL, 'ws_ship_customer_sk' AS COL_NAME, date_dim0.d_year AS D_YEAR, date_dim0.d_qoy AS D_QOY, item0.i_category AS I_CATEGORY, t1.ws_ext_sales_price AS EXT_SALES_PRICE
FROM (SELECT *
FROM web_sales
WHERE ws_ship_customer_sk IS NULL) AS t1
INNER JOIN item AS item0 ON t1.ws_item_sk = item0.i_item_sk
INNER JOIN date_dim AS date_dim0 ON t1.ws_sold_date_sk = date_dim0.d_date_sk)
UNION ALL
SELECT 'catalog' AS CHANNEL, 'cs_ship_addr_sk' AS COL_NAME, date_dim1.d_year AS D_YEAR, date_dim1.d_qoy AS D_QOY, item1.i_category AS I_CATEGORY, t4.cs_ext_sales_price AS EXT_SALES_PRICE
FROM (SELECT *
FROM catalog_sales
WHERE cs_ship_addr_sk IS NULL) AS t4
INNER JOIN item AS item1 ON t4.cs_item_sk = item1.i_item_sk
INNER JOIN date_dim AS date_dim1 ON t4.cs_sold_date_sk = date_dim1.d_date_sk) AS t6
GROUP BY CHANNEL, COL_NAME, D_YEAR, D_QOY, I_CATEGORY
ORDER BY CHANNEL NULLS FIRST, COL_NAME NULLS FIRST, D_YEAR NULLS FIRST, D_QOY NULLS FIRST, I_CATEGORY NULLS FIRST
FETCH NEXT 100 ROWS ONLY