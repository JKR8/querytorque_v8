SELECT store.s_store_name AS S_STORE_NAME, item.i_item_desc AS I_ITEM_DESC, t7.REVENUE, item.i_current_price AS I_CURRENT_PRICE, item.i_wholesale_cost AS I_WHOLESALE_COST, item.i_brand AS I_BRAND
FROM store
CROSS JOIN item
INNER JOIN (SELECT t2.SS_STORE_SK, AVG(t2.REVENUE) AS AVE
FROM (SELECT store_sales.ss_store_sk AS SS_STORE_SK, SUM(store_sales.ss_sales_price) AS REVENUE
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1176 AND d_month_seq <= 1176 + 11) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
GROUP BY store_sales.ss_store_sk, store_sales.ss_item_sk) AS t2
GROUP BY t2.SS_STORE_SK) AS t3 ON store.s_store_sk = t3.SS_STORE_SK
INNER JOIN (SELECT store_sales0.ss_store_sk AS SS_STORE_SK, store_sales0.ss_item_sk AS SS_ITEM_SK, SUM(store_sales0.ss_sales_price) AS REVENUE
FROM store_sales AS store_sales0
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1176 AND d_month_seq <= 1176 + 11) AS t4 ON store_sales0.ss_sold_date_sk = t4.d_date_sk
GROUP BY store_sales0.ss_store_sk, store_sales0.ss_item_sk) AS t7 ON t7.REVENUE <= 0.1 * t3.AVE AND t3.SS_STORE_SK = t7.SS_STORE_SK AND item.i_item_sk = t7.SS_ITEM_SK
ORDER BY store.s_store_name NULLS FIRST, item.i_item_desc NULLS FIRST
FETCH NEXT 100 ROWS ONLY