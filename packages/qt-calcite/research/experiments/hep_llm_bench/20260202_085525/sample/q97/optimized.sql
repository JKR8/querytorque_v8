SELECT SUM(CASE WHEN t2.CUSTOMER_SK IS NOT NULL AND t6.CUSTOMER_SK IS NULL THEN 1 ELSE 0 END) AS STORE_ONLY, SUM(CASE WHEN t2.CUSTOMER_SK IS NULL AND t6.CUSTOMER_SK IS NOT NULL THEN 1 ELSE 0 END) AS CATALOG_ONLY, SUM(CASE WHEN t2.CUSTOMER_SK IS NOT NULL AND t6.CUSTOMER_SK IS NOT NULL THEN 1 ELSE 0 END) AS STORE_AND_CATALOG
FROM (SELECT store_sales.ss_customer_sk AS CUSTOMER_SK, store_sales.ss_item_sk AS ITEM_SK
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
GROUP BY store_sales.ss_customer_sk, store_sales.ss_item_sk) AS t2
FULL JOIN (SELECT catalog_sales.cs_bill_customer_sk AS CUSTOMER_SK, catalog_sales.cs_item_sk AS ITEM_SK
FROM catalog_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t3 ON catalog_sales.cs_sold_date_sk = t3.d_date_sk
GROUP BY catalog_sales.cs_bill_customer_sk, catalog_sales.cs_item_sk) AS t6 ON t2.CUSTOMER_SK = t6.CUSTOMER_SK AND t2.ITEM_SK = t6.ITEM_SK
FETCH NEXT 100 ROWS ONLY