SELECT C_LAST_NAME, C_FIRST_NAME, SALES
FROM (SELECT customer.c_last_name AS C_LAST_NAME, customer.c_first_name AS C_FIRST_NAME, SUM(catalog_sales.cs_quantity * catalog_sales.cs_list_price) AS SALES
FROM catalog_sales
INNER JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2000 AND d_moy = 2) AS t ON catalog_sales.cs_sold_date_sk = t.d_date_sk
INNER JOIN (SELECT t1.ITEMDESC, t1.i_item_sk AS ITEM_SK, t0.d_date AS SOLDDATE, COUNT(*) AS CNT
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2000 OR d_year = CAST(2000 + 1 AS BIGINT) OR d_year = CAST(2000 + 2 AS BIGINT) OR d_year = CAST(2000 + 3 AS BIGINT)) AS t0 ON store_sales.ss_sold_date_sk = t0.d_date_sk
INNER JOIN (SELECT SUBSTRING(i_item_desc, 1, 30) AS ITEMDESC, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name
FROM item) AS t1 ON store_sales.ss_item_sk = t1.i_item_sk
GROUP BY t1.ITEMDESC, t1.i_item_sk, t0.d_date
HAVING COUNT(*) > 4) AS t5 ON catalog_sales.cs_item_sk = t5.ITEM_SK
INNER JOIN (SELECT customer0.c_customer_sk AS C_CUSTOMER_SK, SUM(store_sales0.ss_quantity * store_sales0.ss_sales_price) AS SSALES
FROM store_sales AS store_sales0
INNER JOIN customer AS customer0 ON store_sales0.ss_customer_sk = customer0.c_customer_sk
CROSS JOIN (SELECT MAX(t9.CSALES) AS TPCDS_CMAX
FROM (SELECT SUM(store_sales1.ss_quantity * store_sales1.ss_sales_price) AS CSALES
FROM store_sales AS store_sales1
INNER JOIN customer AS customer1 ON store_sales1.ss_customer_sk = customer1.c_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2000 OR d_year = CAST(2000 + 1 AS BIGINT) OR d_year = CAST(2000 + 2 AS BIGINT) OR d_year = CAST(2000 + 3 AS BIGINT)) AS t6 ON store_sales1.ss_sold_date_sk = t6.d_date_sk
GROUP BY customer1.c_customer_sk) AS t9) AS t10
GROUP BY customer0.c_customer_sk
HAVING SUM(store_sales0.ss_quantity * store_sales0.ss_sales_price) > 50 / 100.0 * MAX(t10.TPCDS_CMAX)) AS t14 ON catalog_sales.cs_bill_customer_sk = t14.C_CUSTOMER_SK
GROUP BY customer.c_last_name, customer.c_first_name
UNION ALL
SELECT customer2.c_last_name AS C_LAST_NAME, customer2.c_first_name AS C_FIRST_NAME, SUM(web_sales.ws_quantity * web_sales.ws_list_price) AS SALES
FROM web_sales
INNER JOIN customer AS customer2 ON web_sales.ws_bill_customer_sk = customer2.c_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2000 AND d_moy = 2) AS t18 ON web_sales.ws_sold_date_sk = t18.d_date_sk
INNER JOIN (SELECT t20.ITEMDESC, t20.i_item_sk AS ITEM_SK, t19.d_date AS SOLDDATE, COUNT(*) AS CNT
FROM store_sales AS store_sales2
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2000 OR d_year = CAST(2000 + 1 AS BIGINT) OR d_year = CAST(2000 + 2 AS BIGINT) OR d_year = CAST(2000 + 3 AS BIGINT)) AS t19 ON store_sales2.ss_sold_date_sk = t19.d_date_sk
INNER JOIN (SELECT SUBSTRING(i_item_desc, 1, 30) AS ITEMDESC, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name
FROM item) AS t20 ON store_sales2.ss_item_sk = t20.i_item_sk
GROUP BY t20.ITEMDESC, t20.i_item_sk, t19.d_date
HAVING COUNT(*) > 4) AS t24 ON web_sales.ws_item_sk = t24.ITEM_SK
INNER JOIN (SELECT customer3.c_customer_sk AS C_CUSTOMER_SK, SUM(store_sales3.ss_quantity * store_sales3.ss_sales_price) AS SSALES
FROM store_sales AS store_sales3
INNER JOIN customer AS customer3 ON store_sales3.ss_customer_sk = customer3.c_customer_sk
CROSS JOIN (SELECT MAX(t28.CSALES) AS TPCDS_CMAX
FROM (SELECT SUM(store_sales4.ss_quantity * store_sales4.ss_sales_price) AS CSALES
FROM store_sales AS store_sales4
INNER JOIN customer AS customer4 ON store_sales4.ss_customer_sk = customer4.c_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2000 OR d_year = CAST(2000 + 1 AS BIGINT) OR d_year = CAST(2000 + 2 AS BIGINT) OR d_year = CAST(2000 + 3 AS BIGINT)) AS t25 ON store_sales4.ss_sold_date_sk = t25.d_date_sk
GROUP BY customer4.c_customer_sk) AS t28) AS t29
GROUP BY customer3.c_customer_sk
HAVING SUM(store_sales3.ss_quantity * store_sales3.ss_sales_price) > 50 / 100.0 * MAX(t29.TPCDS_CMAX)) AS t33 ON web_sales.ws_bill_customer_sk = t33.C_CUSTOMER_SK
GROUP BY customer2.c_last_name, customer2.c_first_name)
ORDER BY C_LAST_NAME NULLS FIRST, C_FIRST_NAME NULLS FIRST, SALES NULLS FIRST
FETCH NEXT 100 ROWS ONLY