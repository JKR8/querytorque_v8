SELECT t18.CUSTOMER_ID, t18.CUSTOMER_FIRST_NAME, t18.CUSTOMER_LAST_NAME
FROM (SELECT *
FROM (SELECT customer.c_customer_id AS CUSTOMER_ID, customer.c_first_name AS CUSTOMER_FIRST_NAME, customer.c_last_name AS CUSTOMER_LAST_NAME, t.d_year AS YEAR_, SUM(store_sales.ss_net_paid) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer
INNER JOIN store_sales ON customer.c_customer_sk = store_sales.ss_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 OR d_year = CAST(2001 + 1 AS BIGINT)) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
GROUP BY customer.c_customer_id, customer.c_first_name, customer.c_last_name, t.d_year
UNION ALL
SELECT customer0.c_customer_id AS CUSTOMER_ID, customer0.c_first_name AS CUSTOMER_FIRST_NAME, customer0.c_last_name AS CUSTOMER_LAST_NAME, t3.d_year AS YEAR_, SUM(web_sales.ws_net_paid) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer0
INNER JOIN web_sales ON customer0.c_customer_sk = web_sales.ws_bill_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 OR d_year = CAST(2001 + 1 AS BIGINT)) AS t3 ON web_sales.ws_sold_date_sk = t3.d_date_sk
GROUP BY customer0.c_customer_id, customer0.c_first_name, customer0.c_last_name, t3.d_year) AS t7
WHERE SALE_TYPE = 's' AND YEAR_ = 2001 AND YEAR_TOTAL > 0) AS t8
INNER JOIN (SELECT *
FROM (SELECT customer1.c_customer_id AS CUSTOMER_ID, customer1.c_first_name AS CUSTOMER_FIRST_NAME, customer1.c_last_name AS CUSTOMER_LAST_NAME, t9.d_year AS YEAR_, SUM(store_sales0.ss_net_paid) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer1
INNER JOIN store_sales AS store_sales0 ON customer1.c_customer_sk = store_sales0.ss_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 OR d_year = CAST(2001 + 1 AS BIGINT)) AS t9 ON store_sales0.ss_sold_date_sk = t9.d_date_sk
GROUP BY customer1.c_customer_id, customer1.c_first_name, customer1.c_last_name, t9.d_year
UNION ALL
SELECT customer2.c_customer_id AS CUSTOMER_ID, customer2.c_first_name AS CUSTOMER_FIRST_NAME, customer2.c_last_name AS CUSTOMER_LAST_NAME, t13.d_year AS YEAR_, SUM(web_sales0.ws_net_paid) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer2
INNER JOIN web_sales AS web_sales0 ON customer2.c_customer_sk = web_sales0.ws_bill_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 OR d_year = CAST(2001 + 1 AS BIGINT)) AS t13 ON web_sales0.ws_sold_date_sk = t13.d_date_sk
GROUP BY customer2.c_customer_id, customer2.c_first_name, customer2.c_last_name, t13.d_year) AS t17
WHERE SALE_TYPE = 's' AND YEAR_ = CAST(2001 + 1 AS BIGINT)) AS t18 ON t8.CUSTOMER_ID = t18.CUSTOMER_ID
INNER JOIN (SELECT *
FROM (SELECT customer3.c_customer_id AS CUSTOMER_ID, customer3.c_first_name AS CUSTOMER_FIRST_NAME, customer3.c_last_name AS CUSTOMER_LAST_NAME, t19.d_year AS YEAR_, SUM(store_sales1.ss_net_paid) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer3
INNER JOIN store_sales AS store_sales1 ON customer3.c_customer_sk = store_sales1.ss_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 OR d_year = CAST(2001 + 1 AS BIGINT)) AS t19 ON store_sales1.ss_sold_date_sk = t19.d_date_sk
GROUP BY customer3.c_customer_id, customer3.c_first_name, customer3.c_last_name, t19.d_year
UNION ALL
SELECT customer4.c_customer_id AS CUSTOMER_ID, customer4.c_first_name AS CUSTOMER_FIRST_NAME, customer4.c_last_name AS CUSTOMER_LAST_NAME, t23.d_year AS YEAR_, SUM(web_sales1.ws_net_paid) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer4
INNER JOIN web_sales AS web_sales1 ON customer4.c_customer_sk = web_sales1.ws_bill_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 OR d_year = CAST(2001 + 1 AS BIGINT)) AS t23 ON web_sales1.ws_sold_date_sk = t23.d_date_sk
GROUP BY customer4.c_customer_id, customer4.c_first_name, customer4.c_last_name, t23.d_year) AS t27
WHERE SALE_TYPE = 'w' AND YEAR_ = 2001 AND YEAR_TOTAL > 0) AS t28 ON t8.CUSTOMER_ID = t28.CUSTOMER_ID
INNER JOIN (SELECT *
FROM (SELECT customer5.c_customer_id AS CUSTOMER_ID, customer5.c_first_name AS CUSTOMER_FIRST_NAME, customer5.c_last_name AS CUSTOMER_LAST_NAME, t29.d_year AS YEAR_, SUM(store_sales2.ss_net_paid) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer5
INNER JOIN store_sales AS store_sales2 ON customer5.c_customer_sk = store_sales2.ss_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 OR d_year = CAST(2001 + 1 AS BIGINT)) AS t29 ON store_sales2.ss_sold_date_sk = t29.d_date_sk
GROUP BY customer5.c_customer_id, customer5.c_first_name, customer5.c_last_name, t29.d_year
UNION ALL
SELECT customer6.c_customer_id AS CUSTOMER_ID, customer6.c_first_name AS CUSTOMER_FIRST_NAME, customer6.c_last_name AS CUSTOMER_LAST_NAME, t33.d_year AS YEAR_, SUM(web_sales2.ws_net_paid) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer6
INNER JOIN web_sales AS web_sales2 ON customer6.c_customer_sk = web_sales2.ws_bill_customer_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 OR d_year = CAST(2001 + 1 AS BIGINT)) AS t33 ON web_sales2.ws_sold_date_sk = t33.d_date_sk
GROUP BY customer6.c_customer_id, customer6.c_first_name, customer6.c_last_name, t33.d_year) AS t37
WHERE SALE_TYPE = 'w' AND YEAR_ = CAST(2001 + 1 AS BIGINT)) AS t38 ON t8.CUSTOMER_ID = t38.CUSTOMER_ID AND CASE WHEN t28.YEAR_TOTAL > 0 THEN t38.YEAR_TOTAL / t28.YEAR_TOTAL ELSE NULL END > CASE WHEN t8.YEAR_TOTAL > 0 THEN t18.YEAR_TOTAL / t8.YEAR_TOTAL ELSE NULL END
ORDER BY t18.CUSTOMER_ID NULLS FIRST
FETCH NEXT 100 ROWS ONLY