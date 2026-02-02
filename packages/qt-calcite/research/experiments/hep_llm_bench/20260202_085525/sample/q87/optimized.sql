SELECT COUNT(*)
FROM (SELECT customer.c_last_name AS C_LAST_NAME, customer.c_first_name AS C_FIRST_NAME, t.d_date AS D_DATE
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
GROUP BY customer.c_last_name, customer.c_first_name, t.d_date
EXCEPT
SELECT customer0.c_last_name AS C_LAST_NAME, customer0.c_first_name AS C_FIRST_NAME, t2.d_date AS D_DATE
FROM catalog_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t2 ON catalog_sales.cs_sold_date_sk = t2.d_date_sk
INNER JOIN customer AS customer0 ON catalog_sales.cs_bill_customer_sk = customer0.c_customer_sk
GROUP BY customer0.c_last_name, customer0.c_first_name, t2.d_date
EXCEPT
SELECT customer1.c_last_name AS C_LAST_NAME, customer1.c_first_name AS C_FIRST_NAME, t5.d_date AS D_DATE
FROM web_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t5 ON web_sales.ws_sold_date_sk = t5.d_date_sk
INNER JOIN customer AS customer1 ON web_sales.ws_bill_customer_sk = customer1.c_customer_sk
GROUP BY customer1.c_last_name, customer1.c_first_name, t5.d_date) AS t8