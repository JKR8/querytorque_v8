SELECT COUNT(*)
FROM (SELECT C_LAST_NAME, C_FIRST_NAME, D_DATE
FROM (SELECT C_LAST_NAME, C_FIRST_NAME, D_DATE, COUNT(*) AS $f3
FROM (SELECT C_LAST_NAME, C_FIRST_NAME, D_DATE
FROM (SELECT t1.C_LAST_NAME, t1.C_FIRST_NAME, t1.D_DATE, COUNT(*) AS $f3
FROM (SELECT customer.c_last_name AS C_LAST_NAME, customer.c_first_name AS C_FIRST_NAME, t.d_date AS D_DATE
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
GROUP BY customer.c_last_name, customer.c_first_name, t.d_date) AS t1
GROUP BY t1.C_LAST_NAME, t1.C_FIRST_NAME, t1.D_DATE
UNION ALL
SELECT t5.C_LAST_NAME, t5.C_FIRST_NAME, t5.D_DATE, COUNT(*) AS $f3
FROM (SELECT customer0.c_last_name AS C_LAST_NAME, customer0.c_first_name AS C_FIRST_NAME, t3.d_date AS D_DATE
FROM catalog_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t3 ON catalog_sales.cs_sold_date_sk = t3.d_date_sk
INNER JOIN customer AS customer0 ON catalog_sales.cs_bill_customer_sk = customer0.c_customer_sk
GROUP BY customer0.c_last_name, customer0.c_first_name, t3.d_date) AS t5
GROUP BY t5.C_LAST_NAME, t5.C_FIRST_NAME, t5.D_DATE) AS t7
GROUP BY C_LAST_NAME, C_FIRST_NAME, D_DATE
HAVING COUNT(*) = 2) AS t10
GROUP BY C_LAST_NAME, C_FIRST_NAME, D_DATE
UNION ALL
SELECT t14.C_LAST_NAME, t14.C_FIRST_NAME, t14.D_DATE, COUNT(*) AS $f3
FROM (SELECT customer1.c_last_name AS C_LAST_NAME, customer1.c_first_name AS C_FIRST_NAME, t12.d_date AS D_DATE
FROM web_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t12 ON web_sales.ws_sold_date_sk = t12.d_date_sk
INNER JOIN customer AS customer1 ON web_sales.ws_bill_customer_sk = customer1.c_customer_sk
GROUP BY customer1.c_last_name, customer1.c_first_name, t12.d_date) AS t14
GROUP BY t14.C_LAST_NAME, t14.C_FIRST_NAME, t14.D_DATE) AS t16
GROUP BY C_LAST_NAME, C_FIRST_NAME, D_DATE
HAVING COUNT(*) = 2) AS t19
FETCH NEXT 100 ROWS ONLY