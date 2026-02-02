SELECT t14.CUSTOMER_ID, t14.CUSTOMER_FIRST_NAME, t14.CUSTOMER_LAST_NAME, t14.CUSTOMER_PREFERRED_CUST_FLAG
FROM (SELECT *
FROM (SELECT customer.c_customer_id AS CUSTOMER_ID, customer.c_first_name AS CUSTOMER_FIRST_NAME, customer.c_last_name AS CUSTOMER_LAST_NAME, customer.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer.c_login AS CUSTOMER_LOGIN, customer.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim.d_year AS DYEAR, SUM(store_sales.ss_ext_list_price - store_sales.ss_ext_discount_amt) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer
INNER JOIN store_sales ON customer.c_customer_sk = store_sales.ss_customer_sk
INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
GROUP BY customer.c_customer_id, customer.c_first_name, customer.c_last_name, customer.c_preferred_cust_flag, customer.c_birth_country, customer.c_login, customer.c_email_address, date_dim.d_year
UNION ALL
SELECT customer0.c_customer_id AS CUSTOMER_ID, customer0.c_first_name AS CUSTOMER_FIRST_NAME, customer0.c_last_name AS CUSTOMER_LAST_NAME, customer0.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer0.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer0.c_login AS CUSTOMER_LOGIN, customer0.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim0.d_year AS DYEAR, SUM(web_sales.ws_ext_list_price - web_sales.ws_ext_discount_amt) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer0
INNER JOIN web_sales ON customer0.c_customer_sk = web_sales.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim0 ON web_sales.ws_sold_date_sk = date_dim0.d_date_sk
GROUP BY customer0.c_customer_id, customer0.c_first_name, customer0.c_last_name, customer0.c_preferred_cust_flag, customer0.c_birth_country, customer0.c_login, customer0.c_email_address, date_dim0.d_year) AS t5
WHERE SALE_TYPE = 's' AND DYEAR = 2001 AND YEAR_TOTAL > 0) AS t6
INNER JOIN (SELECT *
FROM (SELECT customer1.c_customer_id AS CUSTOMER_ID, customer1.c_first_name AS CUSTOMER_FIRST_NAME, customer1.c_last_name AS CUSTOMER_LAST_NAME, customer1.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer1.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer1.c_login AS CUSTOMER_LOGIN, customer1.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim1.d_year AS DYEAR, SUM(store_sales0.ss_ext_list_price - store_sales0.ss_ext_discount_amt) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer1
INNER JOIN store_sales AS store_sales0 ON customer1.c_customer_sk = store_sales0.ss_customer_sk
INNER JOIN date_dim AS date_dim1 ON store_sales0.ss_sold_date_sk = date_dim1.d_date_sk
GROUP BY customer1.c_customer_id, customer1.c_first_name, customer1.c_last_name, customer1.c_preferred_cust_flag, customer1.c_birth_country, customer1.c_login, customer1.c_email_address, date_dim1.d_year
UNION ALL
SELECT customer2.c_customer_id AS CUSTOMER_ID, customer2.c_first_name AS CUSTOMER_FIRST_NAME, customer2.c_last_name AS CUSTOMER_LAST_NAME, customer2.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer2.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer2.c_login AS CUSTOMER_LOGIN, customer2.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim2.d_year AS DYEAR, SUM(web_sales0.ws_ext_list_price - web_sales0.ws_ext_discount_amt) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer2
INNER JOIN web_sales AS web_sales0 ON customer2.c_customer_sk = web_sales0.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim2 ON web_sales0.ws_sold_date_sk = date_dim2.d_date_sk
GROUP BY customer2.c_customer_id, customer2.c_first_name, customer2.c_last_name, customer2.c_preferred_cust_flag, customer2.c_birth_country, customer2.c_login, customer2.c_email_address, date_dim2.d_year) AS t13
WHERE SALE_TYPE = 's' AND DYEAR = CAST(2001 + 1 AS BIGINT)) AS t14 ON t6.CUSTOMER_ID = t14.CUSTOMER_ID
INNER JOIN (SELECT *
FROM (SELECT customer3.c_customer_id AS CUSTOMER_ID, customer3.c_first_name AS CUSTOMER_FIRST_NAME, customer3.c_last_name AS CUSTOMER_LAST_NAME, customer3.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer3.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer3.c_login AS CUSTOMER_LOGIN, customer3.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim3.d_year AS DYEAR, SUM(store_sales1.ss_ext_list_price - store_sales1.ss_ext_discount_amt) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer3
INNER JOIN store_sales AS store_sales1 ON customer3.c_customer_sk = store_sales1.ss_customer_sk
INNER JOIN date_dim AS date_dim3 ON store_sales1.ss_sold_date_sk = date_dim3.d_date_sk
GROUP BY customer3.c_customer_id, customer3.c_first_name, customer3.c_last_name, customer3.c_preferred_cust_flag, customer3.c_birth_country, customer3.c_login, customer3.c_email_address, date_dim3.d_year
UNION ALL
SELECT customer4.c_customer_id AS CUSTOMER_ID, customer4.c_first_name AS CUSTOMER_FIRST_NAME, customer4.c_last_name AS CUSTOMER_LAST_NAME, customer4.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer4.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer4.c_login AS CUSTOMER_LOGIN, customer4.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim4.d_year AS DYEAR, SUM(web_sales1.ws_ext_list_price - web_sales1.ws_ext_discount_amt) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer4
INNER JOIN web_sales AS web_sales1 ON customer4.c_customer_sk = web_sales1.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim4 ON web_sales1.ws_sold_date_sk = date_dim4.d_date_sk
GROUP BY customer4.c_customer_id, customer4.c_first_name, customer4.c_last_name, customer4.c_preferred_cust_flag, customer4.c_birth_country, customer4.c_login, customer4.c_email_address, date_dim4.d_year) AS t21
WHERE SALE_TYPE = 'w' AND DYEAR = 2001 AND YEAR_TOTAL > 0) AS t22 ON t6.CUSTOMER_ID = t22.CUSTOMER_ID
INNER JOIN (SELECT *
FROM (SELECT customer5.c_customer_id AS CUSTOMER_ID, customer5.c_first_name AS CUSTOMER_FIRST_NAME, customer5.c_last_name AS CUSTOMER_LAST_NAME, customer5.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer5.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer5.c_login AS CUSTOMER_LOGIN, customer5.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim5.d_year AS DYEAR, SUM(store_sales2.ss_ext_list_price - store_sales2.ss_ext_discount_amt) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer5
INNER JOIN store_sales AS store_sales2 ON customer5.c_customer_sk = store_sales2.ss_customer_sk
INNER JOIN date_dim AS date_dim5 ON store_sales2.ss_sold_date_sk = date_dim5.d_date_sk
GROUP BY customer5.c_customer_id, customer5.c_first_name, customer5.c_last_name, customer5.c_preferred_cust_flag, customer5.c_birth_country, customer5.c_login, customer5.c_email_address, date_dim5.d_year
UNION ALL
SELECT customer6.c_customer_id AS CUSTOMER_ID, customer6.c_first_name AS CUSTOMER_FIRST_NAME, customer6.c_last_name AS CUSTOMER_LAST_NAME, customer6.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer6.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer6.c_login AS CUSTOMER_LOGIN, customer6.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim6.d_year AS DYEAR, SUM(web_sales2.ws_ext_list_price - web_sales2.ws_ext_discount_amt) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer6
INNER JOIN web_sales AS web_sales2 ON customer6.c_customer_sk = web_sales2.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim6 ON web_sales2.ws_sold_date_sk = date_dim6.d_date_sk
GROUP BY customer6.c_customer_id, customer6.c_first_name, customer6.c_last_name, customer6.c_preferred_cust_flag, customer6.c_birth_country, customer6.c_login, customer6.c_email_address, date_dim6.d_year) AS t29
WHERE SALE_TYPE = 'w' AND DYEAR = CAST(2001 + 1 AS BIGINT)) AS t30 ON t6.CUSTOMER_ID = t30.CUSTOMER_ID AND CASE WHEN t22.YEAR_TOTAL > 0 THEN t30.YEAR_TOTAL * 1.0000 / t22.YEAR_TOTAL ELSE 0.0 END > CASE WHEN t6.YEAR_TOTAL > 0 THEN t14.YEAR_TOTAL * 1.0000 / t6.YEAR_TOTAL ELSE 0.0 END
ORDER BY t14.CUSTOMER_ID NULLS FIRST, t14.CUSTOMER_FIRST_NAME NULLS FIRST, t14.CUSTOMER_LAST_NAME NULLS FIRST, t14.CUSTOMER_PREFERRED_CUST_FLAG NULLS FIRST
FETCH NEXT 100 ROWS ONLY