SELECT t22.CUSTOMER_ID, t22.CUSTOMER_FIRST_NAME, t22.CUSTOMER_LAST_NAME, t22.CUSTOMER_PREFERRED_CUST_FLAG
FROM (SELECT *
FROM (SELECT *
FROM (SELECT customer.c_customer_id AS CUSTOMER_ID, customer.c_first_name AS CUSTOMER_FIRST_NAME, customer.c_last_name AS CUSTOMER_LAST_NAME, customer.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer.c_login AS CUSTOMER_LOGIN, customer.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim.d_year AS DYEAR, SUM((store_sales.ss_ext_list_price - store_sales.ss_ext_wholesale_cost - store_sales.ss_ext_discount_amt + store_sales.ss_ext_sales_price) / 2) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer
INNER JOIN store_sales ON customer.c_customer_sk = store_sales.ss_customer_sk
INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
GROUP BY customer.c_customer_id, customer.c_first_name, customer.c_last_name, customer.c_preferred_cust_flag, customer.c_birth_country, customer.c_login, customer.c_email_address, date_dim.d_year
UNION ALL
SELECT customer0.c_customer_id AS CUSTOMER_ID, customer0.c_first_name AS CUSTOMER_FIRST_NAME, customer0.c_last_name AS CUSTOMER_LAST_NAME, customer0.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer0.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer0.c_login AS CUSTOMER_LOGIN, customer0.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim0.d_year AS DYEAR, SUM((catalog_sales.cs_ext_list_price - catalog_sales.cs_ext_wholesale_cost - catalog_sales.cs_ext_discount_amt + catalog_sales.cs_ext_sales_price) / 2) AS YEAR_TOTAL, 'c' AS SALE_TYPE
FROM customer AS customer0
INNER JOIN catalog_sales ON customer0.c_customer_sk = catalog_sales.cs_bill_customer_sk
INNER JOIN date_dim AS date_dim0 ON catalog_sales.cs_sold_date_sk = date_dim0.d_date_sk
GROUP BY customer0.c_customer_id, customer0.c_first_name, customer0.c_last_name, customer0.c_preferred_cust_flag, customer0.c_birth_country, customer0.c_login, customer0.c_email_address, date_dim0.d_year)
UNION ALL
SELECT customer1.c_customer_id AS CUSTOMER_ID, customer1.c_first_name AS CUSTOMER_FIRST_NAME, customer1.c_last_name AS CUSTOMER_LAST_NAME, customer1.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer1.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer1.c_login AS CUSTOMER_LOGIN, customer1.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim1.d_year AS DYEAR, SUM((web_sales.ws_ext_list_price - web_sales.ws_ext_wholesale_cost - web_sales.ws_ext_discount_amt + web_sales.ws_ext_sales_price) / 2) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer1
INNER JOIN web_sales ON customer1.c_customer_sk = web_sales.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim1 ON web_sales.ws_sold_date_sk = date_dim1.d_date_sk
GROUP BY customer1.c_customer_id, customer1.c_first_name, customer1.c_last_name, customer1.c_preferred_cust_flag, customer1.c_birth_country, customer1.c_login, customer1.c_email_address, date_dim1.d_year) AS t9
WHERE SALE_TYPE = 's' AND DYEAR = 2001 AND YEAR_TOTAL > 0) AS t10
INNER JOIN (SELECT *
FROM (SELECT *
FROM (SELECT customer2.c_customer_id AS CUSTOMER_ID, customer2.c_first_name AS CUSTOMER_FIRST_NAME, customer2.c_last_name AS CUSTOMER_LAST_NAME, customer2.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer2.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer2.c_login AS CUSTOMER_LOGIN, customer2.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim2.d_year AS DYEAR, SUM((store_sales0.ss_ext_list_price - store_sales0.ss_ext_wholesale_cost - store_sales0.ss_ext_discount_amt + store_sales0.ss_ext_sales_price) / 2) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer2
INNER JOIN store_sales AS store_sales0 ON customer2.c_customer_sk = store_sales0.ss_customer_sk
INNER JOIN date_dim AS date_dim2 ON store_sales0.ss_sold_date_sk = date_dim2.d_date_sk
GROUP BY customer2.c_customer_id, customer2.c_first_name, customer2.c_last_name, customer2.c_preferred_cust_flag, customer2.c_birth_country, customer2.c_login, customer2.c_email_address, date_dim2.d_year
UNION ALL
SELECT customer3.c_customer_id AS CUSTOMER_ID, customer3.c_first_name AS CUSTOMER_FIRST_NAME, customer3.c_last_name AS CUSTOMER_LAST_NAME, customer3.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer3.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer3.c_login AS CUSTOMER_LOGIN, customer3.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim3.d_year AS DYEAR, SUM((catalog_sales0.cs_ext_list_price - catalog_sales0.cs_ext_wholesale_cost - catalog_sales0.cs_ext_discount_amt + catalog_sales0.cs_ext_sales_price) / 2) AS YEAR_TOTAL, 'c' AS SALE_TYPE
FROM customer AS customer3
INNER JOIN catalog_sales AS catalog_sales0 ON customer3.c_customer_sk = catalog_sales0.cs_bill_customer_sk
INNER JOIN date_dim AS date_dim3 ON catalog_sales0.cs_sold_date_sk = date_dim3.d_date_sk
GROUP BY customer3.c_customer_id, customer3.c_first_name, customer3.c_last_name, customer3.c_preferred_cust_flag, customer3.c_birth_country, customer3.c_login, customer3.c_email_address, date_dim3.d_year)
UNION ALL
SELECT customer4.c_customer_id AS CUSTOMER_ID, customer4.c_first_name AS CUSTOMER_FIRST_NAME, customer4.c_last_name AS CUSTOMER_LAST_NAME, customer4.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer4.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer4.c_login AS CUSTOMER_LOGIN, customer4.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim4.d_year AS DYEAR, SUM((web_sales0.ws_ext_list_price - web_sales0.ws_ext_wholesale_cost - web_sales0.ws_ext_discount_amt + web_sales0.ws_ext_sales_price) / 2) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer4
INNER JOIN web_sales AS web_sales0 ON customer4.c_customer_sk = web_sales0.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim4 ON web_sales0.ws_sold_date_sk = date_dim4.d_date_sk
GROUP BY customer4.c_customer_id, customer4.c_first_name, customer4.c_last_name, customer4.c_preferred_cust_flag, customer4.c_birth_country, customer4.c_login, customer4.c_email_address, date_dim4.d_year) AS t21
WHERE SALE_TYPE = 's' AND DYEAR = CAST(2001 + 1 AS BIGINT)) AS t22 ON t10.CUSTOMER_ID = t22.CUSTOMER_ID
INNER JOIN (SELECT *
FROM (SELECT *
FROM (SELECT customer5.c_customer_id AS CUSTOMER_ID, customer5.c_first_name AS CUSTOMER_FIRST_NAME, customer5.c_last_name AS CUSTOMER_LAST_NAME, customer5.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer5.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer5.c_login AS CUSTOMER_LOGIN, customer5.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim5.d_year AS DYEAR, SUM((store_sales1.ss_ext_list_price - store_sales1.ss_ext_wholesale_cost - store_sales1.ss_ext_discount_amt + store_sales1.ss_ext_sales_price) / 2) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer5
INNER JOIN store_sales AS store_sales1 ON customer5.c_customer_sk = store_sales1.ss_customer_sk
INNER JOIN date_dim AS date_dim5 ON store_sales1.ss_sold_date_sk = date_dim5.d_date_sk
GROUP BY customer5.c_customer_id, customer5.c_first_name, customer5.c_last_name, customer5.c_preferred_cust_flag, customer5.c_birth_country, customer5.c_login, customer5.c_email_address, date_dim5.d_year
UNION ALL
SELECT customer6.c_customer_id AS CUSTOMER_ID, customer6.c_first_name AS CUSTOMER_FIRST_NAME, customer6.c_last_name AS CUSTOMER_LAST_NAME, customer6.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer6.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer6.c_login AS CUSTOMER_LOGIN, customer6.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim6.d_year AS DYEAR, SUM((catalog_sales1.cs_ext_list_price - catalog_sales1.cs_ext_wholesale_cost - catalog_sales1.cs_ext_discount_amt + catalog_sales1.cs_ext_sales_price) / 2) AS YEAR_TOTAL, 'c' AS SALE_TYPE
FROM customer AS customer6
INNER JOIN catalog_sales AS catalog_sales1 ON customer6.c_customer_sk = catalog_sales1.cs_bill_customer_sk
INNER JOIN date_dim AS date_dim6 ON catalog_sales1.cs_sold_date_sk = date_dim6.d_date_sk
GROUP BY customer6.c_customer_id, customer6.c_first_name, customer6.c_last_name, customer6.c_preferred_cust_flag, customer6.c_birth_country, customer6.c_login, customer6.c_email_address, date_dim6.d_year)
UNION ALL
SELECT customer7.c_customer_id AS CUSTOMER_ID, customer7.c_first_name AS CUSTOMER_FIRST_NAME, customer7.c_last_name AS CUSTOMER_LAST_NAME, customer7.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer7.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer7.c_login AS CUSTOMER_LOGIN, customer7.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim7.d_year AS DYEAR, SUM((web_sales1.ws_ext_list_price - web_sales1.ws_ext_wholesale_cost - web_sales1.ws_ext_discount_amt + web_sales1.ws_ext_sales_price) / 2) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer7
INNER JOIN web_sales AS web_sales1 ON customer7.c_customer_sk = web_sales1.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim7 ON web_sales1.ws_sold_date_sk = date_dim7.d_date_sk
GROUP BY customer7.c_customer_id, customer7.c_first_name, customer7.c_last_name, customer7.c_preferred_cust_flag, customer7.c_birth_country, customer7.c_login, customer7.c_email_address, date_dim7.d_year) AS t33
WHERE SALE_TYPE = 'c' AND DYEAR = 2001 AND YEAR_TOTAL > 0) AS t34 ON t10.CUSTOMER_ID = t34.CUSTOMER_ID
INNER JOIN (SELECT *
FROM (SELECT *
FROM (SELECT customer8.c_customer_id AS CUSTOMER_ID, customer8.c_first_name AS CUSTOMER_FIRST_NAME, customer8.c_last_name AS CUSTOMER_LAST_NAME, customer8.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer8.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer8.c_login AS CUSTOMER_LOGIN, customer8.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim8.d_year AS DYEAR, SUM((store_sales2.ss_ext_list_price - store_sales2.ss_ext_wholesale_cost - store_sales2.ss_ext_discount_amt + store_sales2.ss_ext_sales_price) / 2) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer8
INNER JOIN store_sales AS store_sales2 ON customer8.c_customer_sk = store_sales2.ss_customer_sk
INNER JOIN date_dim AS date_dim8 ON store_sales2.ss_sold_date_sk = date_dim8.d_date_sk
GROUP BY customer8.c_customer_id, customer8.c_first_name, customer8.c_last_name, customer8.c_preferred_cust_flag, customer8.c_birth_country, customer8.c_login, customer8.c_email_address, date_dim8.d_year
UNION ALL
SELECT customer9.c_customer_id AS CUSTOMER_ID, customer9.c_first_name AS CUSTOMER_FIRST_NAME, customer9.c_last_name AS CUSTOMER_LAST_NAME, customer9.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer9.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer9.c_login AS CUSTOMER_LOGIN, customer9.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim9.d_year AS DYEAR, SUM((catalog_sales2.cs_ext_list_price - catalog_sales2.cs_ext_wholesale_cost - catalog_sales2.cs_ext_discount_amt + catalog_sales2.cs_ext_sales_price) / 2) AS YEAR_TOTAL, 'c' AS SALE_TYPE
FROM customer AS customer9
INNER JOIN catalog_sales AS catalog_sales2 ON customer9.c_customer_sk = catalog_sales2.cs_bill_customer_sk
INNER JOIN date_dim AS date_dim9 ON catalog_sales2.cs_sold_date_sk = date_dim9.d_date_sk
GROUP BY customer9.c_customer_id, customer9.c_first_name, customer9.c_last_name, customer9.c_preferred_cust_flag, customer9.c_birth_country, customer9.c_login, customer9.c_email_address, date_dim9.d_year)
UNION ALL
SELECT customer10.c_customer_id AS CUSTOMER_ID, customer10.c_first_name AS CUSTOMER_FIRST_NAME, customer10.c_last_name AS CUSTOMER_LAST_NAME, customer10.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer10.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer10.c_login AS CUSTOMER_LOGIN, customer10.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim10.d_year AS DYEAR, SUM((web_sales2.ws_ext_list_price - web_sales2.ws_ext_wholesale_cost - web_sales2.ws_ext_discount_amt + web_sales2.ws_ext_sales_price) / 2) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer10
INNER JOIN web_sales AS web_sales2 ON customer10.c_customer_sk = web_sales2.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim10 ON web_sales2.ws_sold_date_sk = date_dim10.d_date_sk
GROUP BY customer10.c_customer_id, customer10.c_first_name, customer10.c_last_name, customer10.c_preferred_cust_flag, customer10.c_birth_country, customer10.c_login, customer10.c_email_address, date_dim10.d_year) AS t45
WHERE SALE_TYPE = 'c' AND DYEAR = CAST(2001 + 1 AS BIGINT)) AS t46 ON t10.CUSTOMER_ID = t46.CUSTOMER_ID AND CASE WHEN t34.YEAR_TOTAL > 0 THEN t46.YEAR_TOTAL / t34.YEAR_TOTAL ELSE NULL END > CASE WHEN t10.YEAR_TOTAL > 0 THEN t22.YEAR_TOTAL / t10.YEAR_TOTAL ELSE NULL END
INNER JOIN (SELECT *
FROM (SELECT *
FROM (SELECT customer11.c_customer_id AS CUSTOMER_ID, customer11.c_first_name AS CUSTOMER_FIRST_NAME, customer11.c_last_name AS CUSTOMER_LAST_NAME, customer11.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer11.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer11.c_login AS CUSTOMER_LOGIN, customer11.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim11.d_year AS DYEAR, SUM((store_sales3.ss_ext_list_price - store_sales3.ss_ext_wholesale_cost - store_sales3.ss_ext_discount_amt + store_sales3.ss_ext_sales_price) / 2) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer11
INNER JOIN store_sales AS store_sales3 ON customer11.c_customer_sk = store_sales3.ss_customer_sk
INNER JOIN date_dim AS date_dim11 ON store_sales3.ss_sold_date_sk = date_dim11.d_date_sk
GROUP BY customer11.c_customer_id, customer11.c_first_name, customer11.c_last_name, customer11.c_preferred_cust_flag, customer11.c_birth_country, customer11.c_login, customer11.c_email_address, date_dim11.d_year
UNION ALL
SELECT customer12.c_customer_id AS CUSTOMER_ID, customer12.c_first_name AS CUSTOMER_FIRST_NAME, customer12.c_last_name AS CUSTOMER_LAST_NAME, customer12.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer12.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer12.c_login AS CUSTOMER_LOGIN, customer12.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim12.d_year AS DYEAR, SUM((catalog_sales3.cs_ext_list_price - catalog_sales3.cs_ext_wholesale_cost - catalog_sales3.cs_ext_discount_amt + catalog_sales3.cs_ext_sales_price) / 2) AS YEAR_TOTAL, 'c' AS SALE_TYPE
FROM customer AS customer12
INNER JOIN catalog_sales AS catalog_sales3 ON customer12.c_customer_sk = catalog_sales3.cs_bill_customer_sk
INNER JOIN date_dim AS date_dim12 ON catalog_sales3.cs_sold_date_sk = date_dim12.d_date_sk
GROUP BY customer12.c_customer_id, customer12.c_first_name, customer12.c_last_name, customer12.c_preferred_cust_flag, customer12.c_birth_country, customer12.c_login, customer12.c_email_address, date_dim12.d_year)
UNION ALL
SELECT customer13.c_customer_id AS CUSTOMER_ID, customer13.c_first_name AS CUSTOMER_FIRST_NAME, customer13.c_last_name AS CUSTOMER_LAST_NAME, customer13.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer13.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer13.c_login AS CUSTOMER_LOGIN, customer13.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim13.d_year AS DYEAR, SUM((web_sales3.ws_ext_list_price - web_sales3.ws_ext_wholesale_cost - web_sales3.ws_ext_discount_amt + web_sales3.ws_ext_sales_price) / 2) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer13
INNER JOIN web_sales AS web_sales3 ON customer13.c_customer_sk = web_sales3.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim13 ON web_sales3.ws_sold_date_sk = date_dim13.d_date_sk
GROUP BY customer13.c_customer_id, customer13.c_first_name, customer13.c_last_name, customer13.c_preferred_cust_flag, customer13.c_birth_country, customer13.c_login, customer13.c_email_address, date_dim13.d_year) AS t57
WHERE SALE_TYPE = 'w' AND DYEAR = 2001 AND YEAR_TOTAL > 0) AS t58 ON t10.CUSTOMER_ID = t58.CUSTOMER_ID
INNER JOIN (SELECT *
FROM (SELECT *
FROM (SELECT customer14.c_customer_id AS CUSTOMER_ID, customer14.c_first_name AS CUSTOMER_FIRST_NAME, customer14.c_last_name AS CUSTOMER_LAST_NAME, customer14.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer14.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer14.c_login AS CUSTOMER_LOGIN, customer14.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim14.d_year AS DYEAR, SUM((store_sales4.ss_ext_list_price - store_sales4.ss_ext_wholesale_cost - store_sales4.ss_ext_discount_amt + store_sales4.ss_ext_sales_price) / 2) AS YEAR_TOTAL, 's' AS SALE_TYPE
FROM customer AS customer14
INNER JOIN store_sales AS store_sales4 ON customer14.c_customer_sk = store_sales4.ss_customer_sk
INNER JOIN date_dim AS date_dim14 ON store_sales4.ss_sold_date_sk = date_dim14.d_date_sk
GROUP BY customer14.c_customer_id, customer14.c_first_name, customer14.c_last_name, customer14.c_preferred_cust_flag, customer14.c_birth_country, customer14.c_login, customer14.c_email_address, date_dim14.d_year
UNION ALL
SELECT customer15.c_customer_id AS CUSTOMER_ID, customer15.c_first_name AS CUSTOMER_FIRST_NAME, customer15.c_last_name AS CUSTOMER_LAST_NAME, customer15.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer15.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer15.c_login AS CUSTOMER_LOGIN, customer15.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim15.d_year AS DYEAR, SUM((catalog_sales4.cs_ext_list_price - catalog_sales4.cs_ext_wholesale_cost - catalog_sales4.cs_ext_discount_amt + catalog_sales4.cs_ext_sales_price) / 2) AS YEAR_TOTAL, 'c' AS SALE_TYPE
FROM customer AS customer15
INNER JOIN catalog_sales AS catalog_sales4 ON customer15.c_customer_sk = catalog_sales4.cs_bill_customer_sk
INNER JOIN date_dim AS date_dim15 ON catalog_sales4.cs_sold_date_sk = date_dim15.d_date_sk
GROUP BY customer15.c_customer_id, customer15.c_first_name, customer15.c_last_name, customer15.c_preferred_cust_flag, customer15.c_birth_country, customer15.c_login, customer15.c_email_address, date_dim15.d_year)
UNION ALL
SELECT customer16.c_customer_id AS CUSTOMER_ID, customer16.c_first_name AS CUSTOMER_FIRST_NAME, customer16.c_last_name AS CUSTOMER_LAST_NAME, customer16.c_preferred_cust_flag AS CUSTOMER_PREFERRED_CUST_FLAG, customer16.c_birth_country AS CUSTOMER_BIRTH_COUNTRY, customer16.c_login AS CUSTOMER_LOGIN, customer16.c_email_address AS CUSTOMER_EMAIL_ADDRESS, date_dim16.d_year AS DYEAR, SUM((web_sales4.ws_ext_list_price - web_sales4.ws_ext_wholesale_cost - web_sales4.ws_ext_discount_amt + web_sales4.ws_ext_sales_price) / 2) AS YEAR_TOTAL, 'w' AS SALE_TYPE
FROM customer AS customer16
INNER JOIN web_sales AS web_sales4 ON customer16.c_customer_sk = web_sales4.ws_bill_customer_sk
INNER JOIN date_dim AS date_dim16 ON web_sales4.ws_sold_date_sk = date_dim16.d_date_sk
GROUP BY customer16.c_customer_id, customer16.c_first_name, customer16.c_last_name, customer16.c_preferred_cust_flag, customer16.c_birth_country, customer16.c_login, customer16.c_email_address, date_dim16.d_year) AS t69
WHERE SALE_TYPE = 'w' AND DYEAR = CAST(2001 + 1 AS BIGINT)) AS t70 ON t10.CUSTOMER_ID = t70.CUSTOMER_ID AND CASE WHEN t34.YEAR_TOTAL > 0 THEN t46.YEAR_TOTAL / t34.YEAR_TOTAL ELSE NULL END > CASE WHEN t58.YEAR_TOTAL > 0 THEN t70.YEAR_TOTAL / t58.YEAR_TOTAL ELSE NULL END
ORDER BY t22.CUSTOMER_ID NULLS FIRST, t22.CUSTOMER_FIRST_NAME NULLS FIRST, t22.CUSTOMER_LAST_NAME NULLS FIRST, t22.CUSTOMER_PREFERRED_CUST_FLAG NULLS FIRST
FETCH NEXT 100 ROWS ONLY