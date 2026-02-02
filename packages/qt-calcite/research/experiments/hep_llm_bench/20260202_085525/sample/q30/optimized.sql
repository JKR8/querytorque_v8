SELECT customer.c_customer_id AS C_CUSTOMER_ID, customer.c_salutation AS C_SALUTATION, customer.c_first_name AS C_FIRST_NAME, customer.c_last_name AS C_LAST_NAME, customer.c_preferred_cust_flag AS C_PREFERRED_CUST_FLAG, customer.c_birth_day AS C_BIRTH_DAY, customer.c_birth_month AS C_BIRTH_MONTH, customer.c_birth_year AS C_BIRTH_YEAR, customer.c_birth_country AS C_BIRTH_COUNTRY, customer.c_login AS C_LOGIN, customer.c_email_address AS C_EMAIL_ADDRESS, customer.c_last_review_date_sk AS C_LAST_REVIEW_DATE_SK, t12.CTR_TOTAL_RETURN
FROM (SELECT $cor0.wr_returning_customer_sk AS CTR_CUSTOMER_SK, $cor0.ca_state AS CTR_STATE, $cor0.CTR_TOTAL_RETURN
FROM (SELECT web_returns.wr_returning_customer_sk, customer_address.ca_state, SUM(web_returns.wr_return_amt) AS CTR_TOTAL_RETURN
FROM web_returns
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2002) AS t ON web_returns.wr_returned_date_sk = t.d_date_sk
INNER JOIN customer_address ON web_returns.wr_returning_addr_sk = customer_address.ca_address_sk
GROUP BY web_returns.wr_returning_customer_sk, customer_address.ca_state) AS $cor0,
LATERAL (SELECT AVG(CTR_TOTAL_RETURN) * 1.2 AS EXPR$0
FROM (SELECT web_returns0.wr_returning_customer_sk AS CTR_CUSTOMER_SK, customer_address0.ca_state AS CTR_STATE, SUM(web_returns0.wr_return_amt) AS CTR_TOTAL_RETURN
FROM web_returns AS web_returns0,
date_dim AS date_dim0,
customer_address AS customer_address0
WHERE web_returns0.wr_returned_date_sk = date_dim0.d_date_sk AND date_dim0.d_year = 2002 AND web_returns0.wr_returning_addr_sk = customer_address0.ca_address_sk
GROUP BY web_returns0.wr_returning_customer_sk, customer_address0.ca_state) AS t5
WHERE $cor0.ca_state = t5.CTR_STATE) AS t9
WHERE $cor0.CTR_TOTAL_RETURN > $cor0.EXPR$0) AS t12
CROSS JOIN (SELECT *
FROM customer_address
WHERE ca_state = 'GA') AS t13
INNER JOIN customer ON t13.ca_address_sk = customer.c_current_addr_sk AND t12.CTR_CUSTOMER_SK = customer.c_customer_sk
ORDER BY customer.c_customer_id NULLS FIRST, customer.c_salutation NULLS FIRST, customer.c_first_name NULLS FIRST, customer.c_last_name NULLS FIRST, customer.c_preferred_cust_flag NULLS FIRST, customer.c_birth_day NULLS FIRST, customer.c_birth_month NULLS FIRST, customer.c_birth_year NULLS FIRST, customer.c_birth_country NULLS FIRST, customer.c_login NULLS FIRST, customer.c_email_address NULLS FIRST, customer.c_last_review_date_sk NULLS FIRST, t12.CTR_TOTAL_RETURN NULLS FIRST
FETCH NEXT 100 ROWS ONLY