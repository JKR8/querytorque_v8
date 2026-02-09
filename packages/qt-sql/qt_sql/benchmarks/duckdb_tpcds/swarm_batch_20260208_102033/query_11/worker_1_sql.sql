WITH store_sales_agg AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_preferred_cust_flag AS customer_preferred_cust_flag,
    c_birth_country AS customer_birth_country,
    c_login AS customer_login,
    c_email_address AS customer_email_address,
    d_year AS dyear,
    SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total,
    's' AS sale_type
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year IN (2001, 2001 + 1)
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address,
    d_year
), web_sales_agg AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_preferred_cust_flag AS customer_preferred_cust_flag,
    c_birth_country AS customer_birth_country,
    c_login AS customer_login,
    c_email_address AS customer_email_address,
    d_year AS dyear,
    SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total,
    'w' AS sale_type
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year IN (2001, 2001 + 1)
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_country,
    c_login,
    c_email_address,
    d_year
)
SELECT
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name,
  t_s_secyear.customer_birth_country
FROM store_sales_agg AS t_s_firstyear
JOIN store_sales_agg AS t_s_secyear 
  ON t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_secyear.dyear = 2001 + 1
  AND t_s_firstyear.dyear = 2001
JOIN web_sales_agg AS t_w_firstyear 
  ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_w_firstyear.dyear = 2001
JOIN web_sales_agg AS t_w_secyear 
  ON t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_w_secyear.dyear = 2001 + 1
WHERE
  t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
    WHEN t_w_firstyear.year_total > 0
    THEN t_w_secyear.year_total / t_w_firstyear.year_total
    ELSE 0.0
  END > CASE
    WHEN t_s_firstyear.year_total > 0
    THEN t_s_secyear.year_total / t_s_firstyear.year_total
    ELSE 0.0
  END
ORDER BY
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name,
  t_s_secyear.customer_birth_country
LIMIT 100