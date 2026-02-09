WITH year_1999_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1999
),
year_2000_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000
),
store_sales_1999 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total,
    's' AS sale_type
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN year_1999_dates ON ss_sold_date_sk = d_date_sk
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
),
store_sales_2000 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total,
    's' AS sale_type
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN year_2000_dates ON ss_sold_date_sk = d_date_sk
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
),
catalog_sales_1999 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2) AS year_total,
    'c' AS sale_type
  FROM customer
  JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk
  JOIN year_1999_dates ON cs_sold_date_sk = d_date_sk
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
),
catalog_sales_2000 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2) AS year_total,
    'c' AS sale_type
  FROM customer
  JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk
  JOIN year_2000_dates ON cs_sold_date_sk = d_date_sk
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
),
web_sales_1999 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2) AS year_total,
    'w' AS sale_type
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN year_1999_dates ON ws_sold_date_sk = d_date_sk
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
),
web_sales_2000 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2) AS year_total,
    'w' AS sale_type
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN year_2000_dates ON ws_sold_date_sk = d_date_sk
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
)
SELECT
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name,
  t_s_secyear.customer_birth_country
FROM store_sales_1999 AS t_s_firstyear
JOIN store_sales_2000 AS t_s_secyear ON t_s_firstyear.customer_id = t_s_secyear.customer_id
JOIN catalog_sales_1999 AS t_c_firstyear ON t_s_firstyear.customer_id = t_c_firstyear.customer_id
JOIN catalog_sales_2000 AS t_c_secyear ON t_s_firstyear.customer_id = t_c_secyear.customer_id
JOIN web_sales_1999 AS t_w_firstyear ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
JOIN web_sales_2000 AS t_w_secyear ON t_s_firstyear.customer_id = t_w_secyear.customer_id
WHERE
  t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND t_c_secyear.year_total / t_c_firstyear.year_total > t_s_secyear.year_total / t_s_firstyear.year_total
  AND t_c_secyear.year_total / t_c_firstyear.year_total > t_w_secyear.year_total / t_w_firstyear.year_total
ORDER BY
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name,
  t_s_secyear.customer_birth_country
LIMIT 100