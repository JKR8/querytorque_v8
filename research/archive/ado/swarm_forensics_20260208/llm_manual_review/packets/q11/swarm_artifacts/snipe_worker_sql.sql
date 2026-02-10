WITH store_2001 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year = 2001
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
),
store_2002 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year = 2002
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
),
web_2001 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year = 2001
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
),
web_2002 AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    c_birth_country AS customer_birth_country,
    SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year = 2002
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    c_birth_country
)
SELECT
  s2.customer_id,
  s2.customer_first_name,
  s2.customer_last_name,
  s2.customer_birth_country
FROM store_2001 s1
JOIN store_2002 s2 ON s1.customer_id = s2.customer_id
JOIN web_2001 w1 ON s1.customer_id = w1.customer_id
JOIN web_2002 w2 ON s1.customer_id = w2.customer_id
WHERE
  s1.year_total > 0
  AND w1.year_total > 0
  AND CASE
    WHEN w1.year_total > 0
    THEN w2.year_total / w1.year_total
    ELSE 0.0
  END > CASE
    WHEN s1.year_total > 0
    THEN s2.year_total / s1.year_total
    ELSE 0.0
  END
ORDER BY
  s2.customer_id,
  s2.customer_first_name,
  s2.customer_last_name,
  s2.customer_birth_country
LIMIT 100