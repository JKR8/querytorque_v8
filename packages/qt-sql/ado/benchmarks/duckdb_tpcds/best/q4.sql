WITH filtered_dates AS (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_year IN (1999, 2000)
),
store_totals AS (
  SELECT
    c.c_customer_id AS customer_id,
    d.d_year AS dyear,
    SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total
  FROM customer c
  JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
  GROUP BY c.c_customer_id, d.d_year
),
catalog_totals AS (
  SELECT
    c.c_customer_id AS customer_id,
    d.d_year AS dyear,
    SUM(((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2) AS year_total
  FROM customer c
  JOIN catalog_sales cs ON c.c_customer_sk = cs.cs_bill_customer_sk
  JOIN filtered_dates d ON cs.cs_sold_date_sk = d.d_date_sk
  GROUP BY c.c_customer_id, d.d_year
),
web_totals AS (
  SELECT
    c.c_customer_id AS customer_id,
    d.d_year AS dyear,
    SUM(((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2) AS year_total
  FROM customer c
  JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
  JOIN filtered_dates d ON ws.ws_sold_date_sk = d.d_date_sk
  GROUP BY c.c_customer_id, d.d_year
),
pivoted AS (
  SELECT
    customer_id,
    MAX(CASE WHEN channel = 's' AND dyear = 1999 THEN year_total END) AS s_1999,
    MAX(CASE WHEN channel = 's' AND dyear = 2000 THEN year_total END) AS s_2000,
    MAX(CASE WHEN channel = 'c' AND dyear = 1999 THEN year_total END) AS c_1999,
    MAX(CASE WHEN channel = 'c' AND dyear = 2000 THEN year_total END) AS c_2000,
    MAX(CASE WHEN channel = 'w' AND dyear = 1999 THEN year_total END) AS w_1999,
    MAX(CASE WHEN channel = 'w' AND dyear = 2000 THEN year_total END) AS w_2000
  FROM (
    SELECT customer_id, dyear, year_total, 's' AS channel FROM store_totals
    UNION ALL
    SELECT customer_id, dyear, year_total, 'c' AS channel FROM catalog_totals
    UNION ALL
    SELECT customer_id, dyear, year_total, 'w' AS channel FROM web_totals
  ) t
  GROUP BY customer_id
)
SELECT
  c.c_customer_id AS customer_id,
  c.c_first_name AS customer_first_name,
  c.c_last_name AS customer_last_name,
  c.c_birth_country AS customer_birth_country
FROM pivoted p
JOIN customer c ON p.customer_id = c.c_customer_id
WHERE
  p.s_1999 > 0
  AND p.c_1999 > 0
  AND p.w_1999 > 0
  AND p.c_2000 / p.c_1999 > p.s_2000 / p.s_1999
  AND p.c_2000 / p.c_1999 > p.w_2000 / p.w_1999
ORDER BY
  c.c_customer_id,
  c.c_first_name,
  c.c_last_name,
  c.c_birth_country
LIMIT 100
