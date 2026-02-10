WITH store_aggregates AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    d_year AS year,
    STDDEV_SAMP(ss_net_paid) AS year_total
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year IN (1999, 2000)
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    d_year
),
web_aggregates AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    d_year AS year,
    STDDEV_SAMP(ws_net_paid) AS year_total
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year IN (1999, 2000)
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    d_year
)
SELECT
  s1.customer_id,
  s1.customer_first_name,
  s1.customer_last_name
FROM store_aggregates s1
JOIN store_aggregates s2 
  ON s1.customer_id = s2.customer_id
JOIN web_aggregates w1 
  ON s1.customer_id = w1.customer_id
JOIN web_aggregates w2 
  ON s1.customer_id = w2.customer_id
WHERE s1.year = 1999
  AND s2.year = 2000
  AND w1.year = 1999
  AND w2.year = 2000
  AND s1.year_total > 0
  AND w1.year_total > 0
  AND w2.year_total / w1.year_total > s2.year_total / s1.year_total
ORDER BY
  2,
  1,
  3
LIMIT 100