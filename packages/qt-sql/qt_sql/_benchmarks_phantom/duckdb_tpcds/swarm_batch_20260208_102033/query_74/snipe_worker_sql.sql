WITH store_sales_agg AS (
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
  GROUP BY c_customer_id, c_first_name, c_last_name, d_year
),
web_sales_agg AS (
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
  GROUP BY c_customer_id, c_first_name, c_last_name, d_year
)
SELECT
  ss2000.customer_id,
  ss2000.customer_first_name,
  ss2000.customer_last_name
FROM store_sales_agg ss1999
JOIN store_sales_agg ss2000 
  ON ss1999.customer_id = ss2000.customer_id
  AND ss1999.year = 1999
  AND ss2000.year = 2000
  AND ss1999.year_total > 0
JOIN web_sales_agg ws1999 
  ON ss1999.customer_id = ws1999.customer_id
  AND ws1999.year = 1999
  AND ws1999.year_total > 0
JOIN web_sales_agg ws2000 
  ON ss1999.customer_id = ws2000.customer_id
  AND ws2000.year = 2000
WHERE ws2000.year_total / ws1999.year_total > ss2000.year_total / ss1999.year_total
ORDER BY
  ss2000.customer_first_name,
  ss2000.customer_id,
  ss2000.customer_last_name
LIMIT 100;