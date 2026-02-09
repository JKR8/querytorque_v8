WITH filtered_customers AS (
  SELECT 
    c_customer_sk,
    c_customer_id,
    c_first_name,
    c_last_name
  FROM customer
  WHERE EXISTS (
    SELECT 1 FROM store_sales, date_dim
    WHERE c_customer_sk = ss_customer_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_year IN (1999, 2000)
  ) AND EXISTS (
    SELECT 1 FROM web_sales, date_dim
    WHERE c_customer_sk = ws_bill_customer_sk
      AND ws_sold_date_sk = d_date_sk
      AND d_year IN (1999, 2000)
  )
),
store_agg AS (
  SELECT
    fc.c_customer_id AS customer_id,
    fc.c_first_name AS customer_first_name,
    fc.c_last_name AS customer_last_name,
    STDDEV_SAMP(ss_net_paid) FILTER (WHERE d.d_year = 1999) AS store_1999,
    STDDEV_SAMP(ss_net_paid) FILTER (WHERE d.d_year = 2000) AS store_2000
  FROM filtered_customers fc
  JOIN store_sales ss ON fc.c_customer_sk = ss.ss_customer_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  WHERE d.d_year IN (1999, 2000)
  GROUP BY
    fc.c_customer_id,
    fc.c_first_name,
    fc.c_last_name
),
web_agg AS (
  SELECT
    fc.c_customer_id AS customer_id,
    fc.c_first_name AS customer_first_name,
    fc.c_last_name AS customer_last_name,
    STDDEV_SAMP(ws_net_paid) FILTER (WHERE d.d_year = 1999) AS web_1999,
    STDDEV_SAMP(ws_net_paid) FILTER (WHERE d.d_year = 2000) AS web_2000
  FROM filtered_customers fc
  JOIN web_sales ws ON fc.c_customer_sk = ws.ws_bill_customer_sk
  JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
  WHERE d.d_year IN (1999, 2000)
  GROUP BY
    fc.c_customer_id,
    fc.c_first_name,
    fc.c_last_name
)
SELECT
  s.customer_id,
  s.customer_first_name,
  s.customer_last_name
FROM store_agg s
JOIN web_agg w ON s.customer_id = w.customer_id
WHERE
  s.store_1999 > 0
  AND w.web_1999 > 0
  AND (w.web_2000 / w.web_1999) > (s.store_2000 / s.store_1999)
ORDER BY
  2,
  1,
  3
LIMIT 100