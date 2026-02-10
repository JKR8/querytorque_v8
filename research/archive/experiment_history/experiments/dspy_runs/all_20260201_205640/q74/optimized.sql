WITH year_data AS (
  SELECT 
    c.c_customer_id AS customer_id,
    c.c_first_name AS customer_first_name,
    c.c_last_name AS customer_last_name,
    d.d_year AS year,
    's' AS sale_type,
    ss.ss_net_paid AS net_paid
  FROM customer c
  JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  WHERE d.d_year IN (1999, 2000)
  
  UNION ALL
  
  SELECT 
    c.c_customer_id AS customer_id,
    c.c_first_name AS customer_first_name,
    c.c_last_name AS customer_last_name,
    d.d_year AS year,
    'w' AS sale_type,
    ws.ws_net_paid AS net_paid
  FROM customer c
  JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
  JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
  WHERE d.d_year IN (1999, 2000)
),
year_aggregates AS (
  SELECT 
    customer_id,
    customer_first_name,
    customer_last_name,
    year,
    sale_type,
    STDDEV_SAMP(net_paid) AS year_total
  FROM year_data
  GROUP BY customer_id, customer_first_name, customer_last_name, year, sale_type
),
year_ratios AS (
  SELECT 
    customer_id,
    customer_first_name,
    customer_last_name,
    MAX(CASE WHEN year = 1999 AND sale_type = 's' THEN year_total END) AS s_firstyear,
    MAX(CASE WHEN year = 2000 AND sale_type = 's' THEN year_total END) AS s_secyear,
    MAX(CASE WHEN year = 1999 AND sale_type = 'w' THEN year_total END) AS w_firstyear,
    MAX(CASE WHEN year = 2000 AND sale_type = 'w' THEN year_total END) AS w_secyear
  FROM year_aggregates
  GROUP BY customer_id, customer_first_name, customer_last_name
)
SELECT 
  customer_id,
  customer_first_name,
  customer_last_name
FROM year_ratios
WHERE s_firstyear > 0
  AND w_firstyear > 0
  AND (w_secyear / w_firstyear) > (s_secyear / s_firstyear)
ORDER BY customer_first_name, customer_id, customer_last_name
LIMIT 100;