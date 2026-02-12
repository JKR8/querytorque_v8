WITH store_agg AS (SELECT c.c_customer_id AS customer_id,
       c.c_first_name AS customer_first_name,
       c.c_last_name AS customer_last_name,
       d.d_year AS year,
       STDDEV_SAMP(ss.ss_net_paid) AS year_total,
       's' AS sale_type
FROM customer c
JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year IN (1999, 2000)
GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, d.d_year),
     web_agg AS (SELECT c.c_customer_id AS customer_id,
       c.c_first_name AS customer_first_name,
       c.c_last_name AS customer_last_name,
       d.d_year AS year,
       STDDEV_SAMP(ws.ws_net_paid) AS year_total,
       'w' AS sale_type
FROM customer c
JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
WHERE d.d_year IN (1999, 2000)
GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, d.d_year)
SELECT s2000.customer_id,
       s2000.customer_first_name,
       s2000.customer_last_name
FROM store_agg s1999
JOIN store_agg s2000 ON s1999.customer_id = s2000.customer_id
JOIN web_agg w1999 ON s1999.customer_id = w1999.customer_id
JOIN web_agg w2000 ON s1999.customer_id = w2000.customer_id
WHERE s1999.year = 1999 AND s1999.sale_type = 's'
  AND s2000.year = 2000 AND s2000.sale_type = 's'
  AND w1999.year = 1999 AND w1999.sale_type = 'w'
  AND w2000.year = 2000 AND w2000.sale_type = 'w'
  AND s1999.year_total > 0
  AND w1999.year_total > 0
  AND (w2000.year_total / w1999.year_total) > (s2000.year_total / s1999.year_total)
ORDER BY s2000.customer_first_name ASC,
         s2000.customer_id ASC,
         s2000.customer_last_name ASC
LIMIT 100