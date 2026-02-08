WITH customer_total_return AS (
  SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    SUM(SR_FEE) AS ctr_total_return
  FROM store_returns
  JOIN date_dim ON sr_returned_date_sk = d_date_sk
  WHERE d_year = 2000
  GROUP BY
    sr_customer_sk,
    sr_store_sk
),
store_avg_return AS (
  SELECT
    ctr_store_sk,
    AVG(ctr_total_return) * 1.2 AS avg_return_threshold
  FROM customer_total_return
  GROUP BY ctr_store_sk
),
sd_stores AS (
  SELECT s_store_sk
  FROM store
  WHERE s_state = 'SD'
)
SELECT
  c_customer_id
FROM customer_total_return ctr1
JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk
JOIN sd_stores s ON ctr1.ctr_store_sk = s.s_store_sk
JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk
WHERE ctr1.ctr_total_return > sar.avg_return_threshold
ORDER BY c_customer_id
LIMIT 100