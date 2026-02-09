WITH sd_stores AS (
  SELECT s_store_sk
  FROM store
  WHERE s_state = 'SD'
),
year2000 AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000
),
customer_returns AS (
  SELECT
    sr_customer_sk,
    sr_store_sk,
    SUM(SR_FEE) AS ctr_total_return
  FROM store_returns
  JOIN year2000 ON sr_returned_date_sk = d_date_sk
  JOIN sd_stores ON sr_store_sk = s_store_sk
  GROUP BY sr_customer_sk, sr_store_sk
),
store_averages AS (
  SELECT
    sr_store_sk,
    AVG(ctr_total_return) AS store_avg_return
  FROM customer_returns
  GROUP BY sr_store_sk
),
filtered_returns AS (
  SELECT
    cr.sr_customer_sk AS ctr_customer_sk,
    cr.sr_store_sk AS ctr_store_sk,
    cr.ctr_total_return,
    sa.store_avg_return
  FROM customer_returns cr
  JOIN store_averages sa ON cr.sr_store_sk = sa.sr_store_sk
  WHERE cr.ctr_total_return > sa.store_avg_return * 1.2
)
SELECT c_customer_id
FROM filtered_returns
JOIN customer ON ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100