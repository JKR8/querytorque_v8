WITH customer_total_return_filtered AS (SELECT
  sr_customer_sk AS ctr_customer_sk,
  sr_store_sk AS ctr_store_sk,
  SUM(SR_FEE) AS ctr_total_return
FROM store_returns
INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
INNER JOIN store ON sr_store_sk = s_store_sk
WHERE d_year = 2000
  AND s_state = 'SD'
GROUP BY sr_customer_sk, sr_store_sk),
store_averages AS (SELECT
  ctr_store_sk,
  AVG(ctr_total_return) AS store_avg_return
FROM customer_total_return_filtered
GROUP BY ctr_store_sk),
filtered_ids AS (SELECT
  ctr1.ctr_customer_sk
FROM customer_total_return_filtered AS ctr1
INNER JOIN store_averages ON ctr1.ctr_store_sk = store_averages.ctr_store_sk
WHERE ctr1.ctr_total_return > store_averages.store_avg_return * 1.2),
customer_lookup AS (SELECT
  c_customer_id
FROM filtered_ids
INNER JOIN customer ON filtered_ids.ctr_customer_sk = c_customer_sk)
SELECT
  c_customer_id
FROM customer_lookup
ORDER BY c_customer_id ASC
LIMIT 100