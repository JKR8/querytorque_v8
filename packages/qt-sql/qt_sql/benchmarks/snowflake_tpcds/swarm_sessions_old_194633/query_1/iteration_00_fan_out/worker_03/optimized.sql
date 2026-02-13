WITH enhanced_ctr AS (SELECT
  sr_customer_sk,
  sr_store_sk,
  SUM(SR_FEE) AS ctr_total_return,
  AVG(SUM(SR_FEE)) OVER (PARTITION BY sr_store_sk) AS store_avg
FROM store_returns
INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
WHERE d_year = 2000
GROUP BY sr_customer_sk, sr_store_sk) SELECT c_customer_id
FROM enhanced_ctr ctr1
INNER JOIN store ON ctr1.sr_store_sk = store.s_store_sk
INNER JOIN customer ON ctr1.sr_customer_sk = customer.c_customer_sk
WHERE store.s_state = 'SD'
  AND ctr1.ctr_total_return > (ctr1.store_avg * 1.2)
ORDER BY c_customer_id
LIMIT 100