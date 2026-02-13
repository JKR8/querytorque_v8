WITH ctr_qualified AS (SELECT
    sr_customer_sk,
    sr_store_sk,
    SUM(SR_FEE) AS ctr_total_return,
    AVG(SUM(SR_FEE)) OVER (PARTITION BY sr_store_sk) AS store_avg
FROM store_returns
JOIN date_dim ON sr_returned_date_sk = d_date_sk
JOIN store ON sr_store_sk = s_store_sk
WHERE d_year = 2000
  AND s_state = 'SD'
GROUP BY sr_customer_sk, sr_store_sk
QUALIFY ctr_total_return > store_avg * 1.2) SELECT
    c_customer_id
FROM ctr_qualified
JOIN store ON ctr_qualified.sr_store_sk = store.s_store_sk
JOIN customer ON ctr_qualified.sr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100