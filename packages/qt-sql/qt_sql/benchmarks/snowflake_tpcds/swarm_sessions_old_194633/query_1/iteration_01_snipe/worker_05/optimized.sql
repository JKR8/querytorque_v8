WITH store_returns_prefilter AS (
  SELECT 
    sr_customer_sk,
    sr_store_sk,
    SUM(SR_FEE) AS ctr_total_return,
    AVG(SUM(SR_FEE)) OVER (PARTITION BY sr_store_sk) * 1.2 AS store_avg_threshold
  FROM store_returns
  JOIN date_dim ON sr_returned_date_sk = d_date_sk
  JOIN store ON sr_store_sk = s_store_sk
  WHERE d_year = 2000
    AND s_state = 'SD'
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT 
  c_customer_id
FROM store_returns_prefilter
JOIN customer ON sr_customer_sk = c_customer_sk
WHERE ctr_total_return > store_avg_threshold
ORDER BY c_customer_id
LIMIT 100;