WITH store_returns_agg AS (SELECT
    sr_customer_sk,
    sr_store_sk,
    SUM(SR_FEE) AS ctr_total_return
FROM store_returns
JOIN date_dim ON sr_returned_date_sk = d_date_sk
WHERE d_year = 2000
GROUP BY sr_customer_sk, sr_store_sk),
store_avg AS (SELECT
    sr_store_sk,
    AVG(ctr_total_return) * 1.2 AS store_avg
FROM store_returns_agg
GROUP BY sr_store_sk)
SELECT
    c_customer_id
FROM store_returns_agg ctr1
JOIN store ON ctr1.sr_store_sk = store.s_store_sk
JOIN store_avg ON ctr1.sr_store_sk = store_avg.sr_store_sk
JOIN customer ON ctr1.sr_customer_sk = customer.c_customer_sk
WHERE store.s_state = 'SD'
  AND ctr1.ctr_total_return > store_avg.store_avg
ORDER BY c_customer_id
LIMIT 100