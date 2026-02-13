WITH ctr_base AS (SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    SUM(SR_FEE) AS ctr_total_return
FROM store_returns
JOIN date_dim ON store_returns.sr_returned_date_sk = date_dim.d_date_sk
WHERE date_dim.d_year = 2000
GROUP BY sr_customer_sk, sr_store_sk), ctr_sd AS (SELECT
    ctr_base.ctr_customer_sk,
    ctr_base.ctr_store_sk,
    ctr_total_return
FROM ctr_base
JOIN store ON ctr_base.ctr_store_sk = store.s_store_sk
WHERE store.s_state = 'SD'), ctr_avg AS (SELECT
    ctr_store_sk,
    AVG(ctr_total_return) * 1.2 AS avg_return
FROM ctr_base
GROUP BY ctr_store_sk) SELECT
    customer.c_customer_id
FROM ctr_sd
JOIN customer ON ctr_sd.ctr_customer_sk = customer.c_customer_sk
JOIN ctr_avg ON ctr_sd.ctr_store_sk = ctr_avg.ctr_store_sk
WHERE ctr_sd.ctr_total_return > ctr_avg.avg_return
ORDER BY customer.c_customer_id
LIMIT 100