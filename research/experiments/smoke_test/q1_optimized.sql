WITH customer_total_return AS (
    SELECT 
        sr.sr_customer_sk AS ctr_customer_sk,
        sr.sr_store_sk AS ctr_store_sk,
        SUM(sr.SR_FEE) AS ctr_total_return
    FROM store_returns sr
    JOIN date_dim d ON sr.sr_returned_date_sk = d.d_date_sk
    JOIN store s ON sr.sr_store_sk = s.s_store_sk
    WHERE d.d_year = 2000
      AND s.s_state = 'SD'
    GROUP BY sr.sr_customer_sk, sr.sr_store_sk
),
store_averages AS (
    SELECT 
        ctr_customer_sk,
        ctr_store_sk,
        ctr_total_return,
        AVG(ctr_total_return) OVER (PARTITION BY ctr_store_sk) * 1.2 AS avg_threshold
    FROM customer_total_return
)
SELECT c.c_customer_id
FROM store_averages sa
JOIN customer c ON sa.ctr_customer_sk = c.c_customer_sk
WHERE sa.ctr_total_return > sa.avg_threshold
ORDER BY c.c_customer_id
LIMIT 100;