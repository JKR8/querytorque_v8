WITH customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_fee) AS ctr_total_return
    FROM store_returns
    INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
    WHERE d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
),
store_avg_returns AS (
    SELECT
        ctr_store_sk,
        AVG(ctr_total_return) * 1.2 AS avg_ctr_total_return
    FROM customer_total_return
    GROUP BY ctr_store_sk
)
SELECT
    c_customer_id
FROM customer_total_return ctr1
INNER JOIN store ON s_store_sk = ctr1.ctr_store_sk
INNER JOIN customer ON c_customer_sk = ctr1.ctr_customer_sk
INNER JOIN store_avg_returns sar ON ctr1.ctr_store_sk = sar.ctr_store_sk
WHERE s_state = 'SD'
    AND ctr1.ctr_total_return > sar.avg_ctr_total_return
ORDER BY c_customer_id
LIMIT 100;
