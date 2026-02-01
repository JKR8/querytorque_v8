WITH store_filtered AS (
    SELECT s_store_sk
    FROM store
    WHERE s_state = 'SD'
),
date_filtered AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2000
),
store_returns_filtered AS (
    SELECT sr_customer_sk, sr_store_sk, SR_FEE
    FROM store_returns
    INNER JOIN date_filtered ON sr_returned_date_sk = d_date_sk
    WHERE EXISTS (SELECT 1 FROM store_filtered WHERE s_store_sk = sr_store_sk)
),
customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(SR_FEE) AS ctr_total_return
    FROM store_returns_filtered
    GROUP BY sr_customer_sk, sr_store_sk
),
store_avg_return AS (
    SELECT
        ctr_store_sk,
        AVG(ctr_total_return) * 1.2 AS avg_threshold
    FROM customer_total_return
    GROUP BY ctr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1
INNER JOIN store_filtered sf ON ctr1.ctr_store_sk = sf.s_store_sk
INNER JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk
INNER JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ctr1.ctr_total_return > sar.avg_threshold
ORDER BY c_customer_id
LIMIT 100;
