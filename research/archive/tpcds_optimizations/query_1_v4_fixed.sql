-- DeepSeek v4 with TRUE predicate pushdown
-- Joins store table INSIDE the aggregation CTE to filter BEFORE aggregating
WITH sd_store_returns AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_fee) AS ctr_total_return
    FROM store_returns
    INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
    INNER JOIN store ON sr_store_sk = s_store_sk  -- JOIN EARLY
    WHERE d_year = 2000
      AND s_state = 'SD'  -- FILTER EARLY (predicate pushdown)
    GROUP BY sr_customer_sk, sr_store_sk
),
store_avg_return AS (
    SELECT
        ctr_store_sk,
        AVG(ctr_total_return) * 1.2 AS avg_return_threshold
    FROM sd_store_returns
    GROUP BY ctr_store_sk
)
SELECT
    c_customer_id
FROM sd_store_returns ctr1
INNER JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk
INNER JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ctr1.ctr_total_return > sar.avg_return_threshold
ORDER BY c_customer_id
LIMIT 100;
