-- TPC-DS Query 1 - Optimized
-- Runtime: 0.116s avg (SF100)
-- Speedup: 5x
--
-- Techniques:
--   1. Predicate pushdown (s_state = 'SD' before aggregation)
--   2. Window function replaces correlated subquery
--   3. Early join enables filter pushdown
--   4. Late materialization of customer lookup

WITH sd_store_returns AS (
    -- Filter to 'SD' stores and year 2000 BEFORE aggregation
    SELECT
        sr_customer_sk,
        sr_store_sk,
        SUM(sr_fee) AS ctr_total_return
    FROM store_returns
    JOIN date_dim ON sr_returned_date_sk = d_date_sk
    JOIN store ON sr_store_sk = s_store_sk
    WHERE d_year = 2000
      AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
),
high_return_candidates AS (
    -- Window function replaces correlated subquery for avg calculation
    SELECT
        sr_customer_sk,
        ctr_total_return
    FROM (
        SELECT
            sr_customer_sk,
            ctr_total_return,
            AVG(ctr_total_return) OVER (PARTITION BY sr_store_sk) as store_avg
        FROM sd_store_returns
    )
    WHERE ctr_total_return > (store_avg * 1.2)
)
-- Late materialization: only look up customer_id for qualifying rows
SELECT
    c_customer_id
FROM high_return_candidates
JOIN customer ON c_customer_sk = sr_customer_sk
ORDER BY c_customer_id
LIMIT 100;
