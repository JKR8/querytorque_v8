-- Q1 Optimized by Kimi K2.5
-- Full DB: 2.44x speedup, CORRECT
-- Pattern: Predicate pushdown + window function for avg
--
-- Changes:
-- 1. Moved store join and s_state='SD' filter into CTE
-- 2. Replaced correlated subquery with window function AVG() OVER (PARTITION BY sr_store_sk)

WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk,
           sr_store_sk AS ctr_store_sk,
           sum(SR_FEE) AS ctr_total_return,
           avg(sum(SR_FEE)) OVER (PARTITION BY sr_store_sk) AS ctr_avg_return
    FROM store_returns, date_dim, store
    WHERE sr_returned_date_sk = d_date_sk
      AND sr_store_sk = s_store_sk
      AND d_year = 2000
      AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > ctr1.ctr_avg_return * 1.2
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100;
