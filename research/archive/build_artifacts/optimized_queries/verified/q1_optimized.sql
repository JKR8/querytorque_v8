-- Q1: Predicate pushdown + window function
-- Sample DB: 1.31x speedup, CORRECT
-- Pattern: Push store filter into CTE, use window function for avg

WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk,
           sr_store_sk AS ctr_store_sk,
           sum(SR_FEE) AS ctr_total_return,
           AVG(sum(SR_FEE)) OVER (PARTITION BY sr_store_sk) AS store_avg
    FROM store_returns, date_dim, store
    WHERE sr_returned_date_sk = d_date_sk
      AND d_year = 2000
      AND sr_store_sk = s_store_sk
      AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > ctr1.store_avg * 1.2
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100;
