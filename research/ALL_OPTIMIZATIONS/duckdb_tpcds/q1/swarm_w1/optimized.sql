WITH customer_total_return AS (
  SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    SUM(SR_FEE) AS ctr_total_return
  FROM store_returns
  JOIN date_dim ON sr_returned_date_sk = d_date_sk
  WHERE d_year = 2000
  GROUP BY
    sr_customer_sk,
    sr_store_sk
),
filtered_stores AS (
  SELECT s_store_sk
  FROM store
  WHERE s_state = 'SD'
),
customer_returns_sd AS (
  SELECT
    ctr.ctr_customer_sk,
    ctr.ctr_store_sk,
    ctr.ctr_total_return
  FROM customer_total_return ctr
  JOIN filtered_stores fs ON ctr.ctr_store_sk = fs.s_store_sk
)
SELECT
  c_customer_id
FROM customer_returns_sd ctr1
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ctr1.ctr_total_return > (
  SELECT AVG(ctr2.ctr_total_return) * 1.2
  FROM customer_total_return ctr2
  WHERE ctr2.ctr_store_sk = ctr1.ctr_store_sk
)
ORDER BY c_customer_id
LIMIT 100