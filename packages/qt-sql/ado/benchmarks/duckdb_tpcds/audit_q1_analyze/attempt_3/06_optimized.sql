WITH filtered_stores AS (
  SELECT s_store_sk
  FROM store
  WHERE s_state = 'SD'
),
filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000
),
store_returns_filtered AS (
  SELECT
    sr.sr_customer_sk,
    sr.sr_store_sk,
    sr.sr_fee
  FROM store_returns sr
  INNER JOIN filtered_dates fd ON sr.sr_returned_date_sk = fd.d_date_sk
  INNER JOIN filtered_stores fs ON sr.sr_store_sk = fs.s_store_sk
),
customer_store_totals AS (
  SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    SUM(sr_fee) AS ctr_total_return,
    AVG(SUM(sr_fee)) OVER (PARTITION BY sr_store_sk) AS store_avg_return
  FROM store_returns_filtered
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT
  c.c_customer_id
FROM customer_store_totals ctr
INNER JOIN customer c ON ctr.ctr_customer_sk = c.c_customer_sk
WHERE ctr.ctr_total_return > ctr.store_avg_return * 1.2
ORDER BY c.c_customer_id
LIMIT 100