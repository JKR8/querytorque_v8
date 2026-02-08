WITH date_keys AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000
),
filtered_returns AS (
  SELECT
    sr_customer_sk,
    sr_store_sk,
    sr_fee
  FROM store_returns
  WHERE sr_returned_date_sk IN (SELECT d_date_sk FROM date_keys)
),
store_aggregates AS (
  SELECT
    sr_store_sk AS store_sk,
    AVG(ctr_total_return) * 1.2 AS store_threshold
  FROM (
    SELECT
      sr_store_sk,
      sr_customer_sk,
      SUM(sr_fee) AS ctr_total_return
    FROM filtered_returns
    GROUP BY sr_store_sk, sr_customer_sk
  ) AS customer_store_totals
  GROUP BY sr_store_sk
),
customer_store_totals AS (
  SELECT
    sr_customer_sk,
    sr_store_sk,
    SUM(sr_fee) AS ctr_total_return
  FROM filtered_returns
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT
  c_customer_id
FROM customer_store_totals ctr1
JOIN store ON s_store_sk = ctr1.sr_store_sk
JOIN customer ON c_customer_sk = ctr1.sr_customer_sk
WHERE EXISTS (
  SELECT 1
  FROM store_aggregates sa
  WHERE sa.store_sk = ctr1.sr_store_sk
    AND ctr1.ctr_total_return > sa.store_threshold
)
AND s_state = 'SD'
ORDER BY c_customer_id
LIMIT 100;