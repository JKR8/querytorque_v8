SELECT
  c_customer_id
FROM customer_total_return AS ctr1
JOIN store
  ON s_store_sk = ctr1.ctr_store_sk
JOIN customer
  ON ctr1.ctr_customer_sk = c_customer_sk
WHERE
  ctr1.ctr_total_return > ctr1.ctr_avg_return_threshold AND s_state = 'SD'
ORDER BY
  c_customer_id
LIMIT 100