
SELECT c_customer_id
FROM customer, store_returns
WHERE sr_customer_sk = c_customer_sk
  AND sr_store_sk IN (
    SELECT s_store_sk FROM store WHERE s_state = 'SD'
  )
LIMIT 100
