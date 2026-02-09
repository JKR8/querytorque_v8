WITH filtered_returns AS (
  SELECT 
    sr_item_sk,
    sr_ticket_number,
    sr_return_quantity
  FROM store_returns
  JOIN reason ON sr_reason_sk = r_reason_sk
  WHERE r_reason_desc = 'duplicate purchase'
)
SELECT
  ss_customer_sk,
  SUM(
    CASE 
      WHEN fr.sr_return_quantity IS NOT NULL
      THEN (ss_quantity - fr.sr_return_quantity) * ss_sales_price
      ELSE ss_quantity * ss_sales_price
    END
  ) AS sumsales
FROM store_sales
LEFT JOIN filtered_returns fr
  ON ss_item_sk = fr.sr_item_sk
  AND ss_ticket_number = fr.sr_ticket_number
GROUP BY ss_customer_sk
ORDER BY sumsales, ss_customer_sk
LIMIT 100