SELECT
  SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales, item, date_dim
LEFT JOIN precomputed_agg
  ON precomputed_agg.correlation_key = i_item_sk
WHERE
  i_manufact_id = 320
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN '2002-02-26' AND (
    CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
  )
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt > COALESCE(precomputed_agg.threshold, 0)
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100