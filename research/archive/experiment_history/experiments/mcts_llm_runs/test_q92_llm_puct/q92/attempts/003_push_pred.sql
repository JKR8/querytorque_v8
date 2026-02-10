SELECT
  SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM (
  SELECT
    ws_ext_discount_amt,
    AVG(ws_ext_discount_amt) OVER (PARTITION BY ws_item_sk) AS avg_discount
  FROM web_sales, item, date_dim
  WHERE
    i_manufact_id = 320
    AND i_item_sk = ws_item_sk
    AND d_date BETWEEN '2002-02-26' AND (
      CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
    )
    AND d_date_sk = ws_sold_date_sk
) AS sub
WHERE
  ws_ext_discount_amt > 1.3 * avg_discount
GROUP BY
  ()
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100