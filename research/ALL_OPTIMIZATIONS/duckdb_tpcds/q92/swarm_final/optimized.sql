SELECT
  SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM (
  SELECT
    ws.ws_ext_discount_amt,
    AVG(ws.ws_ext_discount_amt) OVER (PARTITION BY ws.ws_item_sk) AS avg_disc
  FROM web_sales ws
  JOIN item i ON i.i_item_sk = ws.ws_item_sk
  JOIN date_dim d ON d.d_date_sk = ws.ws_sold_date_sk
  WHERE
    i.i_manufact_id = 320
    AND d.d_date BETWEEN '2002-02-26' AND (
      CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
    )
) filtered
WHERE
  ws_ext_discount_amt > 1.3 * avg_disc
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100