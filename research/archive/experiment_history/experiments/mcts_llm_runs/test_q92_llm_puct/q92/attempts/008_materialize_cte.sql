WITH avg_discount_factor AS (
  SELECT
    ws_item_sk,
    1.3 * AVG(ws_ext_discount_amt) AS avg_discount_factor
  FROM web_sales, date_dim
  WHERE
    d_date BETWEEN '2002-02-26' AND (
      CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
    )
    AND d_date_sk = ws_sold_date_sk
  GROUP BY
    ws_item_sk
)
SELECT
  SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales, item, date_dim, avg_discount_factor
WHERE
  i_manufact_id = 320
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN '2002-02-26' AND (
    CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
  )
  AND d_date_sk = ws_sold_date_sk
  AND ws_item_sk = avg_discount_factor.ws_item_sk
  AND ws_ext_discount_amt > avg_discount_factor.avg_discount_factor
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100