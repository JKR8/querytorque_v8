WITH filtered_dates AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    d_date BETWEEN '2002-02-26' AND (
      CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
    )
)
SELECT
  SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales, item, filtered_dates
WHERE
  i_manufact_id = 320
  AND i_item_sk = ws_item_sk
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt > (
    SELECT
      1.3 * AVG(ws_ext_discount_amt)
    FROM web_sales, filtered_dates
    WHERE
      ws_item_sk = i_item_sk AND d_date_sk = ws_sold_date_sk
  )
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100