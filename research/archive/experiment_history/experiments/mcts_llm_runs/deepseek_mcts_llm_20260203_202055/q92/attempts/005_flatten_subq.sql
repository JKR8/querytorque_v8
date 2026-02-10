WITH filtered_dates AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    d_date BETWEEN '2002-02-26' AND (
      CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
    )
), item_avg_discount AS (
  SELECT
    ws_item_sk,
    1.3 * AVG(ws_ext_discount_amt) AS avg_discount_threshold
  FROM web_sales, filtered_dates
  WHERE
    d_date_sk = ws_sold_date_sk
  GROUP BY
    ws_item_sk
)
SELECT
  SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales, item, filtered_dates, item_avg_discount
WHERE
  i_manufact_id = 320
  AND i_item_sk = ws_item_sk
  AND d_date_sk = ws_sold_date_sk
  AND ws_item_sk = item_avg_discount.ws_item_sk
  AND ws_ext_discount_amt > item_avg_discount.avg_discount_threshold
GROUP BY
  ws_item_sk
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100