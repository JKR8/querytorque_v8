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
FROM web_sales, (
  SELECT
    i_item_sk
  FROM item
  WHERE
    i_manufact_id = 320
) AS filtered_item, filtered_dates, (
  SELECT
    ws_item_sk AS correlation_key,
    1.3 * AVG(ws_ext_discount_amt) AS threshold
  FROM web_sales, filtered_dates
  WHERE
    d_date_sk = ws_sold_date_sk
  GROUP BY
    ws_item_sk
) AS precomputed_agg
WHERE
  filtered_item.i_item_sk = ws_item_sk
  AND filtered_dates.d_date_sk = ws_sold_date_sk
  AND ws_item_sk = precomputed_agg.correlation_key
  AND ws_ext_discount_amt > precomputed_agg.threshold
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100