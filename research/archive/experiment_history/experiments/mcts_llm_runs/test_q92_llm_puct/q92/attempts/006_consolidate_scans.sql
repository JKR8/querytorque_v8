WITH combined_data AS (
  SELECT
    ws_item_sk,
    mfg_discount_sum,
    item_avg_discount,
    mfg_count,
    total_count
  FROM combined_ws_scan
)
SELECT
  SUM(mfg_discount_sum) AS "Excess Discount Amount"
FROM combined_data AS cd
JOIN item AS i
  ON cd.ws_item_sk = i.i_item_sk
WHERE
  i.i_manufact_id = 320
  AND mfg_count > 0
  AND mfg_discount_sum > (
    1.3 * item_avg_discount * mfg_count
  )
ORDER BY
  SUM(mfg_discount_sum)
LIMIT 100