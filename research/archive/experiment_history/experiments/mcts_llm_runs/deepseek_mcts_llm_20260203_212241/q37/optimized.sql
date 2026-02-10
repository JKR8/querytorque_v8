SELECT
  i_item_id,
  i_item_desc,
  i_current_price
FROM (
  SELECT
    i_item_sk,
    i_item_id,
    i_item_desc,
    i_current_price,
    i_manufact_id,
    CASE WHEN i_current_price BETWEEN 45 AND 75 THEN 1 ELSE 0 END AS price_condition,
    CASE WHEN i_manufact_id IN (856, 707, 1000, 747) THEN 1 ELSE 0 END AS manufact_condition
  FROM item
  WHERE
    i_current_price BETWEEN 45 AND 75 OR i_manufact_id IN (856, 707, 1000, 747)
) AS item_scan, inventory, date_dim, catalog_sales
WHERE
  price_condition = 1
  AND manufact_condition = 1
  AND inv_item_sk = i_item_sk
  AND d_date_sk = inv_date_sk
  AND d_date BETWEEN CAST('1999-02-21' AS DATE) AND (
    CAST('1999-02-21' AS DATE) + INTERVAL '60' DAY
  )
  AND inv_quantity_on_hand BETWEEN 100 AND 500
  AND cs_item_sk = i_item_sk
GROUP BY
  i_item_id,
  i_item_desc,
  i_current_price
ORDER BY
  i_item_id
LIMIT 100