SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM item
WHERE i_current_price BETWEEN 45 AND 75
  AND i_manufact_id IN (856, 707, 1000, 747)
  AND EXISTS (
      SELECT 1
      FROM inventory
      JOIN date_dim ON inv_date_sk = d_date_sk
      WHERE inv_item_sk = i_item_sk
        AND inv_quantity_on_hand BETWEEN 100 AND 500
        AND d_date BETWEEN DATE '1999-02-21' AND (DATE '1999-02-21' + INTERVAL '60 DAY')
  )
  AND EXISTS (
      SELECT 1
      FROM catalog_sales
      WHERE cs_item_sk = i_item_sk
  )
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id
LIMIT 100;