WITH filtered_items AS (
    SELECT i_item_sk, i_item_id, i_item_desc, i_current_price
    FROM item
    WHERE i_current_price BETWEEN 17 AND 17 + 30
      AND i_manufact_id IN (639, 169, 138, 339)
),
inventory_dates AS (
    SELECT DISTINCT inv_item_sk
    FROM inventory
    JOIN date_dim ON d_date_sk = inv_date_sk
    WHERE d_date BETWEEN CAST('1999-07-09' AS DATE) 
                     AND (CAST('1999-07-09' AS DATE) + INTERVAL '60' DAY)
      AND inv_quantity_on_hand BETWEEN 100 AND 500
)
SELECT
    i_item_id,
    i_item_desc,
    i_current_price
FROM filtered_items i
WHERE EXISTS (
    SELECT 1 FROM inventory_dates inv 
    WHERE inv.inv_item_sk = i.i_item_sk
)
AND EXISTS (
    SELECT 1 FROM store_sales ss 
    WHERE ss.ss_item_sk = i.i_item_sk
)
GROUP BY
    i_item_id,
    i_item_desc,
    i_current_price
ORDER BY
    i_item_id
LIMIT 100;