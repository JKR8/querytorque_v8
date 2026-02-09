WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('1999-02-21' AS DATE) 
        AND (CAST('1999-02-21' AS DATE) + INTERVAL '60' DAY)
),
filtered_item AS (
    SELECT i_item_sk, i_item_id, i_item_desc, i_current_price
    FROM item
    WHERE i_current_price BETWEEN 45 AND 45 + 30
        AND i_manufact_id IN (856, 707, 1000, 747)
),
filtered_inventory AS (
    SELECT inv_item_sk
    FROM inventory
    JOIN filtered_date ON inventory.inv_date_sk = filtered_date.d_date_sk
    WHERE inv_quantity_on_hand BETWEEN 100 AND 500
)
SELECT 
    filtered_item.i_item_id,
    filtered_item.i_item_desc,
    filtered_item.i_current_price
FROM filtered_item
WHERE EXISTS (
    SELECT 1 FROM filtered_inventory 
    WHERE filtered_inventory.inv_item_sk = filtered_item.i_item_sk
)
AND EXISTS (
    SELECT 1 FROM catalog_sales 
    WHERE catalog_sales.cs_item_sk = filtered_item.i_item_sk
)
GROUP BY
    filtered_item.i_item_id,
    filtered_item.i_item_desc,
    filtered_item.i_current_price
ORDER BY
    filtered_item.i_item_id
LIMIT 100