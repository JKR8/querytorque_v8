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
    INNER JOIN filtered_date ON d_date_sk = inv_date_sk
    WHERE inv_quantity_on_hand BETWEEN 100 AND 500
),
item_inventory_join AS (
    SELECT 
        fi.i_item_sk,
        fi.i_item_id,
        fi.i_item_desc,
        fi.i_current_price
    FROM filtered_item fi
    INNER JOIN filtered_inventory inv ON fi.i_item_sk = inv.inv_item_sk
)
SELECT 
    i_item_id,
    i_item_desc,
    i_current_price
FROM item_inventory_join iij
WHERE EXISTS (
    SELECT 1
    FROM catalog_sales
    WHERE cs_item_sk = iij.i_item_sk
)
GROUP BY 
    i_item_id,
    i_item_desc,
    i_current_price
ORDER BY 
    i_item_id
LIMIT 100;