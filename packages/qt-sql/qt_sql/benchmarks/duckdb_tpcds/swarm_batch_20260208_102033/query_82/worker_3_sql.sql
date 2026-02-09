WITH filtered_item AS (
    SELECT 
        i_item_sk,
        i_item_id,
        i_item_desc,
        i_current_price
    FROM item
    WHERE i_current_price BETWEEN 17 AND 17 + 30
      AND i_manufact_id IN (639, 169, 138, 339)
),
filtered_date AS (
    SELECT 
        d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('1999-07-09' AS DATE) 
                     AND (CAST('1999-07-09' AS DATE) + INTERVAL '60' DAY)
),
filtered_inventory AS (
    SELECT 
        inv_item_sk
    FROM inventory
    JOIN filtered_date ON d_date_sk = inv_date_sk
    WHERE inv_quantity_on_hand BETWEEN 100 AND 500
),
item_inventory AS (
    SELECT 
        i_item_sk,
        i_item_id,
        i_item_desc,
        i_current_price
    FROM filtered_item
    JOIN filtered_inventory ON inv_item_sk = i_item_sk
)
SELECT
    i_item_id,
    i_item_desc,
    i_current_price
FROM item_inventory
WHERE EXISTS (
    SELECT 1
    FROM store_sales
    WHERE ss_item_sk = i_item_sk
)
GROUP BY
    i_item_id,
    i_item_desc,
    i_current_price
ORDER BY
    i_item_id
LIMIT 100;