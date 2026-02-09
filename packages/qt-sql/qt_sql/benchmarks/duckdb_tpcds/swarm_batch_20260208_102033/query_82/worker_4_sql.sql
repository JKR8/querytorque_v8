WITH
filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('1999-07-09' AS DATE) 
        AND (CAST('1999-07-09' AS DATE) + INTERVAL '60' DAY)
),
filtered_inventory AS (
    SELECT inv_item_sk, inv_date_sk
    FROM inventory
    WHERE inv_quantity_on_hand BETWEEN 100 AND 500
),
joined_inv_date AS (
    SELECT inv_item_sk
    FROM filtered_inventory
    JOIN filtered_dates ON filtered_inventory.inv_date_sk = filtered_dates.d_date_sk
),
manufacturer_items AS (
    SELECT i_item_sk, i_item_id, i_item_desc, i_current_price
    FROM item
    WHERE i_current_price BETWEEN 17 AND 17 + 30
      AND i_manufact_id IN (639, 169, 138, 339)
),
store_items AS (
    SELECT DISTINCT ss_item_sk
    FROM store_sales
)
SELECT
    manufacturer_items.i_item_id,
    manufacturer_items.i_item_desc,
    manufacturer_items.i_current_price
FROM manufacturer_items
JOIN joined_inv_date ON manufacturer_items.i_item_sk = joined_inv_date.inv_item_sk
JOIN store_items ON manufacturer_items.i_item_sk = store_items.ss_item_sk
GROUP BY
    manufacturer_items.i_item_id,
    manufacturer_items.i_item_desc,
    manufacturer_items.i_current_price
ORDER BY
    manufacturer_items.i_item_id
LIMIT 100