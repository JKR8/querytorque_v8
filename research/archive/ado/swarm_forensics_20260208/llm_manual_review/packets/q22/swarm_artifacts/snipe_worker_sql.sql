WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1188 AND 1199
),
filtered_inventory AS (
    SELECT 
        inv_item_sk,
        inv_quantity_on_hand
    FROM inventory
    WHERE inv_date_sk IN (SELECT d_date_sk FROM filtered_dates)
)
SELECT
    i_product_name,
    i_brand,
    i_class,
    i_category,
    AVG(inv_quantity_on_hand) AS qoh
FROM filtered_inventory
JOIN item ON filtered_inventory.inv_item_sk = item.i_item_sk
GROUP BY ROLLUP (
    i_product_name,
    i_brand,
    i_class,
    i_category
)
ORDER BY
    qoh,
    i_product_name,
    i_brand,
    i_class,
    i_category
LIMIT 100;