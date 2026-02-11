WITH filtered_item AS (
    SELECT 
        i_item_sk,
        i_item_id,
        i_item_desc,
        i_current_price
    FROM item
    WHERE i_current_price BETWEEN 17 AND 17 + 30
      AND i_manufact_id IN (639, 169, 138, 339)
), filtered_date AS (
    SELECT 
        d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('1999-07-09' AS DATE) 
                     AND (CAST('1999-07-09' AS DATE) + INTERVAL '60' DAY)
), filtered_inventory AS (
    SELECT DISTINCT
        inv_item_sk
    FROM inventory
    JOIN filtered_date ON d_date_sk = inv_date_sk
    WHERE inv_quantity_on_hand BETWEEN 100 AND 500
), qualified_items AS (
    SELECT DISTINCT
        fi.i_item_id,
        fi.i_item_desc,
        fi.i_current_price
    FROM filtered_item fi
    JOIN filtered_inventory finv ON fi.i_item_sk = finv.inv_item_sk
    WHERE EXISTS (
        SELECT 1
        FROM store_sales ss
        WHERE ss.ss_item_sk = fi.i_item_sk
    )
)
SELECT 
    i_item_id,
    i_item_desc,
    i_current_price
FROM qualified_items
ORDER BY i_item_id
LIMIT 100;