WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN 
        (CAST('1999-04-19' AS DATE) - INTERVAL '30 DAY') AND 
        (CAST('1999-04-19' AS DATE) + INTERVAL '30 DAY')
),
filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category = 'Home'
        AND i_manager_id BETWEEN 25 AND 64
),
filtered_returns AS (
    SELECT cr_order_number, cr_item_sk, cr_reason_sk
    FROM catalog_returns
    WHERE cr_reason_sk = 16
)
SELECT
    MIN(w_state),
    MIN(i_item_id),
    MIN(cs_item_sk),
    MIN(cs_order_number),
    MIN(cr_item_sk),
    MIN(cr_order_number)
FROM catalog_sales
INNER JOIN filtered_date ON cs_sold_date_sk = filtered_date.d_date_sk
INNER JOIN filtered_item ON cs_item_sk = filtered_item.i_item_sk
INNER JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
LEFT OUTER JOIN filtered_returns ON (
    cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk
)
WHERE cs_wholesale_cost BETWEEN 17 AND 36;
