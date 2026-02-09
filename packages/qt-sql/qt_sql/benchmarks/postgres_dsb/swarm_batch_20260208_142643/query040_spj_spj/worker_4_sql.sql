WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN (
        CAST('1999-04-19' AS DATE) - INTERVAL '30 DAY'
    ) AND (
        CAST('1999-04-19' AS DATE) + INTERVAL '30 DAY'
    )
),
filtered_items AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category = 'Home'
      AND i_manager_id BETWEEN 25 AND 64
),
filtered_returns AS (
    SELECT cr_order_number, cr_item_sk
    FROM catalog_returns
    WHERE cr_reason_sk = 16
)
SELECT
    MIN(w.w_state),
    MIN(fi.i_item_id),
    MIN(cs.cs_item_sk),
    MIN(cs.cs_order_number),
    MIN(cr.cr_item_sk),
    MIN(cr.cr_order_number)
FROM catalog_sales cs
JOIN filtered_dates fd ON cs.cs_sold_date_sk = fd.d_date_sk
JOIN filtered_items fi ON cs.cs_item_sk = fi.i_item_sk
JOIN warehouse w ON cs.cs_warehouse_sk = w.w_warehouse_sk
LEFT JOIN filtered_returns cr ON (
    cs.cs_order_number = cr.cr_order_number 
    AND cs.cs_item_sk = cr.cr_item_sk
)
WHERE cs.cs_wholesale_cost BETWEEN 17 AND 36
  AND EXISTS (
    SELECT 1 
    FROM filtered_returns cr2
    WHERE cs.cs_order_number = cr2.cr_order_number 
      AND cs.cs_item_sk = cr2.cr_item_sk
  );