WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN (CAST('1999-04-19' AS DATE) - INTERVAL '30 DAY')
                     AND (CAST('1999-04-19' AS DATE) + INTERVAL '30 DAY')
),
filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category = 'Home'
      AND i_manager_id BETWEEN 25 AND 64
),
filtered_warehouse AS (
    SELECT w_warehouse_sk, w_state
    FROM warehouse
),
filtered_cs AS (
    SELECT cs_item_sk, cs_order_number, cs_warehouse_sk, cs_sold_date_sk
    FROM catalog_sales
    WHERE cs_wholesale_cost BETWEEN 17 AND 36
),
filtered_cr AS (
    SELECT cr_item_sk, cr_order_number
    FROM catalog_returns
    WHERE cr_reason_sk = 16
)
SELECT
    MIN(w.w_state),
    MIN(i.i_item_id),
    MIN(cs.cs_item_sk),
    MIN(cs.cs_order_number),
    MIN(cr.cr_item_sk),
    MIN(cr.cr_order_number)
FROM filtered_cs cs
LEFT OUTER JOIN filtered_cr cr
    ON cs.cs_order_number = cr.cr_order_number
    AND cs.cs_item_sk = cr.cr_item_sk
INNER JOIN filtered_item i
    ON cs.cs_item_sk = i.i_item_sk
INNER JOIN filtered_warehouse w
    ON cs.cs_warehouse_sk = w.w_warehouse_sk
INNER JOIN filtered_date d
    ON cs.cs_sold_date_sk = d.d_date_sk;