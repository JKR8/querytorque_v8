WITH filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_current_price BETWEEN 0.99 AND 1.49
), filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_date BETWEEN (
        CAST('2001-04-02' AS DATE) - INTERVAL '30' DAY
    ) AND (
        CAST('2001-04-02' AS DATE) + INTERVAL '30' DAY
    )
), sales_with_dims AS (
    SELECT
        cs.cs_item_sk,
        cs.cs_order_number,
        cs.cs_sales_price,
        cs.cs_warehouse_sk,
        fi.i_item_id,
        fd.d_date
    FROM catalog_sales cs
    JOIN filtered_item fi ON cs.cs_item_sk = fi.i_item_sk
    JOIN filtered_date fd ON cs.cs_sold_date_sk = fd.d_date_sk
)
SELECT
    w.w_state,
    swd.i_item_id,
    SUM(
        CASE
            WHEN CAST(swd.d_date AS DATE) < CAST('2001-04-02' AS DATE)
            THEN swd.cs_sales_price - COALESCE(cr.cr_refunded_cash, 0)
            ELSE 0
        END
    ) AS sales_before,
    SUM(
        CASE
            WHEN CAST(swd.d_date AS DATE) >= CAST('2001-04-02' AS DATE)
            THEN swd.cs_sales_price - COALESCE(cr.cr_refunded_cash, 0)
            ELSE 0
        END
    ) AS sales_after
FROM sales_with_dims swd
LEFT OUTER JOIN catalog_returns cr ON (
    swd.cs_order_number = cr.cr_order_number 
    AND swd.cs_item_sk = cr.cr_item_sk
)
JOIN warehouse w ON swd.cs_warehouse_sk = w.w_warehouse_sk
GROUP BY w.w_state, swd.i_item_id
ORDER BY w.w_state, swd.i_item_id
LIMIT 100