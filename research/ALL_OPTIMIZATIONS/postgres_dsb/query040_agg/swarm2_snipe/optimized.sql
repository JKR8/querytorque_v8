WITH filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_date BETWEEN CAST('1999-04-19' AS DATE) - INTERVAL '30 DAY'
                    AND CAST('1999-04-19' AS DATE) + INTERVAL '30 DAY'
),
filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category = 'Home'
      AND i_manager_id BETWEEN 25 AND 64
),
filtered_sales AS (
    SELECT 
        cs.cs_item_sk,
        cs.cs_order_number,
        cs.cs_sales_price,
        cs.cs_warehouse_sk,
        fd.d_date
    FROM catalog_sales cs
    JOIN filtered_date fd ON cs.cs_sold_date_sk = fd.d_date_sk
    JOIN filtered_item fi ON cs.cs_item_sk = fi.i_item_sk
    WHERE cs.cs_wholesale_cost BETWEEN 17 AND 36
)
SELECT
    w.w_state,
    fi.i_item_id,
    SUM(
        CASE
            WHEN fs.d_date < CAST('1999-04-19' AS DATE)
            THEN fs.cs_sales_price - COALESCE(cr.cr_refunded_cash, 0)
            ELSE 0
        END
    ) AS sales_before,
    SUM(
        CASE
            WHEN fs.d_date >= CAST('1999-04-19' AS DATE)
            THEN fs.cs_sales_price - COALESCE(cr.cr_refunded_cash, 0)
            ELSE 0
        END
    ) AS sales_after
FROM filtered_sales fs
LEFT JOIN catalog_returns cr ON (
    fs.cs_order_number = cr.cr_order_number 
    AND fs.cs_item_sk = cr.cr_item_sk
    AND cr.cr_reason_sk = 16
)
JOIN warehouse w ON fs.cs_warehouse_sk = w.w_warehouse_sk
JOIN filtered_item fi ON fs.cs_item_sk = fi.i_item_sk
GROUP BY
    w.w_state,
    fi.i_item_id
ORDER BY
    w.w_state,
    fi.i_item_id
LIMIT 100