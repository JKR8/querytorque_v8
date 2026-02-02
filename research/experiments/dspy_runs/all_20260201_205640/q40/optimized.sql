SELECT 
    w_state,
    i_item_id,
    SUM(CASE WHEN d_date < DATE '2001-04-02' THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_before,
    SUM(CASE WHEN d_date >= DATE '2001-04-02' THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_after
FROM (
    SELECT 
        cs_sales_price,
        cs_warehouse_sk,
        cs_item_sk,
        cs_order_number,
        d_date
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_date BETWEEN DATE '2001-04-02' - INTERVAL '30 days' AND DATE '2001-04-02' + INTERVAL '30 days'
) cs
JOIN item ON i_item_sk = cs.cs_item_sk
    AND i_current_price BETWEEN 0.99 AND 1.49
LEFT JOIN catalog_returns cr ON cs.cs_order_number = cr.cr_order_number 
    AND cs.cs_item_sk = cr.cr_item_sk
JOIN warehouse ON cs.cs_warehouse_sk = w_warehouse_sk
GROUP BY w_state, i_item_id
ORDER BY w_state, i_item_id
LIMIT 100;