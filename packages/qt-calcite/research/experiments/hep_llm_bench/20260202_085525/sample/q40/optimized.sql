SELECT warehouse.w_state AS W_STATE, t.i_item_id AS I_ITEM_ID, SUM(CASE WHEN t0.d_date < DATE '2000-03-11' THEN catalog_sales.cs_sales_price - CASE WHEN catalog_returns.cr_refunded_cash IS NOT NULL THEN CAST(catalog_returns.cr_refunded_cash AS DECIMAL(19, 0)) ELSE 0 END ELSE 0 END) AS SALES_BEFORE, SUM(CASE WHEN t0.d_date >= DATE '2000-03-11' THEN catalog_sales.cs_sales_price - CASE WHEN catalog_returns.cr_refunded_cash IS NOT NULL THEN CAST(catalog_returns.cr_refunded_cash AS DECIMAL(19, 0)) ELSE 0 END ELSE 0 END) AS SALES_AFTER
FROM catalog_sales
LEFT JOIN catalog_returns ON catalog_sales.cs_order_number = catalog_returns.cr_order_number AND catalog_sales.cs_item_sk = catalog_returns.cr_item_sk
INNER JOIN warehouse ON catalog_sales.cs_warehouse_sk = warehouse.w_warehouse_sk
INNER JOIN (SELECT *
FROM item
WHERE i_current_price >= 0.99 AND i_current_price <= 1.49) AS t ON catalog_sales.cs_item_sk = t.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-02-10' AND d_date <= DATE '2000-04-10') AS t0 ON catalog_sales.cs_sold_date_sk = t0.d_date_sk
GROUP BY warehouse.w_state, t.i_item_id
ORDER BY warehouse.w_state, t.i_item_id
FETCH NEXT 100 ROWS ONLY