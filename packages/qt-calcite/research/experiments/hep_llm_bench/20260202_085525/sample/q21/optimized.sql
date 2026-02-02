SELECT warehouse.w_warehouse_name AS W_WAREHOUSE_NAME, t.i_item_id AS I_ITEM_ID, SUM(CASE WHEN t0.d_date < DATE '2000-03-11' THEN inventory.inv_quantity_on_hand ELSE 0 END) AS INV_BEFORE, SUM(CASE WHEN t0.d_date >= DATE '2000-03-11' THEN inventory.inv_quantity_on_hand ELSE 0 END) AS INV_AFTER
FROM inventory
INNER JOIN warehouse ON inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
INNER JOIN (SELECT *
FROM item
WHERE i_current_price >= 0.99 AND i_current_price <= 1.49) AS t ON inventory.inv_item_sk = t.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-02-10' AND d_date <= DATE '2000-04-10') AS t0 ON inventory.inv_date_sk = t0.d_date_sk
GROUP BY warehouse.w_warehouse_name, t.i_item_id
HAVING CASE WHEN SUM(CASE WHEN t0.d_date < DATE '2000-03-11' THEN inventory.inv_quantity_on_hand ELSE 0 END) > 0 THEN SUM(CASE WHEN t0.d_date >= DATE '2000-03-11' THEN inventory.inv_quantity_on_hand ELSE 0 END) * 1.000 / SUM(CASE WHEN t0.d_date < DATE '2000-03-11' THEN inventory.inv_quantity_on_hand ELSE 0 END) ELSE NULL END >= 2.000 / 3.000 AND CASE WHEN SUM(CASE WHEN t0.d_date < DATE '2000-03-11' THEN inventory.inv_quantity_on_hand ELSE 0 END) > 0 THEN SUM(CASE WHEN t0.d_date >= DATE '2000-03-11' THEN inventory.inv_quantity_on_hand ELSE 0 END) * 1.000 / SUM(CASE WHEN t0.d_date < DATE '2000-03-11' THEN inventory.inv_quantity_on_hand ELSE 0 END) ELSE NULL END <= 3.000 / 2.000
ORDER BY warehouse.w_warehouse_name NULLS FIRST, t.i_item_id NULLS FIRST
FETCH NEXT 100 ROWS ONLY