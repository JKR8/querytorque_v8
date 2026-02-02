SELECT t.i_item_id AS I_ITEM_ID, t.i_item_desc AS I_ITEM_DESC, t.i_current_price AS I_CURRENT_PRICE
FROM (SELECT *
FROM item
WHERE i_current_price >= 68 AND i_current_price <= 68 + 30 AND i_manufact_id IN (677, 694, 808, 940)) AS t
INNER JOIN (SELECT *
FROM inventory
WHERE inv_quantity_on_hand >= 100 AND inv_quantity_on_hand <= 500) AS t0 ON t.i_item_sk = t0.inv_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-02-01' AND d_date <= DATE '2000-04-01') AS t1 ON t0.inv_date_sk = t1.d_date_sk
INNER JOIN catalog_sales ON t.i_item_sk = catalog_sales.cs_item_sk
GROUP BY t.i_item_id, t.i_item_desc, t.i_current_price
ORDER BY t.i_item_id
FETCH NEXT 100 ROWS ONLY