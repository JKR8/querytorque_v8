-- start query 21 in stream 0 using template query21.tpl
SELECT w_warehouse_name,
       i_item_id,
       SUM(CASE WHEN d_date < DATE '2002-02-27' THEN inv_quantity_on_hand ELSE 0 END) AS inv_before,
       SUM(CASE WHEN d_date >= DATE '2002-02-27' THEN inv_quantity_on_hand ELSE 0 END) AS inv_after
FROM (
    SELECT inv_quantity_on_hand, inv_warehouse_sk, inv_item_sk, d_date
    FROM inventory
    JOIN date_dim ON inv_date_sk = d_date_sk
    WHERE d_date BETWEEN DATE '2002-02-27' - INTERVAL '30' DAY 
                     AND DATE '2002-02-27' + INTERVAL '30' DAY
) AS filtered_inv
JOIN item ON i_item_sk = filtered_inv.inv_item_sk
JOIN warehouse ON w_warehouse_sk = filtered_inv.inv_warehouse_sk
WHERE i_current_price BETWEEN 0.99 AND 1.49
GROUP BY w_warehouse_name, i_item_id
HAVING (CASE WHEN SUM(CASE WHEN d_date < DATE '2002-02-27' THEN inv_quantity_on_hand ELSE 0 END) > 0 
             THEN SUM(CASE WHEN d_date >= DATE '2002-02-27' THEN inv_quantity_on_hand ELSE 0 END)::DECIMAL / 
                  SUM(CASE WHEN d_date < DATE '2002-02-27' THEN inv_quantity_on_hand ELSE 0 END)
             ELSE NULL END) BETWEEN 2.0/3.0 AND 3.0/2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100;

-- end query 21 in stream 0 using template query21.tpl