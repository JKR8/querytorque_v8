SELECT t4.W_WAREHOUSE_SK AS WSK1, t4.I_ITEM_SK AS ISK1, t4.D_MOY AS DMOY1, t4.MEAN AS MEAN1, t4.COV AS COV1, t10.W_WAREHOUSE_SK, t10.I_ITEM_SK, t10.D_MOY, t10.MEAN, t10.COV
FROM (SELECT w_warehouse_name AS W_WAREHOUSE_NAME, w_warehouse_sk AS W_WAREHOUSE_SK, i_item_sk AS I_ITEM_SK, d_moy AS D_MOY, CAST($f4 AS DECIMAL(14, 3)) AS STDEV, MEAN, CASE WHEN MEAN = 0 THEN NULL ELSE CAST($f4 AS DECIMAL(14, 3)) / MEAN END AS COV
FROM (SELECT *
FROM (SELECT warehouse.w_warehouse_name, warehouse.w_warehouse_sk, item.i_item_sk, t.d_moy, STDDEV_SAMP(inventory.inv_quantity_on_hand) AS $f4, AVG(inventory.inv_quantity_on_hand) AS MEAN
FROM inventory
INNER JOIN item ON inventory.inv_item_sk = item.i_item_sk
INNER JOIN warehouse ON inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001) AS t ON inventory.inv_date_sk = t.d_date_sk
GROUP BY item.i_item_sk, warehouse.w_warehouse_sk, warehouse.w_warehouse_name, t.d_moy) AS t1
WHERE CASE WHEN t1.MEAN = 0 THEN 0 ELSE CAST(t1.$f4 AS DECIMAL(14, 3)) / t1.MEAN END > 1) AS t2
WHERE d_moy = 1) AS t4
INNER JOIN (SELECT w_warehouse_name AS W_WAREHOUSE_NAME, w_warehouse_sk AS W_WAREHOUSE_SK, i_item_sk AS I_ITEM_SK, d_moy AS D_MOY, CAST($f4 AS DECIMAL(14, 3)) AS STDEV, MEAN, CASE WHEN MEAN = 0 THEN NULL ELSE CAST($f4 AS DECIMAL(14, 3)) / MEAN END AS COV
FROM (SELECT *
FROM (SELECT warehouse0.w_warehouse_name, warehouse0.w_warehouse_sk, item0.i_item_sk, t5.d_moy, STDDEV_SAMP(inventory0.inv_quantity_on_hand) AS $f4, AVG(inventory0.inv_quantity_on_hand) AS MEAN
FROM inventory AS inventory0
INNER JOIN item AS item0 ON inventory0.inv_item_sk = item0.i_item_sk
INNER JOIN warehouse AS warehouse0 ON inventory0.inv_warehouse_sk = warehouse0.w_warehouse_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001) AS t5 ON inventory0.inv_date_sk = t5.d_date_sk
GROUP BY item0.i_item_sk, warehouse0.w_warehouse_sk, warehouse0.w_warehouse_name, t5.d_moy) AS t7
WHERE CASE WHEN t7.MEAN = 0 THEN 0 ELSE CAST(t7.$f4 AS DECIMAL(14, 3)) / t7.MEAN END > 1) AS t8
WHERE d_moy = CAST(1 + 1 AS BIGINT)) AS t10 ON t4.I_ITEM_SK = t10.I_ITEM_SK AND t4.W_WAREHOUSE_SK = t10.W_WAREHOUSE_SK
ORDER BY t4.W_WAREHOUSE_SK NULLS FIRST, t4.I_ITEM_SK NULLS FIRST, t4.D_MOY NULLS FIRST, t4.MEAN NULLS FIRST, t4.COV NULLS FIRST, t10.D_MOY NULLS FIRST, t10.MEAN NULLS FIRST, t10.COV NULLS FIRST