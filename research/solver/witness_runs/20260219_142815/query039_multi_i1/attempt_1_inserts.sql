INSERT INTO date_dim (d_date_sk, d_moy, d_year) VALUES (3000, 2, 2002);
INSERT INTO date_dim (d_date_sk, d_moy, d_year) VALUES (3001, 3, 2002);
INSERT INTO inventory (inv_date_sk, inv_item_sk, inv_quantity_on_hand, inv_warehouse_sk) VALUES (3000, 2000, 0, 1000);
INSERT INTO inventory (inv_date_sk, inv_item_sk, inv_quantity_on_hand, inv_warehouse_sk) VALUES (3000, 2000, 0, 1000);
INSERT INTO inventory (inv_date_sk, inv_item_sk, inv_quantity_on_hand, inv_warehouse_sk) VALUES (3000, 2000, 200, 1000);
INSERT INTO inventory (inv_date_sk, inv_item_sk, inv_quantity_on_hand, inv_warehouse_sk) VALUES (3001, 2000, 0, 1000);
INSERT INTO inventory (inv_date_sk, inv_item_sk, inv_quantity_on_hand, inv_warehouse_sk) VALUES (3001, 2000, 200, 1000);
INSERT INTO item (i_category, i_item_sk, i_manager_id) VALUES ('Men', 2000, 90);
INSERT INTO warehouse (w_warehouse_name, w_warehouse_sk) VALUES ('Warehouse1', 1000);