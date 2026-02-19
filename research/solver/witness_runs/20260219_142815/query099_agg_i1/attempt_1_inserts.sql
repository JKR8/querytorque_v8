INSERT INTO warehouse (w_warehouse_sk, w_warehouse_name, w_gmt_offset) VALUES (1000, 'WarehouseAlphaLocation1', -5);
INSERT INTO warehouse (w_warehouse_sk, w_warehouse_name, w_gmt_offset) VALUES (1001, 'WarehouseBetaLocation2', -5);
INSERT INTO call_center (cc_call_center_sk, cc_class, cc_name) VALUES (2000, 'small', 'CallCenterA');
INSERT INTO call_center (cc_call_center_sk, cc_class, cc_name) VALUES (2001, 'small', 'CallCenterB');
INSERT INTO ship_mode (sm_ship_mode_sk, sm_type) VALUES (3000, 'EXPRESS');
INSERT INTO date_dim (d_date_sk, d_month_seq) VALUES (4000, 1195);
INSERT INTO catalog_sales (cs_call_center_sk, cs_list_price, cs_ship_date_sk, cs_ship_mode_sk, cs_sold_date_sk, cs_warehouse_sk) VALUES (2000, 100.0, 4000, 3000, 3980, 1000);
INSERT INTO catalog_sales (cs_call_center_sk, cs_list_price, cs_ship_date_sk, cs_ship_mode_sk, cs_sold_date_sk, cs_warehouse_sk) VALUES (2001, 90.0, 4000, 3000, 3980, 1001);