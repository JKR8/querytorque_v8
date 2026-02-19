INSERT INTO customer_address (ca_address_sk, ca_state) VALUES (1000, 'TX');
INSERT INTO date_dim (d_date_sk, d_date) VALUES (2000, '1999-10-10');
INSERT INTO web_site (web_site_sk, web_gmt_offset) VALUES (3000, -4.0);
INSERT INTO web_sales (ws_order_number, ws_ship_addr_sk, ws_ship_date_sk, ws_web_site_sk, ws_list_price, ws_ext_ship_cost, ws_net_profit, ws_warehouse_sk) VALUES (1000, 1000, 2000, 3000, 260.0, 50.0, 100.0, 2000);
INSERT INTO web_sales (ws_order_number, ws_ship_addr_sk, ws_ship_date_sk, ws_web_site_sk, ws_list_price, ws_ext_ship_cost, ws_net_profit, ws_warehouse_sk) VALUES (1000, 1000, 2000, 3000, NULL, NULL, NULL, 2001);
INSERT INTO web_sales (ws_order_number, ws_ship_addr_sk, ws_ship_date_sk, ws_web_site_sk, ws_list_price, ws_ext_ship_cost, ws_net_profit, ws_warehouse_sk) VALUES (1001, 1000, 2000, 3000, 260.0, 60.0, 120.0, 2000);
INSERT INTO web_sales (ws_order_number, ws_ship_addr_sk, ws_ship_date_sk, ws_web_site_sk, ws_list_price, ws_ext_ship_cost, ws_net_profit, ws_warehouse_sk) VALUES (1001, 1000, 2000, 3000, NULL, NULL, NULL, 2001);