INSERT INTO catalog_sales (cs_bill_customer_sk, cs_list_price, cs_sold_date_sk, cs_wholesale_cost) VALUES (1000, 250.0, 1000, 80.0);
INSERT INTO catalog_sales (cs_bill_customer_sk, cs_list_price, cs_sold_date_sk, cs_wholesale_cost) VALUES (1001, 250.0, 1000, 80.0);
INSERT INTO customer (c_birth_month, c_customer_sk) VALUES (2, 1000);
INSERT INTO customer (c_birth_month, c_customer_sk) VALUES (3, 1001);
INSERT INTO date_dim (d_date_sk, d_month_seq) VALUES (1000, 1207);
INSERT INTO store_sales (ss_customer_sk, ss_list_price, ss_sold_date_sk, ss_wholesale_cost) VALUES (1000, 250.0, 1000, 80.0);
INSERT INTO store_sales (ss_customer_sk, ss_list_price, ss_sold_date_sk, ss_wholesale_cost) VALUES (1001, 250.0, 1000, 80.0);
INSERT INTO web_sales (ws_bill_customer_sk, ws_list_price, ws_sold_date_sk, ws_wholesale_cost) VALUES (1000, 250.0, 1000, 80.0);
INSERT INTO web_sales (ws_bill_customer_sk, ws_list_price, ws_sold_date_sk, ws_wholesale_cost) VALUES (1001, 250.0, 1000, 80.0);