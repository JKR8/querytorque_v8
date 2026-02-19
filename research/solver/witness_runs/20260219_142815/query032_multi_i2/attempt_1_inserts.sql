INSERT INTO date_dim (d_date_sk, d_date) VALUES (2000, '2001-02-23');
INSERT INTO item (i_item_sk, i_manager_id, i_manufact_id) VALUES (1000, 50, 184);
INSERT INTO item (i_item_sk, i_manager_id, i_manufact_id) VALUES (1001, 50, 184);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1000, 2000, 120, 36, 10);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1000, 2000, 120, 36, 10);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1000, 2000, 120, 36, 30);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1001, 2000, 120, 36, 12);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1001, 2000, 120, 36, 12);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1001, 2000, 120, 36, 32);