INSERT INTO date_dim (d_date_sk, d_date) VALUES (1000, '1998-01-06');
INSERT INTO date_dim (d_date_sk, d_date) VALUES (1001, '1998-01-07');
INSERT INTO item (i_item_sk, i_manager_id, i_manufact_id) VALUES (1000, 75, 47);
INSERT INTO item (i_item_sk, i_manager_id, i_manufact_id) VALUES (1001, 80, 226);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1000, 1000, 120, 36, 100);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1000, 1001, 120, 36, 200);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1001, 1000, 120, 36, 300);
INSERT INTO catalog_sales (cs_item_sk, cs_sold_date_sk, cs_list_price, cs_sales_price, cs_ext_discount_amt) VALUES (1001, 1001, 120, 36, 500);