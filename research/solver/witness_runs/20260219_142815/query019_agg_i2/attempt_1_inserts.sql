INSERT INTO customer (c_customer_sk, c_current_addr_sk, c_birth_month) VALUES (1000, 1000, 2);
INSERT INTO customer_address (ca_address_sk, ca_state) VALUES (1000, 'KS');
INSERT INTO date_dim (d_date_sk, d_moy, d_year) VALUES (1000, 2, 1999);
INSERT INTO item (i_item_sk, i_brand, i_brand_id, i_category, i_manufact, i_manufact_id) VALUES (1000, 'BrandA', 1, 'Shoes', 'ManufactX', 10);
INSERT INTO item (i_item_sk, i_brand, i_brand_id, i_category, i_manufact, i_manufact_id) VALUES (1001, 'BrandB', 2, 'Shoes', 'ManufactY', 11);
INSERT INTO store (s_store_sk) VALUES (1000);
INSERT INTO store_sales (ss_item_sk, ss_customer_sk, ss_sold_date_sk, ss_store_sk, ss_wholesale_cost, ss_ext_sales_price) VALUES (1000, 1000, 1000, 1000, 80.0, 100.0);
INSERT INTO store_sales (ss_item_sk, ss_customer_sk, ss_sold_date_sk, ss_store_sk, ss_wholesale_cost, ss_ext_sales_price) VALUES (1001, 1000, 1000, 1000, 85.0, 150.0);