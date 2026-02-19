INSERT INTO customer_demographics (cd_demo_sk, cd_education_status, cd_gender, cd_marital_status) VALUES (1000, 'Secondary', 'M', 'W');
INSERT INTO date_dim (d_date_sk, d_year) VALUES (1000, 1999);
INSERT INTO item (i_category, i_item_id, i_item_sk) VALUES (1000, 'Music', 'ITEM001');
INSERT INTO item (i_category, i_item_id, i_item_sk) VALUES (1001, 'Music', 'ITEM002');
INSERT INTO store (s_state, s_store_sk) VALUES (1000, 'OH', 1000);
INSERT INTO store_sales (ss_cdemo_sk, ss_coupon_amt, ss_item_sk, ss_list_price, ss_quantity, ss_sales_price, ss_sold_date_sk, ss_store_sk) VALUES (1000, 1000, 5.0, 1000, 25.0, 3, 20.0, 1000, 1000);
INSERT INTO store_sales (ss_cdemo_sk, ss_coupon_amt, ss_item_sk, ss_list_price, ss_quantity, ss_sales_price, ss_sold_date_sk, ss_store_sk) VALUES (1001, 1000, 3.0, 1001, 30.0, 4, 25.0, 1000, 1000);