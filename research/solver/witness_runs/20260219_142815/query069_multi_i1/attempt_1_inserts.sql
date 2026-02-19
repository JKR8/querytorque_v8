INSERT INTO customer (c_customer_sk, c_current_addr_sk, c_current_cdemo_sk) VALUES (1000, 2000, 3000);
INSERT INTO customer (c_customer_sk, c_current_addr_sk, c_current_cdemo_sk) VALUES (1001, 2001, 3001);
INSERT INTO customer_address (ca_address_sk, ca_state) VALUES (2000, 'CO');
INSERT INTO customer_address (ca_address_sk, ca_state) VALUES (2001, 'NC');
INSERT INTO customer_demographics (cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating) VALUES (3000, 'M', 'S', 'Primary', 500, 'Good');
INSERT INTO customer_demographics (cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating) VALUES (3001, 'F', 'M', 'College', 750, 'Excellent');
INSERT INTO date_dim (d_date_sk, d_moy, d_year) VALUES (4000, 10, 2002);
INSERT INTO store_sales (ss_customer_sk, ss_list_price, ss_sold_date_sk) VALUES (1000, 100.0, 4000);
INSERT INTO store_sales (ss_customer_sk, ss_list_price, ss_sold_date_sk) VALUES (1001, 120.0, 4000);