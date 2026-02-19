INSERT INTO customer (c_birth_year, c_customer_sk, c_last_name, c_first_name) VALUES (1970, 1000, 'Smith', 'John');
INSERT INTO customer (c_birth_year, c_customer_sk, c_last_name, c_first_name) VALUES (1972, 1001, 'Doe', 'Jane');
INSERT INTO date_dim (d_date_sk, d_month_seq, d_date) VALUES (2000, 1213, '2002-06-15');
INSERT INTO store_sales (ss_customer_sk, ss_list_price, ss_sold_date_sk, ss_wholesale_cost) VALUES (1000, 170.0, 2000, 80.0);
INSERT INTO store_sales (ss_customer_sk, ss_list_price, ss_sold_date_sk, ss_wholesale_cost) VALUES (1001, 180.0, 2000, 78.0);