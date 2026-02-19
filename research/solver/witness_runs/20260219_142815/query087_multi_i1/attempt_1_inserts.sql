INSERT INTO customer (c_birth_year, c_customer_sk, c_last_name, c_first_name) VALUES (1956, 1000, 'Smith', 'John');
INSERT INTO customer (c_birth_year, c_customer_sk, c_last_name, c_first_name) VALUES (1958, 1001, 'Doe', 'Jane');
INSERT INTO date_dim (d_date_sk, d_month_seq, d_date) VALUES (2000, 1214, '2002-06-15');
INSERT INTO date_dim (d_date_sk, d_month_seq, d_date) VALUES (2001, 1214, '2002-06-16');
INSERT INTO store_sales (ss_customer_sk, ss_list_price, ss_sold_date_sk, ss_wholesale_cost) VALUES (1000, 250.0, 2000, 40.0);
INSERT INTO store_sales (ss_customer_sk, ss_list_price, ss_sold_date_sk, ss_wholesale_cost) VALUES (1001, 260.0, 2001, 38.0);