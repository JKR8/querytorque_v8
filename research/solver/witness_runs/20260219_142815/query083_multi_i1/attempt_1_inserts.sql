INSERT INTO item (i_item_sk, i_item_id, i_category, i_manager_id) VALUES (1000, 'item_1000', 'Home', 10);
INSERT INTO item (i_item_sk, i_item_id, i_category, i_manager_id) VALUES (1001, 'item_1001', 'Home', 10);
INSERT INTO date_dim (d_date_sk, d_date, d_month_seq) VALUES (2000, '2002-02-26', 100);
INSERT INTO store_returns (sr_item_sk, sr_returned_date_sk, sr_reason_sk, sr_return_amt, sr_return_quantity) VALUES (1000, 2000, 6, 250.0, 1);
INSERT INTO store_returns (sr_item_sk, sr_returned_date_sk, sr_reason_sk, sr_return_amt, sr_return_quantity) VALUES (1001, 2000, 6, 250.0, 1);
INSERT INTO catalog_returns (cr_item_sk, cr_returned_date_sk, cr_reason_sk, cr_return_amount, cr_return_quantity) VALUES (1000, 2000, 6, 250.0, 1);
INSERT INTO catalog_returns (cr_item_sk, cr_returned_date_sk, cr_reason_sk, cr_return_amount, cr_return_quantity) VALUES (1001, 2000, 6, 250.0, 1);
INSERT INTO web_returns (wr_item_sk, wr_returned_date_sk, wr_reason_sk, wr_return_amt, wr_return_quantity) VALUES (1000, 2000, 6, 250.0, 1);
INSERT INTO web_returns (wr_item_sk, wr_returned_date_sk, wr_reason_sk, wr_return_amt, wr_return_quantity) VALUES (1001, 2000, 6, 250.0, 1);