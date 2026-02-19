-- DSB indexes adapted from dsb_index_pg.sql for MySQL 8.0
-- MySQL doesn't support INCLUDE() covering index syntax
-- Key columns from INCLUDE are added as trailing index columns instead

-- store_sales indexes
CREATE INDEX idx_ss_date_profit_price ON store_sales (ss_sold_date_sk, ss_net_profit, ss_sales_price, ss_hdemo_sk, ss_store_sk, ss_cdemo_sk, ss_addr_sk);
CREATE INDEX idx_ss_date_cdemo_store_item ON store_sales (ss_sold_date_sk, ss_cdemo_sk, ss_store_sk, ss_item_sk);
CREATE INDEX idx_ss_date_item_ticket_cust_store ON store_sales (ss_sold_date_sk, ss_item_sk, ss_ticket_number, ss_customer_sk, ss_store_sk);
CREATE INDEX idx_ss_cust ON store_sales (ss_customer_sk, ss_sold_date_sk, ss_item_sk, ss_ticket_number);
CREATE INDEX idx_ss_date_store ON store_sales (ss_sold_date_sk, ss_store_sk, ss_item_sk, ss_ext_sales_price);
CREATE INDEX idx_ss_date_item ON store_sales (ss_sold_date_sk, ss_item_sk, ss_quantity, ss_list_price);
CREATE INDEX idx_ss_date_addr_item ON store_sales (ss_sold_date_sk, ss_addr_sk, ss_item_sk, ss_ext_sales_price);
CREATE INDEX idx_ss_item_ticket_cust ON store_sales (ss_item_sk, ss_ticket_number, ss_customer_sk);
CREATE INDEX idx_ss_ticket_item ON store_sales (ss_ticket_number, ss_item_sk);
CREATE INDEX idx_ss_cust_store_item_date ON store_sales (ss_customer_sk, ss_store_sk, ss_item_sk, ss_sold_date_sk, ss_ext_sales_price);

-- catalog_sales indexes
CREATE INDEX idx_cs_date_item_cdemo_cust ON catalog_sales (cs_sold_date_sk, cs_item_sk, cs_bill_cdemo_sk, cs_bill_customer_sk);
CREATE INDEX idx_cs_promo_hdemo_ship_cdemo ON catalog_sales (cs_promo_sk, cs_bill_hdemo_sk, cs_ship_date_sk, cs_bill_cdemo_sk, cs_sold_date_sk, cs_item_sk);
CREATE INDEX idx_cs_date ON catalog_sales (cs_sold_date_sk, cs_bill_customer_sk, cs_item_sk, cs_order_number);
CREATE INDEX idx_cs_ship_cc_mode_wh ON catalog_sales (cs_ship_date_sk, cs_call_center_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_order_number);
CREATE INDEX idx_cs_date_item_cust ON catalog_sales (cs_sold_date_sk, cs_item_sk, cs_bill_customer_sk, cs_order_number, cs_net_profit);
CREATE INDEX idx_cs_item_cust_date ON catalog_sales (cs_item_sk, cs_bill_customer_sk, cs_sold_date_sk, cs_net_profit);
CREATE INDEX idx_cs_item_order ON catalog_sales (cs_item_sk, cs_order_number, cs_ext_list_price);
CREATE INDEX idx_cs_date_cust ON catalog_sales (cs_sold_date_sk, cs_bill_customer_sk);

-- web_sales indexes
CREATE INDEX idx_ws_ship_order_addr_site ON web_sales (ws_ship_date_sk, ws_order_number, ws_ship_addr_sk, ws_web_site_sk, ws_warehouse_sk, ws_ext_ship_cost, ws_net_profit);
CREATE INDEX idx_ws_date_item_cust ON web_sales (ws_sold_date_sk, ws_item_sk, ws_bill_customer_sk, ws_order_number);
CREATE INDEX idx_ws_date_addr ON web_sales (ws_sold_date_sk, ws_bill_addr_sk, ws_ext_sales_price);
CREATE INDEX idx_ws_order ON web_sales (ws_order_number, ws_warehouse_sk);
CREATE INDEX idx_ws_date_cust ON web_sales (ws_sold_date_sk, ws_bill_customer_sk);

-- store_returns indexes
CREATE INDEX idx_sr_date ON store_returns (sr_returned_date_sk, sr_item_sk, sr_customer_sk, sr_ticket_number, sr_net_loss);
CREATE INDEX idx_sr_cdemo ON store_returns (sr_cdemo_sk, sr_returned_date_sk, sr_item_sk, sr_ticket_number, sr_return_quantity);

-- customer indexes
CREATE INDEX idx_cust_name ON customer (c_first_name, c_last_name);
CREATE INDEX idx_cust_addr ON customer (c_customer_sk, c_current_addr_sk);
CREATE INDEX idx_cust_birth ON customer (c_birth_month, c_current_addr_sk);

-- item indexes
CREATE INDEX idx_item_id ON item (i_item_sk, i_item_id, i_item_desc(100));
CREATE INDEX idx_item_cat_class ON item (i_category, i_class, i_item_sk);
CREATE INDEX idx_item_color ON item (i_color);
CREATE INDEX idx_item_id_sk ON item (i_item_id, i_item_sk);

-- date_dim indexes
CREATE INDEX idx_dd_year_month_sk ON date_dim (d_year, d_month_seq, d_moy, d_date_sk);
CREATE INDEX idx_dd_year_moy_sk ON date_dim (d_year, d_moy, d_date_sk);
CREATE INDEX idx_dd_sk_year_moy ON date_dim (d_date_sk, d_year, d_moy);
CREATE INDEX idx_dd_year_qoy_sk ON date_dim (d_year, d_qoy, d_date_sk);
CREATE INDEX idx_dd_moy_year_sk ON date_dim (d_moy, d_year, d_date_sk);
CREATE INDEX idx_dd_monthseq ON date_dim (d_month_seq, d_date_sk);
CREATE INDEX idx_dd_year_sk ON date_dim (d_year, d_date_sk);
CREATE INDEX idx_dd_year_moy ON date_dim (d_year, d_moy);
CREATE INDEX idx_dd_monthseq_date ON date_dim (d_month_seq, d_date);

-- store indexes
CREATE INDEX idx_store_sk ON store (s_store_sk, s_store_id, s_store_name);
CREATE INDEX idx_store_state ON store (s_state, s_store_sk);
