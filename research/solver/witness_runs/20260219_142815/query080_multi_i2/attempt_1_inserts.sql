INSERT INTO promotion (p_promo_sk, p_channel_email, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event) VALUES (1000, 'Y', 'N', 'N', 'N', 'N');
INSERT INTO item (i_item_sk, i_category, i_current_price) VALUES (1000, 'Men', 51.0);
INSERT INTO date_dim (d_date_sk, d_date) VALUES (1000, '1999-10-21');
INSERT INTO store (s_store_sk, s_store_id) VALUES (1000, 'S1');
INSERT INTO catalog_page (cp_catalog_page_sk, cp_catalog_page_id) VALUES (1000, 'CP1');
INSERT INTO web_site (web_site_sk, web_site_id) VALUES (1000, 'W1');
INSERT INTO store_sales (ss_item_sk, ss_promo_sk, ss_store_sk, ss_sold_date_sk, ss_wholesale_cost, ss_ext_sales_price, ss_net_profit) VALUES (1000, 1000, 1000, 1000, 25.0, 100.0, 20.0);
INSERT INTO catalog_sales (cs_item_sk, cs_promo_sk, cs_catalog_page_sk, cs_sold_date_sk, cs_wholesale_cost, cs_ext_sales_price, cs_net_profit) VALUES (1000, 1000, 1000, 1000, 25.0, 100.0, 20.0);
INSERT INTO web_sales (ws_item_sk, ws_promo_sk, ws_web_site_sk, ws_sold_date_sk, ws_wholesale_cost, ws_ext_sales_price, ws_net_profit) VALUES (1000, 1000, 1000, 1000, 25.0, 100.0, 20.0);