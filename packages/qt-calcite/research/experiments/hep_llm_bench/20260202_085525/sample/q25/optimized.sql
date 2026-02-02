SELECT item.i_item_id AS I_ITEM_ID, item.i_item_desc AS I_ITEM_DESC, store.s_store_id AS S_STORE_ID, store.s_store_name AS S_STORE_NAME, SUM(store_sales.ss_net_profit) AS STORE_SALES_PROFIT, SUM(store_returns.sr_net_loss) AS STORE_RETURNS_LOSS, SUM(catalog_sales.cs_net_profit) AS CATALOG_SALES_PROFIT
FROM store_sales
INNER JOIN store_returns ON store_sales.ss_customer_sk = store_returns.sr_customer_sk AND store_sales.ss_item_sk = store_returns.sr_item_sk AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
INNER JOIN catalog_sales ON store_returns.sr_customer_sk = catalog_sales.cs_bill_customer_sk AND store_returns.sr_item_sk = catalog_sales.cs_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy = 4 AND d_year = 2001) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy >= 4 AND d_moy <= 10 AND d_year = 2001) AS t0 ON store_returns.sr_returned_date_sk = t0.d_date_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy >= 4 AND d_moy <= 10 AND d_year = 2001) AS t1 ON catalog_sales.cs_sold_date_sk = t1.d_date_sk
INNER JOIN store ON store_sales.ss_store_sk = store.s_store_sk
INNER JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY item.i_item_id, item.i_item_desc, store.s_store_id, store.s_store_name
ORDER BY item.i_item_id, item.i_item_desc, store.s_store_id, store.s_store_name
FETCH NEXT 100 ROWS ONLY