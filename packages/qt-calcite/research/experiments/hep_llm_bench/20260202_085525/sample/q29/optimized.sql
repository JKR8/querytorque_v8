SELECT item.i_item_id AS I_ITEM_ID, item.i_item_desc AS I_ITEM_DESC, store.s_store_id AS S_STORE_ID, store.s_store_name AS S_STORE_NAME, SUM(store_sales.ss_quantity) AS STORE_SALES_QUANTITY, SUM(store_returns.sr_return_quantity) AS STORE_RETURNS_QUANTITY, SUM(catalog_sales.cs_quantity) AS CATALOG_SALES_QUANTITY
FROM store_sales
INNER JOIN store_returns ON store_sales.ss_customer_sk = store_returns.sr_customer_sk AND store_sales.ss_item_sk = store_returns.sr_item_sk AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
INNER JOIN catalog_sales ON store_returns.sr_customer_sk = catalog_sales.cs_bill_customer_sk AND store_returns.sr_item_sk = catalog_sales.cs_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy = 9 AND d_year = 1999) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy >= 9 AND d_moy <= 9 + 3 AND d_year = 1999) AS t0 ON store_returns.sr_returned_date_sk = t0.d_date_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 1999 OR d_year = CAST(1999 + 1 AS BIGINT) OR d_year = CAST(1999 + 2 AS BIGINT)) AS t1 ON catalog_sales.cs_sold_date_sk = t1.d_date_sk
INNER JOIN store ON store_sales.ss_store_sk = store.s_store_sk
INNER JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY item.i_item_id, item.i_item_desc, store.s_store_id, store.s_store_name
ORDER BY item.i_item_id, item.i_item_desc, store.s_store_id, store.s_store_name
FETCH NEXT 100 ROWS ONLY