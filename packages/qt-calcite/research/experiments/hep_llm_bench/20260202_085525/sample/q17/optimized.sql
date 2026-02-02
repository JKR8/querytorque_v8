SELECT item.i_item_id AS I_ITEM_ID, item.i_item_desc AS I_ITEM_DESC, store.s_state AS S_STATE, COUNT(store_sales.ss_quantity) AS STORE_SALES_QUANTITYCOUNT, AVG(store_sales.ss_quantity) AS STORE_SALES_QUANTITYAVE, STDDEV_SAMP(store_sales.ss_quantity) AS STORE_SALES_QUANTITYSTDEV, STDDEV_SAMP(store_sales.ss_quantity) / AVG(store_sales.ss_quantity) AS STORE_SALES_QUANTITYCOV, COUNT(store_returns.sr_return_quantity) AS STORE_RETURNS_QUANTITYCOUNT, AVG(store_returns.sr_return_quantity) AS STORE_RETURNS_QUANTITYAVE, STDDEV_SAMP(store_returns.sr_return_quantity) AS STORE_RETURNS_QUANTITYSTDEV, STDDEV_SAMP(store_returns.sr_return_quantity) / AVG(store_returns.sr_return_quantity) AS STORE_RETURNS_QUANTITYCOV, COUNT(catalog_sales.cs_quantity) AS CATALOG_SALES_QUANTITYCOUNT, AVG(catalog_sales.cs_quantity) AS CATALOG_SALES_QUANTITYAVE, STDDEV_SAMP(catalog_sales.cs_quantity) AS CATALOG_SALES_QUANTITYSTDEV, STDDEV_SAMP(catalog_sales.cs_quantity) / AVG(catalog_sales.cs_quantity) AS CATALOG_SALES_QUANTITYCOV
FROM store_sales
INNER JOIN store_returns ON store_sales.ss_customer_sk = store_returns.sr_customer_sk AND store_sales.ss_item_sk = store_returns.sr_item_sk AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
INNER JOIN catalog_sales ON store_returns.sr_customer_sk = catalog_sales.cs_bill_customer_sk AND store_returns.sr_item_sk = catalog_sales.cs_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_quarter_name = '2001Q1') AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')) AS t0 ON store_returns.sr_returned_date_sk = t0.d_date_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')) AS t1 ON catalog_sales.cs_sold_date_sk = t1.d_date_sk
INNER JOIN store ON store_sales.ss_store_sk = store.s_store_sk
INNER JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY item.i_item_id, item.i_item_desc, store.s_state
ORDER BY item.i_item_id NULLS FIRST, item.i_item_desc NULLS FIRST, store.s_state NULLS FIRST
FETCH NEXT 100 ROWS ONLY