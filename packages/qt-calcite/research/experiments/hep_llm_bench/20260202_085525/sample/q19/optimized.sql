SELECT t3.i_brand_id AS BRAND_ID, t3.i_brand AS BRAND, t3.i_manufact_id AS I_MANUFACT_ID, t3.i_manufact AS I_MANUFACT, t3.EXT_PRICE
FROM (SELECT t0.i_brand, t0.i_brand_id, t0.i_manufact_id, t0.i_manufact, SUM(store_sales.ss_ext_sales_price) AS EXT_PRICE
FROM (SELECT *
FROM date_dim
WHERE d_moy = 11 AND d_year = 1998) AS t
INNER JOIN store_sales ON t.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN (SELECT *
FROM item
WHERE i_manager_id = 8) AS t0 ON store_sales.ss_item_sk = t0.i_item_sk
INNER JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
INNER JOIN store ON SUBSTRING(customer_address.ca_zip, 1, 5) <> SUBSTRING(store.s_zip, 1, 5) AND store_sales.ss_store_sk = store.s_store_sk
GROUP BY t0.i_brand, t0.i_brand_id, t0.i_manufact_id, t0.i_manufact
ORDER BY 5 DESC, t0.i_brand, t0.i_brand_id, t0.i_manufact_id, t0.i_manufact
FETCH NEXT 100 ROWS ONLY) AS t3