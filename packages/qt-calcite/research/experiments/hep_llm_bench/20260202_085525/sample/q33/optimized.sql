SELECT I_MANUFACT_ID, SUM(TOTAL_SALES) AS TOTAL_SALES
FROM (SELECT *
FROM (SELECT t3.i_manufact_id AS I_MANUFACT_ID, SUM(store_sales.ss_ext_sales_price) AS TOTAL_SALES
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 1998 AND d_moy = 5) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN (SELECT *
FROM customer_address
WHERE ca_gmt_offset = -5) AS t0 ON store_sales.ss_addr_sk = t0.ca_address_sk
INNER JOIN (SELECT *
FROM item
WHERE i_manufact_id IN (SELECT i_manufact_id AS I_MANUFACT_ID
FROM item
WHERE i_category = 'Electronics')) AS t3 ON store_sales.ss_item_sk = t3.i_item_sk
GROUP BY t3.i_manufact_id
UNION ALL
SELECT t11.i_manufact_id AS I_MANUFACT_ID, SUM(catalog_sales.cs_ext_sales_price) AS TOTAL_SALES
FROM catalog_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 1998 AND d_moy = 5) AS t7 ON catalog_sales.cs_sold_date_sk = t7.d_date_sk
INNER JOIN (SELECT *
FROM customer_address
WHERE ca_gmt_offset = -5) AS t8 ON catalog_sales.cs_bill_addr_sk = t8.ca_address_sk
INNER JOIN (SELECT *
FROM item
WHERE i_manufact_id IN (SELECT i_manufact_id AS I_MANUFACT_ID
FROM item
WHERE i_category = 'Electronics')) AS t11 ON catalog_sales.cs_item_sk = t11.i_item_sk
GROUP BY t11.i_manufact_id)
UNION ALL
SELECT t20.i_manufact_id AS I_MANUFACT_ID, SUM(web_sales.ws_ext_sales_price) AS TOTAL_SALES
FROM web_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 1998 AND d_moy = 5) AS t16 ON web_sales.ws_sold_date_sk = t16.d_date_sk
INNER JOIN (SELECT *
FROM customer_address
WHERE ca_gmt_offset = -5) AS t17 ON web_sales.ws_bill_addr_sk = t17.ca_address_sk
INNER JOIN (SELECT *
FROM item
WHERE i_manufact_id IN (SELECT i_manufact_id AS I_MANUFACT_ID
FROM item
WHERE i_category = 'Electronics')) AS t20 ON web_sales.ws_item_sk = t20.i_item_sk
GROUP BY t20.i_manufact_id) AS t24
GROUP BY I_MANUFACT_ID
ORDER BY 2
FETCH NEXT 100 ROWS ONLY