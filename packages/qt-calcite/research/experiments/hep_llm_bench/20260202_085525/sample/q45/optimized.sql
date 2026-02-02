SELECT customer_address.ca_zip AS CA_ZIP, customer_address.ca_city AS CA_CITY, SUM(web_sales.ws_sales_price)
FROM web_sales
INNER JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
INNER JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_qoy = 2 AND d_year = 2001) AS t ON web_sales.ws_sold_date_sk = t.d_date_sk
INNER JOIN item ON web_sales.ws_item_sk = item.i_item_sk AND (SUBSTRING(customer_address.ca_zip, 1, 5) = '85669' OR SUBSTRING(customer_address.ca_zip, 1, 5) = '86197' OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '88274' OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '83405' OR SUBSTRING(customer_address.ca_zip, 1, 5) = '86475')) OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '85392' OR SUBSTRING(customer_address.ca_zip, 1, 5) = '85460' OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '80348' OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '81792' OR item.i_item_id IN (SELECT i_item_id AS I_ITEM_ID
FROM item
WHERE i_item_sk = 2 OR i_item_sk = 3 OR (i_item_sk = 5 OR (i_item_sk = 7 OR i_item_sk = 11)) OR (i_item_sk = 13 OR i_item_sk = 17 OR (i_item_sk = 19 OR (i_item_sk = 23 OR i_item_sk = 29))))))))
GROUP BY customer_address.ca_zip, customer_address.ca_city
ORDER BY customer_address.ca_zip, customer_address.ca_city
FETCH NEXT 100 ROWS ONLY