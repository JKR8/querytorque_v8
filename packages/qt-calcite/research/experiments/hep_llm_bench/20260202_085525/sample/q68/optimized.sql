SELECT customer.c_last_name AS C_LAST_NAME, customer.c_first_name AS C_FIRST_NAME, customer_address0.ca_city AS CA_CITY, t4.BOUGHT_CITY, t4.SS_TICKET_NUMBER, t4.EXTENDED_PRICE, t4.EXTENDED_TAX, t4.LIST_PRICE
FROM (SELECT store_sales.ss_ticket_number AS SS_TICKET_NUMBER, store_sales.ss_customer_sk AS SS_CUSTOMER_SK, customer_address.ca_city AS BOUGHT_CITY, SUM(store_sales.ss_ext_sales_price) AS EXTENDED_PRICE, SUM(store_sales.ss_ext_list_price) AS LIST_PRICE, SUM(store_sales.ss_ext_tax) AS EXTENDED_TAX
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_dom >= 1 AND d_dom <= 2 AND (d_year = 1999 OR d_year = CAST(1999 + 1 AS BIGINT) OR d_year = CAST(1999 + 2 AS BIGINT))) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN (SELECT *
FROM store
WHERE s_city IN ('Fairview', 'Midway')) AS t0 ON store_sales.ss_store_sk = t0.s_store_sk
INNER JOIN (SELECT *
FROM household_demographics
WHERE hd_dep_count = 4 OR hd_vehicle_count = 3) AS t1 ON store_sales.ss_hdemo_sk = t1.hd_demo_sk
INNER JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
GROUP BY store_sales.ss_ticket_number, store_sales.ss_customer_sk, store_sales.ss_addr_sk, customer_address.ca_city) AS t4
INNER JOIN customer ON t4.SS_CUSTOMER_SK = customer.c_customer_sk
INNER JOIN customer_address AS customer_address0 ON customer.c_current_addr_sk = customer_address0.ca_address_sk AND customer_address0.ca_city <> t4.BOUGHT_CITY
ORDER BY customer.c_last_name NULLS FIRST, t4.SS_TICKET_NUMBER NULLS FIRST
FETCH NEXT 100 ROWS ONLY