SELECT t3.C_LAST_NAME, t3.C_FIRST_NAME, t3.S_STORE_NAME, SUM(t3.NETPAID) AS PAID
FROM (SELECT customer.c_last_name AS C_LAST_NAME, customer.c_first_name AS C_FIRST_NAME, t.s_store_name AS S_STORE_NAME, SUM(store_sales.ss_net_paid) AS NETPAID
FROM store_sales
INNER JOIN store_returns ON store_sales.ss_ticket_number = store_returns.sr_ticket_number AND store_sales.ss_item_sk = store_returns.sr_item_sk
INNER JOIN (SELECT *
FROM store
WHERE s_market_id = 8) AS t ON store_sales.ss_store_sk = t.s_store_sk
INNER JOIN item ON store_sales.ss_item_sk = item.i_item_sk
INNER JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk AND customer.c_birth_country <> UPPER(customer_address.ca_country) AND t.s_zip = customer_address.ca_zip
GROUP BY customer.c_last_name, customer.c_first_name, t.s_store_name, customer_address.ca_state, t.s_state, item.i_color, item.i_current_price, item.i_manager_id, item.i_units, item.i_size
HAVING item.i_color = 'peach') AS t3
GROUP BY t3.C_LAST_NAME, t3.C_FIRST_NAME, t3.S_STORE_NAME
HAVING SUM(t3.NETPAID) > (((SELECT 0.05 * AVG(t8.NETPAID)
FROM (SELECT SUM(store_sales0.ss_net_paid) AS NETPAID
FROM store_sales AS store_sales0,
store_returns AS store_returns0,
store AS store0,
item AS item0,
customer AS customer0,
customer_address AS customer_address0
WHERE store_sales0.ss_ticket_number = store_returns0.sr_ticket_number AND store_sales0.ss_item_sk = store_returns0.sr_item_sk AND (store_sales0.ss_customer_sk = customer0.c_customer_sk AND store_sales0.ss_item_sk = item0.i_item_sk) AND (store_sales0.ss_store_sk = store0.s_store_sk AND customer0.c_current_addr_sk = customer_address0.ca_address_sk AND (customer0.c_birth_country <> UPPER(customer_address0.ca_country) AND (store0.s_zip = customer_address0.ca_zip AND store0.s_market_id = 8)))
GROUP BY customer0.c_last_name, customer0.c_first_name, store0.s_store_name, customer_address0.ca_state, store0.s_state, item0.i_color, item0.i_current_price, item0.i_manager_id, item0.i_units, item0.i_size) AS t8)))
ORDER BY t3.C_LAST_NAME, t3.C_FIRST_NAME, t3.S_STORE_NAME