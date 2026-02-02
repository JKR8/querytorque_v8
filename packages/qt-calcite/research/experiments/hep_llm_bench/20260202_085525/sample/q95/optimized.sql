SELECT COUNT(DISTINCT t5.ws_order_number) AS order count, SUM(t5.ws_ext_ship_cost) AS total shipping cost, SUM(t5.ws_net_profit) AS total net profit
FROM (SELECT *
FROM web_sales
WHERE ws_order_number IN (SELECT web_sales0.ws_order_number AS WS_ORDER_NUMBER
FROM web_sales AS web_sales0,
web_sales AS web_sales1
WHERE web_sales0.ws_order_number = web_sales1.ws_order_number AND web_sales0.ws_warehouse_sk <> web_sales1.ws_warehouse_sk) AND ws_order_number IN (SELECT web_returns.wr_order_number AS WR_ORDER_NUMBER
FROM web_returns,
(SELECT web_sales2.ws_order_number AS WS_ORDER_NUMBER, web_sales2.ws_warehouse_sk AS WH1, web_sales3.ws_warehouse_sk AS WH2
FROM web_sales AS web_sales2,
web_sales AS web_sales3
WHERE web_sales2.ws_order_number = web_sales3.ws_order_number AND web_sales2.ws_warehouse_sk <> web_sales3.ws_warehouse_sk) AS t2
WHERE web_returns.wr_order_number = t2.WS_ORDER_NUMBER)) AS t5
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '1999-02-01' AND d_date <= DATE '1999-04-02') AS t6 ON t5.ws_ship_date_sk = t6.d_date_sk
INNER JOIN (SELECT *
FROM customer_address
WHERE ca_state = 'IL') AS t7 ON t5.ws_ship_addr_sk = t7.ca_address_sk
INNER JOIN (SELECT *
FROM web_site
WHERE web_company_name = 'pri') AS t8 ON t5.ws_web_site_sk = t8.web_site_sk