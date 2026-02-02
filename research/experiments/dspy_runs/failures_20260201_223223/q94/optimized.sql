-- start query 94 in stream 0 using template query94.tpl
WITH filtered_date AS MATERIALIZED (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '2000-2-01' AND (CAST('2000-2-01' AS DATE) + INTERVAL 60 DAY)
), filtered_customer_address AS MATERIALIZED (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state = 'OK'
), filtered_web_site AS MATERIALIZED (
    SELECT web_site_sk
    FROM web_site
    WHERE web_company_name = 'pri'
), different_warehouse_orders AS MATERIALIZED (
    SELECT ws_order_number
    FROM web_sales
    GROUP BY ws_order_number
    HAVING COUNT(DISTINCT ws_warehouse_sk) > 1
), returned_orders AS MATERIALIZED (
    SELECT DISTINCT wr_order_number
    FROM web_returns
)
SELECT 
   COUNT(DISTINCT ws1.ws_order_number) AS "order count",
   SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
   SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
INNER JOIN filtered_date ON ws1.ws_ship_date_sk = filtered_date.d_date_sk
INNER JOIN filtered_customer_address ON ws1.ws_ship_addr_sk = filtered_customer_address.ca_address_sk
INNER JOIN filtered_web_site ON ws1.ws_web_site_sk = filtered_web_site.web_site_sk
INNER JOIN different_warehouse_orders ON ws1.ws_order_number = different_warehouse_orders.ws_order_number
LEFT JOIN returned_orders ON ws1.ws_order_number = returned_orders.wr_order_number
WHERE returned_orders.wr_order_number IS NULL
ORDER BY COUNT(DISTINCT ws1.ws_order_number)
LIMIT 100;

-- end query 94 in stream 0 using template query94.tpl