WITH ws_wh AS (
  SELECT
    ws1.ws_order_number,
    ws1.ws_warehouse_sk AS wh1,
    ws2.ws_warehouse_sk AS wh2
  FROM web_sales AS ws1, web_sales AS ws2
  WHERE
    ws1.ws_order_number = ws2.ws_order_number
    AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
),
filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN '1999-2-01' AND (
    CAST('1999-2-01' AS DATE) + INTERVAL '60' DAY
  )
),
filtered_customer_address AS (
  SELECT ca_address_sk
  FROM customer_address
  WHERE ca_state = 'NC'
),
filtered_web_site AS (
  SELECT web_site_sk
  FROM web_site
  WHERE web_company_name = 'pri'
),
filtered_web_sales AS (
  SELECT
    ws1.ws_order_number,
    ws1.ws_ext_ship_cost,
    ws1.ws_net_profit
  FROM web_sales AS ws1
  JOIN filtered_date ON ws1.ws_ship_date_sk = filtered_date.d_date_sk
  JOIN filtered_customer_address ON ws1.ws_ship_addr_sk = filtered_customer_address.ca_address_sk
  JOIN filtered_web_site ON ws1.ws_web_site_sk = filtered_web_site.web_site_sk
),
multi_warehouse_orders AS (
  SELECT DISTINCT ws_order_number
  FROM ws_wh
),
returned_multi_warehouse_orders AS (
  SELECT DISTINCT wr_order_number
  FROM web_returns
  WHERE EXISTS (
    SELECT 1
    FROM ws_wh
    WHERE wr_order_number = ws_wh.ws_order_number
  )
)
SELECT
  COUNT(DISTINCT ws_order_number) AS "order count",
  SUM(ws_ext_ship_cost) AS "total shipping cost",
  SUM(ws_net_profit) AS "total net profit"
FROM filtered_web_sales AS ws1
WHERE
  ws1.ws_order_number IN (SELECT ws_order_number FROM multi_warehouse_orders)
  AND ws1.ws_order_number IN (SELECT wr_order_number FROM returned_multi_warehouse_orders)
ORDER BY
  COUNT(DISTINCT ws_order_number)
LIMIT 100