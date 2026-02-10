WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '2002-10-01' AND CAST('2002-10-01' AS DATE) + INTERVAL '60 DAY'
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('IL', 'KS', 'LA', 'MN', 'MT', 'SC')
),
filtered_web_site AS (
    SELECT web_site_sk
    FROM web_site
    WHERE web_gmt_offset >= -5
)
SELECT
  COUNT(DISTINCT ws_order_number) AS "order count",
  SUM(ws_ext_ship_cost) AS "total shipping cost",
  SUM(ws_net_profit) AS "total net profit"
FROM web_sales AS ws1
JOIN filtered_date ON ws1.ws_ship_date_sk = filtered_date.d_date_sk
JOIN filtered_customer_address ON ws1.ws_ship_addr_sk = filtered_customer_address.ca_address_sk
JOIN filtered_web_site ON ws1.ws_web_site_sk = filtered_web_site.web_site_sk
WHERE ws1.ws_list_price BETWEEN 184 AND 213
  AND EXISTS(
    SELECT *
    FROM web_sales AS ws2
    WHERE ws1.ws_order_number = ws2.ws_order_number
      AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
  )
  AND NOT EXISTS(
    SELECT *
    FROM web_returns AS wr1
    WHERE ws1.ws_order_number = wr1.wr_order_number
      AND wr1.wr_reason_sk IN (17, 48, 50, 56, 68)
  )
ORDER BY
  COUNT(DISTINCT ws_order_number)
LIMIT 100
