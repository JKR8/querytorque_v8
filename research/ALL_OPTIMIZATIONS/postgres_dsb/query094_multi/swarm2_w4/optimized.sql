WITH filtered_dates AS (
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
),
multi_warehouse_orders AS (
    SELECT ws_order_number
    FROM web_sales
    GROUP BY ws_order_number
    HAVING COUNT(DISTINCT ws_warehouse_sk) > 1
),
returned_orders AS (
    SELECT DISTINCT wr_order_number
    FROM web_returns
    WHERE wr_reason_sk IN (17, 48, 50, 56, 68)
)
SELECT
    COUNT(DISTINCT ws1.ws_order_number) AS "order count",
    SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
    SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN filtered_dates ON ws1.ws_ship_date_sk = filtered_dates.d_date_sk
JOIN filtered_customer_address ON ws1.ws_ship_addr_sk = filtered_customer_address.ca_address_sk
JOIN filtered_web_site ON ws1.ws_web_site_sk = filtered_web_site.web_site_sk
JOIN multi_warehouse_orders mwo ON ws1.ws_order_number = mwo.ws_order_number
LEFT JOIN returned_orders ro ON ws1.ws_order_number = ro.wr_order_number
WHERE ws1.ws_list_price BETWEEN 184 AND 213
    AND ro.wr_order_number IS NULL
ORDER BY
    COUNT(DISTINCT ws_order_number)
LIMIT 100