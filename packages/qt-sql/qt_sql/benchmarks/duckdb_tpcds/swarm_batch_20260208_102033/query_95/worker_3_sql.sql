WITH date_filtered AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1999-2-01' AND (
        CAST('1999-2-01' AS DATE) + INTERVAL '60' DAY
    )
), address_filtered AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state = 'NC'
), website_filtered AS (
    SELECT web_site_sk
    FROM web_site
    WHERE web_company_name = 'pri'
), ws_wh_agg AS (
    SELECT ws_order_number
    FROM web_sales
    GROUP BY ws_order_number
    HAVING COUNT(DISTINCT ws_warehouse_sk) > 1
), returned_multi_warehouse_orders AS (
    SELECT DISTINCT wr_order_number
    FROM web_returns
    INNER JOIN ws_wh_agg ON web_returns.wr_order_number = ws_wh_agg.ws_order_number
)
SELECT
    COUNT(DISTINCT ws1.ws_order_number) AS "order count",
    SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
    SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
INNER JOIN date_filtered ON ws1.ws_ship_date_sk = date_filtered.d_date_sk
INNER JOIN address_filtered ON ws1.ws_ship_addr_sk = address_filtered.ca_address_sk
INNER JOIN website_filtered ON ws1.ws_web_site_sk = website_filtered.web_site_sk
WHERE ws1.ws_order_number IN (
    SELECT wr_order_number
    FROM returned_multi_warehouse_orders
)
ORDER BY
    COUNT(DISTINCT ws1.ws_order_number)
LIMIT 100