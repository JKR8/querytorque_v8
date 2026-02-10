WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2000
),
filtered_reason AS (
    SELECT r_reason_sk, r_reason_desc
    FROM reason
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE (ca_country = 'United States' AND ca_state IN ('FL', 'TX', 'DE'))
       OR (ca_country = 'United States' AND ca_state IN ('IN', 'ND', 'ID'))
       OR (ca_country = 'United States' AND ca_state IN ('MT', 'IL', 'OH'))
),
filtered_demographics AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'M' AND cd_education_status = '4 yr Degree')
       OR (cd_marital_status = 'S' AND cd_education_status = 'Secondary')
       OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree')
)
SELECT
    SUBSTRING(r.r_reason_desc, 1, 20),
    AVG(ws.ws_quantity),
    AVG(wr.wr_refunded_cash),
    AVG(wr.wr_fee)
FROM web_returns wr
JOIN web_sales ws ON ws.ws_item_sk = wr.wr_item_sk 
    AND ws.ws_order_number = wr.wr_order_number
JOIN web_page wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
JOIN filtered_date fd ON ws.ws_sold_date_sk = fd.d_date_sk
JOIN filtered_address ca ON ca.ca_address_sk = wr.wr_refunded_addr_sk
JOIN filtered_demographics cd1 ON cd1.cd_demo_sk = wr.wr_refunded_cdemo_sk
JOIN filtered_demographics cd2 ON cd2.cd_demo_sk = wr.wr_returning_cdemo_sk
JOIN filtered_reason r ON r.r_reason_sk = wr.wr_reason_sk
WHERE (
    (
        cd1.cd_marital_status = 'M'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = '4 yr Degree'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws.ws_sales_price BETWEEN 100.00 AND 150.00
    )
    OR (
        cd1.cd_marital_status = 'S'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Secondary'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws.ws_sales_price BETWEEN 50.00 AND 100.00
    )
    OR (
        cd1.cd_marital_status = 'W'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Advanced Degree'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws.ws_sales_price BETWEEN 150.00 AND 200.00
    )
)
AND (
    (
        ws.ws_net_profit BETWEEN 100 AND 200
        AND ca.ca_state IN ('FL', 'TX', 'DE')
    )
    OR (
        ws.ws_net_profit BETWEEN 150 AND 300
        AND ca.ca_state IN ('IN', 'ND', 'ID')
    )
    OR (
        ws.ws_net_profit BETWEEN 50 AND 250
        AND ca.ca_state IN ('MT', 'IL', 'OH')
    )
)
GROUP BY r.r_reason_desc
ORDER BY
    SUBSTRING(r.r_reason_desc, 1, 20),
    AVG(ws.ws_quantity),
    AVG(wr.wr_refunded_cash),
    AVG(wr.wr_fee)
LIMIT 100