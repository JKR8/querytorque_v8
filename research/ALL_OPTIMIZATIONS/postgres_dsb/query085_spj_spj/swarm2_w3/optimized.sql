WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_country = 'United States'
      AND (
        (ca_state IN ('IA', 'PA', 'TX'))
        OR (ca_state IN ('GA', 'MO', 'SD'))
        OR (ca_state IN ('LA', 'TX', 'VA'))
      )
),
-- Pre-filter web_sales by both OR conditions
filtered_web_sales AS (
    SELECT 
        ws_quantity,
        ws_item_sk,
        ws_order_number,
        ws_web_page_sk,
        ws_sold_date_sk,
        ws_sales_price,
        ws_net_profit
    FROM web_sales
    WHERE (
        (ws_sales_price BETWEEN 100.00 AND 150.00 AND ws_net_profit BETWEEN 100 AND 200)
        OR (ws_sales_price BETWEEN 50.00 AND 100.00 AND ws_net_profit BETWEEN 150 AND 300)
        OR (ws_sales_price BETWEEN 150.00 AND 200.00 AND ws_net_profit BETWEEN 50 AND 250)
    )
),
-- Pre-filter web_returns with join columns
filtered_web_returns AS (
    SELECT 
        wr_refunded_cash,
        wr_fee,
        wr_item_sk,
        wr_order_number,
        wr_refunded_cdemo_sk,
        wr_returning_cdemo_sk,
        wr_refunded_addr_sk,
        wr_reason_sk
    FROM web_returns
),
-- First branch of demographic condition
demo_condition1 AS (
    SELECT cd1.cd_demo_sk AS cd1_sk, cd2.cd_demo_sk AS cd2_sk
    FROM customer_demographics cd1
    JOIN customer_demographics cd2 ON cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = cd2.cd_education_status
    WHERE cd1.cd_marital_status = 'D'
        AND cd1.cd_education_status = 'Secondary'
),
-- Second branch of demographic condition
demo_condition2 AS (
    SELECT cd1.cd_demo_sk AS cd1_sk, cd2.cd_demo_sk AS cd2_sk
    FROM customer_demographics cd1
    JOIN customer_demographics cd2 ON cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = cd2.cd_education_status
    WHERE cd1.cd_marital_status = 'M'
        AND cd1.cd_education_status = '4 yr Degree'
),
-- Third branch of demographic condition
demo_condition3 AS (
    SELECT cd1.cd_demo_sk AS cd1_sk, cd2.cd_demo_sk AS cd2_sk
    FROM customer_demographics cd1
    JOIN customer_demographics cd2 ON cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = cd2.cd_education_status
    WHERE cd1.cd_marital_status = 'U'
        AND cd1.cd_education_status = 'Unknown'
),
-- Combine all demographic conditions
all_demo_conditions AS (
    SELECT cd1_sk, cd2_sk FROM demo_condition1
    UNION ALL
    SELECT cd1_sk, cd2_sk FROM demo_condition2
    UNION ALL
    SELECT cd1_sk, cd2_sk FROM demo_condition3
)
SELECT
    MIN(ws.ws_quantity),
    MIN(wr.wr_refunded_cash),
    MIN(wr.wr_fee),
    MIN(ws.ws_item_sk),
    MIN(wr.wr_order_number),
    MIN(dc.cd1_sk),
    MIN(dc.cd2_sk)
FROM filtered_web_sales ws
JOIN filtered_web_returns wr ON ws.ws_item_sk = wr.wr_item_sk 
    AND ws.ws_order_number = wr.wr_order_number
JOIN web_page wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
JOIN all_demo_conditions dc ON wr.wr_refunded_cdemo_sk = dc.cd1_sk 
    AND wr.wr_returning_cdemo_sk = dc.cd2_sk
JOIN filtered_customer_address ca ON wr.wr_refunded_addr_sk = ca.ca_address_sk
JOIN filtered_date dd ON ws.ws_sold_date_sk = dd.d_date_sk
JOIN reason r ON wr.wr_reason_sk = r.r_reason_sk;