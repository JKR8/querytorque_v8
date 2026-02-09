WITH date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
),
addr_filter AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_country = 'United States'
      AND (ca_state IN ('IA', 'PA', 'TX')
           OR ca_state IN ('GA', 'MO', 'SD')
           OR ca_state IN ('LA', 'TX', 'VA'))
),
cd1_filter AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'D' AND cd_education_status = 'Secondary')
       OR (cd_marital_status = 'M' AND cd_education_status = '4 yr Degree')
       OR (cd_marital_status = 'U' AND cd_education_status = 'Unknown')
),
qualified_sales AS (
    SELECT 
        ws_item_sk,
        ws_order_number,
        ws_quantity,
        ws_sales_price,
        ws_net_profit,
        ws_sold_date_sk,
        ws_web_page_sk
    FROM web_sales
    WHERE (ws_sales_price BETWEEN 100.00 AND 150.00
           OR ws_sales_price BETWEEN 50.00 AND 100.00
           OR ws_sales_price BETWEEN 150.00 AND 200.00)
      AND (ws_net_profit BETWEEN 100 AND 200
           OR ws_net_profit BETWEEN 150 AND 300
           OR ws_net_profit BETWEEN 50 AND 250)
),
joined_facts AS (
    SELECT 
        r_reason_desc,
        ws_quantity,
        wr_refunded_cash,
        wr_fee,
        ws_sales_price,
        ws_net_profit,
        cd1.cd_marital_status AS cd1_marital_status,
        cd1.cd_education_status AS cd1_education_status,
        cd2.cd_marital_status AS cd2_marital_status,
        cd2.cd_education_status AS cd2_education_status,
        ca_state
    FROM qualified_sales ws
    JOIN web_returns wr ON ws.ws_item_sk = wr.wr_item_sk
        AND ws.ws_order_number = wr.wr_order_number
    JOIN web_page wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
    JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
    JOIN cd1_filter cd1 ON wr.wr_refunded_cdemo_sk = cd1.cd_demo_sk
    JOIN cd1_filter cd2 ON wr.wr_returning_cdemo_sk = cd2.cd_demo_sk
    JOIN addr_filter af ON wr.wr_refunded_addr_sk = af.ca_address_sk
    JOIN customer_address ca ON wr.wr_refunded_addr_sk = ca.ca_address_sk
    JOIN reason r ON wr.wr_reason_sk = r.r_reason_sk
),
filtered_facts AS (
    SELECT 
        r_reason_desc,
        ws_quantity,
        wr_refunded_cash,
        wr_fee
    FROM joined_facts
    WHERE (
        (cd1_marital_status = 'D' 
         AND cd1_marital_status = cd2_marital_status
         AND cd1_education_status = 'Secondary'
         AND cd1_education_status = cd2_education_status
         AND ws_sales_price BETWEEN 100.00 AND 150.00)
        OR
        (cd1_marital_status = 'M'
         AND cd1_marital_status = cd2_marital_status
         AND cd1_education_status = '4 yr Degree'
         AND cd1_education_status = cd2_education_status
         AND ws_sales_price BETWEEN 50.00 AND 100.00)
        OR
        (cd1_marital_status = 'U'
         AND cd1_marital_status = cd2_marital_status
         AND cd1_education_status = 'Unknown'
         AND cd1_education_status = cd2_education_status
         AND ws_sales_price BETWEEN 150.00 AND 200.00)
    ) AND (
        (ca_state IN ('IA', 'PA', 'TX') AND ws_net_profit BETWEEN 100 AND 200)
        OR
        (ca_state IN ('GA', 'MO', 'SD') AND ws_net_profit BETWEEN 150 AND 300)
        OR
        (ca_state IN ('LA', 'TX', 'VA') AND ws_net_profit BETWEEN 50 AND 250)
    )
)
SELECT
    SUBSTRING(r_reason_desc FROM 1 FOR 20),
    AVG(ws_quantity),
    AVG(wr_refunded_cash),
    AVG(wr_fee)
FROM filtered_facts
GROUP BY r_reason_desc
ORDER BY
    SUBSTRING(r_reason_desc FROM 1 FOR 20),
    AVG(ws_quantity),
    AVG(wr_refunded_cash),
    AVG(wr_fee)
LIMIT 100;