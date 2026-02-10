WITH filtered_sales AS (
    SELECT ws_item_sk, ws_order_number, ws_web_page_sk, ws_sold_date_sk,
           ws_quantity, ws_sales_price, ws_net_profit
    FROM web_sales
    WHERE ws_item_sk BETWEEN 2 AND 203999
      AND ((ws_sales_price BETWEEN 100.00 AND 150.00 AND ws_net_profit BETWEEN 100 AND 200)
        OR (ws_sales_price BETWEEN 50.00 AND 100.00 AND ws_net_profit BETWEEN 150 AND 300)
        OR (ws_sales_price BETWEEN 150.00 AND 200.00 AND ws_net_profit BETWEEN 50 AND 250))
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_country = 'United States'
      AND ca_state IN ('FL', 'TX', 'DE', 'IN', 'ND', 'ID', 'MT', 'IL', 'OH')
),
filtered_demographics AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE cd_demo_sk BETWEEN 78 AND 1920786
      AND cd_marital_status IN ('M', 'S', 'W')
      AND cd_education_status IN ('4 yr Degree', 'Secondary', 'Advanced Degree')
),
date_filtered AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2000
)
SELECT substr(r_reason_desc, 1, 20) as reason_desc,
       avg(ws_quantity) as avg_quantity,
       avg(wr_refunded_cash) as avg_refunded_cash,
       avg(wr_fee) as avg_fee
FROM filtered_sales ws
JOIN web_returns wr ON ws.ws_item_sk = wr.wr_item_sk 
                    AND ws.ws_order_number = wr.wr_order_number
JOIN web_page wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
JOIN filtered_demographics cd1 ON wr.wr_refunded_cdemo_sk = cd1.cd_demo_sk
JOIN filtered_demographics cd2 ON wr.wr_returning_cdemo_sk = cd2.cd_demo_sk
JOIN filtered_address ca ON wr.wr_refunded_addr_sk = ca.ca_address_sk
JOIN date_filtered d ON ws.ws_sold_date_sk = d.d_date_sk
JOIN reason r ON wr.wr_reason_sk = r.r_reason_sk
WHERE (cd1.cd_marital_status = 'M' AND cd1.cd_marital_status = cd2.cd_marital_status
       AND cd1.cd_education_status = '4 yr Degree' AND cd1.cd_education_status = cd2.cd_education_status)
   OR (cd1.cd_marital_status = 'S' AND cd1.cd_marital_status = cd2.cd_marital_status
       AND cd1.cd_education_status = 'Secondary' AND cd1.cd_education_status = cd2.cd_education_status)
   OR (cd1.cd_marital_status = 'W' AND cd1.cd_marital_status = cd2.cd_marital_status
       AND cd1.cd_education_status = 'Advanced Degree' AND cd1.cd_education_status = cd2.cd_education_status)
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 20),
         avg(ws_quantity),
         avg(wr_refunded_cash),
         avg(wr_fee)
LIMIT 100;