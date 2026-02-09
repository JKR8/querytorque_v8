WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1998-03-13' AND CAST('1998-03-13' AS DATE) + INTERVAL '90 DAY'
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id BETWEEN 341 AND 540 
       OR i_category IN ('Home', 'Men', 'Music')
),
web_sales_filtered AS (
    SELECT 
        ws.ws_item_sk,
        ws.ws_ext_discount_amt,
        ws.ws_wholesale_cost,
        ws.ws_sales_price,
        ws.ws_list_price
    FROM web_sales ws
    INNER JOIN filtered_date fd ON fd.d_date_sk = ws.ws_sold_date_sk
    WHERE ws.ws_wholesale_cost BETWEEN 26 AND 46
),
item_avg_discount AS (
    SELECT 
        wsf.ws_item_sk,
        1.3 * AVG(wsf.ws_ext_discount_amt) AS avg_discount
    FROM web_sales_filtered wsf
    WHERE wsf.ws_sales_price / wsf.ws_list_price BETWEEN 34 * 0.01 AND 49 * 0.01
    GROUP BY wsf.ws_item_sk
)
SELECT
    SUM(wsf.ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales_filtered wsf
INNER JOIN filtered_item fi ON fi.i_item_sk = wsf.ws_item_sk
INNER JOIN item_avg_discount iad ON iad.ws_item_sk = wsf.ws_item_sk
WHERE wsf.ws_ext_discount_amt > iad.avg_discount
GROUP BY ()  -- Ensures single row aggregation
ORDER BY SUM(wsf.ws_ext_discount_amt)
LIMIT 100;