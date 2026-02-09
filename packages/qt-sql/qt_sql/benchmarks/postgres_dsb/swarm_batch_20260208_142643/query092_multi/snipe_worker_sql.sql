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
item_avg_discount AS (
    SELECT 
        ws_item_sk,
        1.3 * AVG(ws_ext_discount_amt) AS avg_threshold
    FROM web_sales
    JOIN filtered_date ON ws_sold_date_sk = filtered_date.d_date_sk
    WHERE ws_wholesale_cost BETWEEN 26 AND 46
      AND ws_sales_price / ws_list_price BETWEEN 34 * 0.01 AND 49 * 0.01
    GROUP BY ws_item_sk
)
SELECT
    SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales
JOIN filtered_item ON web_sales.ws_item_sk = filtered_item.i_item_sk
JOIN filtered_date ON web_sales.ws_sold_date_sk = filtered_date.d_date_sk
LEFT JOIN item_avg_discount ON web_sales.ws_item_sk = item_avg_discount.ws_item_sk
WHERE ws_wholesale_cost BETWEEN 26 AND 46
  AND ws_ext_discount_amt > item_avg_discount.avg_threshold
ORDER BY
    SUM(ws_ext_discount_amt)
LIMIT 100