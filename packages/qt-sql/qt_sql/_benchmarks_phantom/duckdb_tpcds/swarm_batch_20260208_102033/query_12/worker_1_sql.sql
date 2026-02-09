WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) 
                     AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)
), filtered_item AS (
    SELECT 
        i_item_sk,
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price
    FROM item
    WHERE i_category IN ('Books', 'Sports', 'Men')
)
SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(ws_ext_sales_price) AS itemrevenue,
    SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (
        PARTITION BY i_class
    ) AS revenueratio
FROM web_sales
JOIN filtered_item ON ws_item_sk = i_item_sk
JOIN filtered_date ON ws_sold_date_sk = d_date_sk
GROUP BY
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
ORDER BY
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT 100;