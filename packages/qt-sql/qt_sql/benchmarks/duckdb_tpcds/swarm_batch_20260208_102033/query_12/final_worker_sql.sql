WITH base AS (
    SELECT 
        ws_ext_sales_price,
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price
    FROM web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE i_category IN ('Books', 'Sports', 'Men')
        AND d_date BETWEEN CAST('1998-04-06' AS DATE) 
                        AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)
),
item_agg AS (
    SELECT 
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price,
        SUM(ws_ext_sales_price) AS itemrevenue
    FROM base
    GROUP BY 
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price
),
class_agg AS (
    SELECT 
        i_class,
        SUM(ws_ext_sales_price) AS classrevenue
    FROM base
    GROUP BY i_class
)
SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    itemrevenue,
    itemrevenue * 100 / classrevenue AS revenueratio
FROM item_agg
JOIN class_agg USING (i_class)
ORDER BY 
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT 100