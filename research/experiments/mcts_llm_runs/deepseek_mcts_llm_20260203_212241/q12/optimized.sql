WITH filtered_sales AS (
    SELECT 
        ws_item_sk,
        ws_ext_sales_price,
        d_date
    FROM 
        web_sales,
        date_dim
    WHERE 
        ws_sold_date_sk = d_date_sk
        AND d_date BETWEEN CAST('1998-04-06' AS DATE) 
                      AND (CAST('1998-04-06' AS DATE) + INTERVAL 30 DAY)
),
item_sales AS (
    SELECT 
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price,
        SUM(ws_ext_sales_price) AS itemrevenue
    FROM 
        filtered_sales,
        item
    WHERE 
        ws_item_sk = i_item_sk
        AND i_category IN ('Books', 'Sports', 'Men')
    GROUP BY 
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price
),
class_totals AS (
    SELECT 
        i_class,
        SUM(itemrevenue) AS class_total
    FROM 
        item_sales
    GROUP BY 
        i_class
)
SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    itemrevenue,
    itemrevenue * 100 / class_total AS revenueratio
FROM 
    item_sales
    JOIN class_totals USING (i_class)
ORDER BY 
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT 100