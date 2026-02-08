WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) 
                    AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)
),
category_books AS (
    SELECT 
        ws_ext_sales_price,
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price
    FROM web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
    WHERE i_category = 'Books'
),
category_sports AS (
    SELECT 
        ws_ext_sales_price,
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price
    FROM web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
    WHERE i_category = 'Sports'
),
category_men AS (
    SELECT 
        ws_ext_sales_price,
        i_item_id,
        i_item_desc,
        i_category,
        i_class,
        i_current_price
    FROM web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
    WHERE i_category = 'Men'
),
unioned_categories AS (
    SELECT * FROM category_books
    UNION ALL
    SELECT * FROM category_sports
    UNION ALL
    SELECT * FROM category_men
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
FROM unioned_categories
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