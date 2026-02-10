WITH date_range AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('2002-01-26' AS DATE) 
                     AND (CAST('2002-01-26' AS DATE) + INTERVAL 30 DAY)
), filtered_sales AS (
    SELECT cs_item_sk, cs_ext_sales_price
    FROM catalog_sales
    WHERE cs_sold_date_sk IN (SELECT d_date_sk FROM date_range)
), item_sales AS (
    SELECT 
        i.i_item_sk,
        i.i_item_id,
        i.i_item_desc,
        i.i_category,
        i.i_class,
        i.i_current_price,
        SUM(fs.cs_ext_sales_price) AS itemrevenue
    FROM item i
    JOIN filtered_sales fs ON i.i_item_sk = fs.cs_item_sk
    WHERE i.i_category IN ('Shoes', 'Books', 'Women')
    GROUP BY i.i_item_sk, i.i_item_id, i.i_item_desc, i.i_category, i.i_class, i.i_current_price
)
SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    itemrevenue,
    (itemrevenue * 100.0) / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio
FROM item_sales
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100;