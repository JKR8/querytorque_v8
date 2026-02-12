WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2002-01-26' AS DATE) AND (CAST('2002-01-26' AS DATE) + INTERVAL 30 DAY)),
     filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Shoes', 'Books', 'Women')),
     joined_sales AS (SELECT 
    i.i_item_sk,
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class,
    i.i_current_price,
    cs.cs_ext_sales_price
FROM catalog_sales cs
INNER JOIN filtered_dates d ON cs.cs_sold_date_sk = d.d_date_sk
INNER JOIN filtered_items i ON cs.cs_item_sk = i.i_item_sk),
     item_aggregates AS (SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(cs_ext_sales_price) AS itemrevenue
FROM joined_sales
GROUP BY 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price),
     class_aggregates AS (SELECT 
    i.i_class,
    SUM(js.cs_ext_sales_price) AS classrevenue
FROM filtered_items i
INNER JOIN joined_sales js ON i.i_item_sk = js.i_item_sk
GROUP BY i.i_class)
SELECT 
    ia.i_item_id,
    ia.i_item_desc,
    ia.i_category,
    ia.i_class,
    ia.i_current_price,
    ia.itemrevenue,
    ia.itemrevenue * 100 / ca.classrevenue AS revenueratio
FROM item_aggregates ia
INNER JOIN class_aggregates ca ON ia.i_class = ca.i_class
ORDER BY 
    ia.i_category,
    ia.i_class,
    ia.i_item_id,
    ia.i_item_desc,
    revenueratio
LIMIT 100