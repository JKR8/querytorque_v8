WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN CAST('2002-01-26' AS DATE) AND (CAST('2002-01-26' AS DATE) + INTERVAL '30' DAY)
),
filtered_item AS (
    SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price
    FROM item
    WHERE i_category IN ('Shoes', 'Books', 'Women')
),
class_totals AS (
    SELECT 
        i.i_class,
        SUM(cs.cs_ext_sales_price) AS class_revenue
    FROM catalog_sales cs
    JOIN filtered_item i ON cs.cs_item_sk = i.i_item_sk
    JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
    GROUP BY i.i_class
),
item_sales AS (
    SELECT 
        i.i_item_id,
        i.i_item_desc,
        i.i_category,
        i.i_class,
        i.i_current_price,
        SUM(cs.cs_ext_sales_price) AS itemrevenue
    FROM catalog_sales cs
    JOIN filtered_item i ON cs.cs_item_sk = i.i_item_sk
    JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
    GROUP BY 
        i.i_item_id,
        i.i_item_desc,
        i.i_category,
        i.i_class,
        i.i_current_price
),
ranked_items AS (
    SELECT 
        it.i_item_id,
        it.i_item_desc,
        it.i_category,
        it.i_class,
        it.i_current_price,
        it.itemrevenue,
        (it.itemrevenue * 100.0 / ct.class_revenue) AS revenueratio,
        ROW_NUMBER() OVER (
            PARTITION BY it.i_category, it.i_class 
            ORDER BY (it.itemrevenue * 100.0 / ct.class_revenue) DESC,
                     it.i_item_id,
                     it.i_item_desc
        ) AS rn
    FROM item_sales it
    JOIN class_totals ct ON it.i_class = ct.i_class
)
SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    itemrevenue,
    revenueratio
FROM ranked_items
WHERE rn <= 100
ORDER BY 
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT 100;