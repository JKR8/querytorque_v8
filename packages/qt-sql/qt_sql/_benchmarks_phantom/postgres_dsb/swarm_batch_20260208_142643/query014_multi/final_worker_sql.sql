WITH filtered_date AS (
    SELECT d_date_sk, d_year, d_week_seq
    FROM date_dim
    WHERE d_year BETWEEN 2000 AND 2002
),
filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_class_id, i_category_id
    FROM item
    WHERE i_category IN ('Electronics', 'Home', 'Men')
      AND i_manager_id BETWEEN 25 AND 34
),
fused_sales AS (
    SELECT 
        ss_item_sk AS item_sk,
        fi.i_brand_id,
        fi.i_class_id,
        fi.i_category_id,
        'store' AS channel,
        ss_quantity * ss_list_price AS sales_amount,
        fd.d_year,
        fd.d_week_seq
    FROM store_sales
    JOIN filtered_date fd ON ss_sold_date_sk = fd.d_date_sk
    JOIN filtered_item fi ON ss_item_sk = fi.i_item_sk
    WHERE ss_wholesale_cost BETWEEN 34 AND 54
    
    UNION ALL
    
    SELECT 
        cs_item_sk AS item_sk,
        fi.i_brand_id,
        fi.i_class_id,
        fi.i_category_id,
        'catalog' AS channel,
        cs_quantity * cs_list_price AS sales_amount,
        fd.d_year,
        fd.d_week_seq
    FROM catalog_sales
    JOIN filtered_date fd ON cs_sold_date_sk = fd.d_date_sk
    JOIN filtered_item fi ON cs_item_sk = fi.i_item_sk
    WHERE cs_wholesale_cost BETWEEN 34 AND 54
    
    UNION ALL
    
    SELECT 
        ws_item_sk AS item_sk,
        fi.i_brand_id,
        fi.i_class_id,
        fi.i_category_id,
        'web' AS channel,
        ws_quantity * ws_list_price AS sales_amount,
        fd.d_year,
        fd.d_week_seq
    FROM web_sales
    JOIN filtered_date fd ON ws_sold_date_sk = fd.d_date_sk
    JOIN filtered_item fi ON ws_item_sk = fi.i_item_sk
    WHERE ws_wholesale_cost BETWEEN 34 AND 54
),
cross_items AS (
    SELECT DISTINCT item_sk AS ss_item_sk
    FROM fused_sales
    GROUP BY item_sk
    HAVING COUNT(DISTINCT channel) = 3
),
avg_sales AS (
    SELECT AVG(sales_amount) AS average_sales
    FROM fused_sales
),
week_seq_2000 AS (
    SELECT d_week_seq
    FROM date_dim
    WHERE d_year = 2000 AND d_moy = 12 AND d_dom = 17
),
week_seq_2001 AS (
    SELECT d_week_seq
    FROM date_dim
    WHERE d_year = 2000 + 1 AND d_moy = 12 AND d_dom = 17
),
weekly_aggregates AS (
    SELECT 
        fs.i_brand_id,
        fs.i_class_id,
        fs.i_category_id,
        SUM(CASE WHEN fs.d_week_seq = (SELECT d_week_seq FROM week_seq_2001) THEN fs.sales_amount ELSE 0 END) AS ty_sales,
        COUNT(CASE WHEN fs.d_week_seq = (SELECT d_week_seq FROM week_seq_2001) THEN 1 END) AS ty_number_sales,
        SUM(CASE WHEN fs.d_week_seq = (SELECT d_week_seq FROM week_seq_2000) THEN fs.sales_amount ELSE 0 END) AS ly_sales,
        COUNT(CASE WHEN fs.d_week_seq = (SELECT d_week_seq FROM week_seq_2000) THEN 1 END) AS ly_number_sales
    FROM fused_sales fs
    WHERE fs.channel = 'store'
      AND fs.item_sk IN (SELECT ss_item_sk FROM cross_items)
    GROUP BY fs.i_brand_id, fs.i_class_id, fs.i_category_id
    HAVING 
        SUM(CASE WHEN fs.d_week_seq = (SELECT d_week_seq FROM week_seq_2001) THEN fs.sales_amount ELSE 0 END) > (SELECT average_sales FROM avg_sales)
        AND SUM(CASE WHEN fs.d_week_seq = (SELECT d_week_seq FROM week_seq_2000) THEN fs.sales_amount ELSE 0 END) > (SELECT average_sales FROM avg_sales)
)
SELECT 
    'store' AS ty_channel,
    i_brand_id AS ty_brand,
    i_class_id AS ty_class,
    i_category_id AS ty_category,
    ty_sales,
    ty_number_sales,
    'store' AS ly_channel,
    i_brand_id AS ly_brand,
    i_class_id AS ly_class,
    i_category_id AS ly_category,
    ly_sales,
    ly_number_sales
FROM weekly_aggregates
ORDER BY ty_channel, ty_brand, ty_class, ty_category
LIMIT 100;