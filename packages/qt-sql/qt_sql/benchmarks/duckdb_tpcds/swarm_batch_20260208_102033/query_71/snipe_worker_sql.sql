WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy = 12 AND d_year = 1998
),
filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_brand
    FROM item
    WHERE i_manager_id = 1
),
filtered_time AS (
    SELECT t_time_sk, t_hour, t_minute
    FROM time_dim
    WHERE t_meal_time = 'breakfast' OR t_meal_time = 'dinner'
),
unioned_sales AS (
    SELECT
        ws_ext_sales_price AS ext_price,
        ws_item_sk AS sold_item_sk,
        ws_sold_time_sk AS time_sk
    FROM web_sales
    JOIN filtered_date ON ws_sold_date_sk = d_date_sk
    
    UNION ALL
    
    SELECT
        cs_ext_sales_price AS ext_price,
        cs_item_sk AS sold_item_sk,
        cs_sold_time_sk AS time_sk
    FROM catalog_sales
    JOIN filtered_date ON cs_sold_date_sk = d_date_sk
    
    UNION ALL
    
    SELECT
        ss_ext_sales_price AS ext_price,
        ss_item_sk AS sold_item_sk,
        ss_sold_time_sk AS time_sk
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
)
SELECT
    i_brand_id AS brand_id,
    i_brand AS brand,
    t_hour,
    t_minute,
    SUM(ext_price) AS ext_price
FROM unioned_sales
JOIN filtered_item ON sold_item_sk = i_item_sk
JOIN filtered_time ON time_sk = t_time_sk
GROUP BY i_brand_id, i_brand, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id