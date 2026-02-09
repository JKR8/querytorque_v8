WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy = 12 
      AND d_year = 1998
),
filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_brand
    FROM item
    WHERE i_manager_id = 1
),
filtered_time AS (
    SELECT t_time_sk, t_hour, t_minute
    FROM time_dim
    WHERE t_meal_time = 'breakfast' 
       OR t_meal_time = 'dinner'
),
web_agg AS (
    SELECT 
        ws_item_sk AS sold_item_sk,
        t_hour,
        t_minute,
        SUM(ws_ext_sales_price) AS ext_price
    FROM web_sales
    JOIN filtered_date ON d_date_sk = ws_sold_date_sk
    JOIN filtered_item ON ws_item_sk = i_item_sk
    JOIN filtered_time ON ws_sold_time_sk = t_time_sk
    GROUP BY ws_item_sk, t_hour, t_minute
),
catalog_agg AS (
    SELECT 
        cs_item_sk AS sold_item_sk,
        t_hour,
        t_minute,
        SUM(cs_ext_sales_price) AS ext_price
    FROM catalog_sales
    JOIN filtered_date ON d_date_sk = cs_sold_date_sk
    JOIN filtered_item ON cs_item_sk = i_item_sk
    JOIN filtered_time ON cs_sold_time_sk = t_time_sk
    GROUP BY cs_item_sk, t_hour, t_minute
),
store_agg AS (
    SELECT 
        ss_item_sk AS sold_item_sk,
        t_hour,
        t_minute,
        SUM(ss_ext_sales_price) AS ext_price
    FROM store_sales
    JOIN filtered_date ON d_date_sk = ss_sold_date_sk
    JOIN filtered_item ON ss_item_sk = i_item_sk
    JOIN filtered_time ON ss_sold_time_sk = t_time_sk
    GROUP BY ss_item_sk, t_hour, t_minute
),
union_agg AS (
    SELECT sold_item_sk, t_hour, t_minute, ext_price FROM web_agg
    UNION ALL
    SELECT sold_item_sk, t_hour, t_minute, ext_price FROM catalog_agg
    UNION ALL
    SELECT sold_item_sk, t_hour, t_minute, ext_price FROM store_agg
)
SELECT
    i_brand_id AS brand_id,
    i_brand AS brand,
    t_hour,
    t_minute,
    SUM(ext_price) AS ext_price
FROM union_agg
JOIN filtered_item ON sold_item_sk = i_item_sk
GROUP BY i_brand_id, i_brand, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id