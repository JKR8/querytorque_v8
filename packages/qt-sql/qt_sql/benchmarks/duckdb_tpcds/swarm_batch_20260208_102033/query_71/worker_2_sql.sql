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
    WHERE t_meal_time IN ('breakfast', 'dinner')
),
web_sales_filtered AS (
    SELECT
        ws_ext_sales_price AS ext_price,
        ws_item_sk AS sold_item_sk,
        ws_sold_time_sk AS time_sk
    FROM web_sales
    JOIN filtered_date ON ws_sold_date_sk = filtered_date.d_date_sk
),
catalog_sales_filtered AS (
    SELECT
        cs_ext_sales_price AS ext_price,
        cs_item_sk AS sold_item_sk,
        cs_sold_time_sk AS time_sk
    FROM catalog_sales
    JOIN filtered_date ON cs_sold_date_sk = filtered_date.d_date_sk
),
store_sales_filtered AS (
    SELECT
        ss_ext_sales_price AS ext_price,
        ss_item_sk AS sold_item_sk,
        ss_sold_time_sk AS time_sk
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = filtered_date.d_date_sk
),
combined_sales AS (
    SELECT ext_price, sold_item_sk, time_sk FROM web_sales_filtered
    UNION ALL
    SELECT ext_price, sold_item_sk, time_sk FROM catalog_sales_filtered
    UNION ALL
    SELECT ext_price, sold_item_sk, time_sk FROM store_sales_filtered
)
SELECT
    filtered_item.i_brand_id AS brand_id,
    filtered_item.i_brand AS brand,
    filtered_time.t_hour,
    filtered_time.t_minute,
    SUM(combined_sales.ext_price) AS ext_price
FROM combined_sales
JOIN filtered_item ON combined_sales.sold_item_sk = filtered_item.i_item_sk
JOIN filtered_time ON combined_sales.time_sk = filtered_time.t_time_sk
GROUP BY
    filtered_item.i_brand,
    filtered_item.i_brand_id,
    filtered_time.t_hour,
    filtered_time.t_minute
ORDER BY
    ext_price DESC,
    brand_id