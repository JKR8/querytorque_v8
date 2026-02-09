WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1221 AND 1221 + 11
),
filtered_sales AS (
    SELECT 
        ss_store_sk,
        ss_item_sk,
        SUM(ss_sales_price) AS revenue
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    GROUP BY ss_store_sk, ss_item_sk
),
store_avg AS (
    SELECT 
        ss_store_sk,
        AVG(revenue) AS ave
    FROM filtered_sales
    GROUP BY ss_store_sk
),
filtered_low_revenue AS (
    SELECT 
        fs.ss_store_sk,
        fs.ss_item_sk,
        fs.revenue
    FROM filtered_sales fs
    JOIN store_avg sa ON fs.ss_store_sk = sa.ss_store_sk
    WHERE fs.revenue <= 0.1 * sa.ave
)
SELECT 
    s_store_name,
    i_item_desc,
    flr.revenue,
    i_current_price,
    i_wholesale_cost,
    i_brand
FROM filtered_low_revenue flr
JOIN store ON s_store_sk = flr.ss_store_sk
JOIN item ON i_item_sk = flr.ss_item_sk
ORDER BY s_store_name, i_item_desc
LIMIT 100;