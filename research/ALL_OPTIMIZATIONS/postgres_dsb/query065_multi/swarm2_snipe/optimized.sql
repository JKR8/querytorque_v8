WITH date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1213 AND 1224
),
store_sales_revenue AS (
    SELECT 
        ss_store_sk,
        ss_item_sk,
        SUM(ss_sales_price) AS revenue
    FROM store_sales
    JOIN date_filter ON ss_sold_date_sk = d_date_sk
    WHERE ss_sales_price / ss_list_price BETWEEN 38 * 0.01 AND 48 * 0.01
    GROUP BY ss_store_sk, ss_item_sk
),
store_avg_revenue AS (
    SELECT 
        ss_store_sk,
        AVG(revenue) AS ave
    FROM store_sales_revenue
    GROUP BY ss_store_sk
),
filtered_store AS (
    SELECT 
        s_store_sk,
        s_store_name
    FROM store
    WHERE s_state IN ('TN', 'TX', 'VA')
),
filtered_item AS (
    SELECT 
        i_item_sk,
        i_item_desc,
        i_current_price,
        i_wholesale_cost,
        i_brand
    FROM item
    WHERE i_manager_id BETWEEN 32 AND 36
)
SELECT 
    s.s_store_name,
    i.i_item_desc,
    sc.revenue,
    i.i_current_price,
    i.i_wholesale_cost,
    i.i_brand
FROM store_avg_revenue sb
JOIN store_sales_revenue sc ON sb.ss_store_sk = sc.ss_store_sk
JOIN filtered_store s ON sc.ss_store_sk = s.s_store_sk
JOIN filtered_item i ON sc.ss_item_sk = i.i_item_sk
WHERE sc.revenue <= 0.1 * sb.ave
ORDER BY s.s_store_name, i.i_item_desc
LIMIT 100;