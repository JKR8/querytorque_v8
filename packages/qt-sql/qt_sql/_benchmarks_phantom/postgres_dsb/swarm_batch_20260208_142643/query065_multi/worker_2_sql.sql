WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1213 AND 1213 + 11
),
store_sales_filtered AS (
    SELECT
        ss_store_sk,
        ss_item_sk,
        SUM(ss_sales_price) AS revenue
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = filtered_dates.d_date_sk
    WHERE ss_sales_price / ss_list_price BETWEEN 38 * 0.01 AND 48 * 0.01
    GROUP BY ss_store_sk, ss_item_sk
),
store_avg AS (
    SELECT
        ss_store_sk,
        AVG(revenue) AS ave
    FROM store_sales_filtered
    GROUP BY ss_store_sk
),
filtered_store AS (
    SELECT s_store_sk, s_store_name
    FROM store
    WHERE s_state IN ('TN', 'TX', 'VA')
),
filtered_item AS (
    SELECT i_item_sk, i_item_desc, i_current_price, i_wholesale_cost, i_brand
    FROM item
    WHERE i_manager_id BETWEEN 32 AND 36
)
SELECT
    filtered_store.s_store_name,
    filtered_item.i_item_desc,
    store_sales_filtered.revenue,
    filtered_item.i_current_price,
    filtered_item.i_wholesale_cost,
    filtered_item.i_brand
FROM store_sales_filtered
JOIN store_avg ON store_sales_filtered.ss_store_sk = store_avg.ss_store_sk
JOIN filtered_store ON store_sales_filtered.ss_store_sk = filtered_store.s_store_sk
JOIN filtered_item ON store_sales_filtered.ss_item_sk = filtered_item.i_item_sk
WHERE store_sales_filtered.revenue <= 0.1 * store_avg.ave
ORDER BY filtered_store.s_store_name, filtered_item.i_item_desc
LIMIT 100;