WITH filtered_dates AS (
    SELECT d_date_sk 
    FROM date_dim 
    WHERE d_year = 2002
),
filtered_stores AS (
    SELECT s_store_sk 
    FROM store 
    WHERE s_state IN ('SD', 'TN', 'GA', 'SC', 'MO', 'AL', 'MI', 'OH')
),
joined_data AS (
    SELECT 
        ss_net_profit,
        ss_ext_sales_price,
        i_category,
        i_class
    FROM store_sales
    JOIN filtered_dates ON d_date_sk = ss_sold_date_sk
    JOIN item ON i_item_sk = ss_item_sk
    JOIN filtered_stores ON s_store_sk = ss_store_sk
)
SELECT
    SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
    i_category,
    i_class,
    GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(i_category) + GROUPING(i_class), 
                     CASE WHEN GROUPING(i_class) = 0 THEN i_category END
        ORDER BY SUM(ss_net_profit) / SUM(ss_ext_sales_price) ASC
    ) AS rank_within_parent
FROM joined_data
GROUP BY ROLLUP(i_category, i_class)
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN i_category END,
    rank_within_parent
LIMIT 100