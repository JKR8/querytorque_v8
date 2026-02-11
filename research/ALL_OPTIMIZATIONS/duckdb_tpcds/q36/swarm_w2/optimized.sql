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
filtered_items AS (
    SELECT i_item_sk, i_category, i_class
    FROM item
),
joined_sales AS (
    SELECT 
        i.i_category,
        i.i_class,
        ss.ss_net_profit,
        ss.ss_ext_sales_price
    FROM store_sales ss
    JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_stores s ON ss.ss_store_sk = s.s_store_sk
    JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
),
aggregated AS (
    SELECT 
        i_category,
        i_class,
        SUM(ss_net_profit) AS sum_net_profit,
        SUM(ss_ext_sales_price) AS sum_ext_sales_price,
        SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
        GROUPING(i_category) AS g_cat,
        GROUPING(i_class) AS g_class
    FROM joined_sales
    GROUP BY ROLLUP(i_category, i_class)
)
SELECT 
    gross_margin,
    i_category,
    i_class,
    g_cat + g_class AS lochierarchy,
    RANK() OVER (
        PARTITION BY g_cat + g_class,
                     CASE WHEN g_class = 0 THEN i_category END
        ORDER BY gross_margin ASC
    ) AS rank_within_parent
FROM aggregated
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN i_category END,
    rank_within_parent
LIMIT 100