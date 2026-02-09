WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
),
filtered_stores AS (
    SELECT s_store_sk, s_state
    FROM store
    WHERE s_state IN ('SD', 'TN', 'GA', 'SC', 'MO', 'AL', 'MI', 'OH')
),
joined_base AS (
    SELECT
        i.i_category,
        i.i_class,
        ss.ss_net_profit,
        ss.ss_ext_sales_price
    FROM store_sales ss
    JOIN filtered_dates d ON d.d_date_sk = ss.ss_sold_date_sk
    JOIN item i ON i.i_item_sk = ss.ss_item_sk
    JOIN filtered_stores s ON s.s_store_sk = ss.ss_store_sk
),
level_aggregates AS (
    -- Level 0: category + class
    SELECT
        i_category,
        i_class,
        0 AS lochierarchy,
        SUM(ss_net_profit) AS sum_net_profit,
        SUM(ss_ext_sales_price) AS sum_sales_price
    FROM joined_base
    GROUP BY i_category, i_class
    
    UNION ALL
    
    -- Level 1: category only
    SELECT
        i_category,
        NULL AS i_class,
        1 AS lochierarchy,
        SUM(ss_net_profit) AS sum_net_profit,
        SUM(ss_ext_sales_price) AS sum_sales_price
    FROM joined_base
    GROUP BY i_category
    
    UNION ALL
    
    -- Level 2: total
    SELECT
        NULL AS i_category,
        NULL AS i_class,
        2 AS lochierarchy,
        SUM(ss_net_profit) AS sum_net_profit,
        SUM(ss_ext_sales_price) AS sum_sales_price
    FROM joined_base
),
with_margin AS (
    SELECT
        CASE 
            WHEN sum_sales_price = 0 THEN NULL
            ELSE sum_net_profit / sum_sales_price 
        END AS gross_margin,
        i_category,
        i_class,
        lochierarchy
    FROM level_aggregates
    WHERE sum_sales_price > 0
)
SELECT
    gross_margin,
    i_category,
    i_class,
    lochierarchy,
    RANK() OVER (
        PARTITION BY lochierarchy, 
                     CASE WHEN lochierarchy = 0 THEN i_category END
        ORDER BY gross_margin ASC
    ) AS rank_within_parent
FROM with_margin
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN i_category END,
    rank_within_parent
LIMIT 100