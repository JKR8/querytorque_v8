-- TPC-DS Query 36 (Optimized - ROLLUP decomposition + predicate pushdown)
-- Transform: rollup_to_union_decomposition (DuckDB 1.56x winner, adapted for Snowflake)
-- Key changes:
--   1. Pre-filter dates + stores into CTEs (predicate pushdown)
--   2. Single scan of store_sales with early joins
--   3. Aggregate ONCE at detail level (category x class = ~300 rows)
--   4. Decompose ROLLUP into 3 UNION ALL levels on pre-aggregated data
--   5. Window function on small result set only
WITH filtered_dates AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2002
),
filtered_stores AS (
    SELECT s_store_sk FROM store
    WHERE s_state IN ('SD','TN','GA','SC','MO','AL','MI','OH')
),
base_agg AS (
    SELECT
        i.i_category,
        i.i_class,
        SUM(ss.ss_net_profit) AS sum_net_profit,
        SUM(ss.ss_ext_sales_price) AS sum_sales_price
    FROM store_sales ss
        JOIN filtered_dates d ON d.d_date_sk = ss.ss_sold_date_sk
        JOIN item i ON i.i_item_sk = ss.ss_item_sk
        JOIN filtered_stores s ON s.s_store_sk = ss.ss_store_sk
    GROUP BY i.i_category, i.i_class
),
rollup_levels AS (
    -- Level 0: category + class detail
    SELECT
        sum_net_profit / sum_sales_price AS gross_margin,
        i_category,
        i_class,
        0 AS lochierarchy,
        0 AS g_class
    FROM base_agg

    UNION ALL

    -- Level 1: category totals
    SELECT
        SUM(sum_net_profit) / SUM(sum_sales_price) AS gross_margin,
        i_category,
        NULL AS i_class,
        1 AS lochierarchy,
        1 AS g_class
    FROM base_agg
    GROUP BY i_category

    UNION ALL

    -- Level 2: grand total
    SELECT
        SUM(sum_net_profit) / SUM(sum_sales_price) AS gross_margin,
        NULL AS i_category,
        NULL AS i_class,
        2 AS lochierarchy,
        1 AS g_class
    FROM base_agg
)
SELECT
    gross_margin,
    i_category,
    i_class,
    lochierarchy,
    RANK() OVER (
        PARTITION BY lochierarchy,
                     CASE WHEN g_class = 0 THEN i_category END
        ORDER BY gross_margin ASC
    ) AS rank_within_parent
FROM rollup_levels
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN i_category END,
    rank_within_parent
LIMIT 100;
