WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1213 AND 1213 + 11
),
filtered_sales AS (
    SELECT 
        ss.ss_net_profit,
        s.s_state,
        s.s_county
    FROM store_sales ss
    JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN store s ON ss.ss_store_sk = s.s_store_sk
),
top_states AS (
    SELECT s_state
    FROM (
        SELECT 
            s_state,
            RANK() OVER (PARTITION BY s_state ORDER BY SUM(ss_net_profit) DESC) AS ranking
        FROM filtered_sales
        GROUP BY s_state
    ) AS tmp1
    WHERE ranking <= 5
)
SELECT
    SUM(ss_net_profit) AS total_sum,
    s_state,
    s_county,
    GROUPING(s_state) + GROUPING(s_county) AS lochierarchy,
    RANK() OVER (
        PARTITION BY 
            GROUPING(s_state) + GROUPING(s_county),
            CASE WHEN GROUPING(s_county) = 0 THEN s_state END
        ORDER BY SUM(ss_net_profit) DESC
    ) AS rank_within_parent
FROM filtered_sales
WHERE s_state IN (SELECT s_state FROM top_states)
GROUP BY ROLLUP(s_state, s_county)
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN s_state END,
    rank_within_parent
LIMIT 100