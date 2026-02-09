WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1213 AND 1213 + 11
),
eligible_states AS (
    SELECT 
        s_state
    FROM (
        SELECT 
            s_state,
            RANK() OVER (
                PARTITION BY s_state 
                ORDER BY SUM(ss_net_profit) DESC
            ) AS ranking
        FROM store_sales
        JOIN store ON s_store_sk = ss_store_sk
        JOIN filtered_date ON d_date_sk = ss_sold_date_sk
        GROUP BY s_state
    ) AS tmp1
    WHERE ranking <= 5
),
filtered_base AS (
    SELECT 
        ss_net_profit,
        s_state,
        s_county
    FROM store_sales
    JOIN filtered_date d1 ON d1.d_date_sk = ss_sold_date_sk
    JOIN store ON s_store_sk = ss_store_sk
    WHERE s_state IN (SELECT s_state FROM eligible_states)
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
FROM filtered_base
GROUP BY ROLLUP(s_state, s_county)
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN s_state END,
    rank_within_parent
LIMIT 100