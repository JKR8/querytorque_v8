WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1213 AND 1213 + 11
),
filtered_stores AS (
    SELECT s_store_sk, s_state, s_county
    FROM store
),
joined_facts AS (
    SELECT
        ss_net_profit,
        s_state,
        s_county
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    JOIN filtered_stores ON ss_store_sk = s_store_sk
),
state_ranking AS (
    SELECT
        s_state,
        RANK() OVER (ORDER BY SUM(ss_net_profit) DESC) AS ranking
    FROM joined_facts
    GROUP BY s_state
),
top_states AS (
    SELECT s_state
    FROM state_ranking
    WHERE ranking <= 5
),
aggregated AS (
    SELECT
        SUM(ss_net_profit) AS total_sum,
        s_state,
        s_county,
        GROUPING(s_state) + GROUPING(s_county) AS lochierarchy
    FROM joined_facts
    WHERE s_state IN (SELECT s_state FROM top_states)
    GROUP BY ROLLUP(s_state, s_county)
)
SELECT
    total_sum,
    s_state,
    s_county,
    lochierarchy,
    RANK() OVER (
        PARTITION BY lochierarchy,
                     CASE WHEN lochierarchy = 0 THEN s_state END
        ORDER BY total_sum DESC
    ) AS rank_within_parent
FROM aggregated
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN s_state END,
    rank_within_parent
LIMIT 100