WITH state_rankings AS (
    SELECT s_state,
           RANK() OVER (ORDER BY SUM(ss_net_profit) DESC) as ranking
    FROM store_sales
    JOIN date_dim ON d_date_sk = ss_sold_date_sk
    JOIN store ON s_store_sk = ss_store_sk
    WHERE d_month_seq BETWEEN 1213 AND 1213+11
    GROUP BY s_state
),
top_states AS (
    SELECT s_state
    FROM state_rankings
    WHERE ranking <= 5
)
SELECT 
    SUM(ss_net_profit) as total_sum,
    s_state,
    s_county,
    GROUPING(s_state) + GROUPING(s_county) as lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(s_state) + GROUPING(s_county),
        CASE WHEN GROUPING(s_county) = 0 THEN s_state END 
        ORDER BY SUM(ss_net_profit) DESC
    ) as rank_within_parent
FROM store_sales
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN store ON s_store_sk = ss_store_sk
WHERE d1.d_month_seq BETWEEN 1213 AND 1213+11
  AND s_state IN (SELECT s_state FROM top_states)
GROUP BY ROLLUP(s_state, s_county)
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN s_state END,
    rank_within_parent
LIMIT 100;