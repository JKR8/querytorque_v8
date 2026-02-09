WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_month_seq BETWEEN 1224 AND 1224 + 11
), filtered_web_sales AS (
  SELECT ws_item_sk, ws_net_paid
  FROM web_sales
  JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
)
SELECT
  SUM(ws_net_paid) AS total_sum,
  i_category,
  i_class,
  GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
  RANK() OVER (
    PARTITION BY GROUPING(i_category) + GROUPING(i_class), 
                 CASE WHEN GROUPING(i_class) = 0 THEN i_category END
    ORDER BY SUM(ws_net_paid) DESC
  ) AS rank_within_parent
FROM filtered_web_sales
JOIN item ON i_item_sk = ws_item_sk
GROUP BY ROLLUP(i_category, i_class)
ORDER BY
  lochierarchy DESC,
  CASE WHEN lochierarchy = 0 THEN i_category END,
  rank_within_parent
LIMIT 100