WITH store_threshold AS (
    SELECT AVG(ss_net_profit) * 0.9 AS threshold
    FROM store_sales
    WHERE ss_store_sk = 146
      AND ss_addr_sk IS NULL
    GROUP BY ss_store_sk
),
item_aggregates AS (
    SELECT ss_item_sk AS item_sk,
           AVG(ss_net_profit) AS avg_profit
    FROM store_sales
    WHERE ss_store_sk = 146
    GROUP BY ss_item_sk
    HAVING AVG(ss_net_profit) > (SELECT threshold FROM store_threshold)
),
ascending_ranks AS (
    SELECT item_sk,
           RANK() OVER (ORDER BY avg_profit ASC) AS rnk
    FROM item_aggregates
),
descending_ranks AS (
    SELECT item_sk,
           RANK() OVER (ORDER BY avg_profit DESC) AS rnk
    FROM item_aggregates
)
SELECT a.rnk,
       i1.i_product_name AS best_performing,
       i2.i_product_name AS worst_performing
FROM ascending_ranks a
JOIN descending_ranks d ON a.rnk = d.rnk
JOIN item i1 ON i1.i_item_sk = a.item_sk
JOIN item i2 ON i2.i_item_sk = d.item_sk
WHERE a.rnk < 11
  AND d.rnk < 11
ORDER BY a.rnk
LIMIT 100;