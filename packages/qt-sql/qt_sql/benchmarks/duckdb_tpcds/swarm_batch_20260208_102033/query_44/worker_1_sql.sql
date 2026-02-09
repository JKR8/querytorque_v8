WITH store_avg AS (
    SELECT AVG(ss_net_profit) AS avg_profit
    FROM store_sales
    WHERE ss_store_sk = 146 
      AND ss_addr_sk IS NULL
    GROUP BY ss_store_sk
),
item_stats AS (
    SELECT 
        ss_item_sk AS item_sk,
        AVG(ss_net_profit) AS avg_net_profit
    FROM store_sales
    WHERE ss_store_sk = 146
    GROUP BY ss_item_sk
    HAVING AVG(ss_net_profit) > 0.9 * (SELECT avg_profit FROM store_avg)
),
ranked_items AS (
    SELECT 
        item_sk,
        avg_net_profit,
        RANK() OVER (ORDER BY avg_net_profit ASC) AS rnk_asc,
        RANK() OVER (ORDER BY avg_net_profit DESC) AS rnk_desc
    FROM item_stats
),
asceding AS (
    SELECT item_sk, rnk_asc AS rnk
    FROM ranked_items
    WHERE rnk_asc < 11
),
descending AS (
    SELECT item_sk, rnk_desc AS rnk
    FROM ranked_items
    WHERE rnk_desc < 11
)
SELECT
    asceding.rnk,
    i1.i_product_name AS best_performing,
    i2.i_product_name AS worst_performing
FROM asceding
JOIN descending ON asceding.rnk = descending.rnk
JOIN item AS i1 ON i1.i_item_sk = asceding.item_sk
JOIN item AS i2 ON i2.i_item_sk = descending.item_sk
ORDER BY asceding.rnk
LIMIT 100;