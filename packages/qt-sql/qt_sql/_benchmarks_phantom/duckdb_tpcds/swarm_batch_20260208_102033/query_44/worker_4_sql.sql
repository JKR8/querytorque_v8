WITH store_avg AS (
    SELECT
        AVG(ss_net_profit) AS avg_profit
    FROM store_sales
    WHERE
        ss_store_sk = 146
        AND ss_addr_sk IS NULL
    GROUP BY
        ss_store_sk
),
item_profits AS (
    SELECT
        ss_item_sk AS item_sk,
        AVG(ss_net_profit) AS avg_profit
    FROM store_sales
    WHERE
        ss_store_sk = 146
    GROUP BY
        ss_item_sk
    HAVING
        AVG(ss_net_profit) > 0.9 * (SELECT avg_profit FROM store_avg)
),
ranked_items AS (
    SELECT
        item_sk,
        avg_profit,
        RANK() OVER (ORDER BY avg_profit ASC) AS asc_rnk,
        RANK() OVER (ORDER BY avg_profit DESC) AS desc_rnk
    FROM item_profits
),
ascending_top AS (
    SELECT
        item_sk,
        asc_rnk AS rnk
    FROM ranked_items
    WHERE
        asc_rnk < 11
),
descending_top AS (
    SELECT
        item_sk,
        desc_rnk AS rnk
    FROM ranked_items
    WHERE
        desc_rnk < 11
)
SELECT
    a.rnk,
    i1.i_product_name AS best_performing,
    i2.i_product_name AS worst_performing
FROM ascending_top a
JOIN descending_top d ON a.rnk = d.rnk
JOIN item i1 ON a.item_sk = i1.i_item_sk
JOIN item i2 ON d.item_sk = i2.i_item_sk
ORDER BY
    a.rnk
LIMIT 100;