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
item_averages AS (
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
        RANK() OVER (ORDER BY avg_profit ASC) AS rnk_asc,
        RANK() OVER (ORDER BY avg_profit DESC) AS rnk_desc
    FROM item_averages
),
filtered_ranks AS (
    SELECT
        a.rnk_asc AS rnk,
        a.item_sk AS asc_item_sk,
        d.item_sk AS desc_item_sk
    FROM ranked_items a
    INNER JOIN ranked_items d
        ON a.rnk_asc = d.rnk_desc
    WHERE
        a.rnk_asc < 11
        AND d.rnk_desc < 11
)
SELECT
    fr.rnk,
    i1.i_product_name AS best_performing,
    i2.i_product_name AS worst_performing
FROM filtered_ranks fr
INNER JOIN item i1
    ON fr.asc_item_sk = i1.i_item_sk
INNER JOIN item i2
    ON fr.desc_item_sk = i2.i_item_sk
ORDER BY
    fr.rnk
LIMIT 100