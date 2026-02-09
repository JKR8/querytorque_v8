WITH filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id = 320
),
date_range AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '2002-02-26' AND (
        CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
    )
),
item_avg_discount AS (
    SELECT
        ws_item_sk,
        1.3 * AVG(ws_ext_discount_amt) AS avg_disc
    FROM web_sales
    INNER JOIN date_range ON ws_sold_date_sk = d_date_sk
    WHERE ws_item_sk IN (SELECT i_item_sk FROM filtered_items)
    GROUP BY ws_item_sk
)
SELECT
    SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales
INNER JOIN filtered_items ON ws_item_sk = i_item_sk
INNER JOIN date_range ON ws_sold_date_sk = d_date_sk
INNER JOIN item_avg_discount ON web_sales.ws_item_sk = item_avg_discount.ws_item_sk
WHERE ws_ext_discount_amt > avg_disc
ORDER BY SUM(ws_ext_discount_amt)
LIMIT 100;