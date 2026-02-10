WITH base_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id = 320
),
avg_discount AS (
    SELECT 
        ws_item_sk,
        1.3 * AVG(ws_ext_discount_amt) AS avg_threshold
    FROM web_sales
    INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk
    WHERE d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL 90 DAY)
    GROUP BY ws_item_sk
),
branch1 AS (
    SELECT ws_ext_discount_amt
    FROM web_sales
    INNER JOIN base_items ON ws_item_sk = base_items.i_item_sk
    INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk
    INNER JOIN avg_discount ON ws_item_sk = avg_discount.ws_item_sk
    WHERE d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL 90 DAY)
      AND ws_ext_discount_amt > avg_discount.avg_threshold
      AND EXISTS (SELECT 1 FROM base_items WHERE i_item_sk = ws_item_sk)
)
SELECT SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM branch1
ORDER BY SUM(ws_ext_discount_amt)
LIMIT 100