WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1999-03-07' AND CAST('1999-03-07' AS DATE) + INTERVAL '90 DAY'
),
filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id IN (1, 78, 97, 516, 521) 
       OR i_manager_id BETWEEN 25 AND 54
),
item_avg_discount AS (
    SELECT 
        cs_item_sk,
        1.3 * AVG(cs_ext_discount_amt) AS avg_discount_threshold
    FROM catalog_sales
    JOIN filtered_dates ON d_date_sk = cs_sold_date_sk
    WHERE cs_list_price BETWEEN 16 AND 45
      AND cs_sales_price / cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01
    GROUP BY cs_item_sk
)
SELECT
    SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales
JOIN filtered_items ON i_item_sk = cs_item_sk
JOIN filtered_dates ON d_date_sk = cs_sold_date_sk
LEFT JOIN item_avg_discount ON cs_item_sk = item_avg_discount.cs_item_sk
WHERE cs_ext_discount_amt > avg_discount_threshold
ORDER BY SUM(cs_ext_discount_amt)
LIMIT 100