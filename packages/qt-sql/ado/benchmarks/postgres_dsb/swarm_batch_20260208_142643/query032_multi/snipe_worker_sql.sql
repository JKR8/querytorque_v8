WITH date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1999-03-07' AND CAST('1999-03-07' AS DATE) + INTERVAL '90 DAY'
),
item_filter AS (
    SELECT i_item_sk, i_manufact_id, i_manager_id
    FROM item
    WHERE i_manufact_id IN (1, 78, 97, 516, 521) 
       OR i_manager_id BETWEEN 25 AND 54
),
item_avg_discount AS (
    SELECT 
        cs_item_sk,
        1.3 * AVG(cs_ext_discount_amt) AS threshold
    FROM catalog_sales
    JOIN date_filter ON cs_sold_date_sk = date_filter.d_date_sk
    WHERE cs_list_price BETWEEN 16 AND 45
      AND cs_sales_price / cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01
    GROUP BY cs_item_sk
)
SELECT
    SUM(cs.cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales cs
JOIN item_filter i ON cs.cs_item_sk = i.i_item_sk
JOIN date_filter d ON cs.cs_sold_date_sk = d.d_date_sk
LEFT JOIN item_avg_discount avgd ON cs.cs_item_sk = avgd.cs_item_sk
WHERE cs.cs_ext_discount_amt > COALESCE(avgd.threshold, 0)
ORDER BY SUM(cs.cs_ext_discount_amt)
LIMIT 100;