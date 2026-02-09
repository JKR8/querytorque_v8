WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1999-03-07' AND CAST('1999-03-07' AS DATE) + INTERVAL '90 DAY'
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id IN (1, 78, 97, 516, 521)
       OR i_manager_id BETWEEN 25 AND 54
),
catalog_avg_discount AS (
    SELECT 
        cs_item_sk,
        1.3 * AVG(cs_ext_discount_amt) AS threshold
    FROM catalog_sales
    INNER JOIN filtered_date ON cs_sold_date_sk = d_date_sk
    WHERE cs_list_price BETWEEN 16 AND 45
      AND cs_sales_price / cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01
    GROUP BY cs_item_sk
)
SELECT
    SUM(cs.cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales cs
INNER JOIN filtered_item i ON cs.cs_item_sk = i.i_item_sk
INNER JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
LEFT JOIN catalog_avg_discount avg ON cs.cs_item_sk = avg.cs_item_sk
WHERE cs.cs_ext_discount_amt > avg.threshold
ORDER BY SUM(cs.cs_ext_discount_amt)
LIMIT 100;