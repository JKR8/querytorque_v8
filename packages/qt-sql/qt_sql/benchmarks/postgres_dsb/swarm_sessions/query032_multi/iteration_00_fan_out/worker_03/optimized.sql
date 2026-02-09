WITH filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id IN (1, 78, 97, 516, 521)
       OR i_manager_id BETWEEN 25 AND 54
),
filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1999-03-07' AND CAST('1999-03-07' AS DATE) + INTERVAL '90 DAY'
),
date_filtered_sales AS (
    SELECT cs_item_sk, cs_ext_discount_amt,
           cs_list_price, cs_sales_price
    FROM catalog_sales
    WHERE cs_sold_date_sk IN (SELECT d_date_sk FROM filtered_dates)
),
item_avg_discount AS (
    SELECT dfs.cs_item_sk,
           1.3 * AVG(dfs.cs_ext_discount_amt) AS threshold
    FROM date_filtered_sales dfs
    JOIN filtered_items fi ON fi.i_item_sk = dfs.cs_item_sk
    WHERE dfs.cs_list_price BETWEEN 16 AND 45
      AND dfs.cs_sales_price / dfs.cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01
    GROUP BY dfs.cs_item_sk
)
SELECT SUM(dfs.cs_ext_discount_amt) AS "excess discount amount"
FROM date_filtered_sales dfs
JOIN filtered_items fi ON fi.i_item_sk = dfs.cs_item_sk
JOIN item_avg_discount iad ON iad.cs_item_sk = dfs.cs_item_sk
WHERE dfs.cs_ext_discount_amt > iad.threshold
ORDER BY SUM(dfs.cs_ext_discount_amt)
LIMIT 100;