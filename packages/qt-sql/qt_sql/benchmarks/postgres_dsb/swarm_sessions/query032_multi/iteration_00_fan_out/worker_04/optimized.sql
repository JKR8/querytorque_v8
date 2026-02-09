WITH filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id IN (1, 78, 97, 516, 521)
       OR i_manager_id BETWEEN 25 AND 54
),
date_range AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1999-03-07' AND CAST('1999-03-07' AS DATE) + INTERVAL '90 DAY'
),
filtered_sales AS (
    SELECT cs_item_sk, cs_ext_discount_amt
    FROM catalog_sales
    JOIN date_range ON cs_sold_date_sk = d_date_sk
),
item_thresholds AS (
    SELECT fs.cs_item_sk,
           1.3 * AVG(cs_ext_discount_amt) AS threshold
    FROM filtered_sales fs
    JOIN catalog_sales cs ON fs.cs_item_sk = cs.cs_item_sk
    JOIN date_range dr ON cs.cs_sold_date_sk = dr.d_date_sk
    WHERE cs.cs_list_price BETWEEN 16 AND 45
      AND cs.cs_sales_price / cs.cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01
    GROUP BY fs.cs_item_sk
)
SELECT SUM(fs.cs_ext_discount_amt) AS "excess discount amount"
FROM filtered_sales fs
JOIN filtered_items fi ON fs.cs_item_sk = fi.i_item_sk
JOIN item_thresholds it ON fs.cs_item_sk = it.cs_item_sk
WHERE fs.cs_ext_discount_amt > it.threshold
ORDER BY SUM(fs.cs_ext_discount_amt)
LIMIT 100