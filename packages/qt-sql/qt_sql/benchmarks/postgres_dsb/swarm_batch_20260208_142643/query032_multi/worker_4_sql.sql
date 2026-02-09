WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1999-03-07' AND CAST('1999-03-07' AS DATE) + INTERVAL '90 DAY'
),
item_avg_discount AS (
    SELECT
        cs_item_sk,
        1.3 * AVG(cs_ext_discount_amt) AS avg_discount
    FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    WHERE cs_list_price BETWEEN 16 AND 45
      AND cs_sales_price / cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01
    GROUP BY cs_item_sk
),
filtered_catalog AS (
    SELECT
        cs_item_sk,
        cs_sold_date_sk,
        cs_ext_discount_amt
    FROM catalog_sales
    WHERE EXISTS (SELECT 1 FROM filtered_dates WHERE d_date_sk = cs_sold_date_sk)
),
item_manufact AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id IN (1, 78, 97, 516, 521)
),
item_manager AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manager_id BETWEEN 25 AND 54
      AND i_manufact_id NOT IN (1, 78, 97, 516, 521)
)
SELECT SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM (
    -- Branch 1: manufacturer condition
    SELECT fc.cs_ext_discount_amt
    FROM filtered_catalog fc
    JOIN item_manufact im ON fc.cs_item_sk = im.i_item_sk
    JOIN filtered_dates fd ON fc.cs_sold_date_sk = fd.d_date_sk
    JOIN item_avg_discount iad ON fc.cs_item_sk = iad.cs_item_sk
    WHERE fc.cs_ext_discount_amt > iad.avg_discount
    
    UNION ALL
    
    -- Branch 2: manager condition (excluding manufacturer items)
    SELECT fc.cs_ext_discount_amt
    FROM filtered_catalog fc
    JOIN item_manager im ON fc.cs_item_sk = im.i_item_sk
    JOIN filtered_dates fd ON fc.cs_sold_date_sk = fd.d_date_sk
    JOIN item_avg_discount iad ON fc.cs_item_sk = iad.cs_item_sk
    WHERE fc.cs_ext_discount_amt > iad.avg_discount
) combined
ORDER BY SUM(cs_ext_discount_amt)
LIMIT 100