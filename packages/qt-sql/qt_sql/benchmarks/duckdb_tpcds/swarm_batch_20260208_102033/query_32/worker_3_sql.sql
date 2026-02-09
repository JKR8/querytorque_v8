WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1999-01-07' AND (CAST('1999-01-07' AS DATE) + INTERVAL '90' DAY)
),
filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id = 29
),
sales_with_dates AS (
    SELECT 
        cs_item_sk,
        cs_ext_discount_amt,
        cs_sold_date_sk
    FROM catalog_sales
    WHERE cs_sold_date_sk IN (SELECT d_date_sk FROM filtered_dates)
),
item_avg_discount AS (
    SELECT 
        cs_item_sk,
        1.3 * AVG(cs_ext_discount_amt) AS threshold
    FROM sales_with_dates
    GROUP BY cs_item_sk
),
filtered_sales AS (
    SELECT 
        s.cs_ext_discount_amt
    FROM sales_with_dates s
    JOIN filtered_items i ON s.cs_item_sk = i.i_item_sk
    JOIN filtered_dates d ON s.cs_sold_date_sk = d.d_date_sk
    JOIN item_avg_discount a ON s.cs_item_sk = a.cs_item_sk
    WHERE s.cs_ext_discount_amt > a.threshold
)
SELECT
    SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM filtered_sales
LIMIT 100