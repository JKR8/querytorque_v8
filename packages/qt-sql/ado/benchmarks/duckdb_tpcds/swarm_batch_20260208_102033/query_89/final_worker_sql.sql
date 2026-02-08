WITH filtered_date AS (
  SELECT d_date_sk, d_moy
  FROM date_dim
  WHERE d_year = 1999
),
filtered_store AS (
  SELECT s_store_sk, s_store_name, s_company_name
  FROM store
),
item_condition1 AS (
  SELECT i_item_sk, i_category, i_class, i_brand
  FROM item
  WHERE i_category IN ('Jewelry', 'Shoes', 'Electronics')
    AND i_class IN ('semi-precious', 'athletic', 'portable')
),
item_condition2 AS (
  SELECT i_item_sk, i_category, i_class, i_brand
  FROM item
  WHERE i_category IN ('Men', 'Music', 'Women')
    AND i_class IN ('accessories', 'rock', 'maternity')
),
filtered_items AS (
  SELECT * FROM item_condition1
  UNION ALL
  SELECT * FROM item_condition2
),
monthly_sales AS (
  SELECT
    i.i_category,
    i.i_class,
    i.i_brand,
    s.s_store_name,
    s.s_company_name,
    d.d_moy,
    SUM(ss.ss_sales_price) AS sum_sales
  FROM store_sales ss
  JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
  JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN filtered_store s ON ss.ss_store_sk = s.s_store_sk
  GROUP BY
    i.i_category,
    i.i_class,
    i.i_brand,
    s.s_store_name,
    s.s_company_name,
    d.d_moy
)
SELECT *
FROM (
  SELECT
    i_category,
    i_class,
    i_brand,
    s_store_name,
    s_company_name,
    d_moy,
    sum_sales,
    AVG(sum_sales) OVER (
      PARTITION BY i_category, i_brand, s_store_name, s_company_name
    ) AS avg_monthly_sales
  FROM monthly_sales
) AS tmp1
WHERE
  CASE
    WHEN avg_monthly_sales <> 0
    THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
    ELSE NULL
  END > 0.1
ORDER BY
  sum_sales - avg_monthly_sales,
  s_store_name
LIMIT 100