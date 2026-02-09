WITH filtered_dates AS (
  SELECT d_date_sk, d_year, d_moy
  FROM date_dim
  WHERE d_year = 1999
     OR (d_year = 1998 AND d_moy = 12)
     OR (d_year = 2000 AND d_moy = 1)
),
filtered_sales AS (
  SELECT 
    cs_item_sk,
    cs_call_center_sk,
    cs_sales_price,
    d_year,
    d_moy
  FROM catalog_sales
  JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
),
v1 AS (
  SELECT
    i_category,
    i_brand,
    cc_name,
    d_year,
    d_moy,
    SUM(cs_sales_price) AS sum_sales,
    AVG(SUM(cs_sales_price)) OVER (
      PARTITION BY i_category, i_brand, cc_name, d_year
    ) AS avg_monthly_sales,
    LAG(SUM(cs_sales_price)) OVER (
      PARTITION BY i_category, i_brand, cc_name 
      ORDER BY d_year, d_moy
    ) AS psum,
    LEAD(SUM(cs_sales_price)) OVER (
      PARTITION BY i_category, i_brand, cc_name 
      ORDER BY d_year, d_moy
    ) AS nsum
  FROM filtered_sales
  JOIN item ON cs_item_sk = i_item_sk
  JOIN call_center ON cs_call_center_sk = cc_call_center_sk
  GROUP BY i_category, i_brand, cc_name, d_year, d_moy
)
SELECT
  i_brand,
  d_year,
  avg_monthly_sales,
  sum_sales,
  psum,
  nsum
FROM v1
WHERE d_year = 1999
  AND avg_monthly_sales > 0
  AND psum IS NOT NULL
  AND nsum IS NOT NULL
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales, nsum
LIMIT 100