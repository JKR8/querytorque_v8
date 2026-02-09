WITH filtered_dates AS (
  SELECT d_date_sk, d_year, d_moy
  FROM date_dim
  WHERE 
    d_year = 1999
    OR (d_year = 1998 AND d_moy = 12)
    OR (d_year = 2000 AND d_moy = 1)
),
prejoined AS (
  SELECT
    i_category,
    i_brand,
    cc_name,
    d.d_year,
    d.d_moy,
    cs_sales_price
  FROM catalog_sales cs
  JOIN filtered_dates d ON cs.cs_sold_date_sk = d.d_date_sk
  JOIN item i ON cs.cs_item_sk = i.i_item_sk
  JOIN call_center cc ON cs.cs_call_center_sk = cc.cc_call_center_sk
),
aggregated AS (
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
    RANK() OVER (
      PARTITION BY i_category, i_brand, cc_name 
      ORDER BY d_year, d_moy
    ) AS rn
  FROM prejoined
  GROUP BY i_category, i_brand, cc_name, d_year, d_moy
),
windowed AS (
  SELECT
    i_brand,
    d_year,
    avg_monthly_sales,
    sum_sales,
    LAG(sum_sales) OVER (
      PARTITION BY i_category, i_brand, cc_name 
      ORDER BY d_year, d_moy
    ) AS psum,
    LEAD(sum_sales) OVER (
      PARTITION BY i_category, i_brand, cc_name 
      ORDER BY d_year, d_moy
    ) AS nsum
  FROM aggregated
)
SELECT
  i_brand,
  d_year,
  avg_monthly_sales,
  sum_sales,
  psum,
  nsum
FROM windowed
WHERE
  d_year = 1999
  AND avg_monthly_sales > 0
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
  AND psum IS NOT NULL
  AND nsum IS NOT NULL
ORDER BY
  sum_sales - avg_monthly_sales,
  nsum
LIMIT 100;