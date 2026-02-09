WITH monthly_sales AS (
  SELECT
    i.i_category,
    i.i_brand,
    cc.cc_name,
    d.d_year,
    d.d_moy,
    SUM(cs.cs_sales_price) AS sum_sales
  FROM catalog_sales cs
  JOIN item i ON cs.cs_item_sk = i.i_item_sk
  JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
  JOIN call_center cc ON cs.cs_call_center_sk = cc.cc_call_center_sk
  WHERE (d.d_year = 1999)
     OR (d.d_year = 1998 AND d.d_moy = 12)
     OR (d.d_year = 2000 AND d.d_moy = 1)
  GROUP BY i.i_category, i.i_brand, cc.cc_name, d.d_year, d.d_moy
),
yearly_avg AS (
  SELECT
    i_category,
    i_brand,
    cc_name,
    d_year,
    AVG(sum_sales) AS avg_monthly_sales
  FROM monthly_sales
  WHERE d_year = 1999
  GROUP BY i_category, i_brand, cc_name, d_year
),
with_adjacent AS (
  SELECT
    m.i_brand,
    m.d_year,
    a.avg_monthly_sales,
    m.sum_sales,
    LAG(m.sum_sales) OVER (PARTITION BY m.i_category, m.i_brand, m.cc_name ORDER BY m.d_year, m.d_moy) AS psum,
    LEAD(m.sum_sales) OVER (PARTITION BY m.i_category, m.i_brand, m.cc_name ORDER BY m.d_year, m.d_moy) AS nsum
  FROM monthly_sales m
  LEFT JOIN yearly_avg a ON m.i_category = a.i_category
    AND m.i_brand = a.i_brand
    AND m.cc_name = a.cc_name
    AND m.d_year = a.d_year
  WHERE m.d_year = 1999
)
SELECT
  i_brand,
  d_year,
  avg_monthly_sales,
  sum_sales,
  psum,
  nsum
FROM with_adjacent
WHERE avg_monthly_sales > 0
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales, nsum
LIMIT 100