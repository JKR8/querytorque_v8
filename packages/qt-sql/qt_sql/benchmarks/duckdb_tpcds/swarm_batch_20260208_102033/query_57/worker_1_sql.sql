WITH filtered_date AS (
  SELECT d_date_sk, d_year, d_moy
  FROM date_dim
  WHERE (d_year = 1999)
     OR (d_year = 1998 AND d_moy = 12)
     OR (d_year = 2000 AND d_moy = 1)
),
v1 AS (
  SELECT
    i.i_category,
    i.i_brand,
    cc.cc_name,
    d.d_year,
    d.d_moy,
    SUM(cs.cs_sales_price) AS sum_sales,
    AVG(SUM(cs.cs_sales_price)) OVER (
      PARTITION BY i.i_category, i.i_brand, cc.cc_name, d.d_year
    ) AS avg_monthly_sales,
    RANK() OVER (
      PARTITION BY i.i_category, i.i_brand, cc.cc_name
      ORDER BY d.d_year, d.d_moy
    ) AS rn
  FROM catalog_sales cs
  JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
  JOIN item i ON cs.cs_item_sk = i.i_item_sk
  JOIN call_center cc ON cc.cc_call_center_sk = cs.cs_call_center_sk
  GROUP BY
    i.i_category,
    i.i_brand,
    cc.cc_name,
    d.d_year,
    d.d_moy
)
SELECT
  v1.i_brand,
  v1.d_year,
  v1.avg_monthly_sales,
  v1.sum_sales,
  v1_lag.sum_sales AS psum,
  v1_lead.sum_sales AS nsum
FROM v1
JOIN v1 AS v1_lag
  ON v1.i_category = v1_lag.i_category
 AND v1.i_brand = v1_lag.i_brand
 AND v1.cc_name = v1_lag.cc_name
 AND v1.rn = v1_lag.rn + 1
JOIN v1 AS v1_lead
  ON v1.i_category = v1_lead.i_category
 AND v1.i_brand = v1_lead.i_brand
 AND v1.cc_name = v1_lead.cc_name
 AND v1.rn = v1_lead.rn - 1
WHERE v1.d_year = 1999
  AND v1.avg_monthly_sales > 0
  AND ABS(v1.sum_sales - v1.avg_monthly_sales) / v1.avg_monthly_sales > 0.1
ORDER BY
  v1.sum_sales - v1.avg_monthly_sales,
  v1_lead.sum_sales
LIMIT 100