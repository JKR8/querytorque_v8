WITH monthly_agg AS (
  SELECT
    ss_item_sk,
    ss_store_sk,
    d_year,
    d_moy,
    SUM(ss_sales_price) AS monthly_sales
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE (
    d_year = 2001
    OR (d_year = 2000 AND d_moy = 12)
    OR (d_year = 2002 AND d_moy = 1)
  )
  GROUP BY ss_item_sk, ss_store_sk, d_year, d_moy
),
categorized AS (
  SELECT
    i.i_category,
    i.i_brand,
    s.s_store_name,
    s.s_company_name,
    m.d_year,
    m.d_moy,
    m.monthly_sales,
    AVG(m.monthly_sales) OVER (
      PARTITION BY i.i_category, i.i_brand, s.s_store_name, s.s_company_name, m.d_year
    ) AS avg_monthly_sales,
    LAG(m.monthly_sales, 1) OVER (
      PARTITION BY i.i_category, i.i_brand, s.s_store_name, s.s_company_name
      ORDER BY m.d_year, m.d_moy
    ) AS psum,
    LEAD(m.monthly_sales, 1) OVER (
      PARTITION BY i.i_category, i.i_brand, s.s_store_name, s.s_company_name
      ORDER BY m.d_year, m.d_moy
    ) AS nsum
  FROM monthly_agg m
  JOIN item i ON m.ss_item_sk = i.i_item_sk
  JOIN store s ON m.ss_store_sk = s.s_store_sk
),
final_data AS (
  SELECT
    s_store_name,
    d_year,
    avg_monthly_sales,
    monthly_sales AS sum_sales,
    psum,
    nsum
  FROM categorized
  WHERE d_year = 2001
    AND avg_monthly_sales > 0
)
SELECT *
FROM final_data
WHERE ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales, nsum
LIMIT 100