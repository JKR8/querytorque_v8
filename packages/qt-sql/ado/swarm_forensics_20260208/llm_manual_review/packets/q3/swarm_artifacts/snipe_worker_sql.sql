WITH filtered_dates AS (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_moy = 11
),
filtered_items AS (
  SELECT i_item_sk, i_brand_id, i_brand
  FROM item
  WHERE i_manufact_id = 816
)
SELECT
  dt.d_year,
  fi.i_brand_id AS brand_id,
  fi.i_brand AS brand,
  SUM(ss.ss_sales_price) AS sum_agg
FROM store_sales ss
JOIN filtered_dates dt ON ss.ss_sold_date_sk = dt.d_date_sk
JOIN filtered_items fi ON ss.ss_item_sk = fi.i_item_sk
GROUP BY
  dt.d_year,
  fi.i_brand,
  fi.i_brand_id
ORDER BY
  dt.d_year,
  sum_agg DESC,
  brand_id
LIMIT 100