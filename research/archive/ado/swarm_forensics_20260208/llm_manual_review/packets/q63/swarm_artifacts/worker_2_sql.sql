WITH filtered_item AS (
  SELECT i_item_sk, i_manager_id
  FROM item
  WHERE (
    i_category IN ('Books', 'Children', 'Electronics')
    AND i_class IN ('personal', 'portable', 'reference', 'self-help')
    AND i_brand IN (
      'scholaramalgamalg #14',
      'scholaramalgamalg #7',
      'exportiunivamalg #9',
      'scholaramalgamalg #9'
    )
  ) OR (
    i_category IN ('Women', 'Music', 'Men')
    AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
    AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
  )
),
filtered_date AS (
  SELECT d_date_sk, d_moy
  FROM date_dim
  WHERE d_month_seq IN (1181, 1181+1, 1181+2, 1181+3, 1181+4, 1181+5, 1181+6, 1181+7, 1181+8, 1181+9, 1181+10, 1181+11)
)
SELECT *
FROM (
  SELECT
    fi.i_manager_id,
    SUM(ss.ss_sales_price) AS sum_sales,
    AVG(SUM(ss.ss_sales_price)) OVER (PARTITION BY fi.i_manager_id) AS avg_monthly_sales
  FROM store_sales ss
  JOIN filtered_item fi ON ss.ss_item_sk = fi.i_item_sk
  JOIN filtered_date fd ON ss.ss_sold_date_sk = fd.d_date_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  GROUP BY fi.i_manager_id, fd.d_moy
) tmp1
WHERE
  CASE
    WHEN avg_monthly_sales > 0
    THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
    ELSE NULL
  END > 0.1
ORDER BY
  i_manager_id,
  avg_monthly_sales,
  sum_sales
LIMIT 100