WITH filtered_dates AS (
  SELECT d_date_sk, d_qoy
  FROM date_dim
  WHERE d_month_seq BETWEEN 1200 AND 1211
),
item_filter_1 AS (
  SELECT i_item_sk, i_manufact_id
  FROM item
  WHERE i_category IN ('Books', 'Children', 'Electronics')
    AND i_class IN ('personal', 'portable', 'reference', 'self-help')
    AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
),
item_filter_2 AS (
  SELECT i_item_sk, i_manufact_id
  FROM item
  WHERE i_category IN ('Women', 'Music', 'Men')
    AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
    AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
),
qualified_sales AS (
  SELECT
    i1.i_manufact_id,
    ss.ss_sales_price,
    d.d_qoy
  FROM store_sales ss
  JOIN item_filter_1 i1 ON ss.ss_item_sk = i1.i_item_sk
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  UNION ALL
  SELECT
    i2.i_manufact_id,
    ss.ss_sales_price,
    d.d_qoy
  FROM store_sales ss
  JOIN item_filter_2 i2 ON ss.ss_item_sk = i2.i_item_sk
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
),
aggregated AS (
  SELECT
    i_manufact_id,
    d_qoy,
    SUM(ss_sales_price) AS sum_sales,
    AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) AS avg_quarterly_sales
  FROM qualified_sales
  GROUP BY i_manufact_id, d_qoy
)
SELECT *
FROM aggregated
WHERE
  CASE
    WHEN avg_quarterly_sales > 0
    THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
    ELSE NULL
  END > 0.1
ORDER BY avg_quarterly_sales, sum_sales, i_manufact_id
LIMIT 100;