WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN CAST('2002-05-20' AS DATE) AND (
    CAST('2002-05-20' AS DATE) + INTERVAL '30' DAY
  )
),
filtered_items AS (
  SELECT 
    i_item_sk,
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
  FROM item
  WHERE i_category IN ('Sports', 'Music', 'Shoes')
),
sales_with_items AS (
  SELECT 
    fi.i_item_id,
    fi.i_item_desc,
    fi.i_category,
    fi.i_class,
    fi.i_current_price,
    ss.ss_ext_sales_price
  FROM store_sales ss
  JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
  JOIN filtered_items fi ON ss.ss_item_sk = fi.i_item_sk
)
SELECT
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  SUM(ss_ext_sales_price) AS itemrevenue,
  SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM sales_with_items
GROUP BY
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price
ORDER BY
  i_category,
  i_class,
  i_item_id,
  i_item_desc,
  revenueratio