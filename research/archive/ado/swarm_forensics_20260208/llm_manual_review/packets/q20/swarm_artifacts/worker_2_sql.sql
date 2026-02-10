WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN CAST('2002-01-26' AS DATE) 
                   AND (CAST('2002-01-26' AS DATE) + INTERVAL '30' DAY)
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
  WHERE i_category IN ('Shoes', 'Books', 'Women')
)
SELECT
  fi.i_item_id,
  fi.i_item_desc,
  fi.i_category,
  fi.i_class,
  fi.i_current_price,
  SUM(cs.cs_ext_sales_price) AS itemrevenue,
  SUM(cs.cs_ext_sales_price) * 100 / SUM(SUM(cs.cs_ext_sales_price)) OVER (PARTITION BY fi.i_class) AS revenueratio
FROM catalog_sales cs
JOIN filtered_items fi ON cs.cs_item_sk = fi.i_item_sk
JOIN filtered_dates fd ON cs.cs_sold_date_sk = fd.d_date_sk
GROUP BY
  fi.i_item_id,
  fi.i_item_desc,
  fi.i_category,
  fi.i_class,
  fi.i_current_price
ORDER BY
  fi.i_category,
  fi.i_class,
  fi.i_item_id,
  fi.i_item_desc,
  revenueratio
LIMIT 100