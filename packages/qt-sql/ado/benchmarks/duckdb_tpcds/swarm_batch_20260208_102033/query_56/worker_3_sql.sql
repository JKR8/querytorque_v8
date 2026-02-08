WITH filtered_items AS (
  SELECT
    i_item_id,
    i_item_sk
  FROM item
  WHERE
    i_color IN ('powder', 'green', 'cyan')
),
filtered_dates AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    d_year = 2000
    AND d_moy = 2
),
filtered_addresses AS (
  SELECT
    ca_address_sk
  FROM customer_address
  WHERE
    ca_gmt_offset = -6
),
ss AS (
  SELECT
    fi.i_item_id,
    SUM(ss_ext_sales_price) AS total_sales
  FROM store_sales
  JOIN filtered_items fi ON ss_item_sk = fi.i_item_sk
  JOIN filtered_dates fd ON ss_sold_date_sk = fd.d_date_sk
  JOIN filtered_addresses fa ON ss_addr_sk = fa.ca_address_sk
  GROUP BY
    fi.i_item_id
),
cs AS (
  SELECT
    fi.i_item_id,
    SUM(cs_ext_sales_price) AS total_sales
  FROM catalog_sales
  JOIN filtered_items fi ON cs_item_sk = fi.i_item_sk
  JOIN filtered_dates fd ON cs_sold_date_sk = fd.d_date_sk
  JOIN filtered_addresses fa ON cs_bill_addr_sk = fa.ca_address_sk
  GROUP BY
    fi.i_item_id
),
ws AS (
  SELECT
    fi.i_item_id,
    SUM(ws_ext_sales_price) AS total_sales
  FROM web_sales
  JOIN filtered_items fi ON ws_item_sk = fi.i_item_sk
  JOIN filtered_dates fd ON ws_sold_date_sk = fd.d_date_sk
  JOIN filtered_addresses fa ON ws_bill_addr_sk = fa.ca_address_sk
  GROUP BY
    fi.i_item_id
)
SELECT
  i_item_id,
  SUM(total_sales) AS total_sales
FROM (
  SELECT * FROM ss
  UNION ALL
  SELECT * FROM cs
  UNION ALL
  SELECT * FROM ws
) AS tmp1
GROUP BY
  i_item_id
ORDER BY
  total_sales,
  i_item_id
LIMIT 100