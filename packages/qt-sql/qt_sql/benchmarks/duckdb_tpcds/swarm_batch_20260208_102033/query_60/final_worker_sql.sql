WITH filtered_items AS (
  SELECT i_item_id, i_item_sk
  FROM item
  WHERE i_category = 'Children'
),
filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000 AND d_moy = 8
),
filtered_address AS (
  SELECT ca_address_sk
  FROM customer_address
  WHERE ca_gmt_offset = -7
),
unified_sales AS (
  -- Store sales
  SELECT
    i.i_item_id,
    ss.ss_ext_sales_price AS sales_price
  FROM store_sales ss
  JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN filtered_address ca ON ss.ss_addr_sk = ca.ca_address_sk

  UNION ALL

  -- Catalog sales
  SELECT
    i.i_item_id,
    cs.cs_ext_sales_price AS sales_price
  FROM catalog_sales cs
  JOIN filtered_items i ON cs.cs_item_sk = i.i_item_sk
  JOIN filtered_dates d ON cs.cs_sold_date_sk = d.d_date_sk
  JOIN filtered_address ca ON cs.cs_bill_addr_sk = ca.ca_address_sk

  UNION ALL

  -- Web sales
  SELECT
    i.i_item_id,
    ws.ws_ext_sales_price AS sales_price
  FROM web_sales ws
  JOIN filtered_items i ON ws.ws_item_sk = i.i_item_sk
  JOIN filtered_dates d ON ws.ws_sold_date_sk = d.d_date_sk
  JOIN filtered_address ca ON ws.ws_bill_addr_sk = ca.ca_address_sk
)
SELECT
  i_item_id,
  SUM(sales_price) AS total_sales
FROM unified_sales
GROUP BY i_item_id
ORDER BY i_item_id, total_sales
LIMIT 100