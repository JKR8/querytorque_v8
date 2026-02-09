WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN '2002-4-01' AND (
    CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY
  )
),
filtered_addresses AS (
  SELECT ca_address_sk
  FROM customer_address
  WHERE ca_state = 'WV'
),
filtered_call_centers AS (
  SELECT cc_call_center_sk
  FROM call_center
  WHERE cc_county IN (
    'Ziebach County',
    'Luce County',
    'Richland County',
    'Daviess County',
    'Barrow County'
  )
),
filtered_catalog_sales AS (
  SELECT
    cs_order_number,
    cs_ext_ship_cost,
    cs_net_profit,
    cs_warehouse_sk
  FROM catalog_sales
  WHERE cs_ship_date_sk IN (SELECT d_date_sk FROM filtered_dates)
    AND cs_ship_addr_sk IN (SELECT ca_address_sk FROM filtered_addresses)
    AND cs_call_center_sk IN (SELECT cc_call_center_sk FROM filtered_call_centers)
),
multi_warehouse_orders AS (
  SELECT DISTINCT cs_order_number
  FROM (
    SELECT
      cs_order_number,
      COUNT(DISTINCT cs_warehouse_sk) OVER (PARTITION BY cs_order_number) AS warehouse_count
    FROM filtered_catalog_sales
  )
  WHERE warehouse_count > 1
),
returned_orders AS (
  SELECT DISTINCT cr_order_number
  FROM catalog_returns
  WHERE cr_order_number IN (SELECT cs_order_number FROM filtered_catalog_sales)
),
final_orders AS (
  SELECT
    cs_order_number,
    cs_ext_ship_cost,
    cs_net_profit
  FROM filtered_catalog_sales
  WHERE cs_order_number IN (SELECT cs_order_number FROM multi_warehouse_orders)
    AND cs_order_number NOT IN (SELECT cr_order_number FROM returned_orders)
)
SELECT
  COUNT(DISTINCT cs_order_number) AS "order count",
  SUM(cs_ext_ship_cost) AS "total shipping cost",
  SUM(cs_net_profit) AS "total net profit"
FROM final_orders
ORDER BY
  COUNT(DISTINCT cs_order_number)
LIMIT 100