WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY)
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state = 'WV'
),
filtered_call_center AS (
    SELECT cc_call_center_sk
    FROM call_center
    WHERE cc_county IN (
        'Ziebach County',
        'Luce County',
        'Richland County',
        'Daviess County',
        'Barrow County'
    )
)
SELECT
    COUNT(DISTINCT cs1.cs_order_number) AS "order count",
    SUM(cs1.cs_ext_ship_cost) AS "total shipping cost",
    SUM(cs1.cs_net_profit) AS "total net profit"
FROM catalog_sales cs1
INNER JOIN filtered_dates ON cs1.cs_ship_date_sk = filtered_dates.d_date_sk
INNER JOIN filtered_address ON cs1.cs_ship_addr_sk = filtered_address.ca_address_sk
INNER JOIN filtered_call_center ON cs1.cs_call_center_sk = filtered_call_center.cc_call_center_sk
WHERE
    EXISTS(
        SELECT *
        FROM catalog_sales cs2
        WHERE cs1.cs_order_number = cs2.cs_order_number
          AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
    )
    AND NOT EXISTS(
        SELECT *
        FROM catalog_returns cr1
        WHERE cs1.cs_order_number = cr1.cr_order_number
    )
ORDER BY COUNT(DISTINCT cs1.cs_order_number)
LIMIT 100