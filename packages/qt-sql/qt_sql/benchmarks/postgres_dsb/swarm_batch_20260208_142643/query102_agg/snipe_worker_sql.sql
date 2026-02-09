WITH filtered_d1 AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_year = 2002
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Home', 'Men', 'Music')
      AND i_manager_id IN (1, 2, 4, 11, 12, 14, 21, 29, 32, 52)
),
filtered_ca AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('GA', 'MD', 'MO', 'NC', 'OR')
)
SELECT
    cd_gender,
    cd_marital_status,
    cd_education_status,
    hd_vehicle_count,
    COUNT(*) AS cnt
FROM store_sales
JOIN filtered_item ON ss_item_sk = i_item_sk
JOIN filtered_d1 d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN inventory ON inv_item_sk = ss_item_sk AND inv_date_sk = ss_sold_date_sk
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_demographics ON c_current_cdemo_sk = cd_demo_sk
JOIN household_demographics ON c_current_hdemo_sk = hd_demo_sk
JOIN filtered_ca ON c_current_addr_sk = ca_address_sk
JOIN web_sales ON ws_item_sk = ss_item_sk
                 AND ws_bill_customer_sk = c_customer_sk
                 AND ws_warehouse_sk = inv_warehouse_sk
JOIN date_dim d2 ON ws_sold_date_sk = d2.d_date_sk
JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
JOIN store ON s_state = w_state
WHERE d2.d_date BETWEEN d1.d_date AND (d1.d_date + INTERVAL '30 DAY')
  AND inv_quantity_on_hand >= ss_quantity
  AND ws_wholesale_cost BETWEEN 34 AND 54
GROUP BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    hd_vehicle_count
ORDER BY
    cnt