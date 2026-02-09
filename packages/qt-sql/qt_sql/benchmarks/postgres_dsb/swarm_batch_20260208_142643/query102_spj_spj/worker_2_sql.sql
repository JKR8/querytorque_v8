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
    MIN(ss_item_sk),
    MIN(ss_ticket_number),
    MIN(ws_order_number),
    MIN(c_customer_sk),
    MIN(cd_demo_sk),
    MIN(hd_demo_sk)
FROM store_sales
JOIN filtered_d1 d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN filtered_item i ON ss_item_sk = i.i_item_sk
JOIN inventory inv ON inv_item_sk = ss_item_sk AND inv_date_sk = ss_sold_date_sk
JOIN customer c ON ss_customer_sk = c_customer_sk
JOIN customer_demographics cd ON c_current_cdemo_sk = cd_demo_sk
JOIN household_demographics hd ON c_current_hdemo_sk = hd_demo_sk
JOIN filtered_ca ca ON c_current_addr_sk = ca.ca_address_sk
JOIN warehouse w ON w_warehouse_sk = inv_warehouse_sk
JOIN store s ON s_state = w_state
JOIN web_sales ws ON ws_item_sk = ss_item_sk 
                  AND ws_bill_customer_sk = c_customer_sk
                  AND ws_warehouse_sk = w_warehouse_sk
JOIN date_dim d2 ON ws_sold_date_sk = d2.d_date_sk
WHERE d2.d_date BETWEEN d1.d_date AND (d1.d_date + INTERVAL '30 DAY')
  AND inv_quantity_on_hand >= ss_quantity
  AND ws_wholesale_cost BETWEEN 34 AND 54;