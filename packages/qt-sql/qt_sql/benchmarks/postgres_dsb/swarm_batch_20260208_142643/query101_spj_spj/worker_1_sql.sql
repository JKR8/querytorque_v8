WITH filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Children', 'Electronics', 'Women')
),
filtered_date1 AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_year = 2000
),
filtered_ca AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('GA', 'IA', 'LA', 'MO', 'SD')
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_income_band_sk BETWEEN 12 AND 18
      AND hd_buy_potential = '0-500'
)
SELECT
    MIN(c_customer_sk),
    MIN(ss_item_sk),
    MIN(sr_ticket_number),
    MIN(ws_order_number)
FROM store_sales
JOIN store_returns ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
JOIN web_sales ON ss_customer_sk = ws_bill_customer_sk AND sr_item_sk = ws_item_sk
JOIN filtered_item ON i_item_sk = ss_item_sk
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN filtered_ca ON c_current_addr_sk = ca_address_sk
JOIN filtered_hd ON c_current_hdemo_sk = hd_demo_sk
JOIN filtered_date1 d1 ON sr_returned_date_sk = d1.d_date_sk
JOIN date_dim d2 ON ws_sold_date_sk = d2.d_date_sk
WHERE ss_sales_price / ss_list_price BETWEEN 34 * 0.01 AND 54 * 0.01
  AND d2.d_date BETWEEN d1.d_date AND (d1.d_date + INTERVAL '90 DAY')