WITH filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Children', 'Electronics', 'Women')
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
),
filtered_customer AS (
    SELECT c_customer_sk, c_current_addr_sk, c_current_hdemo_sk
    FROM customer
    WHERE c_current_addr_sk IN (SELECT ca_address_sk FROM filtered_ca)
      AND c_current_hdemo_sk IN (SELECT hd_demo_sk FROM filtered_hd)
),
filtered_d1 AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_year = 2000
),
filtered_store_sales AS (
    SELECT ss_ticket_number, ss_item_sk, ss_customer_sk
    FROM store_sales
    WHERE ss_sales_price / ss_list_price BETWEEN 34 * 0.01 AND 54 * 0.01
      AND ss_item_sk IN (SELECT i_item_sk FROM filtered_item)
      AND ss_customer_sk IN (SELECT c_customer_sk FROM filtered_customer)
),
filtered_store_returns AS (
    SELECT sr_ticket_number, sr_item_sk, sr_returned_date_sk
    FROM store_returns
    WHERE sr_item_sk IN (SELECT ss_item_sk FROM filtered_store_sales)
      AND sr_ticket_number IN (SELECT ss_ticket_number FROM filtered_store_sales)
      AND sr_returned_date_sk IN (SELECT d_date_sk FROM filtered_d1)
),
store_sales_returns_joined AS (
    SELECT ss.ss_ticket_number, ss.ss_item_sk, ss.ss_customer_sk,
           sr.sr_returned_date_sk
    FROM filtered_store_sales ss
    JOIN filtered_store_returns sr
      ON ss.ss_ticket_number = sr.sr_ticket_number
     AND ss.ss_item_sk = sr.sr_item_sk
),
filtered_web_sales AS (
    SELECT ws_order_number, ws_bill_customer_sk, ws_item_sk, ws_sold_date_sk
    FROM web_sales
    WHERE ws_item_sk IN (SELECT sr_item_sk FROM filtered_store_returns)
      AND ws_bill_customer_sk IN (SELECT ss_customer_sk FROM store_sales_returns_joined)
),
final_join AS (
    SELECT c.c_customer_sk,
           ssr.ss_item_sk,
           ssr.ss_ticket_number AS sr_ticket_number,
           ws.ws_order_number,
           d1.d_date,
           ws.ws_sold_date_sk
    FROM store_sales_returns_joined ssr
    JOIN filtered_customer c ON ssr.ss_customer_sk = c.c_customer_sk
    JOIN filtered_web_sales ws ON ws.ws_bill_customer_sk = ssr.ss_customer_sk
                              AND ws.ws_item_sk = ssr.ss_item_sk
    JOIN filtered_d1 d1 ON ssr.sr_returned_date_sk = d1.d_date_sk
)
SELECT MIN(c_customer_sk),
       MIN(ss_item_sk),
       MIN(sr_ticket_number),
       MIN(ws_order_number)
FROM final_join f
JOIN date_dim d2 ON f.ws_sold_date_sk = d2.d_date_sk
WHERE d2.d_date BETWEEN f.d_date AND (f.d_date + INTERVAL '90 DAY')