WITH filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Children', 'Electronics', 'Women')
),
filtered_customer AS (
    SELECT 
        c_customer_sk,
        c_current_addr_sk,
        c_current_hdemo_sk
    FROM customer
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('GA', 'IA', 'LA', 'MO', 'SD')
),
filtered_household_demographics AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_income_band_sk BETWEEN 12 AND 18
      AND hd_buy_potential = '0-500'
),
filtered_d1 AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_year = 2000
),
store_sales_filtered AS (
    SELECT 
        ss_ticket_number,
        ss_item_sk,
        ss_customer_sk,
        ss_sales_price,
        ss_list_price
    FROM store_sales
    WHERE ss_sales_price / ss_list_price BETWEEN 34 * 0.01 AND 54 * 0.01
      AND ss_item_sk IN (SELECT i_item_sk FROM filtered_item)
      AND ss_customer_sk IN (SELECT c_customer_sk FROM filtered_customer)
),
qualified_ss_sr AS (
    SELECT 
        ss.ss_ticket_number,
        ss.ss_item_sk,
        ss.ss_customer_sk,
        sr.sr_item_sk,
        sr.sr_returned_date_sk
    FROM store_sales_filtered ss
    JOIN store_returns sr 
        ON ss.ss_ticket_number = sr.sr_ticket_number 
       AND ss.ss_item_sk = sr.sr_item_sk
),
qualified_customers AS (
    SELECT 
        c.c_customer_sk,
        c.c_current_addr_sk,
        c.c_current_hdemo_sk
    FROM filtered_customer c
    JOIN filtered_customer_address ca 
        ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN filtered_household_demographics hd 
        ON c.c_current_hdemo_sk = hd.hd_demo_sk
)
SELECT
    MIN(c.c_customer_sk),
    MIN(ss.ss_item_sk),
    MIN(sr.sr_ticket_number),
    MIN(ws.ws_order_number)
FROM qualified_ss_sr ss
JOIN qualified_customers c 
    ON ss.ss_customer_sk = c.c_customer_sk
JOIN web_sales ws 
    ON ss.ss_customer_sk = ws.ws_bill_customer_sk 
   AND ss.ss_item_sk = ws.ws_item_sk
JOIN filtered_d1 d1 
    ON ss.sr_returned_date_sk = d1.d_date_sk
JOIN date_dim d2 
    ON ws.ws_sold_date_sk = d2.d_date_sk
WHERE d2.d_date BETWEEN d1.d_date AND (d1.d_date + INTERVAL '90 DAY');