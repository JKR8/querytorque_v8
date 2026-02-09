WITH filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Children', 'Electronics', 'Women')
),
filtered_date_d1 AS (
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
),
store_return_flow AS (
    SELECT
        ss.ss_ticket_number,
        ss.ss_item_sk,
        ss.ss_customer_sk,
        ss.ss_sales_price,
        ss.ss_list_price,
        sr.sr_returned_date_sk,
        d1.d_date AS return_date
    FROM store_sales ss
    JOIN store_returns sr ON ss.ss_ticket_number = sr.sr_ticket_number
                         AND ss.ss_item_sk = sr.sr_item_sk
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    JOIN filtered_date_d1 d1 ON sr.sr_returned_date_sk = d1.d_date_sk
    WHERE ss.ss_sales_price / ss.ss_list_price BETWEEN 34 * 0.01 AND 54 * 0.01
)
SELECT
    c.c_customer_sk,
    c.c_first_name,
    c.c_last_name,
    COUNT(*) AS cnt
FROM store_return_flow srf
JOIN web_sales ws ON srf.ss_customer_sk = ws.ws_bill_customer_sk
                  AND srf.ss_item_sk = ws.ws_item_sk
JOIN date_dim d2 ON ws.ws_sold_date_sk = d2.d_date_sk
JOIN customer c ON srf.ss_customer_sk = c.c_customer_sk
JOIN filtered_ca ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN filtered_hd hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
WHERE d2.d_date BETWEEN srf.return_date AND srf.return_date + INTERVAL '90 DAY'
GROUP BY
    c.c_customer_sk,
    c.c_first_name,
    c.c_last_name
ORDER BY
    cnt