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
filtered_date_d1 AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_year = 2000
),
filtered_date_d2 AS (
    SELECT d_date_sk, d_date
    FROM date_dim
),
store_sales_filtered AS (
    SELECT 
        ss_ticket_number,
        ss_customer_sk,
        ss_item_sk,
        ss_sales_price,
        ss_list_price
    FROM store_sales
    JOIN filtered_item ON ss_item_sk = i_item_sk
    WHERE ss_sales_price / ss_list_price BETWEEN 34 * 0.01 AND 54 * 0.01
),
sr_base AS (
    SELECT 
        sr_ticket_number,
        sr_item_sk,
        sr_returned_date_sk
    FROM store_returns
    JOIN filtered_item ON sr_item_sk = i_item_sk
),
ss_sr_joined AS (
    SELECT 
        ss.ss_customer_sk,
        ss.ss_item_sk,
        sr.sr_returned_date_sk
    FROM store_sales_filtered ss
    JOIN sr_base sr ON ss.ss_ticket_number = sr.sr_ticket_number 
                    AND ss.ss_item_sk = sr.sr_item_sk
),
core_chain AS (
    SELECT 
        ssr.ss_customer_sk,
        ssr.ss_item_sk,
        ssr.sr_returned_date_sk,
        c.c_customer_sk,
        c.c_first_name,
        c.c_last_name,
        ca.ca_address_sk,
        hd.hd_demo_sk
    FROM ss_sr_joined ssr
    JOIN customer c ON ssr.ss_customer_sk = c.c_customer_sk
    JOIN filtered_ca ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN filtered_hd hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
),
final_join AS (
    SELECT 
        cc.c_customer_sk,
        cc.c_first_name,
        cc.c_last_name,
        cc.sr_returned_date_sk,
        cc.ss_item_sk,
        ws.ws_sold_date_sk
    FROM core_chain cc
    JOIN web_sales ws ON cc.ss_customer_sk = ws.ws_bill_customer_sk 
                      AND cc.ss_item_sk = ws.ws_item_sk
),
date_filtered AS (
    SELECT 
        fj.c_customer_sk,
        fj.c_first_name,
        fj.c_last_name
    FROM final_join fj
    JOIN filtered_date_d1 d1 ON fj.sr_returned_date_sk = d1.d_date_sk
    JOIN filtered_date_d2 d2 ON fj.ws_sold_date_sk = d2.d_date_sk
    WHERE d2.d_date BETWEEN d1.d_date AND (d1.d_date + INTERVAL '90 DAY')
)
SELECT 
    c_customer_sk,
    c_first_name,
    c_last_name,
    COUNT(*) AS cnt
FROM date_filtered
GROUP BY c_customer_sk, c_first_name, c_last_name
ORDER BY cnt
