WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_dow IN (6, 0)
      AND d_year IN (1999, 2000, 2001)
),
filtered_stores AS (
    SELECT s_store_sk
    FROM store
    WHERE s_city IN ('Five Points', 'Centerville', 'Oak Grove', 'Fairview', 'Liberty')
),
filtered_hhdemo AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_dep_count = 6 OR hd_vehicle_count = 0
),
filtered_sales AS (
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        ss_addr_sk,
        ca_city AS bought_city,
        SUM(ss_coupon_amt) AS amt,
        SUM(ss_net_profit) AS profit
    FROM store_sales
    INNER JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    INNER JOIN filtered_stores ON store_sales.ss_store_sk = filtered_stores.s_store_sk
    INNER JOIN filtered_hhdemo ON store_sales.ss_hdemo_sk = filtered_hhdemo.hd_demo_sk
    INNER JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
    GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)
SELECT
    c_last_name,
    c_first_name,
    current_addr.ca_city,
    filtered_sales.bought_city,
    filtered_sales.ss_ticket_number,
    filtered_sales.amt,
    filtered_sales.profit
FROM filtered_sales
INNER JOIN customer ON filtered_sales.ss_customer_sk = customer.c_customer_sk
INNER JOIN customer_address current_addr ON customer.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> filtered_sales.bought_city
ORDER BY
    c_last_name,
    c_first_name,
    current_addr.ca_city,
    filtered_sales.bought_city,
    filtered_sales.ss_ticket_number
LIMIT 100