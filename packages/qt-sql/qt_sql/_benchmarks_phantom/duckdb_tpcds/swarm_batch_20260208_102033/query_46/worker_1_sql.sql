WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_dow IN (6, 0)
      AND d_year IN (1999, 1999 + 1, 1999 + 2)
), filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_city IN ('Five Points', 'Centerville', 'Oak Grove', 'Fairview', 'Liberty')
), filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_dep_count = 6 OR hd_vehicle_count = 0
), sales_agg AS (
    SELECT
        ss.ss_ticket_number,
        ss.ss_customer_sk,
        ca.ca_city AS bought_city,
        SUM(ss.ss_coupon_amt) AS amt,
        SUM(ss.ss_net_profit) AS profit
    FROM store_sales ss
    JOIN filtered_date fd ON ss.ss_sold_date_sk = fd.d_date_sk
    JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
    JOIN filtered_hd fh ON ss.ss_hdemo_sk = fh.hd_demo_sk
    JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
    GROUP BY ss.ss_ticket_number, ss.ss_customer_sk, ss.ss_addr_sk, ca.ca_city
)
SELECT
    c.c_last_name,
    c.c_first_name,
    current_addr.ca_city,
    sa.bought_city,
    sa.ss_ticket_number,
    sa.amt,
    sa.profit
FROM sales_agg sa
JOIN customer c ON sa.ss_customer_sk = c.c_customer_sk
JOIN customer_address current_addr ON c.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> sa.bought_city
ORDER BY
    c.c_last_name,
    c.c_first_name,
    current_addr.ca_city,
    sa.bought_city,
    sa.ss_ticket_number
LIMIT 100