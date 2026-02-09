WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_dow = 1
      AND d_year IN (1998, 1999, 2000)
), filtered_store AS (
    SELECT s_store_sk, s_city
    FROM store
    WHERE s_number_employees BETWEEN 200 AND 295
), filtered_household AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_dep_count = 5 OR hd_vehicle_count > 4
), aggregated_sales AS (
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        s_city,
        SUM(ss_coupon_amt) AS amt,
        SUM(ss_net_profit) AS profit
    FROM store_sales
    INNER JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
    INNER JOIN filtered_store ON store_sales.ss_store_sk = filtered_store.s_store_sk
    INNER JOIN filtered_household ON store_sales.ss_hdemo_sk = filtered_household.hd_demo_sk
    GROUP BY
        ss_ticket_number,
        ss_customer_sk,
        ss_addr_sk,
        s_city
)
SELECT
    c_last_name,
    c_first_name,
    SUBSTRING(s_city, 1, 30),
    ss_ticket_number,
    amt,
    profit
FROM aggregated_sales
INNER JOIN customer ON aggregated_sales.ss_customer_sk = customer.c_customer_sk
ORDER BY
    c_last_name,
    c_first_name,
    SUBSTRING(s_city, 1, 30),
    profit
LIMIT 100