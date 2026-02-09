WITH filtered_store AS (
    SELECT s_store_sk, s_city
    FROM store
    WHERE s_number_employees BETWEEN 200 AND 295
), filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_dow = 1
      AND d_year IN (1998, 1998 + 1, 1998 + 2)
), filtered_hh AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_dep_count = 5 OR hd_vehicle_count > 4
), sales_preagg AS (
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        s_city,
        SUM(ss_coupon_amt) AS amt,
        SUM(ss_net_profit) AS profit
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN filtered_store ON ss_store_sk = s_store_sk
    WHERE ss_hdemo_sk IN (SELECT hd_demo_sk FROM filtered_hh)
    GROUP BY ss_ticket_number, ss_customer_sk, s_city
)
SELECT
    c_last_name,
    c_first_name,
    SUBSTRING(s_city, 1, 30),
    ss_ticket_number,
    amt,
    profit
FROM sales_preagg
JOIN customer ON ss_customer_sk = c_customer_sk
ORDER BY
    c_last_name,
    c_first_name,
    SUBSTRING(s_city, 1, 30),
    profit
LIMIT 100;