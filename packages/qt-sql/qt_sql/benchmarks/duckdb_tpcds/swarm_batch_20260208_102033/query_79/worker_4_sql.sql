WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_dow = 1
      AND d_year IN (1998, 1999, 2000)
),
filtered_stores AS (
    SELECT s_store_sk, s_city
    FROM store
    WHERE s_number_employees BETWEEN 200 AND 295
),
branch1_sales AS (
    SELECT 
        ss_ticket_number,
        ss_customer_sk,
        ss_addr_sk,
        fs.s_city,
        ss_coupon_amt,
        ss_net_profit
    FROM store_sales
    JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN filtered_stores fs ON store_sales.ss_store_sk = fs.s_store_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    WHERE household_demographics.hd_dep_count = 5
      AND household_demographics.hd_vehicle_count <= 4
),
branch2_sales AS (
    SELECT 
        ss_ticket_number,
        ss_customer_sk,
        ss_addr_sk,
        fs.s_city,
        ss_coupon_amt,
        ss_net_profit
    FROM store_sales
    JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN filtered_stores fs ON store_sales.ss_store_sk = fs.s_store_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    WHERE household_demographics.hd_vehicle_count > 4
),
combined_sales AS (
    SELECT * FROM branch1_sales
    UNION ALL
    SELECT * FROM branch2_sales
),
aggregated_sales AS (
    SELECT
        ss_ticket_number,
        ss_customer_sk,
        s_city,
        SUM(ss_coupon_amt) AS amt,
        SUM(ss_net_profit) AS profit
    FROM combined_sales
    GROUP BY
        ss_ticket_number,
        ss_customer_sk,
        ss_addr_sk,
        s_city
)
SELECT
    c_last_name,
    c_first_name,
    SUBSTRING(s_city, 1, 30) AS city_substr,
    ss_ticket_number,
    amt,
    profit
FROM aggregated_sales ms
JOIN customer ON ms.ss_customer_sk = customer.c_customer_sk
ORDER BY
    c_last_name,
    c_first_name,
    city_substr,
    profit
LIMIT 100