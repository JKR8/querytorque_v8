WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_dow = 1
      AND d_year IN (1998, 1998 + 1, 1998 + 2)
),
filtered_store AS (
    SELECT s_store_sk, s_city
    FROM store
    WHERE s_number_employees BETWEEN 200 AND 295
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_dep_count = 5
       OR hd_vehicle_count > 4
),
filtered_sales AS (
    SELECT
        ss.ss_ticket_number,
        ss.ss_customer_sk,
        ss.ss_addr_sk,
        fs.s_city,
        SUM(ss.ss_coupon_amt) AS amt,
        SUM(ss.ss_net_profit) AS profit
    FROM store_sales ss
    JOIN filtered_date fd ON ss.ss_sold_date_sk = fd.d_date_sk
    JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
    JOIN filtered_hd fh ON ss.ss_hdemo_sk = fh.hd_demo_sk
    GROUP BY
        ss.ss_ticket_number,
        ss.ss_customer_sk,
        ss.ss_addr_sk,
        fs.s_city
)
SELECT
    c.c_last_name,
    c.c_first_name,
    SUBSTRING(fs.s_city, 1, 30),
    fs.ss_ticket_number,
    fs.amt,
    fs.profit
FROM filtered_sales fs
JOIN customer c ON fs.ss_customer_sk = c.c_customer_sk
ORDER BY
    c.c_last_name,
    c.c_first_name,
    SUBSTRING(fs.s_city, 1, 30),
    fs.profit
LIMIT 100;