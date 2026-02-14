-- TPC-DS Query 46 (Optimized - Triple dimension isolate)
-- Transform: triple_dimension_isolate (DuckDB 3.23x winner)
-- Key changes:
--   1. Pre-filter date_dim, store, household_demographics into CTEs
--   2. Use explicit JOINs instead of comma joins
--   3. Dimension CTEs enable runtime partition pruning on store_sales
-- Original: 5 comma joins = optimizer blind to dimension selectivity
-- Optimized: 3 CTEs + explicit JOINs = optimizer can prune partitions
WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_dow IN (6, 0)
      AND d_year IN (1999, 2000, 2001)
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_city IN ('Five Points', 'Centerville', 'Oak Grove', 'Fairview', 'Liberty')
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_dep_count = 6
       OR hd_vehicle_count = 0
)
SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt, profit
FROM
   (SELECT ss_ticket_number,
           ss_customer_sk,
           ss_addr_sk,
           ca_city AS bought_city,
           SUM(ss_coupon_amt) AS amt,
           SUM(ss_net_profit) AS profit
    FROM store_sales
    JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN filtered_store ON store_sales.ss_store_sk = filtered_store.s_store_sk
    JOIN filtered_hd ON store_sales.ss_hdemo_sk = filtered_hd.hd_demo_sk
    JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
    GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city) AS dn,
   customer,
   customer_address AS current_addr
WHERE dn.ss_customer_sk = customer.c_customer_sk
  AND customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
LIMIT 100
