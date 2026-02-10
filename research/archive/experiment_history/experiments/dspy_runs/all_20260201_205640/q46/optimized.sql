-- start query 46 in stream 0 using template query46.tpl
SELECT c_last_name,
       c_first_name,
       current_addr.ca_city,
       dn.bought_city,
       dn.ss_ticket_number,
       dn.amt,
       dn.profit
FROM (
    SELECT ss_ticket_number,
           ss_customer_sk,
           customer_address.ca_city AS bought_city,
           SUM(ss_coupon_amt) AS amt,
           SUM(ss_net_profit) AS profit
    FROM store_sales
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
    WHERE (household_demographics.hd_dep_count = 6 
           OR household_demographics.hd_vehicle_count = 0)
      AND date_dim.d_dow IN (6, 0)
      AND date_dim.d_year BETWEEN 1999 AND 2001
      AND store.s_city IN ('Five Points', 'Centerville', 'Oak Grove', 'Fairview', 'Liberty')
    GROUP BY ss_ticket_number, ss_customer_sk, customer_address.ca_city
) dn
JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
JOIN customer_address current_addr ON customer.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> dn.bought_city
  AND customer.c_customer_sk <= 1999999
ORDER BY c_last_name,
         c_first_name,
         current_addr.ca_city,
         dn.bought_city,
         dn.ss_ticket_number
LIMIT 100;

-- end query 46 in stream 0 using template query46.tpl