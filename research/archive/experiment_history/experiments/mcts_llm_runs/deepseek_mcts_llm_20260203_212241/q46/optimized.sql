WITH sales_transactions AS (
  SELECT
    ss_ticket_number,
    ss_customer_sk,
    ca_city AS bought_city,
    SUM(ss_coupon_amt) AS amt,
    SUM(ss_net_profit) AS profit
  FROM date_dim
  JOIN store
    ON 1 = 1
  JOIN household_demographics
    ON 1 = 1
  JOIN store_sales
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND store_sales.ss_store_sk = store.s_store_sk
    AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN customer_address
    ON store_sales.ss_addr_sk = customer_address.ca_address_sk
  WHERE
    (
      household_demographics.hd_dep_count = 6
      OR household_demographics.hd_vehicle_count = 0
    )
    AND date_dim.d_dow IN (6, 0)
    AND date_dim.d_year IN (1999, 2000, 2001)
    AND store.s_city IN ('Five Points', 'Centerville', 'Oak Grove', 'Fairview', 'Liberty')
  GROUP BY
    ss_ticket_number,
    ss_customer_sk,
    ss_addr_sk,
    ca_city
)
SELECT
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number,
  amt,
  profit
FROM sales_transactions AS dn
JOIN customer
  ON dn.ss_customer_sk = customer.c_customer_sk
JOIN customer_address AS current_addr
  ON customer.c_current_addr_sk = current_addr.ca_address_sk
WHERE
  current_addr.ca_city <> dn.bought_city
ORDER BY
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number
LIMIT 100