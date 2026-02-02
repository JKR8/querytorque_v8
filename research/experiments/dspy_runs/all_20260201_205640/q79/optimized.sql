-- start query 79 in stream 0 using template query79.tpl
SELECT
  c_last_name,
  c_first_name,
  SUBSTR(s_city, 1, 30) AS city_prefix,
  ss_ticket_number,
  amt,
  profit
FROM (
  SELECT
    ss_ticket_number,
    ss_customer_sk,
    s.s_city,
    SUM(ss_coupon_amt) AS amt,
    SUM(ss_net_profit) AS profit
  FROM store_sales ss
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
  WHERE d.d_dow = 1
    AND d.d_year BETWEEN 1998 AND 2000
    AND s.s_number_employees BETWEEN 200 AND 295
    AND (hd.hd_dep_count = 5 OR hd.hd_vehicle_count > 4)
  GROUP BY ss_ticket_number, ss_customer_sk, s.s_city
) ms
JOIN customer c ON ms.ss_customer_sk = c.c_customer_sk
ORDER BY c_last_name, c_first_name, city_prefix, profit
LIMIT 100;

-- end query 79 in stream 0 using template query79.tpl