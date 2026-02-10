SELECT
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
FROM (
  SELECT
    ss_ticket_number,
    ss_customer_sk,
    COUNT(*) AS cnt
  FROM store_sales
  WHERE
    ss_sold_date_sk IN (
      SELECT d_date_sk 
      FROM date_dim 
      WHERE d_dom BETWEEN 1 AND 2
        AND d_year IN (2000, 2000 + 1, 2000 + 2)
    )
    AND ss_store_sk IN (
      SELECT s_store_sk 
      FROM store 
      WHERE s_county IN ('Fairfield County', 'Walker County', 'Daviess County', 'Barrow County')
    )
    AND ss_hdemo_sk IN (
      SELECT hd_demo_sk 
      FROM household_demographics 
      WHERE (hd_buy_potential = '501-1000' OR hd_buy_potential = 'Unknown')
        AND hd_vehicle_count > 0
        AND (hd_dep_count / hd_vehicle_count) > 1
    )
  GROUP BY
    ss_ticket_number,
    ss_customer_sk
  HAVING COUNT(*) BETWEEN 1 AND 5
) AS dj
JOIN customer ON ss_customer_sk = c_customer_sk
ORDER BY
  cnt DESC,
  c_last_name ASC