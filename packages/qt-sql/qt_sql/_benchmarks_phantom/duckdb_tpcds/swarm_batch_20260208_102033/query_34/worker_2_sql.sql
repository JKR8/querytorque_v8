WITH filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE (d_dom BETWEEN 1 AND 3 OR d_dom BETWEEN 25 AND 28)
    AND d_year IN (1998, 1999, 2000)
),
filtered_store AS (
  SELECT s_store_sk
  FROM store
  WHERE s_county IN (
    'Ziebach County', 'Daviess County', 'Walker County',
    'Richland County', 'Barrow County', 'Franklin Parish',
    'Williamson County', 'Luce County'
  )
),
filtered_hd AS (
  SELECT hd_demo_sk
  FROM household_demographics
  WHERE (hd_buy_potential = '1001-5000' OR hd_buy_potential = '0-500')
    AND hd_vehicle_count > 0
    AND (hd_dep_count / hd_vehicle_count) > 1.2
),
aggregated_sales AS (
  SELECT
    ss_ticket_number,
    ss_customer_sk,
    COUNT(*) AS cnt
  FROM store_sales
  JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
  JOIN filtered_store ON store_sales.ss_store_sk = filtered_store.s_store_sk
  JOIN filtered_hd ON store_sales.ss_hdemo_sk = filtered_hd.hd_demo_sk
  GROUP BY ss_ticket_number, ss_customer_sk
)
SELECT
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
FROM aggregated_sales
JOIN customer ON aggregated_sales.ss_customer_sk = customer.c_customer_sk
WHERE cnt BETWEEN 15 AND 20
ORDER BY
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag DESC,
  ss_ticket_number