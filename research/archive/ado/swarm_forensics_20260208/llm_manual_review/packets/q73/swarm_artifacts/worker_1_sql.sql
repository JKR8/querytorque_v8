WITH filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_dom BETWEEN 1 AND 2
    AND d_year IN (2000, 2000 + 1, 2000 + 2)
),
filtered_store AS (
  SELECT s_store_sk
  FROM store
  WHERE s_county IN ('Fairfield County', 'Walker County', 'Daviess County', 'Barrow County')
),
filtered_household AS (
  SELECT hd_demo_sk
  FROM household_demographics
  WHERE (hd_buy_potential = '501-1000' OR hd_buy_potential = 'Unknown')
    AND hd_vehicle_count > 0
    AND hd_dep_count > hd_vehicle_count
),
store_sales_agg AS (
  SELECT
    ss_ticket_number,
    ss_customer_sk,
    COUNT(*) AS cnt
  FROM store_sales
  JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
  JOIN filtered_store ON store_sales.ss_store_sk = filtered_store.s_store_sk
  JOIN filtered_household ON store_sales.ss_hdemo_sk = filtered_household.hd_demo_sk
  GROUP BY ss_ticket_number, ss_customer_sk
  HAVING cnt BETWEEN 1 AND 5
)
SELECT
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  store_sales_agg.ss_ticket_number,
  store_sales_agg.cnt
FROM store_sales_agg
JOIN customer ON store_sales_agg.ss_customer_sk = customer.c_customer_sk
ORDER BY
  store_sales_agg.cnt DESC,
  c_last_name ASC