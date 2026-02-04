WITH filtered_dates AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    (
      d_dom BETWEEN 1 AND 3 OR d_dom BETWEEN 25 AND 28
    )
    AND d_year IN (1998, 1999, 2000)
)
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
  FROM store_sales, filtered_dates, store, household_demographics
  WHERE
    store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    AND store_sales.ss_store_sk = store.s_store_sk
    AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND (
      household_demographics.hd_buy_potential = '1001-5000'
      OR household_demographics.hd_buy_potential = '0-500'
    )
    AND household_demographics.hd_vehicle_count > 0
    AND (
      CASE
        WHEN household_demographics.hd_vehicle_count > 0
        THEN household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
        ELSE NULL
      END
    ) > 1.2
    AND store.s_county IN (
      'Ziebach County',
      'Daviess County',
      'Walker County',
      'Richland County',
      'Barrow County',
      'Franklin Parish',
      'Williamson County',
      'Luce County'
    )
  GROUP BY
    ss_ticket_number,
    ss_customer_sk
) AS dn, customer
WHERE
  ss_customer_sk = c_customer_sk AND cnt BETWEEN 15 AND 20
ORDER BY
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag DESC,
  ss_ticket_number