WITH filtered_dates AS (
  SELECT d_date_sk, d_date
  FROM date_dim
  WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
)
SELECT
  COUNT(*)
FROM (
  (
    SELECT DISTINCT
      customer.c_last_name,
      customer.c_first_name,
      filtered_dates.d_date
    FROM store_sales
    JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
  )
  EXCEPT
  (
    SELECT DISTINCT
      customer.c_last_name,
      customer.c_first_name,
      filtered_dates.d_date
    FROM catalog_sales
    JOIN filtered_dates ON catalog_sales.cs_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
  )
  EXCEPT
  (
    SELECT DISTINCT
      customer.c_last_name,
      customer.c_first_name,
      filtered_dates.d_date
    FROM web_sales
    JOIN filtered_dates ON web_sales.ws_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
  )
) AS cool_cust