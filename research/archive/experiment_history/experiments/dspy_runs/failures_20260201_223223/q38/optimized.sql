-- start query 38 in stream 0 using template query38.tpl
WITH filtered_dates AS MATERIALIZED (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1183 AND 1183 + 11
)
select count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from store_sales, filtered_dates, customer
          where store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
  intersect
    select distinct c_last_name, c_first_name, d_date
    from catalog_sales, filtered_dates, customer
          where catalog_sales.cs_sold_date_sk = filtered_dates.d_date_sk
      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
  intersect
    select distinct c_last_name, c_first_name, d_date
    from web_sales, filtered_dates, customer
          where web_sales.ws_sold_date_sk = filtered_dates.d_date_sk
      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
) hot_cust
 LIMIT 100;

-- end query 38 in stream 0 using template query38.tpl