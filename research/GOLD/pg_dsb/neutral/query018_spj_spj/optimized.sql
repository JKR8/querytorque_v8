SELECT
  MIN(i_item_id),
  MIN(ca_country),
  MIN(ca_state),
  MIN(ca_county),
  MIN(cs_quantity),
  MIN(cs_list_price),
  MIN(cs_coupon_amt),
  MIN(cs_sales_price),
  MIN(cs_net_profit),
  MIN(c_birth_year),
  MIN(cd_dep_count)
FROM (
  SELECT
    i_item_id,
    ca_country,
    ca_state,
    ca_county,
    cs_quantity,
    cs_list_price,
    cs_coupon_amt,
    cs_sales_price,
    cs_net_profit,
    c_birth_year,
    cd_dep_count
  FROM catalog_sales
  JOIN (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
  ) date_filter ON cs_sold_date_sk = date_filter.d_date_sk
  JOIN (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category = 'Jewelry'
  ) item_filter ON cs_item_sk = item_filter.i_item_sk
  JOIN (
    SELECT cd_demo_sk, cd_dep_count
    FROM customer_demographics
    WHERE cd_gender = 'F'
      AND cd_education_status = 'College'
  ) cd_filter ON cs_bill_cdemo_sk = cd_filter.cd_demo_sk
  JOIN (
    SELECT c_customer_sk, c_birth_year, c_current_addr_sk
    FROM customer
    WHERE c_birth_month = 1
  ) cust_filter ON cs_bill_customer_sk = cust_filter.c_customer_sk
  JOIN (
    SELECT ca_address_sk, ca_country, ca_state, ca_county
    FROM customer_address
    WHERE ca_state IN ('GA', 'LA', 'SD')
  ) ca_filter ON cust_filter.c_current_addr_sk = ca_filter.ca_address_sk
  WHERE cs_wholesale_cost BETWEEN 52 AND 57
) combined
