WITH cs_ui AS (
  SELECT
    cs_item_sk,
    SUM(cs_ext_list_price) AS sale,
    SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
  FROM catalog_sales
  JOIN catalog_returns 
    ON cs_item_sk = cr_item_sk
   AND cs_order_number = cr_order_number
  WHERE cs_wholesale_cost BETWEEN 34 AND 54
  GROUP BY cs_item_sk
  HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)
), cross_sales_base AS (
  SELECT
    i_product_name AS product_name,
    i_item_sk AS item_sk,
    s_store_name AS store_name,
    s_zip AS store_zip,
    ad1.ca_street_number AS b_street_number,
    ad1.ca_street_name AS b_street_name,
    ad1.ca_city AS b_city,
    ad1.ca_zip AS b_zip,
    ad2.ca_street_number AS c_street_number,
    ad2.ca_street_name AS c_street_name,
    ad2.ca_city AS c_city,
    ad2.ca_zip AS c_zip,
    d1.d_year AS syear,
    d2.d_year AS fsyear,
    d3.d_year AS s2year,
    ss_wholesale_cost,
    ss_list_price,
    ss_coupon_amt
  FROM store_sales
  JOIN store_returns 
    ON ss_item_sk = sr_item_sk
   AND ss_ticket_number = sr_ticket_number
  JOIN cs_ui 
    ON ss_item_sk = cs_ui.cs_item_sk
  JOIN date_dim AS d1 
    ON ss_sold_date_sk = d1.d_date_sk
  JOIN date_dim AS d2 
    ON c_first_sales_date_sk = d2.d_date_sk
  JOIN date_dim AS d3 
    ON c_first_shipto_date_sk = d3.d_date_sk
  JOIN store 
    ON ss_store_sk = s_store_sk
  JOIN customer 
    ON ss_customer_sk = c_customer_sk
  JOIN customer_demographics AS cd1 
    ON ss_cdemo_sk = cd1.cd_demo_sk
  JOIN customer_demographics AS cd2 
    ON c_current_cdemo_sk = cd2.cd_demo_sk
  JOIN promotion 
    ON ss_promo_sk = p_promo_sk
  JOIN household_demographics AS hd1 
    ON ss_hdemo_sk = hd1.hd_demo_sk
  JOIN household_demographics AS hd2 
    ON c_current_hdemo_sk = hd2.hd_demo_sk
  JOIN customer_address AS ad1 
    ON ss_addr_sk = ad1.ca_address_sk
  JOIN customer_address AS ad2 
    ON c_current_addr_sk = ad2.ca_address_sk
  JOIN income_band AS ib1 
    ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
  JOIN income_band AS ib2 
    ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
  JOIN item 
    ON ss_item_sk = i_item_sk
  WHERE cd1.cd_marital_status <> cd2.cd_marital_status
    AND i_current_price BETWEEN 20 AND 20 + 10
    AND p_channel_email = 'Y'
    AND p_channel_tv = 'N'
    AND p_channel_radio = 'Y'
    AND ad2.ca_state IN ('LA', 'TX', 'VA')
    AND ss_wholesale_cost BETWEEN 34 AND 54
    AND cd1.cd_marital_status IN ('M', 'M', 'U')
    AND cd1.cd_education_status IN ('Unknown', 'College', 'College')
    AND cd2.cd_marital_status IN ('M', 'M', 'U')
    AND cd2.cd_education_status IN ('Unknown', 'College', 'College')
    AND d1.d_year IN (1998, 1999)
), cross_sales AS (
  SELECT
    product_name,
    item_sk,
    store_name,
    store_zip,
    b_street_number,
    b_street_name,
    b_city,
    b_zip,
    c_street_number,
    c_street_name,
    c_city,
    c_zip,
    syear,
    fsyear,
    s2year,
    COUNT(*) AS cnt,
    SUM(ss_wholesale_cost) AS s1,
    SUM(ss_list_price) AS s2,
    SUM(ss_coupon_amt) AS s3
  FROM cross_sales_base
  GROUP BY
    product_name,
    item_sk,
    store_name,
    store_zip,
    b_street_number,
    b_street_name,
    b_city,
    b_zip,
    c_street_number,
    c_street_name,
    c_city,
    c_zip,
    syear,
    fsyear,
    s2year
), year_aggregates AS (
  SELECT
    product_name,
    item_sk,
    store_name,
    store_zip,
    b_street_number,
    b_street_name,
    b_city,
    b_zip,
    c_street_number,
    c_street_name,
    c_city,
    c_zip,
    MAX(CASE WHEN syear = 1998 THEN syear END) AS syear_1998,
    MAX(CASE WHEN syear = 1998 THEN cnt END) AS cnt_1998,
    MAX(CASE WHEN syear = 1998 THEN s1 END) AS s1_1998,
    MAX(CASE WHEN syear = 1998 THEN s2 END) AS s2_1998,
    MAX(CASE WHEN syear = 1998 THEN s3 END) AS s3_1998,
    MAX(CASE WHEN syear = 1999 THEN syear END) AS syear_1999,
    MAX(CASE WHEN syear = 1999 THEN cnt END) AS cnt_1999,
    MAX(CASE WHEN syear = 1999 THEN s1 END) AS s1_1999,
    MAX(CASE WHEN syear = 1999 THEN s2 END) AS s2_1999,
    MAX(CASE WHEN syear = 1999 THEN s3 END) AS s3_1999
  FROM cross_sales
  WHERE syear IN (1998, 1999)
  GROUP BY
    product_name,
    item_sk,
    store_name,
    store_zip,
    b_street_number,
    b_street_name,
    b_city,
    b_zip,
    c_street_number,
    c_street_name,
    c_city,
    c_zip
)
SELECT
  product_name,
  store_name,
  store_zip,
  b_street_number,
  b_street_name,
  b_city,
  b_zip,
  c_street_number,
  c_street_name,
  c_city,
  c_zip,
  syear_1998 AS syear,
  cnt_1998 AS cnt,
  s1_1998 AS s11,
  s2_1998 AS s21,
  s3_1998 AS s31,
  s1_1999 AS s12,
  s2_1999 AS s22,
  s3_1999 AS s32,
  syear_1999 AS syear,
  cnt_1999 AS cnt
FROM year_aggregates
WHERE cnt_1999 <= cnt_1998
  AND cnt_1998 IS NOT NULL
  AND cnt_1999 IS NOT NULL
ORDER BY
  product_name,
  store_name,
  cnt_1999,
  s1_1998,
  s1_1999;