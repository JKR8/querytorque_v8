WITH cs_ui AS (
  SELECT
    cs_item_sk,
    SUM(cs_ext_list_price) AS sale,
    SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
  FROM catalog_sales, catalog_returns
  WHERE
    cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
  GROUP BY
    cs_item_sk
  HAVING
    SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)
),
filtered_item AS (
  SELECT
    i_item_sk,
    i_product_name
  FROM item
  WHERE
    i_color IN ('blanched', 'medium', 'brown', 'chocolate', 'burlywood', 'drab')
    AND i_current_price BETWEEN 23 AND 23 + 10
    AND i_current_price BETWEEN 23 + 1 AND 23 + 15
),
filtered_date AS (
  SELECT
    d_date_sk,
    d_year
  FROM date_dim
  WHERE
    d_year IN (2001, 2002)
),
filtered_fact AS (
  SELECT
    ss.ss_item_sk,
    ss.ss_store_sk,
    ss.ss_customer_sk,
    ss.ss_cdemo_sk,
    ss.ss_hdemo_sk,
    ss.ss_addr_sk,
    ss.ss_ticket_number,
    ss.ss_promo_sk,
    ss.ss_wholesale_cost,
    ss.ss_list_price,
    ss.ss_coupon_amt,
    fd.d_year AS syear
  FROM store_sales ss
  JOIN filtered_item fi ON ss.ss_item_sk = fi.i_item_sk
  JOIN filtered_date fd ON ss.ss_sold_date_sk = fd.d_date_sk
  JOIN store_returns sr ON ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number
  JOIN cs_ui ON ss.ss_item_sk = cs_ui.cs_item_sk
),
cross_sales AS (
  SELECT
    fi.i_product_name AS product_name,
    ff.ss_item_sk AS item_sk,
    s.s_store_name AS store_name,
    s.s_zip AS store_zip,
    ad1.ca_street_number AS b_street_number,
    ad1.ca_street_name AS b_street_name,
    ad1.ca_city AS b_city,
    ad1.ca_zip AS b_zip,
    ad2.ca_street_number AS c_street_number,
    ad2.ca_street_name AS c_street_name,
    ad2.ca_city AS c_city,
    ad2.ca_zip AS c_zip,
    ff.syear,
    d2.d_year AS fsyear,
    d3.d_year AS s2year,
    COUNT(*) AS cnt,
    SUM(ff.ss_wholesale_cost) AS s1,
    SUM(ff.ss_list_price) AS s2,
    SUM(ff.ss_coupon_amt) AS s3
  FROM filtered_fact ff
  JOIN store s ON ff.ss_store_sk = s.s_store_sk
  JOIN customer c ON ff.ss_customer_sk = c.c_customer_sk
  JOIN customer_demographics cd1 ON ff.ss_cdemo_sk = cd1.cd_demo_sk
  JOIN household_demographics hd1 ON ff.ss_hdemo_sk = hd1.hd_demo_sk
  JOIN customer_address ad1 ON ff.ss_addr_sk = ad1.ca_address_sk
  JOIN customer_demographics cd2 ON c.c_current_cdemo_sk = cd2.cd_demo_sk
  JOIN household_demographics hd2 ON c.c_current_hdemo_sk = hd2.hd_demo_sk
  JOIN customer_address ad2 ON c.c_current_addr_sk = ad2.ca_address_sk
  JOIN date_dim d2 ON c.c_first_sales_date_sk = d2.d_date_sk
  JOIN date_dim d3 ON c.c_first_shipto_date_sk = d3.d_date_sk
  JOIN promotion p ON ff.ss_promo_sk = p.p_promo_sk
  JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
  JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
  JOIN filtered_item fi ON ff.ss_item_sk = fi.i_item_sk
  WHERE
    cd1.cd_marital_status <> cd2.cd_marital_status
  GROUP BY
    fi.i_product_name,
    ff.ss_item_sk,
    s.s_store_name,
    s.s_zip,
    ad1.ca_street_number,
    ad1.ca_street_name,
    ad1.ca_city,
    ad1.ca_zip,
    ad2.ca_street_number,
    ad2.ca_street_name,
    ad2.ca_city,
    ad2.ca_zip,
    ff.syear,
    d2.d_year,
    d3.d_year
)
SELECT
  cs1.product_name,
  cs1.store_name,
  cs1.store_zip,
  cs1.b_street_number,
  cs1.b_street_name,
  cs1.b_city,
  cs1.b_zip,
  cs1.c_street_number,
  cs1.c_street_name,
  cs1.c_city,
  cs1.c_zip,
  cs1.syear,
  cs1.cnt,
  cs1.s1 AS s11,
  cs1.s2 AS s21,
  cs1.s3 AS s31,
  cs2.s1 AS s12,
  cs2.s2 AS s22,
  cs2.s3 AS s32,
  cs2.syear,
  cs2.cnt
FROM cross_sales AS cs1, cross_sales AS cs2
WHERE
  cs1.item_sk = cs2.item_sk
  AND cs1.syear = 2001
  AND cs2.syear = 2001 + 1
  AND cs2.cnt <= cs1.cnt
  AND cs1.store_name = cs2.store_name
  AND cs1.store_zip = cs2.store_zip
ORDER BY
  cs1.product_name,
  cs1.store_name,
  cs2.cnt,
  cs1.s1,
  cs2.s1