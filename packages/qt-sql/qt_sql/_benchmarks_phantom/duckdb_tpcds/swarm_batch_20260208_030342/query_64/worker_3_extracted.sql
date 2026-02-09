WITH filtered_items AS (
  SELECT i_item_sk, i_product_name, i_current_price
  FROM item
  WHERE i_color IN ('blanched', 'medium', 'brown', 'chocolate', 'burlywood', 'drab')
    AND i_current_price BETWEEN 24 AND 33  -- Combined overlapping ranges: 23+1=24, 23+10=33
),
sold_dates_2001 AS (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_year = 2001
),
sold_dates_2002 AS (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_year = 2002
),
first_sales_dates AS (
  SELECT d_date_sk, d_year
  FROM date_dim
),
first_shipto_dates AS (
  SELECT d_date_sk, d_year
  FROM date_dim
),
cs_ui AS (
  SELECT
    cs_item_sk,
    SUM(cs_ext_list_price) AS sale,
    SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
  FROM catalog_sales
  JOIN catalog_returns
    ON cs_item_sk = cr_item_sk
   AND cs_order_number = cr_order_number
  GROUP BY cs_item_sk
  HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)
),
filtered_store_sales AS (
  SELECT
    ss_item_sk,
    ss_ticket_number,
    ss_customer_sk,
    ss_store_sk,
    ss_cdemo_sk,
    ss_hdemo_sk,
    ss_addr_sk,
    ss_promo_sk,
    ss_wholesale_cost,
    ss_list_price,
    ss_coupon_amt,
    d_year AS syear
  FROM store_sales
  JOIN sold_dates_2001 d1 ON ss_sold_date_sk = d1.d_date_sk
  JOIN filtered_items i ON ss_item_sk = i.i_item_sk
  UNION ALL
  SELECT
    ss_item_sk,
    ss_ticket_number,
    ss_customer_sk,
    ss_store_sk,
    ss_cdemo_sk,
    ss_hdemo_sk,
    ss_addr_sk,
    ss_promo_sk,
    ss_wholesale_cost,
    ss_list_price,
    ss_coupon_amt,
    d_year AS syear
  FROM store_sales
  JOIN sold_dates_2002 d1 ON ss_sold_date_sk = d1.d_date_sk
  JOIN filtered_items i ON ss_item_sk = i.i_item_sk
),
joined_sales_data AS (
  SELECT
    fss.*,
    s.s_store_name,
    s.s_zip,
    cd1.cd_marital_status AS cd1_marital_status,
    hd1.hd_income_band_sk AS hd1_income_band_sk,
    ad1.ca_street_number AS b_street_number,
    ad1.ca_street_name AS b_street_name,
    ad1.ca_city AS b_city,
    ad1.ca_zip AS b_zip,
    c.c_current_cdemo_sk,
    c.c_current_hdemo_sk,
    c.c_current_addr_sk,
    c.c_first_sales_date_sk,
    c.c_first_shipto_date_sk,
    i.i_product_name,
    i.i_item_sk
  FROM filtered_store_sales fss
  JOIN store_returns sr
    ON fss.ss_item_sk = sr.sr_item_sk
   AND fss.ss_ticket_number = sr.sr_ticket_number
  JOIN cs_ui
    ON fss.ss_item_sk = cs_ui.cs_item_sk
  JOIN store s
    ON fss.ss_store_sk = s.s_store_sk
  JOIN customer_demographics cd1
    ON fss.ss_cdemo_sk = cd1.cd_demo_sk
  JOIN household_demographics hd1
    ON fss.ss_hdemo_sk = hd1.hd_demo_sk
  JOIN customer_address ad1
    ON fss.ss_addr_sk = ad1.ca_address_sk
  JOIN promotion p
    ON fss.ss_promo_sk = p.p_promo_sk
  JOIN income_band ib1
    ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
  JOIN customer c
    ON fss.ss_customer_sk = c.c_customer_sk
  JOIN filtered_items i
    ON fss.ss_item_sk = i.i_item_sk
),
cross_sales AS (
  SELECT
    jsd.i_product_name AS product_name,
    jsd.i_item_sk AS item_sk,
    jsd.s_store_name AS store_name,
    jsd.s_zip AS store_zip,
    jsd.b_street_number,
    jsd.b_street_name,
    jsd.b_city,
    jsd.b_zip,
    ad2.ca_street_number AS c_street_number,
    ad2.ca_street_name AS c_street_name,
    ad2.ca_city AS c_city,
    ad2.ca_zip AS c_zip,
    jsd.syear,
    d2.d_year AS fsyear,
    d3.d_year AS s2year,
    COUNT(*) AS cnt,
    SUM(jsd.ss_wholesale_cost) AS s1,
    SUM(jsd.ss_list_price) AS s2,
    SUM(jsd.ss_coupon_amt) AS s3
  FROM joined_sales_data jsd
  JOIN customer_demographics cd2
    ON jsd.c_current_cdemo_sk = cd2.cd_demo_sk
  JOIN household_demographics hd2
    ON jsd.c_current_hdemo_sk = hd2.hd_demo_sk
  JOIN customer_address ad2
    ON jsd.c_current_addr_sk = ad2.ca_address_sk
  JOIN income_band ib2
    ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
  JOIN first_sales_dates d2
    ON jsd.c_first_sales_date_sk = d2.d_date_sk
  JOIN first_shipto_dates d3
    ON jsd.c_first_shipto_date_sk = d3.d_date_sk
  WHERE jsd.cd1_marital_status <> cd2.cd_marital_status
  GROUP BY
    jsd.i_product_name,
    jsd.i_item_sk,
    jsd.s_store_name,
    jsd.s_zip,
    jsd.b_street_number,
    jsd.b_street_name,
    jsd.b_city,
    jsd.b_zip,
    ad2.ca_street_number,
    ad2.ca_street_name,
    ad2.ca_city,
    ad2.ca_zip,
    jsd.syear,
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
FROM cross_sales cs1
JOIN cross_sales cs2
  ON cs1.item_sk = cs2.item_sk
 AND cs1.store_name = cs2.store_name
 AND cs1.store_zip = cs2.store_zip
WHERE cs1.syear = 2001
  AND cs2.syear = 2002
  AND cs2.cnt <= cs1.cnt
ORDER BY
  cs1.product_name,
  cs1.store_name,
  cs2.cnt,
  cs1.s1,
  cs2.s1;