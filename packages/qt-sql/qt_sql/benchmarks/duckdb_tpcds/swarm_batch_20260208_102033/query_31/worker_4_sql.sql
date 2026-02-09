WITH filtered_sales AS (
  SELECT
    ca_county,
    d_qoy,
    d_year,
    SUM(ss_ext_sales_price) AS store_sales,
    SUM(ws_ext_sales_price) AS web_sales
  FROM customer_address,
    date_dim,
    store_sales LEFT JOIN web_sales ON
      ws_sold_date_sk = d_date_sk AND
      ws_bill_addr_sk = ca_address_sk AND
      store_sales.ss_sold_date_sk = web_sales.ws_sold_date_sk AND
      store_sales.ss_addr_sk = web_sales.ws_bill_addr_sk
  WHERE
    store_sales.ss_sold_date_sk = d_date_sk AND
    store_sales.ss_addr_sk = ca_address_sk AND
    d_year = 2000 AND
    d_qoy IN (1, 2, 3)
  GROUP BY
    ca_county,
    d_qoy,
    d_year
)
SELECT
  s1.ca_county,
  s1.d_year,
  s2.web_sales / s1.web_sales AS web_q1_q2_increase,
  s2.store_sales / s1.store_sales AS store_q1_q2_increase,
  s3.web_sales / s2.web_sales AS web_q2_q3_increase,
  s3.store_sales / s2.store_sales AS store_q2_q3_increase
FROM filtered_sales s1
JOIN filtered_sales s2 ON
  s1.ca_county = s2.ca_county AND
  s1.d_year = s2.d_year AND
  s1.d_qoy = 1 AND
  s2.d_qoy = 2
JOIN filtered_sales s3 ON
  s2.ca_county = s3.ca_county AND
  s2.d_year = s3.d_year AND
  s3.d_qoy = 3
WHERE
  CASE WHEN s1.web_sales > 0 THEN s2.web_sales / s1.web_sales ELSE NULL END >
  CASE WHEN s1.store_sales > 0 THEN s2.store_sales / s1.store_sales ELSE NULL END
  AND
  CASE WHEN s2.web_sales > 0 THEN s3.web_sales / s2.web_sales ELSE NULL END >
  CASE WHEN s2.store_sales > 0 THEN s3.store_sales / s2.store_sales ELSE NULL END
ORDER BY
  web_q1_q2_increase