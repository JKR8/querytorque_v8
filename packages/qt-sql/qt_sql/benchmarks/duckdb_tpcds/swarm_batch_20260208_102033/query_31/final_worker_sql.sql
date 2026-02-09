WITH ss_pivot AS (
  SELECT
    ca_county,
    d_year,
    SUM(CASE WHEN d_qoy = 1 THEN ss_ext_sales_price END) AS store_q1,
    SUM(CASE WHEN d_qoy = 2 THEN ss_ext_sales_price END) AS store_q2,
    SUM(CASE WHEN d_qoy = 3 THEN ss_ext_sales_price END) AS store_q3
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN customer_address ON ss_addr_sk = ca_address_sk
  WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)
  GROUP BY ca_county, d_year
), ws_pivot AS (
  SELECT
    ca_county,
    d_year,
    SUM(CASE WHEN d_qoy = 1 THEN ws_ext_sales_price END) AS web_q1,
    SUM(CASE WHEN d_qoy = 2 THEN ws_ext_sales_price END) AS web_q2,
    SUM(CASE WHEN d_qoy = 3 THEN ws_ext_sales_price END) AS web_q3
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
  WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)
  GROUP BY ca_county, d_year
)
SELECT
  s.ca_county,
  s.d_year,
  CASE WHEN w.web_q1 > 0 THEN w.web_q2 / w.web_q1 ELSE NULL END AS web_q1_q2_increase,
  CASE WHEN s.store_q1 > 0 THEN s.store_q2 / s.store_q1 ELSE NULL END AS store_q1_q2_increase,
  CASE WHEN w.web_q2 > 0 THEN w.web_q3 / w.web_q2 ELSE NULL END AS web_q2_q3_increase,
  CASE WHEN s.store_q2 > 0 THEN s.store_q3 / s.store_q2 ELSE NULL END AS store_q2_q3_increase
FROM ss_pivot s
JOIN ws_pivot w ON s.ca_county = w.ca_county AND s.d_year = w.d_year
WHERE
  CASE WHEN w.web_q1 > 0 THEN w.web_q2 / w.web_q1 ELSE NULL END
  > CASE WHEN s.store_q1 > 0 THEN s.store_q2 / s.store_q1 ELSE NULL END
  AND CASE WHEN w.web_q2 > 0 THEN w.web_q3 / w.web_q2 ELSE NULL END
  > CASE WHEN s.store_q2 > 0 THEN s.store_q3 / s.store_q2 ELSE NULL END
ORDER BY web_q1_q2_increase