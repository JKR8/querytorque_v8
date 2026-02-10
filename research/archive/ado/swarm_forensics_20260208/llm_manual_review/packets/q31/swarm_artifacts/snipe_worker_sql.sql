WITH ss_2000 AS (
  SELECT
    ca_county,
    d_qoy,
    SUM(ss_ext_sales_price) AS store_sales
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN customer_address ON ss_addr_sk = ca_address_sk
  WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)
  GROUP BY ca_county, d_qoy
), ws_2000 AS (
  SELECT
    ca_county,
    d_qoy,
    SUM(ws_ext_sales_price) AS web_sales
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
  WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)
  GROUP BY ca_county, d_qoy
)
SELECT
  s1.ca_county,
  2000 AS d_year,
  w2.web_sales / NULLIF(w1.web_sales, 0) AS web_q1_q2_increase,
  s2.store_sales / NULLIF(s1.store_sales, 0) AS store_q1_q2_increase,
  w3.web_sales / NULLIF(w2.web_sales, 0) AS web_q2_q3_increase,
  s3.store_sales / NULLIF(s2.store_sales, 0) AS store_q2_q3_increase
FROM ss_2000 s1
JOIN ss_2000 s2 ON s1.ca_county = s2.ca_county AND s2.d_qoy = 2
JOIN ss_2000 s3 ON s2.ca_county = s3.ca_county AND s3.d_qoy = 3
JOIN ws_2000 w1 ON s1.ca_county = w1.ca_county AND w1.d_qoy = 1
JOIN ws_2000 w2 ON w1.ca_county = w2.ca_county AND w2.d_qoy = 2
JOIN ws_2000 w3 ON w2.ca_county = w3.ca_county AND w3.d_qoy = 3
WHERE s1.d_qoy = 1
  AND w2.web_sales / NULLIF(w1.web_sales, 0) > s2.store_sales / NULLIF(s1.store_sales, 0)
  AND w3.web_sales / NULLIF(w2.web_sales, 0) > s3.store_sales / NULLIF(s2.store_sales, 0)
ORDER BY web_q1_q2_increase;