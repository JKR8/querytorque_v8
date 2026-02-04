WITH combined AS (
  SELECT
    ca_county,
    d_qoy,
    d_year,
    SUM(CASE WHEN source = 'store' THEN sales END) AS store_sales,
    SUM(CASE WHEN source = 'web' THEN sales END) AS web_sales
  FROM (
    SELECT
      ca_county,
      d_qoy,
      d_year,
      'store' AS source,
      ss_ext_sales_price AS sales
    FROM store_sales, date_dim, customer_address
    WHERE
      ss_sold_date_sk = d_date_sk
      AND ss_addr_sk = ca_address_sk
      AND d_year = 2000
      AND d_qoy IN (1, 2, 3)
    UNION ALL
    SELECT
      ca_county,
      d_qoy,
      d_year,
      'web' AS source,
      ws_ext_sales_price AS sales
    FROM web_sales, date_dim, customer_address
    WHERE
      ws_sold_date_sk = d_date_sk
      AND ws_bill_addr_sk = ca_address_sk
      AND d_year = 2000
      AND d_qoy IN (1, 2, 3)
  ) AS combined_sales
  GROUP BY
    ca_county,
    d_qoy,
    d_year
)
SELECT
  q1.ca_county,
  q1.d_year,
  q2.web_sales / NULLIF(q1.web_sales, 0) AS web_q1_q2_increase,
  q2.store_sales / NULLIF(q1.store_sales, 0) AS store_q1_q2_increase,
  q3.web_sales / NULLIF(q2.web_sales, 0) AS web_q2_q3_increase,
  q3.store_sales / NULLIF(q2.store_sales, 0) AS store_q2_q3_increase
FROM combined AS q1
JOIN combined AS q2
  ON q1.ca_county = q2.ca_county AND q1.d_year = q2.d_year
JOIN combined AS q3
  ON q1.ca_county = q3.ca_county AND q1.d_year = q3.d_year
WHERE
  q1.d_qoy = 1
  AND q2.d_qoy = 2
  AND q3.d_qoy = 3
  AND (
    q2.web_sales / NULLIF(q1.web_sales, 0)
  ) > (
    q2.store_sales / NULLIF(q1.store_sales, 0)
  )
  AND (
    q3.web_sales / NULLIF(q2.web_sales, 0)
  ) > (
    q3.store_sales / NULLIF(q2.store_sales, 0)
  )
ORDER BY
  web_q1_q2_increase