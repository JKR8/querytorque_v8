WITH filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_qoy = 1
    AND d_year = 2001
)
SELECT
  ca_zip,
  SUM(cs_sales_price)
FROM catalog_sales
INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk
INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk
INNER JOIN filtered_date ON cs_sold_date_sk = d_date_sk
WHERE (
  SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
  OR ca_state IN ('CA', 'WA', 'GA')
  OR cs_sales_price > 500
)
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100