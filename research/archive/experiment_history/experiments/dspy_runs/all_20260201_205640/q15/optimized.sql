WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_qoy = 1 AND d_year = 2001
),
filtered_catalog_sales AS (
    SELECT cs_bill_customer_sk, cs_sales_price, ca_zip
    FROM catalog_sales
    INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk
    INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE substr(ca_zip,1,5) IN ('85669', '86197','88274','83405','86475',
                                 '85392', '85460', '80348', '81792')
    UNION ALL
    SELECT cs_bill_customer_sk, cs_sales_price, ca_zip
    FROM catalog_sales
    INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk
    INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE ca_state IN ('CA','WA','GA')
    UNION ALL
    SELECT cs_bill_customer_sk, cs_sales_price, ca_zip
    FROM catalog_sales
    INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk
    INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE cs_sales_price > 500
)
SELECT ca_zip, SUM(cs_sales_price) AS total_sales
FROM filtered_catalog_sales
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;