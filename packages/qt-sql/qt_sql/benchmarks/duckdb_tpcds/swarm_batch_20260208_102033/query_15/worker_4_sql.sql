WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_qoy = 1
      AND d_year = 2001
),
filtered_sales AS (
    -- Branch 1: Zip code condition
    SELECT cs_sales_price, ca_zip
    FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    JOIN customer ON cs_bill_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
    
    UNION ALL
    
    -- Branch 2: State condition
    SELECT cs_sales_price, ca_zip
    FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    JOIN customer ON cs_bill_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE ca_state IN ('CA', 'WA', 'GA')
    
    UNION ALL
    
    -- Branch 3: High sales price condition
    SELECT cs_sales_price, ca_zip
    FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    JOIN customer ON cs_bill_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE cs_sales_price > 500
)
SELECT ca_zip,
       SUM(cs_sales_price) AS "SUM(cs_sales_price)"
FROM filtered_sales
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100