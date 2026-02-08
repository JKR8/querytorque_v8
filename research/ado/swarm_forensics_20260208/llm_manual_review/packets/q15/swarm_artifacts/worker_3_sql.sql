WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_qoy = 1
      AND d_year = 2001
),
filtered_customers AS (
    SELECT 
        c_customer_sk,
        ca_address_sk,
        ca_zip,
        ca_state
    FROM customer
    JOIN customer_address 
      ON c_current_addr_sk = ca_address_sk
    WHERE 
        SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
        OR ca_state IN ('CA', 'WA', 'GA')
),
filtered_sales AS (
    SELECT 
        cs_sales_price,
        cs_bill_customer_sk
    FROM catalog_sales
    JOIN filtered_dates 
      ON cs_sold_date_sk = d_date_sk
    WHERE cs_sales_price > 500
),
qualified_sales AS (
    -- Sales that match either address condition OR price condition
    SELECT 
        COALESCE(fc.ca_zip, addr.ca_zip) AS ca_zip,
        cs_sales_price
    FROM (
        -- Sales with customers matching address condition
        SELECT cs_sales_price, ca_zip
        FROM catalog_sales
        JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
        JOIN filtered_customers ON cs_bill_customer_sk = c_customer_sk
        
        UNION ALL
        
        -- Sales with price condition (avoiding duplicates from above)
        SELECT fs.cs_sales_price, COALESCE(fc.ca_zip, ca.ca_zip) AS ca_zip
        FROM filtered_sales fs
        JOIN filtered_dates fd ON 1=1
        JOIN catalog_sales cs 
          ON cs.cs_sold_date_sk = fd.d_date_sk 
         AND cs.cs_bill_customer_sk = fs.cs_bill_customer_sk
        JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
        JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
        LEFT JOIN filtered_customers fc ON c.c_customer_sk = fc.c_customer_sk
        WHERE NOT (
            SUBSTRING(ca.ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
            OR ca.ca_state IN ('CA', 'WA', 'GA')
        )
    ) combined
    CROSS JOIN LATERAL (
        SELECT ca_zip FROM filtered_customers fc 
        WHERE fc.c_customer_sk = cs_bill_customer_sk
        LIMIT 1
    ) addr
)
SELECT 
    ca_zip,
    SUM(cs_sales_price) AS "SUM(cs_sales_price)"
FROM qualified_sales
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;