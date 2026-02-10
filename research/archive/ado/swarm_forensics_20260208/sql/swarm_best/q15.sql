WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_qoy = 1
      AND d_year = 2001
),
filtered_addresses AS (
    SELECT 
        ca_address_sk,
        ca_zip,
        CASE 
            WHEN SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
                 OR ca_state IN ('CA', 'WA', 'GA')
            THEN 1 
            ELSE 0 
        END AS address_qualifies
    FROM customer_address
),
qualified_sales AS (
    -- Branch 1: Address qualifies (zip or state)
    SELECT 
        fa.ca_zip,
        cs.cs_sales_price
    FROM catalog_sales cs
    JOIN filtered_dates fd ON cs.cs_sold_date_sk = fd.d_date_sk
    JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
    JOIN filtered_addresses fa ON c.c_current_addr_sk = fa.ca_address_sk
    WHERE fa.address_qualifies = 1
    
    UNION ALL
    
    -- Branch 2: High sales price AND address does NOT already qualify
    SELECT 
        fa.ca_zip,
        cs.cs_sales_price
    FROM catalog_sales cs
    JOIN filtered_dates fd ON cs.cs_sold_date_sk = fd.d_date_sk
    JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
    JOIN filtered_addresses fa ON c.c_current_addr_sk = fa.ca_address_sk
    WHERE cs.cs_sales_price > 500
      AND fa.address_qualifies = 0
)
SELECT 
    ca_zip,
    SUM(cs_sales_price) AS "SUM(cs_sales_price)"
FROM qualified_sales
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100