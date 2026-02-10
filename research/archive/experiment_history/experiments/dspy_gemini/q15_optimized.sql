WITH relevant_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001 AND d_qoy = 1
)
SELECT ca_zip, SUM(cs_sales_price)
FROM (
    -- Branch 1: Filter by Zip Code
    SELECT ca_zip, cs_sales_price
    FROM catalog_sales
    JOIN relevant_dates ON cs_sold_date_sk = d_date_sk
    JOIN customer ON cs_bill_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE substr(ca_zip,1,5) IN ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')

    UNION ALL

    -- Branch 2: Filter by State (Excluding rows already matched by Zip)
    SELECT ca_zip, cs_sales_price
    FROM catalog_sales
    JOIN relevant_dates ON cs_sold_date_sk = d_date_sk
    JOIN customer ON cs_bill_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE ca_state IN ('CA','WA','GA')
      AND substr(ca_zip,1,5) NOT IN ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')

    UNION ALL

    -- Branch 3: Filter by Price (Excluding rows already matched by Zip or State)
    SELECT ca_zip, cs_sales_price
    FROM catalog_sales
    JOIN relevant_dates ON cs_sold_date_sk = d_date_sk
    JOIN customer ON cs_bill_customer_sk = c_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE cs_sales_price > 500
      AND ca_state NOT IN ('CA','WA','GA')
      AND substr(ca_zip,1,5) NOT IN ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
) combined_results
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;