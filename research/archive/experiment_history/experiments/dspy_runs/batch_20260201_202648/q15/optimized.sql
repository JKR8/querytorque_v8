WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_qoy = 1 AND d_year = 2001
),
filtered_customer_address AS (
    SELECT ca_address_sk, ca_zip
    FROM customer_address
    WHERE substr(ca_zip,1,5) IN ('85669','86197','88274','83405','86475',
                                 '85392','85460','80348','81792')
       OR ca_state IN ('CA','WA','GA')
),
sales_with_dates AS (
    SELECT cs_bill_customer_sk, cs_sales_price
    FROM catalog_sales cs
    INNER JOIN filtered_dates fd ON cs.cs_sold_date_sk = fd.d_date_sk
)
SELECT ca_zip, SUM(cs_sales_price) as total_sales
FROM sales_with_dates swd
INNER JOIN customer c ON swd.cs_bill_customer_sk = c.c_customer_sk
INNER JOIN filtered_customer_address fca ON c.c_current_addr_sk = fca.ca_address_sk
WHERE swd.cs_sales_price > 500
   OR fca.ca_address_sk IS NOT NULL  -- Already filtered by zip/state conditions
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;