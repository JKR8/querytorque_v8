WITH q1_2001_sales AS (
    SELECT cs.cs_bill_customer_sk, cs.cs_sales_price
    FROM catalog_sales cs
    JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
    WHERE d.d_qoy = 1 AND d.d_year = 2001
)
SELECT ca.ca_zip,
       SUM(q1.cs_sales_price)
FROM q1_2001_sales q1
JOIN customer c ON q1.cs_bill_customer_sk = c.c_customer_sk
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
WHERE (SUBSTR(ca.ca_zip,1,5) IN ('85669', '86197','88274','83405','86475',
                                  '85392', '85460', '80348', '81792')
       OR ca.ca_state IN ('CA','WA','GA')
       OR q1.cs_sales_price > 500)
GROUP BY ca.ca_zip
ORDER BY ca.ca_zip
LIMIT 100;