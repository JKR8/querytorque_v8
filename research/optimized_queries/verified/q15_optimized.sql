-- Q15: Date filter pushdown (catalog_sales)
-- Sample DB: 6.16x speedup, CORRECT
-- Pattern: Push date filter into CTE before joining customer/address

WITH filtered_sales AS (
    SELECT cs_bill_customer_sk, cs_sales_price
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
      AND d_qoy = 2 AND d_year = 2000
)
SELECT ca_zip, sum(cs_sales_price)
FROM filtered_sales fs, customer c, customer_address ca
WHERE fs.cs_bill_customer_sk = c.c_customer_sk
    AND c.c_current_addr_sk = ca.ca_address_sk
    AND (substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
         or ca_state in ('CA','WA','GA')
         or cs_sales_price > 500)
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;
