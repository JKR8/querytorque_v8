WITH customer_sales AS (
  SELECT 
    c.c_customer_id AS customer_id,
    c.c_first_name AS customer_first_name,
    c.c_last_name AS customer_last_name,
    c.c_birth_country AS customer_birth_country,
    d.d_year,
    -- Store sales
    CASE WHEN ss.ss_customer_sk IS NOT NULL 
         THEN ((ss.ss_ext_list_price-ss.ss_ext_wholesale_cost-ss.ss_ext_discount_amt)+ss.ss_ext_sales_price)/2 
         ELSE 0 END AS store_sale_amt,
    -- Catalog sales  
    CASE WHEN cs.cs_bill_customer_sk IS NOT NULL
         THEN ((cs.cs_ext_list_price-cs.cs_ext_wholesale_cost-cs.cs_ext_discount_amt)+cs.cs_ext_sales_price)/2
         ELSE 0 END AS catalog_sale_amt,
    -- Web sales
    CASE WHEN ws.ws_bill_customer_sk IS NOT NULL
         THEN ((ws.ws_ext_list_price-ws.ws_ext_wholesale_cost-ws.ws_ext_discount_amt)+ws.ws_ext_sales_price)/2
         ELSE 0 END AS web_sale_amt
  FROM customer c
  LEFT JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
  LEFT JOIN date_dim d_ss ON ss.ss_sold_date_sk = d_ss.d_date_sk AND d_ss.d_year IN (1999, 2000)
  LEFT JOIN catalog_sales cs ON c.c_customer_sk = cs.cs_bill_customer_sk
  LEFT JOIN date_dim d_cs ON cs.cs_sold_date_sk = d_cs.d_date_sk AND d_cs.d_year IN (1999, 2000)
  LEFT JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
  LEFT JOIN date_dim d_ws ON ws.ws_sold_date_sk = d_ws.d_date_sk AND d_ws.d_year IN (1999, 2000)
  WHERE (d_ss.d_date_sk IS NOT NULL OR d_cs.d_date_sk IS NOT NULL OR d_ws.d_date_sk IS NOT NULL)
),
year_totals AS (
  SELECT 
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_birth_country,
    d_year,
    SUM(store_sale_amt) AS store_year_total,
    SUM(catalog_sale_amt) AS catalog_year_total,
    SUM(web_sale_amt) AS web_year_total
  FROM customer_sales
  WHERE d_year IN (1999, 2000)
  GROUP BY customer_id, customer_first_name, customer_last_name, customer_birth_country, d_year
),
year_pivot AS (
  SELECT 
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_birth_country,
    MAX(CASE WHEN d_year = 1999 THEN store_year_total END) AS store_1999,
    MAX(CASE WHEN d_year = 2000 THEN store_year_total END) AS store_2000,
    MAX(CASE WHEN d_year = 1999 THEN catalog_year_total END) AS catalog_1999,
    MAX(CASE WHEN d_year = 2000 THEN catalog_year_total END) AS catalog_2000,
    MAX(CASE WHEN d_year = 1999 THEN web_year_total END) AS web_1999,
    MAX(CASE WHEN d_year = 2000 THEN web_year_total END) AS web_2000
  FROM year_totals
  GROUP BY customer_id, customer_first_name, customer_last_name, customer_birth_country
)
SELECT 
  customer_id,
  customer_first_name,
  customer_last_name,
  customer_birth_country
FROM year_pivot
WHERE store_1999 > 0
  AND catalog_1999 > 0
  AND web_1999 > 0
  AND catalog_2000 / NULLIF(catalog_1999, 0) > store_2000 / NULLIF(store_1999, 0)
  AND catalog_2000 / NULLIF(catalog_1999, 0) > web_2000 / NULLIF(web_1999, 0)
ORDER BY customer_id, customer_first_name, customer_last_name, customer_birth_country
LIMIT 100;