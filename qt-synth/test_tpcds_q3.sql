-- TPC-DS Q3 style: Store sales with year filter and aggregation
SELECT 
    d_year,
    i_brand_id,
    i_brand,
    SUM(ss_sales_price) as sum_sales
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE d_year = 2000
  AND i_manufact_id = 128
GROUP BY d_year, i_brand_id, i_brand
ORDER BY sum_sales DESC
LIMIT 100;
