WITH frequent_ss_items AS (
  SELECT substring(i_item_desc,1,30) itemdesc, i_item_sk item_sk, d_date solddate, count(*) cnt
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE d_year IN (2000,2001,2002,2003)
  GROUP BY substring(i_item_desc,1,30), i_item_sk, d_date
  HAVING count(*) > 4
),
max_store_sales AS (
  SELECT max(csales) tpcds_cmax
  FROM (
    SELECT c_customer_sk, sum(ss_quantity*ss_sales_price) csales
    FROM store_sales
    JOIN customer ON ss_customer_sk = c_customer_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year IN (2000,2001,2002,2003)
    GROUP BY c_customer_sk
  )
),
best_ss_customer AS (
  SELECT c_customer_sk, sum(ss_quantity*ss_sales_price) ssales
  FROM store_sales
  JOIN customer ON ss_customer_sk = c_customer_sk
  GROUP BY c_customer_sk
  HAVING sum(ss_quantity*ss_sales_price) > (95/100.0) * (SELECT * FROM max_store_sales)
)
SELECT c_last_name, c_first_name, sales
FROM (
  SELECT c_last_name, c_first_name, sum(cs_quantity*cs_list_price) sales
  FROM catalog_sales
  JOIN customer ON cs_bill_customer_sk = c_customer_sk
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  WHERE d_year = 2000
    AND d_moy = 5
    AND cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
    AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
  GROUP BY c_last_name, c_first_name
  UNION ALL
  SELECT c_last_name, c_first_name, sum(ws_quantity*ws_list_price) sales
  FROM web_sales
  JOIN customer ON ws_bill_customer_sk = c_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year = 2000
    AND d_moy = 5
    AND ws_item_sk IN (SELECT item_sk FROM frequent_ss_items)
    AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
  GROUP BY c_last_name, c_first_name
)
ORDER BY c_last_name, c_first_name, sales
LIMIT 100
