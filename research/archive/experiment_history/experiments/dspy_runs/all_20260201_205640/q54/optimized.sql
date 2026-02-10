WITH date_constants AS (
    SELECT DISTINCT d_month_seq + 1 as month_seq_start,
                    d_month_seq + 3 as month_seq_end
    FROM date_dim 
    WHERE d_year = 1998 AND d_moy = 5
),
filtered_sales AS (
    SELECT cs_bill_customer_sk as customer_sk,
           cs_sold_date_sk as sold_date_sk
    FROM catalog_sales
    INNER JOIN item ON cs_item_sk = i_item_sk
    INNER JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE i_category = 'Women' 
      AND i_class = 'maternity'
      AND d_moy = 5
      AND d_year = 1998
    
    UNION ALL
    
    SELECT ws_bill_customer_sk as customer_sk,
           ws_sold_date_sk as sold_date_sk
    FROM web_sales
    INNER JOIN item ON ws_item_sk = i_item_sk
    INNER JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE i_category = 'Women' 
      AND i_class = 'maternity'
      AND d_moy = 5
      AND d_year = 1998
),
my_customers AS (
    SELECT DISTINCT c_customer_sk,
                    c_current_addr_sk
    FROM filtered_sales fs
    INNER JOIN customer c ON fs.customer_sk = c_customer_sk
),
my_revenue AS (
    SELECT c_customer_sk,
           SUM(ss_ext_sales_price) as revenue
    FROM my_customers mc
    INNER JOIN store_sales ss ON mc.c_customer_sk = ss_customer_sk
    INNER JOIN date_dim d ON ss_sold_date_sk = d_date_sk
    INNER JOIN customer_address ca ON mc.c_current_addr_sk = ca_address_sk
    INNER JOIN store s ON ca_county = s_county AND ca_state = s_state
    CROSS JOIN date_constants dc
    WHERE d_month_seq BETWEEN dc.month_seq_start AND dc.month_seq_end
    GROUP BY c_customer_sk
),
segments AS (
    SELECT CAST((revenue/50) AS INT) as segment
    FROM my_revenue
)
SELECT segment, 
       COUNT(*) as num_customers, 
       segment * 50 as segment_base
FROM segments
GROUP BY segment
ORDER BY segment, num_customers
LIMIT 100;