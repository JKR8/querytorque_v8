WITH filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category = 'Books'
      AND i_class = 'fiction'
),
filtered_date_nov AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy = 11
      AND d_year = 1998
),
filtered_customer AS (
    SELECT c_customer_sk, c_current_addr_sk
    FROM customer
    WHERE c_birth_year BETWEEN 1993 AND 2006
),
catalog_sales_filtered AS (
    SELECT
        cs_bill_customer_sk AS customer_sk,
        c_current_addr_sk
    FROM catalog_sales
    JOIN filtered_date_nov ON cs_sold_date_sk = d_date_sk
    JOIN filtered_item ON cs_item_sk = i_item_sk
    JOIN filtered_customer ON cs_bill_customer_sk = c_customer_sk
    WHERE cs_wholesale_cost BETWEEN 70 AND 100
),
web_sales_filtered AS (
    SELECT
        ws_bill_customer_sk AS customer_sk,
        c_current_addr_sk
    FROM web_sales
    JOIN filtered_date_nov ON ws_sold_date_sk = d_date_sk
    JOIN filtered_item ON ws_item_sk = i_item_sk
    JOIN filtered_customer ON ws_bill_customer_sk = c_customer_sk
    WHERE ws_wholesale_cost BETWEEN 70 AND 100
),
my_customers AS (
    SELECT DISTINCT customer_sk, c_current_addr_sk
    FROM (
        SELECT * FROM catalog_sales_filtered
        UNION ALL
        SELECT * FROM web_sales_filtered
    ) AS combined_sales
),
base_month_seq AS (
    SELECT d_month_seq
    FROM date_dim
    WHERE d_year = 1998 AND d_moy = 11
    LIMIT 1
),
filtered_date_range AS (
    SELECT d_date_sk
    FROM date_dim, base_month_seq
    WHERE d_month_seq BETWEEN base_month_seq.d_month_seq + 1
                         AND base_month_seq.d_month_seq + 3
),
filtered_store AS (
    SELECT s_county, s_state
    FROM store
    WHERE s_state IN ('GA', 'IA', 'LA', 'MO', 'OH', 'PA', 'SD', 'TN', 'TX', 'VA')
),
my_revenue AS (
    SELECT
        mc.customer_sk AS c_customer_sk,
        SUM(ss_ext_sales_price) AS revenue
    FROM my_customers mc
    JOIN customer_address ca ON mc.c_current_addr_sk = ca.ca_address_sk
    JOIN filtered_store s ON ca.ca_county = s.s_county
                         AND ca.ca_state = s.s_state
    JOIN store_sales ss ON mc.customer_sk = ss.ss_customer_sk
    JOIN filtered_date_range dr ON ss.ss_sold_date_sk = dr.d_date_sk
    WHERE ss.ss_wholesale_cost BETWEEN 70 AND 100
    GROUP BY mc.customer_sk
),
segments AS (
    SELECT CAST((revenue / 50) AS INT) AS segment
    FROM my_revenue
)
SELECT
    segment,
    COUNT(*) AS num_customers,
    segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment, num_customers
LIMIT 100;