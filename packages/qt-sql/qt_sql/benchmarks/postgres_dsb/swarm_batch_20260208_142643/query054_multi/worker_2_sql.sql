WITH filtered_dates AS (
    SELECT d_date_sk, d_month_seq
    FROM date_dim
    WHERE d_year = 1998 AND d_moy = 11
),
filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category = 'Books' AND i_class = 'fiction'
),
month_range AS (
    SELECT MIN(d_month_seq) + 1 AS start_seq,
           MIN(d_month_seq) + 3 AS end_seq
    FROM filtered_dates
),
filtered_date_range AS (
    SELECT d_date_sk
    FROM date_dim
    JOIN month_range ON d_month_seq BETWEEN month_range.start_seq AND month_range.end_seq
),
filtered_stores AS (
    SELECT s_store_sk, s_county, s_state
    FROM store
    WHERE s_state IN ('GA', 'IA', 'LA', 'MO', 'OH', 'PA', 'SD', 'TN', 'TX', 'VA')
),
unioned_sales AS (
    SELECT
        cs_sold_date_sk AS sold_date_sk,
        cs_bill_customer_sk AS customer_sk,
        cs_item_sk AS item_sk,
        cs_wholesale_cost AS wholesale_cost
    FROM catalog_sales
    UNION ALL
    SELECT
        ws_sold_date_sk AS sold_date_sk,
        ws_bill_customer_sk AS customer_sk,
        ws_item_sk AS item_sk,
        ws_wholesale_cost AS wholesale_cost
    FROM web_sales
),
my_customers AS (
    SELECT DISTINCT
        customer.c_customer_sk,
        customer.c_current_addr_sk
    FROM unioned_sales
    JOIN filtered_dates ON unioned_sales.sold_date_sk = filtered_dates.d_date_sk
    JOIN filtered_items ON unioned_sales.item_sk = filtered_items.i_item_sk
    JOIN customer ON unioned_sales.customer_sk = customer.c_customer_sk
    WHERE unioned_sales.wholesale_cost BETWEEN 70 AND 100
      AND customer.c_birth_year BETWEEN 1993 AND 2006
),
my_revenue AS (
    SELECT
        my_customers.c_customer_sk,
        SUM(store_sales.ss_ext_sales_price) AS revenue
    FROM my_customers
    JOIN customer_address ON my_customers.c_current_addr_sk = customer_address.ca_address_sk
    JOIN filtered_stores ON customer_address.ca_county = filtered_stores.s_county
                        AND customer_address.ca_state = filtered_stores.s_state
    JOIN store_sales ON my_customers.c_customer_sk = store_sales.ss_customer_sk
    JOIN filtered_date_range ON store_sales.ss_sold_date_sk = filtered_date_range.d_date_sk
    WHERE store_sales.ss_wholesale_cost BETWEEN 70 AND 100
    GROUP BY my_customers.c_customer_sk
),
segments AS (
    SELECT
        CAST((revenue / 50) AS INT) AS segment
    FROM my_revenue
)
SELECT
    segment,
    COUNT(*) AS num_customers,
    segment * 50 AS segment_base
FROM segments
GROUP BY
    segment
ORDER BY
    segment,
    num_customers
LIMIT 100;