WITH filtered_dates AS (
    SELECT d_date_sk, d_quarter_name
    FROM date_dim
    WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')
),
filtered_d1 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_quarter_name = '2001Q1'
),
filtered_store_sales AS (
    SELECT ss_item_sk, ss_store_sk, ss_customer_sk, ss_ticket_number, ss_quantity
    FROM store_sales
    JOIN filtered_d1 ON d_date_sk = ss_sold_date_sk
),
filtered_store_returns AS (
    SELECT sr_item_sk, sr_customer_sk, sr_ticket_number, sr_return_quantity
    FROM store_returns
    JOIN filtered_dates d2 ON d2.d_date_sk = sr_returned_date_sk
),
filtered_catalog_sales AS (
    SELECT cs_item_sk, cs_bill_customer_sk, cs_quantity
    FROM catalog_sales
    JOIN filtered_dates d3 ON d3.d_date_sk = cs_sold_date_sk
)
SELECT i_item_id
       ,i_item_desc
       ,s_state
       ,COUNT(ss_quantity) AS store_sales_quantitycount
       ,AVG(ss_quantity) AS store_sales_quantityave
       ,STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev
       ,STDDEV_SAMP(ss_quantity)/AVG(ss_quantity) AS store_sales_quantitycov
       ,COUNT(sr_return_quantity) AS store_returns_quantitycount
       ,AVG(sr_return_quantity) AS store_returns_quantityave
       ,STDDEV_SAMP(sr_return_quantity) AS store_returns_quantitystdev
       ,STDDEV_SAMP(sr_return_quantity)/AVG(sr_return_quantity) AS store_returns_quantitycov
       ,COUNT(cs_quantity) AS catalog_sales_quantitycount
       ,AVG(cs_quantity) AS catalog_sales_quantityave
       ,STDDEV_SAMP(cs_quantity) AS catalog_sales_quantitystdev
       ,STDDEV_SAMP(cs_quantity)/AVG(cs_quantity) AS catalog_sales_quantitycov
FROM filtered_store_sales ss
JOIN filtered_store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
                               AND ss.ss_item_sk = sr.sr_item_sk
                               AND ss.ss_ticket_number = sr.sr_ticket_number
JOIN filtered_catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
                               AND sr.sr_item_sk = cs.cs_item_sk
JOIN store s ON ss.ss_store_sk = s.s_store_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
GROUP BY i_item_id, i_item_desc, s_state
ORDER BY i_item_id, i_item_desc, s_state
LIMIT 100;