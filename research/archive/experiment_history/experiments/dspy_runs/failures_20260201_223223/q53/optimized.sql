-- start query 53 in stream 0 using template query53.tpl
WITH filtered_date AS (
    SELECT d_date_sk, d_qoy
    FROM date_dim
    WHERE d_month_seq IN (1200,1200+1,1200+2,1200+3,1200+4,1200+5,1200+6,1200+7,1200+8,1200+9,1200+10,1200+11)
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_sk <= 400
),
filtered_item AS (
    SELECT i_item_sk, i_manufact_id
    FROM item
    WHERE ((i_category IN ('Books','Children','Electronics') AND
            i_class IN ('personal','portable','reference','self-help') AND
            i_brand IN ('scholaramalgamalg #14','scholaramalgamalg #7',
                        'exportiunivamalg #9','scholaramalgamalg #9'))
           OR (i_category IN ('Women','Music','Men') AND
               i_class IN ('accessories','classical','fragrances','pants') AND
               i_brand IN ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
                           'importoamalg #1')))
)
SELECT * FROM 
(SELECT i.i_manufact_id,
        sum(ss.ss_sales_price) sum_sales,
        avg(sum(ss.ss_sales_price)) OVER (PARTITION BY i.i_manufact_id) avg_quarterly_sales
 FROM store_sales ss
 JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
 JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
 JOIN filtered_store s ON ss.ss_store_sk = s.s_store_sk
 GROUP BY i.i_manufact_id, d.d_qoy ) tmp1
WHERE CASE WHEN avg_quarterly_sales > 0 
           THEN ABS(sum_sales - avg_quarterly_sales)/ avg_quarterly_sales 
           ELSE NULL END > 0.1
ORDER BY avg_quarterly_sales,
         sum_sales,
         i_manufact_id
 LIMIT 100;

-- end query 53 in stream 0 using template query53.tpl