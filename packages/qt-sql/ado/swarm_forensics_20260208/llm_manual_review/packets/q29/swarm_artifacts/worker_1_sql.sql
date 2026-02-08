WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE (d_moy = 4 AND d_year = 1999)
     OR (d_moy BETWEEN 4 AND 7 AND d_year = 1999)
     OR (d_year IN (1999, 2000, 2001))
),
store_sales_filtered AS (
  SELECT
    ss_quantity,
    ss_item_sk,
    ss_store_sk,
    ss_customer_sk,
    ss_ticket_number,
    ss_sold_date_sk
  FROM store_sales
  WHERE ss_sold_date_sk IN (SELECT d_date_sk FROM filtered_dates WHERE d_moy = 4 AND d_year = 1999)
),
store_returns_filtered AS (
  SELECT
    sr_return_quantity,
    sr_item_sk,
    sr_customer_sk,
    sr_ticket_number,
    sr_returned_date_sk
  FROM store_returns
  WHERE sr_returned_date_sk IN (SELECT d_date_sk FROM filtered_dates WHERE d_moy BETWEEN 4 AND 7 AND d_year = 1999)
),
catalog_sales_filtered AS (
  SELECT
    cs_quantity,
    cs_item_sk,
    cs_bill_customer_sk,
    cs_sold_date_sk
  FROM catalog_sales
  WHERE cs_sold_date_sk IN (SELECT d_date_sk FROM filtered_dates WHERE d_year IN (1999, 2000, 2001))
)
SELECT
  i.i_item_id,
  i.i_item_desc,
  s.s_store_id,
  s.s_store_name,
  AVG(ss.ss_quantity) AS store_sales_quantity,
  AVG(sr.sr_return_quantity) AS store_returns_quantity,
  AVG(cs.cs_quantity) AS catalog_sales_quantity
FROM store_sales_filtered ss
JOIN item i ON i.i_item_sk = ss.ss_item_sk
JOIN store s ON s.s_store_sk = ss.ss_store_sk
JOIN store_returns_filtered sr
  ON sr.sr_customer_sk = ss.ss_customer_sk
  AND sr.sr_item_sk = ss.ss_item_sk
  AND sr.sr_ticket_number = ss.ss_ticket_number
JOIN catalog_sales_filtered cs
  ON cs.cs_bill_customer_sk = sr.sr_customer_sk
  AND cs.cs_item_sk = sr.sr_item_sk
GROUP BY
  i.i_item_id,
  i.i_item_desc,
  s.s_store_id,
  s.s_store_name
ORDER BY
  i.i_item_id,
  i.i_item_desc,
  s.s_store_id,
  s.s_store_name
LIMIT 100;