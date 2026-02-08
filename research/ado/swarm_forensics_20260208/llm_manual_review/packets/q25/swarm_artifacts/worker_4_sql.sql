WITH filtered_dates AS (
  -- Consolidate all date filters into a single CTE
  SELECT d_date_sk
  FROM date_dim 
  WHERE (d_year = 2000 AND d_moy = 4)
     OR (d_year = 2000 AND d_moy BETWEEN 4 AND 10)
),
store_sales_filtered AS (
  -- Early filter store_sales with April 2000 date
  SELECT 
    ss_item_sk,
    ss_customer_sk,
    ss_ticket_number,
    ss_store_sk,
    ss_net_profit
  FROM store_sales
  WHERE ss_sold_date_sk IN (
    SELECT d_date_sk FROM filtered_dates WHERE d_year = 2000 AND d_moy = 4
  )
),
store_returns_filtered AS (
  -- Early filter store_returns with Apr-Oct 2000 date
  SELECT 
    sr_item_sk,
    sr_customer_sk,
    sr_ticket_number,
    sr_net_loss,
    sr_returned_date_sk
  FROM store_returns
  WHERE sr_returned_date_sk IN (
    SELECT d_date_sk FROM filtered_dates WHERE d_year = 2000 AND d_moy BETWEEN 4 AND 10
  )
),
catalog_sales_filtered AS (
  -- Early filter catalog_sales with Apr-Oct 2000 date
  SELECT 
    cs_item_sk,
    cs_bill_customer_sk,
    cs_net_profit,
    cs_sold_date_sk
  FROM catalog_sales
  WHERE cs_sold_date_sk IN (
    SELECT d_date_sk FROM filtered_dates WHERE d_year = 2000 AND d_moy BETWEEN 4 AND 10
  )
),
joined_base AS (
  -- Explicit join chain with filtered fact tables
  SELECT 
    i.i_item_id,
    i.i_item_desc,
    s.s_store_id,
    s.s_store_name,
    ss.ss_net_profit,
    sr.sr_net_loss,
    cs.cs_net_profit
  FROM store_sales_filtered ss
  JOIN item i ON i.i_item_sk = ss.ss_item_sk
  JOIN store s ON s.s_store_sk = ss.ss_store_sk
  JOIN store_returns_filtered sr 
    ON ss.ss_customer_sk = sr.sr_customer_sk
    AND ss.ss_item_sk = sr.sr_item_sk
    AND ss.ss_ticket_number = sr.sr_ticket_number
  JOIN catalog_sales_filtered cs
    ON sr.sr_customer_sk = cs.cs_bill_customer_sk
    AND sr.sr_item_sk = cs.cs_item_sk
)
SELECT 
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name,
  SUM(ss_net_profit) AS store_sales_profit,
  SUM(sr_net_loss) AS store_returns_loss,
  SUM(cs_net_profit) AS catalog_sales_profit
FROM joined_base
GROUP BY 
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name
ORDER BY 
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name
LIMIT 100