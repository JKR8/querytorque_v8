SELECT store_sales.ss_customer_sk AS SS_CUSTOMER_SK, SUM(CASE WHEN store_returns.sr_return_quantity IS NOT NULL THEN (store_sales.ss_quantity - store_returns.sr_return_quantity) * store_sales.ss_sales_price ELSE store_sales.ss_quantity * store_sales.ss_sales_price END) AS SUMSALES
FROM store_sales
LEFT JOIN store_returns ON store_sales.ss_item_sk = store_returns.sr_item_sk AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
INNER JOIN (SELECT *
FROM reason
WHERE r_reason_desc = 'reason 28') AS t ON store_returns.sr_reason_sk = t.r_reason_sk
GROUP BY store_sales.ss_customer_sk
ORDER BY 2 NULLS FIRST, store_sales.ss_customer_sk NULLS FIRST
FETCH NEXT 100 ROWS ONLY