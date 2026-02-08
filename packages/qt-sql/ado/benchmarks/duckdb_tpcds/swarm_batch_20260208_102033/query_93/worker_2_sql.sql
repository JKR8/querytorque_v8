WITH filtered_reason AS (
    SELECT r_reason_sk
    FROM reason
    WHERE r_reason_desc = 'duplicate purchase'
)
SELECT
    ss_customer_sk,
    SUM(act_sales) AS sumsales
FROM (
    SELECT
        ss_customer_sk,
        CASE
            WHEN NOT sr_return_quantity IS NULL
            THEN (ss_quantity - sr_return_quantity) * ss_sales_price
            ELSE (ss_quantity * ss_sales_price)
        END AS act_sales
    FROM store_sales
    LEFT OUTER JOIN store_returns
        ON sr_item_sk = ss_item_sk 
        AND sr_ticket_number = ss_ticket_number
        AND sr_reason_sk IN (SELECT r_reason_sk FROM filtered_reason)
) AS t
WHERE EXISTS (
    SELECT 1 
    FROM filtered_reason fr 
    WHERE t.sr_reason_sk = fr.r_reason_sk
    OR t.sr_reason_sk IS NULL
)
GROUP BY ss_customer_sk
ORDER BY sumsales, ss_customer_sk
LIMIT 100