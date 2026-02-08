WITH filtered_reason AS (
    SELECT r_reason_sk
    FROM reason
    WHERE r_reason_desc = 'duplicate purchase'
),
filtered_returns AS (
    SELECT
        sr_item_sk,
        sr_ticket_number,
        sr_return_quantity
    FROM store_returns
    JOIN filtered_reason ON sr_reason_sk = r_reason_sk
)
SELECT
    ss_customer_sk,
    SUM(
        CASE
            WHEN NOT sr_return_quantity IS NULL
            THEN (ss_quantity - sr_return_quantity) * ss_sales_price
            ELSE (ss_quantity * ss_sales_price)
        END
    ) AS sumsales
FROM store_sales
JOIN filtered_returns ON
    sr_item_sk = ss_item_sk AND
    sr_ticket_number = ss_ticket_number
GROUP BY ss_customer_sk
ORDER BY sumsales, ss_customer_sk
LIMIT 100