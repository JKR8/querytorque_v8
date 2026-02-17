SELECT 
    c.customer_id,
    c.customer_name,
    o.order_date,
    SUM(o.order_amount) as total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= '2020-01-01'
GROUP BY c.customer_id, c.customer_name, o.order_date
HAVING SUM(o.order_amount) > 100
ORDER BY total_amount DESC
LIMIT 1000
