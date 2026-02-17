
SELECT c.customer_id, c.customer_name, o.order_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
LIMIT 1000
