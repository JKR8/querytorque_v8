SELECT 
    customer_id,
    customer_name,
    customer_state
FROM customers
WHERE customer_state IN ('CA', 'TX', 'NY')
ORDER BY customer_id
LIMIT 1000
