SELECT 
    p.product_category,
    c.customer_state,
    COUNT(*) as transaction_count,
    AVG(s.sales_amount) as avg_sales
FROM sales s
JOIN products p ON s.product_sk = p.product_sk
JOIN customers c ON s.customer_sk = c.customer_sk
WHERE s.sales_date BETWEEN '2020-01-01' AND '2022-12-31'
  AND p.product_category IN ('Electronics', 'Clothing', 'Food')
GROUP BY p.product_category, c.customer_state
