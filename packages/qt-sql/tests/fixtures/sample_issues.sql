-- Sample SQL with multiple anti-patterns
-- Should score around 60 or lower

-- Issue 1: SELECT * (SQL-SEL-001)
-- Issue 2: Cartesian join / implicit join (SQL-JOIN-001)
-- Issue 3: Function on indexed column (SQL-WHERE-001)
-- Issue 4: ORDER BY ordinal (SQL-ORDER-004)

SELECT *
FROM users u, orders o, products p
WHERE UPPER(u.email) = 'TEST@EXAMPLE.COM'
    AND u.id = o.user_id
    AND o.product_id = p.id
    AND p.active = true
ORDER BY 1, 2;
