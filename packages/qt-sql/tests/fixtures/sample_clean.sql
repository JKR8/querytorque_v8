-- Sample clean SQL that should score 100
-- No anti-patterns, proper structure

SELECT
    u.id,
    u.name,
    u.email,
    u.created_at,
    COUNT(o.id) AS order_count,
    COALESCE(SUM(o.amount), 0) AS total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.active = true
    AND u.created_at >= '2024-01-01'
GROUP BY u.id, u.name, u.email, u.created_at
HAVING COUNT(o.id) > 0
ORDER BY total_spent DESC
LIMIT 100;
