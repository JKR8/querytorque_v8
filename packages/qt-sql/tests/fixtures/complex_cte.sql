-- Complex CTE SQL for testing multi-reference detection

WITH
-- First CTE: Active users
active_users AS (
    SELECT
        id,
        name,
        email,
        tier
    FROM users
    WHERE active = true
        AND created_at >= '2023-01-01'
),

-- Second CTE: User order summaries (references orders table)
user_orders AS (
    SELECT
        user_id,
        COUNT(*) AS order_count,
        SUM(amount) AS total_amount,
        MAX(order_date) AS last_order_date
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY user_id
),

-- Third CTE: Combines users and orders (references both CTEs)
user_summary AS (
    SELECT
        au.id,
        au.name,
        au.email,
        au.tier,
        COALESCE(uo.order_count, 0) AS order_count,
        COALESCE(uo.total_amount, 0) AS total_amount,
        uo.last_order_date
    FROM active_users au
    LEFT JOIN user_orders uo ON au.id = uo.user_id
),

-- Fourth CTE: Tier statistics (references user_summary)
tier_stats AS (
    SELECT
        tier,
        COUNT(*) AS user_count,
        SUM(order_count) AS total_orders,
        AVG(total_amount) AS avg_spending
    FROM user_summary
    GROUP BY tier
)

-- Final query joins summaries
SELECT
    us.id,
    us.name,
    us.tier,
    us.order_count,
    us.total_amount,
    ts.user_count AS tier_user_count,
    ts.avg_spending AS tier_avg_spending,
    CASE
        WHEN us.total_amount > ts.avg_spending THEN 'Above Average'
        WHEN us.total_amount < ts.avg_spending THEN 'Below Average'
        ELSE 'Average'
    END AS spending_category
FROM user_summary us
JOIN tier_stats ts ON us.tier = ts.tier
WHERE us.order_count > 0
ORDER BY us.total_amount DESC
LIMIT 100;
