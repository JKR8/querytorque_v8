WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1194 AND 1194 + 11
)
SELECT
    SUBSTRING(w_warehouse_name, 1, 20),
    sm_type,
    web_name,
    SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days",
    SUM(
        CASE
            WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
            AND (ws_ship_date_sk - ws_sold_date_sk <= 60)
            THEN 1
            ELSE 0
        END
    ) AS "31-60 days",
    SUM(
        CASE
            WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
            AND (ws_ship_date_sk - ws_sold_date_sk <= 90)
            THEN 1
            ELSE 0
        END
    ) AS "61-90 days",
    SUM(
        CASE
            WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
            AND (ws_ship_date_sk - ws_sold_date_sk <= 120)
            THEN 1
            ELSE 0
        END
    ) AS "91-120 days",
    SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM web_sales
JOIN filtered_date ON ws_ship_date_sk = d_date_sk
JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
JOIN ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk
JOIN web_site ON ws_web_site_sk = web_site_sk
GROUP BY
    SUBSTRING(w_warehouse_name, 1, 20),
    sm_type,
    web_name
ORDER BY
    SUBSTRING(w_warehouse_name, 1, 20),
    sm_type,
    web_name
LIMIT 100