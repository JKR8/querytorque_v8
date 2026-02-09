WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1224 AND 1224 + 11
),
prefiltered_sales AS (
    SELECT 
        cs_sold_date_sk,
        cs_ship_date_sk,
        cs_warehouse_sk,
        cs_ship_mode_sk,
        cs_call_center_sk
    FROM catalog_sales
    INNER JOIN filtered_dates ON cs_ship_date_sk = d_date_sk
)
SELECT
    SUBSTRING(w_warehouse_name, 1, 20),
    sm_type,
    cc_name,
    SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days",
    SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 30 AND cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days",
    SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 60 AND cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days",
    SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 90 AND cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days",
    SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM prefiltered_sales
JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
JOIN ship_mode ON cs_ship_mode_sk = sm_ship_mode_sk
JOIN call_center ON cs_call_center_sk = cc_call_center_sk
GROUP BY
    SUBSTRING(w_warehouse_name, 1, 20),
    sm_type,
    cc_name
ORDER BY
    SUBSTRING(w_warehouse_name, 1, 20),
    sm_type,
    cc_name
LIMIT 100