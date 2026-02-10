SELECT 
    SUM(ws.ws_ext_discount_amt) AS "Excess Discount Amount"
FROM 
    web_sales ws
JOIN 
    item i ON i.i_item_sk = ws.ws_item_sk
JOIN 
    date_dim d ON d.d_date_sk = ws.ws_sold_date_sk
JOIN 
    (
        SELECT 
            ws_inner.ws_item_sk,
            1.3 * AVG(ws_inner.ws_ext_discount_amt) AS avg_discount_threshold
        FROM 
            web_sales ws_inner
        JOIN 
            date_dim d_inner ON d_inner.d_date_sk = ws_inner.ws_sold_date_sk
        WHERE 
            d_inner.d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL 90 DAY)
        GROUP BY 
            ws_inner.ws_item_sk
    ) item_avg ON ws.ws_item_sk = item_avg.ws_item_sk
WHERE 
    i.i_manufact_id = 320
    AND d.d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL 90 DAY)
    AND ws.ws_ext_discount_amt > item_avg.avg_discount_threshold
ORDER BY 
    SUM(ws.ws_ext_discount_amt)
LIMIT 100;