SELECT CASE WHEN t1.EXPR$0 > 74129 THEN t4.EXPR$0 ELSE t7.EXPR$0 END AS BUCKET1, CASE WHEN t9.EXPR$0 > 122840 THEN t12.EXPR$0 ELSE t15.EXPR$0 END AS BUCKET2, CASE WHEN t17.EXPR$0 > 56580 THEN t20.EXPR$0 ELSE t23.EXPR$0 END AS BUCKET3, CASE WHEN t25.EXPR$0 > 10097 THEN t28.EXPR$0 ELSE t31.EXPR$0 END AS BUCKET4, CASE WHEN t33.EXPR$0 > 165306 THEN t36.EXPR$0 ELSE t39.EXPR$0 END AS BUCKET5
FROM (SELECT *
FROM reason
WHERE r_reason_sk = 1) AS t
LEFT JOIN (SELECT COUNT(*) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 1 AND ss_quantity <= 20) AS t1 ON TRUE
LEFT JOIN (SELECT AVG(ss_ext_discount_amt) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 1 AND ss_quantity <= 20) AS t4 ON TRUE
LEFT JOIN (SELECT AVG(ss_net_paid) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 1 AND ss_quantity <= 20) AS t7 ON TRUE
LEFT JOIN (SELECT COUNT(*) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 21 AND ss_quantity <= 40) AS t9 ON TRUE
LEFT JOIN (SELECT AVG(ss_ext_discount_amt) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 21 AND ss_quantity <= 40) AS t12 ON TRUE
LEFT JOIN (SELECT AVG(ss_net_paid) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 21 AND ss_quantity <= 40) AS t15 ON TRUE
LEFT JOIN (SELECT COUNT(*) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 41 AND ss_quantity <= 60) AS t17 ON TRUE
LEFT JOIN (SELECT AVG(ss_ext_discount_amt) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 41 AND ss_quantity <= 60) AS t20 ON TRUE
LEFT JOIN (SELECT AVG(ss_net_paid) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 41 AND ss_quantity <= 60) AS t23 ON TRUE
LEFT JOIN (SELECT COUNT(*) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 61 AND ss_quantity <= 80) AS t25 ON TRUE
LEFT JOIN (SELECT AVG(ss_ext_discount_amt) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 61 AND ss_quantity <= 80) AS t28 ON TRUE
LEFT JOIN (SELECT AVG(ss_net_paid) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 61 AND ss_quantity <= 80) AS t31 ON TRUE
LEFT JOIN (SELECT COUNT(*) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 81 AND ss_quantity <= 100) AS t33 ON TRUE
LEFT JOIN (SELECT AVG(ss_ext_discount_amt) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 81 AND ss_quantity <= 100) AS t36 ON TRUE
LEFT JOIN (SELECT AVG(ss_net_paid) AS EXPR$0
FROM store_sales
WHERE ss_quantity >= 81 AND ss_quantity <= 100) AS t39 ON TRUE