SELECT
  *
FROM (
  WITH consolidated_scan AS (
    SELECT
      CASE
        WHEN ss_quantity BETWEEN 0 AND 5
        AND (
          ss_list_price BETWEEN 131 AND 141
          OR ss_coupon_amt BETWEEN 16798 AND 17798
          OR ss_wholesale_cost BETWEEN 25 AND 45
        )
        THEN ss_list_price
      END AS b1_price,
      CASE
        WHEN ss_quantity BETWEEN 6 AND 10
        AND (
          ss_list_price BETWEEN 145 AND 155
          OR ss_coupon_amt BETWEEN 14792 AND 15792
          OR ss_wholesale_cost BETWEEN 46 AND 66
        )
        THEN ss_list_price
      END AS b2_price,
      CASE
        WHEN ss_quantity BETWEEN 11 AND 15
        AND (
          ss_list_price BETWEEN 150 AND 160
          OR ss_coupon_amt BETWEEN 6600 AND 7600
          OR ss_wholesale_cost BETWEEN 9 AND 29
        )
        THEN ss_list_price
      END AS b3_price,
      CASE
        WHEN ss_quantity BETWEEN 16 AND 20
        AND (
          ss_list_price BETWEEN 91 AND 101
          OR ss_coupon_amt BETWEEN 13493 AND 14493
          OR ss_wholesale_cost BETWEEN 36 AND 56
        )
        THEN ss_list_price
      END AS b4_price,
      CASE
        WHEN ss_quantity BETWEEN 21 AND 25
        AND (
          ss_list_price BETWEEN 0 AND 10
          OR ss_coupon_amt BETWEEN 7629 AND 8629
          OR ss_wholesale_cost BETWEEN 6 AND 26
        )
        THEN ss_list_price
      END AS b5_price,
      CASE
        WHEN ss_quantity BETWEEN 26 AND 30
        AND (
          ss_list_price BETWEEN 89 AND 99
          OR ss_coupon_amt BETWEEN 15257 AND 16257
          OR ss_wholesale_cost BETWEEN 31 AND 51
        )
        THEN ss_list_price
      END AS b6_price
    FROM store_sales
    WHERE
      ss_quantity BETWEEN 0 AND 30
      AND (
        (
          ss_quantity BETWEEN 0 AND 5
          AND (
            ss_list_price BETWEEN 131 AND 141
            OR ss_coupon_amt BETWEEN 16798 AND 17798
            OR ss_wholesale_cost BETWEEN 25 AND 45
          )
        )
        OR (
          ss_quantity BETWEEN 6 AND 10
          AND (
            ss_list_price BETWEEN 145 AND 155
            OR ss_coupon_amt BETWEEN 14792 AND 15792
            OR ss_wholesale_cost BETWEEN 46 AND 66
          )
        )
        OR (
          ss_quantity BETWEEN 11 AND 15
          AND (
            ss_list_price BETWEEN 150 AND 160
            OR ss_coupon_amt BETWEEN 6600 AND 7600
            OR ss_wholesale_cost BETWEEN 9 AND 29
          )
        )
        OR (
          ss_quantity BETWEEN 16 AND 20
          AND (
            ss_list_price BETWEEN 91 AND 101
            OR ss_coupon_amt BETWEEN 13493 AND 14493
            OR ss_wholesale_cost BETWEEN 36 AND 56
          )
        )
        OR (
          ss_quantity BETWEEN 21 AND 25
          AND (
            ss_list_price BETWEEN 0 AND 10
            OR ss_coupon_amt BETWEEN 7629 AND 8629
            OR ss_wholesale_cost BETWEEN 6 AND 26
          )
        )
        OR (
          ss_quantity BETWEEN 26 AND 30
          AND (
            ss_list_price BETWEEN 89 AND 99
            OR ss_coupon_amt BETWEEN 15257 AND 16257
            OR ss_wholesale_cost BETWEEN 31 AND 51
          )
        )
      )
  )
  SELECT
    AVG(b1_price) AS B1_LP,
    COUNT(b1_price) AS B1_CNT,
    COUNT(DISTINCT b1_price) AS B1_CNTD,
    AVG(b2_price) AS B2_LP,
    COUNT(b2_price) AS B2_CNT,
    COUNT(DISTINCT b2_price) AS B2_CNTD,
    AVG(b3_price) AS B3_LP,
    COUNT(b3_price) AS B3_CNT,
    COUNT(DISTINCT b3_price) AS B3_CNTD,
    AVG(b4_price) AS B4_LP,
    COUNT(b4_price) AS B4_CNT,
    COUNT(DISTINCT b4_price) AS B4_CNTD,
    AVG(b5_price) AS B5_LP,
    COUNT(b5_price) AS B5_CNT,
    COUNT(DISTINCT b5_price) AS B5_CNTD,
    AVG(b6_price) AS B6_LP,
    COUNT(b6_price) AS B6_CNT,
    COUNT(DISTINCT b6_price) AS B6_CNTD
  FROM consolidated_scan
) AS consolidated_results
LIMIT 100