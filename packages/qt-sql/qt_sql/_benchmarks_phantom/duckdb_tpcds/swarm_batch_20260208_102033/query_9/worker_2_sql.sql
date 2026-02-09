WITH filtered_reason AS (
    SELECT NULL AS placeholder
    FROM reason
    WHERE r_reason_sk = 1
),
bucket_aggs AS (
    SELECT
        COUNT(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN 1 END) AS cnt1,
        AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_sales_price END) AS avg_ext1,
        AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_profit END) AS avg_net1,
        COUNT(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN 1 END) AS cnt2,
        AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_ext_sales_price END) AS avg_ext2,
        AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_net_profit END) AS avg_net2,
        COUNT(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN 1 END) AS cnt3,
        AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_ext_sales_price END) AS avg_ext3,
        AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_net_profit END) AS avg_net3,
        COUNT(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN 1 END) AS cnt4,
        AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_ext_sales_price END) AS avg_ext4,
        AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_net_profit END) AS avg_net4,
        COUNT(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN 1 END) AS cnt5,
        AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_ext_sales_price END) AS avg_ext5,
        AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_net_profit END) AS avg_net5
    FROM store_sales
)
SELECT
    CASE WHEN cnt1 > 2972190 THEN avg_ext1 ELSE avg_net1 END AS bucket1,
    CASE WHEN cnt2 > 4505785 THEN avg_ext2 ELSE avg_net2 END AS bucket2,
    CASE WHEN cnt3 > 1575726 THEN avg_ext3 ELSE avg_net3 END AS bucket3,
    CASE WHEN cnt4 > 3188917 THEN avg_ext4 ELSE avg_net4 END AS bucket4,
    CASE WHEN cnt5 > 3525216 THEN avg_ext5 ELSE avg_net5 END AS bucket5
FROM filtered_reason, bucket_aggs;