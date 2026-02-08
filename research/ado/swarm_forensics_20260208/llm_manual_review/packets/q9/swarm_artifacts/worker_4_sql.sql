WITH store_sales_agg AS (
    SELECT
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS cnt1,
        AVG(ss_ext_sales_price) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS avg_ext1,
        AVG(ss_net_profit) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS avg_net1,
        
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS cnt2,
        AVG(ss_ext_sales_price) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS avg_ext2,
        AVG(ss_net_profit) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS avg_net2,
        
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS cnt3,
        AVG(ss_ext_sales_price) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS avg_ext3,
        AVG(ss_net_profit) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS avg_net3,
        
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS cnt4,
        AVG(ss_ext_sales_price) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS avg_ext4,
        AVG(ss_net_profit) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS avg_net4,
        
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS cnt5,
        AVG(ss_ext_sales_price) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS avg_ext5,
        AVG(ss_net_profit) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS avg_net5
    FROM store_sales
)
SELECT
    CASE WHEN cnt1 > 2972190 THEN avg_ext1 ELSE avg_net1 END AS bucket1,
    CASE WHEN cnt2 > 4505785 THEN avg_ext2 ELSE avg_net2 END AS bucket2,
    CASE WHEN cnt3 > 1575726 THEN avg_ext3 ELSE avg_net3 END AS bucket3,
    CASE WHEN cnt4 > 3188917 THEN avg_ext4 ELSE avg_net4 END AS bucket4,
    CASE WHEN cnt5 > 3525216 THEN avg_ext5 ELSE avg_net5 END AS bucket5
FROM store_sales_agg
CROSS JOIN reason
WHERE r_reason_sk = 1;