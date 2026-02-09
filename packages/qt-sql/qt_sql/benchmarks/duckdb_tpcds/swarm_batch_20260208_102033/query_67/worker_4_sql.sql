WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq BETWEEN 1206 AND 1206 + 11
),
-- Base aggregation without ROLLUP - all 8 dimensions
base_agg AS (
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        s_store_id,
        SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN item ON ss_item_sk = i_item_sk
    JOIN store ON ss_store_sk = s_store_sk
    GROUP BY
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        s_store_id
),
-- Rollup levels created via UNION ALL instead of ROLLUP
rollup_levels AS (
    -- Level 1: All 8 dimensions
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        s_store_id,
        sumsales
    FROM base_agg
    
    UNION ALL
    
    -- Level 2: Without s_store_id
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        NULL AS s_store_id,
        SUM(sumsales) AS sumsales
    FROM base_agg
    GROUP BY
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy
        
    UNION ALL
    
    -- Level 3: Without d_moy
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        NULL AS d_moy,
        NULL AS s_store_id,
        SUM(sumsales) AS sumsales
    FROM base_agg
    GROUP BY
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy
        
    UNION ALL
    
    -- Level 4: Without d_qoy
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        NULL AS d_qoy,
        NULL AS d_moy,
        NULL AS s_store_id,
        SUM(sumsales) AS sumsales
    FROM base_agg
    GROUP BY
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year
        
    UNION ALL
    
    -- Level 5: Without d_year
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        NULL AS d_year,
        NULL AS d_qoy,
        NULL AS d_moy,
        NULL AS s_store_id,
        SUM(sumsales) AS sumsales
    FROM base_agg
    GROUP BY
        i_category,
        i_class,
        i_brand,
        i_product_name
        
    UNION ALL
    
    -- Level 6: Without i_product_name
    SELECT
        i_category,
        i_class,
        i_brand,
        NULL AS i_product_name,
        NULL AS d_year,
        NULL AS d_qoy,
        NULL AS d_moy,
        NULL AS s_store_id,
        SUM(sumsales) AS sumsales
    FROM base_agg
    GROUP BY
        i_category,
        i_class,
        i_brand
        
    UNION ALL
    
    -- Level 7: Without i_brand
    SELECT
        i_category,
        i_class,
        NULL AS i_brand,
        NULL AS i_product_name,
        NULL AS d_year,
        NULL AS d_qoy,
        NULL AS d_moy,
        NULL AS s_store_id,
        SUM(sumsales) AS sumsales
    FROM base_agg
    GROUP BY
        i_category,
        i_class
        
    UNION ALL
    
    -- Level 8: Without i_class
    SELECT
        i_category,
        NULL AS i_class,
        NULL AS i_brand,
        NULL AS i_product_name,
        NULL AS d_year,
        NULL AS d_qoy,
        NULL AS d_moy,
        NULL AS s_store_id,
        SUM(sumsales) AS sumsales
    FROM base_agg
    GROUP BY
        i_category
        
    UNION ALL
    
    -- Level 9: Grand total
    SELECT
        NULL AS i_category,
        NULL AS i_class,
        NULL AS i_brand,
        NULL AS i_product_name,
        NULL AS d_year,
        NULL AS d_qoy,
        NULL AS d_moy,
        NULL AS s_store_id,
        SUM(sumsales) AS sumsales
    FROM base_agg
),
ranked AS (
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        s_store_id,
        sumsales,
        RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) AS rk
    FROM rollup_levels
)
SELECT
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    sumsales,
    rk
FROM ranked
WHERE rk <= 100
ORDER BY
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    sumsales,
    rk
LIMIT 100;