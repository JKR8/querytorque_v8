WITH store_agg AS (
    SELECT
        'store' AS channel,
        'ss_hdemo_sk' AS col_name,
        d.d_year,
        d.d_qoy,
        i.i_category,
        COUNT(*) AS sales_cnt,
        SUM(ss_ext_sales_price) AS sales_amt
    FROM store_sales ss
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN item i ON ss.ss_item_sk = i.i_item_sk
    WHERE ss.ss_hdemo_sk IS NULL
    GROUP BY d.d_year, d.d_qoy, i.i_category
),
web_agg AS (
    SELECT
        'web' AS channel,
        'ws_bill_addr_sk' AS col_name,
        d.d_year,
        d.d_qoy,
        i.i_category,
        COUNT(*) AS sales_cnt,
        SUM(ws_ext_sales_price) AS sales_amt
    FROM web_sales ws
    JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN item i ON ws.ws_item_sk = i.i_item_sk
    WHERE ws.ws_bill_addr_sk IS NULL
    GROUP BY d.d_year, d.d_qoy, i.i_category
),
catalog_agg AS (
    SELECT
        'catalog' AS channel,
        'cs_warehouse_sk' AS col_name,
        d.d_year,
        d.d_qoy,
        i.i_category,
        COUNT(*) AS sales_cnt,
        SUM(cs_ext_sales_price) AS sales_amt
    FROM catalog_sales cs
    JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN item i ON cs.cs_item_sk = i.i_item_sk
    WHERE cs.cs_warehouse_sk IS NULL
    GROUP BY d.d_year, d.d_qoy, i.i_category
)
SELECT
    channel,
    col_name,
    d_year,
    d_qoy,
    i_category,
    sales_cnt,
    sales_amt
FROM (
    SELECT * FROM store_agg
    UNION ALL
    SELECT * FROM web_agg
    UNION ALL
    SELECT * FROM catalog_agg
) AS combined
ORDER BY
    channel,
    col_name,
    d_year,
    d_qoy,
    i_category
LIMIT 100;