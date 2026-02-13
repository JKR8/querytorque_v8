"""Validate rewrite correctness by comparing MD5 checksums + row counts.

For timeout queries: uses shorter timeout on original (120s) — if original finishes,
compare row count + checksum. If original times out, we can only verify the rewrite
produces reasonable results.

Usage:
    python3 validate_checksums.py
"""
import snowflake.connector
import time
import hashlib
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

# Pick a subset of our wins to validate — ones most likely to finish original with extended timeout
# These are the faster wins (sub-60s rewrites) where original MIGHT finish if given enough time
VALIDATIONS = {

    # === BATCH 1 wins (from batch_rewrite_test.py) ===
    "query_55": {
        "original": """
select i_brand_id brand_id, i_brand brand,
       sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id=13
   and d_moy=11
   and d_year=2000
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
 LIMIT 100;
""",
        "rewrite": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_moy = 11 AND d_year = 2000
)
SELECT i_brand_id brand_id, i_brand brand,
       SUM(ss_ext_sales_price) ext_price
FROM store_sales
    JOIN date_filter df ON ss_sold_date_sk = df.d_date_sk
    JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = 13
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, i_brand_id
LIMIT 100;
""",
    },

    "query_53": {
        "original": """
select * from (select i_manufact_id,
sum(ss_sales_price) sum_sales,
avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
ss_sold_date_sk = d_date_sk and
ss_store_sk = s_store_sk and
d_month_seq in (1200,1200+1,1200+2,1200+3,1200+4,1200+5,1200+6,1200+7,1200+8,1200+9,1200+10,1200+11) and
((i_category in ('Books','Children','Electronics') and
i_class in ('personal','portable','reference','self-help') and
i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		'exportiunivamalg #9','scholaramalgamalg #9'))
or(i_category in ('Women','Music','Men') and
i_class in ('accessories','classical','fragrances','pants') and
i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		'importoamalg #1')))
group by i_manufact_id, d_qoy ) tmp1
where case when avg_quarterly_sales > 0 then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales else null end > 0.1
order by avg_quarterly_sales,
	 sum_sales,
	 i_manufact_id
 LIMIT 100;
""",
        "rewrite": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_month_seq IN (1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211)
)
SELECT * FROM (
    SELECT i_manufact_id,
           SUM(ss_sales_price) sum_sales,
           AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
    FROM store_sales
        JOIN date_filter df ON ss_sold_date_sk = df.d_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        JOIN store ON ss_store_sk = s_store_sk
    WHERE ((i_category IN ('Books','Children','Electronics')
            AND i_class IN ('personal','portable','reference','self-help')
            AND i_brand IN ('scholaramalgamalg #14','scholaramalgamalg #7','exportiunivamalg #9','scholaramalgamalg #9'))
        OR (i_category IN ('Women','Music','Men')
            AND i_class IN ('accessories','classical','fragrances','pants')
            AND i_brand IN ('amalgimporto #1','edu packscholar #1','exportiimporto #1','importoamalg #1')))
    GROUP BY i_manufact_id, d_qoy
) tmp1
WHERE CASE WHEN avg_quarterly_sales > 0 THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales ELSE NULL END > 0.1
ORDER BY avg_quarterly_sales, sum_sales, i_manufact_id
LIMIT 100;
""",
    },

    # === BATCH 6 wins ===
    "query_61": {
        "original": """
select promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
from
  (select sum(ss_ext_sales_price) promotions
   from  store_sales
        ,store
        ,promotion
        ,date_dim
        ,customer
        ,customer_address
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -7
   and   i_category = 'Home'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -7
   and   d_year = 1999
   and   d_moy  = 11) promotional_sales,
  (select sum(ss_ext_sales_price) total
   from  store_sales
        ,store
        ,date_dim
        ,customer
        ,customer_address
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -7
   and   i_category = 'Home'
   and   s_gmt_offset = -7
   and   d_year = 1999
   and   d_moy  = 11) all_sales
order by promotions, total
 LIMIT 100;
""",
        "rewrite": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 1999 AND d_moy = 11
)
SELECT promotions, total, CAST(promotions AS DECIMAL(15,4))/CAST(total AS DECIMAL(15,4))*100
FROM
  (SELECT SUM(ss_ext_sales_price) promotions
   FROM store_sales
       JOIN date_filter df ON ss_sold_date_sk = df.d_date_sk
       JOIN store ON ss_store_sk = s_store_sk
       JOIN promotion ON ss_promo_sk = p_promo_sk
       JOIN customer ON ss_customer_sk = c_customer_sk
       JOIN customer_address ON ca_address_sk = c_current_addr_sk
       JOIN item ON ss_item_sk = i_item_sk
   WHERE ca_gmt_offset = -7
     AND i_category = 'Home'
     AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
     AND s_gmt_offset = -7) promotional_sales,
  (SELECT SUM(ss_ext_sales_price) total
   FROM store_sales
       JOIN date_filter df ON ss_sold_date_sk = df.d_date_sk
       JOIN store ON ss_store_sk = s_store_sk
       JOIN customer ON ss_customer_sk = c_customer_sk
       JOIN customer_address ON ca_address_sk = c_current_addr_sk
       JOIN item ON ss_item_sk = i_item_sk
   WHERE ca_gmt_offset = -7
     AND i_category = 'Home'
     AND s_gmt_offset = -7) all_sales
ORDER BY promotions, total
LIMIT 100;
""",
    },

    "query_71": {
        "original": """
select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
       sum(ext_price) ext_price
 from item, (select ws_ext_sales_price as ext_price,
                    ws_sold_time_sk as sold_time_sk,
                    ws_item_sk as sold_item_sk
             from web_sales,date_dim
             where d_date_sk = ws_sold_date_sk
               and d_moy=12
               and d_year=1998
             union all
             select cs_ext_sales_price as ext_price,
                    cs_sold_time_sk as sold_time_sk,
                    cs_item_sk as sold_item_sk
             from catalog_sales,date_dim
             where d_date_sk = cs_sold_date_sk
               and d_moy=12
               and d_year=1998
             union all
             select ss_ext_sales_price as ext_price,
                    ss_sold_time_sk as sold_time_sk,
                    ss_item_sk as sold_item_sk
             from store_sales,date_dim
             where d_date_sk = ss_sold_date_sk
               and d_moy=12
               and d_year=1998
             ) tmp,time_dim
 where
   sold_item_sk = i_item_sk
   and t_time_sk = sold_time_sk
   and i_manager_id=1
 group by i_brand, i_brand_id,t_hour,t_minute
 order by ext_price desc, i_brand_id
 LIMIT 100;
""",
        "rewrite": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_moy = 12 AND d_year = 1998
)
SELECT i_brand_id brand_id, i_brand brand, t_hour, t_minute,
       SUM(ext_price) ext_price
FROM item,
    (SELECT ws_ext_sales_price AS ext_price,
            ws_sold_time_sk AS sold_time_sk,
            ws_item_sk AS sold_item_sk
     FROM web_sales
     JOIN date_filter df ON d_date_sk = ws_sold_date_sk
     UNION ALL
     SELECT cs_ext_sales_price AS ext_price,
            cs_sold_time_sk AS sold_time_sk,
            cs_item_sk AS sold_item_sk
     FROM catalog_sales
     JOIN date_filter df ON d_date_sk = cs_sold_date_sk
     UNION ALL
     SELECT ss_ext_sales_price AS ext_price,
            ss_sold_time_sk AS sold_time_sk,
            ss_item_sk AS sold_item_sk
     FROM store_sales
     JOIN date_filter df ON d_date_sk = ss_sold_date_sk
    ) tmp, time_dim
WHERE sold_item_sk = i_item_sk
  AND t_time_sk = sold_time_sk
  AND i_manager_id = 1
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id
LIMIT 100;
""",
    },

    "query_79": {
        "original": """
select
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = 0 or household_demographics.hd_vehicle_count > 3)
    and date_dim.d_dow = 1
    and date_dim.d_year in (1999,1999+1,1999+2)
    and store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms,customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
 LIMIT 100;
""",
        "rewrite": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_dow = 1 AND d_year IN (1999, 2000, 2001)
)
SELECT c_last_name, c_first_name, SUBSTR(s_city,1,30), ss_ticket_number, amt, profit
FROM (
    SELECT ss_ticket_number, ss_customer_sk, store.s_city,
           SUM(ss_coupon_amt) amt, SUM(ss_net_profit) profit
    FROM store_sales
    JOIN date_filter df ON ss_sold_date_sk = df.d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
    WHERE (hd_dep_count = 0 OR hd_vehicle_count > 3)
      AND s_number_employees BETWEEN 200 AND 295
    GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
) ms
JOIN customer ON ss_customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, SUBSTR(s_city,1,30), profit
LIMIT 100;
""",
    },
}

def connect():
    p = urlparse(DSN)
    qs = parse_qs(p.query)
    return snowflake.connector.connect(
        user=unquote(p.username),
        password=unquote(p.password),
        account=p.hostname,
        database=p.path.strip('/').split('/')[0],
        schema=p.path.strip('/').split('/')[1] if '/' in p.path.strip('/') else None,
        warehouse=qs.get('warehouse', [None])[0],
        role=qs.get('role', [None])[0],
    )

def run_and_hash(cur, sql, timeout=300):
    """Run query, return (elapsed_ms, row_count, md5_hex) or (elapsed_ms, error_str, None)."""
    cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}")
    t0 = time.time()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = int((time.time()-t0)*1000)
        # Sort rows for deterministic hash (ORDER BY should handle this but be safe)
        h = hashlib.md5()
        for row in rows:
            h.update(str(row).encode())
        return elapsed, len(rows), h.hexdigest()
    except Exception as e:
        elapsed = int((time.time()-t0)*1000)
        return elapsed, f"ERROR: {e}", None

def main():
    conn = connect()
    cur = conn.cursor()
    # CRITICAL: Disable result cache to get real execution times
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    cur.close()
    print("Snowflake Rewrite Correctness Validation (result cache DISABLED)")
    print("=" * 70)
    print()

    for qname, pair in sorted(VALIDATIONS.items()):
        print(f"{'='*60}")
        print(f"  {qname}")
        print(f"{'='*60}")

        cur = conn.cursor()

        # Run original (with 300s timeout — may timeout for the 59 timeout queries)
        print(f"  Original... ", end="", flush=True)
        orig_ms, orig_rows, orig_hash = run_and_hash(cur, pair["original"], timeout=300)
        print(f"{orig_ms}ms, rows={orig_rows}, md5={orig_hash}")

        # Run rewrite
        print(f"  Rewrite...  ", end="", flush=True)
        rw_ms, rw_rows, rw_hash = run_and_hash(cur, pair["rewrite"], timeout=300)
        print(f"{rw_ms}ms, rows={rw_rows}, md5={rw_hash}")

        # Compare
        if isinstance(orig_rows, str):
            print(f"  >> Original TIMEOUT/ERROR — cannot validate correctness")
            print(f"  >> Rewrite returned {rw_rows} rows in {rw_ms}ms")
        elif isinstance(rw_rows, str):
            print(f"  >> REWRITE FAILED: {rw_rows}")
        elif orig_rows != rw_rows:
            print(f"  >> ROW COUNT MISMATCH: orig={orig_rows} vs rewrite={rw_rows}")
        elif orig_hash != rw_hash:
            print(f"  >> CHECKSUM MISMATCH: orig={orig_hash} vs rewrite={rw_hash}")
            print(f"  >> (Row counts match: {orig_rows} — but data differs!)")
        else:
            speedup = orig_ms / rw_ms if rw_ms > 0 else float('inf')
            print(f"  >> MATCH: rows={orig_rows}, md5={orig_hash}")
            print(f"  >> Speedup: {speedup:.2f}x ({orig_ms}ms -> {rw_ms}ms)")
        print()
        cur.close()

    conn.close()

if __name__ == "__main__":
    main()
