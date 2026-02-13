"""Test rewrite for query_12: date CTE isolation for partition pruning."""
import snowflake.connector
import json
import time
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

OPTIMIZED_SQL = """
WITH date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1998-04-06' AND DATEADD(DAY, 30, '1998-04-06'::DATE)
)
SELECT i_item_id
      ,i_item_desc
      ,i_category
      ,i_class
      ,i_current_price
      ,SUM(ws_ext_sales_price) AS itemrevenue
      ,SUM(ws_ext_sales_price)*100/SUM(SUM(ws_ext_sales_price)) OVER
          (PARTITION BY i_class) AS revenueratio
FROM web_sales
    JOIN date_filter ON ws_sold_date_sk = d_date_sk
    JOIN item ON ws_item_sk = i_item_sk
WHERE i_category IN ('Books', 'Sports', 'Men')
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
"""

def connect():
    parsed = urlparse(DSN)
    return snowflake.connector.connect(
        user=unquote(parsed.username),
        password=unquote(parsed.password),
        account=parsed.hostname,
        database=parsed.path.split('/')[1],
        schema=parsed.path.split('/')[2],
        warehouse=parse_qs(parsed.query).get('warehouse', [''])[0],
        role=parse_qs(parsed.query).get('role', [''])[0],
    )

def run_explain(cur, sql):
    cur.execute(f"EXPLAIN {sql}")
    return cur.fetchall()

def run_explain_json(cur, sql):
    cur.execute(f"SELECT SYSTEM$EXPLAIN_PLAN_JSON('{sql.replace(chr(39), chr(39)+chr(39))}')")
    return json.loads(cur.fetchone()[0])

def run_timed(cur, sql, timeout=300):
    """Run query with timeout, return (time_ms, row_count)."""
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}")
    t0 = time.time()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = (time.time() - t0) * 1000
        return elapsed, len(rows)
    except Exception as e:
        elapsed = (time.time() - t0) * 1000
        return elapsed, f"ERROR: {e}"

if __name__ == "__main__":
    conn = connect()
    cur = conn.cursor()

    # 1. EXPLAIN
    print("=== EXPLAIN (optimized) ===")
    rows = run_explain(cur, OPTIMIZED_SQL)
    for r in rows:
        print(r[0])

    # 2. EXPLAIN JSON for partition counts
    print("\n=== EXPLAIN JSON (partition summary) ===")
    try:
        plan = run_explain_json(cur, OPTIMIZED_SQL)
        gs = plan.get("GlobalStats", {})
        print(f"Partitions: {gs.get('partitionsAssigned')}/{gs.get('partitionsTotal')}")
        print(f"Bytes: {gs.get('bytesAssigned', 0)/1e9:.1f} GB")
        for op in plan.get("Operations", [[]])[0]:
            if "partitionsAssigned" in op:
                obj = op.get("objects", ["?"])[0].split(".")[-1]
                print(f"  {obj}: {op['partitionsAssigned']}/{op['partitionsTotal']} parts, {op.get('bytesAssigned',0)/1e9:.1f} GB")
    except Exception as e:
        print(f"JSON explain failed: {e}")

    # 3. Timed run (warmup + measure)
    print("\n=== Timed execution ===")
    print("Warmup...")
    t_warmup, rc_warmup = run_timed(cur, OPTIMIZED_SQL)
    print(f"  Warmup: {t_warmup:.0f}ms, rows={rc_warmup}")

    print("Measure 1...")
    t1, rc1 = run_timed(cur, OPTIMIZED_SQL)
    print(f"  Run 1: {t1:.0f}ms, rows={rc1}")

    print("Measure 2...")
    t2, rc2 = run_timed(cur, OPTIMIZED_SQL)
    print(f"  Run 2: {t2:.0f}ms, rows={rc2}")

    avg = (t1 + t2) / 2
    print(f"\n  Average: {avg:.0f}ms")
    print(f"  Original: TIMEOUT (>300,000ms)")
    if avg > 0:
        print(f"  Speedup: TIMEOUT→{avg/1000:.1f}s")

    conn.close()
