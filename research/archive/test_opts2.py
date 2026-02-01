"""Test more optimizations on sample database."""

import time
import duckdb

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"


def benchmark(sql: str, runs: int = 3) -> tuple[float, list]:
    """Run query and return (avg_time_seconds, results)."""
    conn = duckdb.connect(SAMPLE_DB, read_only=True)
    times = []
    result = None

    for i in range(runs):
        start = time.time()
        try:
            result = conn.execute(sql).fetchall()
        except Exception as e:
            conn.close()
            return -1, str(e)
        elapsed = time.time() - start
        times.append(elapsed)

    conn.close()
    avg_time = sum(times[1:]) / len(times[1:]) if len(times) > 1 else times[0]
    return avg_time, result


def compare(name: str, original: str, optimized: str):
    """Compare original vs optimized query."""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print('='*60)

    orig_time, orig_result = benchmark(original)
    if orig_time < 0:
        print(f"Original FAILED: {orig_result}")
        return None, None

    print(f"Original:  {orig_time:.3f}s ({len(orig_result)} rows)")

    opt_time, opt_result = benchmark(optimized)
    if opt_time < 0:
        print(f"Optimized FAILED: {opt_result}")
        return None, None

    print(f"Optimized: {opt_time:.3f}s ({len(opt_result)} rows)")

    speedup = orig_time / opt_time if opt_time > 0 else 0

    # Compare results
    orig_sorted = sorted([str(r) for r in orig_result])
    opt_sorted = sorted([str(r) for r in opt_result])
    correct = (orig_sorted == opt_sorted)

    status = "CORRECT" if correct else "WRONG"
    emoji = "" if speedup >= 2 else ("" if speedup >= 1.2 else "")

    print(f"Speedup: {speedup:.2f}x {emoji} | {status}")

    return speedup, correct


# Q8: Predicate pushdown for zip codes
Q8_ORIGINAL = open("/mnt/d/TPC-DS/queries_duckdb_converted/query_8.sql").read()

Q8_OPTIMIZED = """
WITH customer_preferred_cust AS (
    SELECT SUBSTRING(ca.ca_zip, 1, 5) AS zip
    FROM customer_address ca
    WHERE SUBSTRING(ca.ca_zip, 1, 5) IN (
        '24128','76232','65084','87816','83926','77556','20548','26231','43848','15126',
        '91137','61265','98294','25782','17920','18426','98235','40081','84093','28577',
        '55565','17183','54601','67897','22752','86284','18376','38607','45200','21756',
        '29741','96765','23932','89360','29839','25989','28898','91068','72550','10390',
        '18845','47770','82636','41367','76638','86198','81312','37126','39192','88424',
        '72175','81426','53672','10445','42666','66864','66708','41248','48583','82276',
        '18842','78890','49448','14089','38122','34425','79077','19849','43285','39861',
        '66162','77610','13695','99543','83444','38790','17264','16802','45338','16438',
        '36275','23082','40830','34972','22927','36488','95612','26014','65841','52460',
        '49406','48498','50412','60966','36498','41202','99048','45430','85816','68621',
        '43814','11645','57855','92712','47442','95995','25084','26792','99135','81218',
        '78474','14779','63411','69314','87350','58892','97621','77447','48131','61748',
        '13153','24027','74493','77564','81815','68892','93672','92862','63973','47383')
)
SELECT s_store_name, SUM(ss_net_profit)
FROM store_sales, date_dim, store,
     (SELECT DISTINCT cpc.zip FROM customer_preferred_cust cpc) AS ca
WHERE ss_sold_date_sk = d_date_sk
  AND d_year = 1998 AND d_qoy = 2
  AND ss_store_sk = s_store_sk
  AND SUBSTRING(s_zip, 1, 2) = SUBSTRING(ca.zip, 1, 2)
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100;
"""


# Q10: Join elimination - customer_demographics for filtering only
Q10_ORIGINAL = open("/mnt/d/TPC-DS/queries_duckdb_converted/query_10.sql").read()

# Can't really eliminate - needs cd_gender, cd_marital_status, etc. in SELECT


# Q16: Correlated NOT EXISTS -> LEFT ANTI JOIN
Q16_ORIGINAL = open("/mnt/d/TPC-DS/queries_duckdb_converted/query_16.sql").read()

Q16_OPTIMIZED = """
WITH catalog_returns_items AS (
    SELECT DISTINCT cr_order_number
    FROM catalog_returns
)
SELECT count(DISTINCT cs_order_number) AS order_count,
       sum(cs_ext_ship_cost) AS total_shipping_cost,
       sum(cs_net_profit) AS total_net_profit
FROM catalog_sales cs1, date_dim, customer_address, call_center
WHERE d_date BETWEEN '2001-2-01' AND CAST('2001-2-01' AS DATE) + INTERVAL '60' DAY
  AND cs1.cs_ship_date_sk = d_date_sk
  AND cs1.cs_ship_addr_sk = ca_address_sk
  AND ca_state = 'IL'
  AND cs1.cs_call_center_sk = cc_call_center_sk
  AND cc_county = 'Ziebach County'
  AND NOT EXISTS (
      SELECT 1 FROM catalog_returns_items cri
      WHERE cri.cr_order_number = cs1.cs_order_number
  )
  AND EXISTS (
      SELECT 1 FROM catalog_sales cs2
      WHERE cs1.cs_order_number = cs2.cs_order_number
        AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
  )
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100;
"""


# Q17: Simple join reordering
Q17_ORIGINAL = open("/mnt/d/TPC-DS/queries_duckdb_converted/query_17.sql").read()

# Complex, not clear optimization


# Q21: Join elimination - warehouse only used for w_warehouse_name
Q21_ORIGINAL = open("/mnt/d/TPC-DS/queries_duckdb_converted/query_21.sql").read()

Q21_OPTIMIZED = """
WITH inv_data AS (
    SELECT inv_warehouse_sk, inv_item_sk, inv_quantity_on_hand, inv_date_sk
    FROM inventory
    WHERE inv_quantity_on_hand IS NOT NULL
)
SELECT w_warehouse_name, i_item_id,
       sum(CASE WHEN d_date < CAST('2000-05-13' AS DATE)
           THEN inv_quantity_on_hand ELSE 0 END) AS inv_before,
       sum(CASE WHEN d_date >= CAST('2000-05-13' AS DATE)
           THEN inv_quantity_on_hand ELSE 0 END) AS inv_after
FROM inv_data, warehouse, item, date_dim
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND i_item_sk = inv_item_sk
  AND inv_warehouse_sk = w_warehouse_sk
  AND inv_date_sk = d_date_sk
  AND d_date BETWEEN CAST('2000-05-13' AS DATE) - INTERVAL '30' DAY
                 AND CAST('2000-05-13' AS DATE) + INTERVAL '30' DAY
GROUP BY w_warehouse_name, i_item_id
HAVING sum(CASE WHEN d_date < CAST('2000-05-13' AS DATE)
               THEN inv_quantity_on_hand ELSE 0 END) > 0
   AND sum(CASE WHEN d_date >= CAST('2000-05-13' AS DATE)
               THEN inv_quantity_on_hand ELSE 0 END) /
       sum(CASE WHEN d_date < CAST('2000-05-13' AS DATE)
               THEN inv_quantity_on_hand ELSE 0 END) BETWEEN 2.0/3.0 AND 3.0/2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100;
"""


if __name__ == "__main__":
    results = {}

    results['Q8'] = compare("Q8: Zip code predicate optimization", Q8_ORIGINAL, Q8_OPTIMIZED)
    results['Q16'] = compare("Q16: Correlated NOT EXISTS optimization", Q16_ORIGINAL, Q16_OPTIMIZED)
    results['Q21'] = compare("Q21: Inventory query optimization", Q21_ORIGINAL, Q21_OPTIMIZED)

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for q, (speedup, correct) in results.items():
        if speedup is not None:
            status = "" if correct else ""
            print(f"{q}: {speedup:.2f}x {status}")
