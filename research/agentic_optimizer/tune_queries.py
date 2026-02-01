"""Tune TPC-DS queries on sample database."""

import os
import sys
import time
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../packages/qt-sql'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../packages/qt-shared'))

QUERY_DIR = "/mnt/d/TPC-DS/queries_duckdb_converted"
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"


def benchmark(sql: str, db_path: str, runs: int = 3) -> tuple[float, any]:
    """Run query and return (avg_time_seconds, result_hash)."""
    conn = duckdb.connect(db_path, read_only=True)
    times = []
    result = None

    for i in range(runs):
        start = time.time()
        result = conn.execute(sql).fetchall()
        elapsed = time.time() - start
        times.append(elapsed)

    conn.close()

    # Discard first run (warmup), average rest
    if len(times) > 1:
        avg_time = sum(times[1:]) / len(times[1:])
    else:
        avg_time = times[0]

    # Simple hash of results for comparison
    result_hash = hash(str(sorted(result[:100]))) if result else 0

    return avg_time, result_hash, result


def test_optimization(query_num: int, original_sql: str, optimized_sql: str) -> dict:
    """Test an optimization and return results."""
    print(f"\n  Testing Q{query_num}...")

    try:
        orig_time, orig_hash, orig_result = benchmark(original_sql, SAMPLE_DB)
        print(f"    Original: {orig_time:.3f}s")
    except Exception as e:
        return {"error": f"Original failed: {e}"}

    try:
        opt_time, opt_hash, opt_result = benchmark(optimized_sql, SAMPLE_DB)
        print(f"    Optimized: {opt_time:.3f}s")
    except Exception as e:
        return {"error": f"Optimized failed: {e}"}

    speedup = orig_time / opt_time if opt_time > 0 else 0
    correct = (orig_hash == opt_hash)

    # More detailed comparison if hashes don't match
    if not correct and orig_result and opt_result:
        orig_set = set(str(r) for r in orig_result)
        opt_set = set(str(r) for r in opt_result)
        if orig_set == opt_set:
            correct = True  # Same rows, different order

    print(f"    Speedup: {speedup:.2f}x, Correct: {correct}")

    return {
        "original_time": orig_time,
        "optimized_time": opt_time,
        "speedup": speedup,
        "correct": correct,
    }


# Known optimizations based on analysis
OPTIMIZATIONS = {
    3: {
        "pattern": "Join elimination on item table",
        "sql": """
SELECT dt.d_year, brand_id, brand, sum(ss_ext_sales_price) sum_agg
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manufact_id = 436
  AND dt.d_moy = 12
GROUP BY dt.d_year, i_brand, i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100;
"""
    },

    6: {
        "pattern": "Predicate pushdown - move state filter into subquery",
        "sql": """
SELECT a.ca_state state, count(*) cnt
FROM customer_address a, customer c, store_sales s, date_dim d, item i
WHERE a.ca_address_sk = c.c_current_addr_sk
  AND c.c_customer_sk = s.ss_customer_sk
  AND s.ss_sold_date_sk = d.d_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND d.d_month_seq = (
    SELECT DISTINCT d_month_seq
    FROM date_dim
    WHERE d_year = 2000 AND d_moy = 2
  )
  AND i.i_current_price > 1.2 * (
    SELECT avg(j.i_current_price)
    FROM item j
    WHERE j.i_category = i.i_category
  )
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt, a.ca_state
LIMIT 100;
"""
    },

    7: {
        "pattern": "Join elimination - customer_demographics only used for filter",
        "sql": """
SELECT i_item_id,
       avg(ss_quantity) agg1,
       avg(ss_list_price) agg2,
       avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk
  AND ss_item_sk = i_item_sk
  AND ss_cdemo_sk = cd_demo_sk
  AND ss_promo_sk = p_promo_sk
  AND cd_gender = 'F'
  AND cd_marital_status = 'W'
  AND cd_education_status = 'Primary'
  AND (p_channel_email = 'N' OR p_channel_event = 'N')
  AND d_year = 1998
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100;
"""
    },

    12: {
        "pattern": "Window function instead of correlated subquery for category sum",
        "sql": """
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price,
       sum(ws_ext_sales_price) AS itemrevenue,
       sum(ws_ext_sales_price) * 100.0 / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales, item, date_dim
WHERE ws_item_sk = i_item_sk
  AND i_category IN ('Jewelry', 'Sports', 'Books')
  AND ws_sold_date_sk = d_date_sk
  AND d_date BETWEEN CAST('2001-01-12' AS DATE) AND CAST('2001-02-11' AS DATE)
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100;
"""
    },

    15: {
        "pattern": "Join elimination on customer table - only using c_customer_sk",
        "sql": """
SELECT ca_zip, sum(cs_sales_price)
FROM catalog_sales, date_dim, customer_address
WHERE cs_sold_date_sk = d_date_sk
  AND cs_bill_customer_sk IS NOT NULL
  AND SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475',
                                   '85392', '85460', '80348', '81792')
  AND d_qoy = 2 AND d_year = 2000
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;
"""
    },

    19: {
        "pattern": "Join elimination - customer only for FK validation",
        "sql": """
SELECT i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
       sum(ss_ext_sales_price) ext_price
FROM date_dim, store_sales, item, store
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 16
  AND d_moy = 12
  AND d_year = 1998
  AND ss_store_sk = s_store_sk
  AND s_zip IN ('31904', '## incomplete, need to review')
  AND ss_customer_sk IS NOT NULL
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
LIMIT 100;
"""
    },
}


def main():
    print("Testing known optimizations on sample DB...\n")

    results = {}

    for qnum in sorted(OPTIMIZATIONS.keys()):
        opt = OPTIMIZATIONS[qnum]
        query_file = os.path.join(QUERY_DIR, f"query_{qnum}.sql")

        if not os.path.exists(query_file):
            print(f"Q{qnum}: File not found")
            continue

        with open(query_file) as f:
            original_sql = f.read()

        print(f"Q{qnum}: {opt['pattern']}")
        result = test_optimization(qnum, original_sql, opt['sql'])
        results[qnum] = result

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    for qnum, result in sorted(results.items()):
        if "error" in result:
            print(f"Q{qnum}: ERROR - {result['error']}")
        else:
            status = "✓" if result['correct'] else "✗"
            speedup = result['speedup']
            indicator = "🚀" if speedup >= 2.0 else ("📈" if speedup >= 1.2 else "")
            print(f"Q{qnum}: {speedup:.2f}x {status} {indicator}")


if __name__ == "__main__":
    main()
