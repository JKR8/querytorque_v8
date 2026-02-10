"""Exhaustive optimization testing for Q1-Q23."""

import time
import duckdb
import os
import json

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
QUERY_DIR = "/mnt/d/TPC-DS/queries_duckdb_converted"
OUTPUT_DIR = "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/optimized_queries"


def benchmark(sql: str, runs: int = 3):
    """Benchmark a query, return (time, result, error)."""
    conn = duckdb.connect(SAMPLE_DB, read_only=True)
    times = []
    result = None
    for i in range(runs):
        start = time.time()
        try:
            result = conn.execute(sql).fetchall()
        except Exception as e:
            conn.close()
            return -1, None, str(e)[:100]
        times.append(time.time() - start)
    conn.close()
    avg = sum(times[1:]) / max(len(times) - 1, 1)
    return avg, result, None


def test_opt(original: str, optimized: str):
    """Test optimization, return (speedup, correct, error)."""
    orig_time, orig_result, orig_err = benchmark(original)
    if orig_err:
        return None, None, f"Original: {orig_err}"

    opt_time, opt_result, opt_err = benchmark(optimized)
    if opt_err:
        return None, None, f"Optimized: {opt_err}"

    speedup = orig_time / opt_time if opt_time > 0 else 0
    correct = set(orig_result) == set(opt_result)
    return speedup, correct, None


def load_query(num: int) -> str:
    path = os.path.join(QUERY_DIR, f"query_{num}.sql")
    with open(path) as f:
        return f.read()


def test_query(qnum: int, variations: list) -> dict:
    """Test all variations for a query."""
    original = load_query(qnum)
    results = []

    for i, (name, sql) in enumerate(variations):
        speedup, correct, error = test_opt(original, sql)
        result = {
            "version": i + 1,
            "name": name,
            "speedup": speedup,
            "correct": correct,
            "error": error
        }
        results.append(result)

        if error:
            status = f"ERR: {error[:40]}"
        elif correct:
            emoji = "🚀" if speedup >= 2 else ("📈" if speedup >= 1.2 else "")
            status = f"{speedup:.2f}x {emoji}"
        else:
            status = f"{speedup:.2f}x ❌ WRONG"

        print(f"  v{i+1:02d} {name[:35]:35s} {status}")

    return results


# Q1 Variations
Q1_VARS = [
    ("original", """
SELECT c_customer_id FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (SELECT avg(ctr_total_return)*1.2 FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD' AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v1_window_avg", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
           sum(SR_FEE) AS ctr_total_return,
           AVG(sum(SR_FEE)) OVER (PARTITION BY sr_store_sk) AS store_avg
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > ctr1.store_avg * 1.2
  AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD' AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v2_predicate_pushdown", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns, date_dim, store
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
      AND sr_store_sk = s_store_sk AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > (SELECT avg(ctr_total_return)*1.2 FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v3_both_window_and_pushdown", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
           sum(SR_FEE) AS ctr_total_return,
           AVG(sum(SR_FEE)) OVER (PARTITION BY sr_store_sk) AS store_avg
    FROM store_returns, date_dim, store
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
      AND sr_store_sk = s_store_sk AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > ctr1.store_avg * 1.2
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v4_explicit_join", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns
    INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
    WHERE d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1
INNER JOIN store ON s_store_sk = ctr1.ctr_store_sk
INNER JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ctr1.ctr_total_return > (SELECT avg(ctr_total_return)*1.2 FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_state = 'SD'
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v5_date_cte", """
WITH date_filter AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000),
customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns, date_filter
    WHERE sr_returned_date_sk = d_date_sk
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (SELECT avg(ctr_total_return)*1.2 FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD' AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v6_store_filter_first", """
WITH sd_stores AS (SELECT s_store_sk FROM store WHERE s_state = 'SD'),
customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
      AND sr_store_sk IN (SELECT s_store_sk FROM sd_stores)
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > (SELECT avg(ctr_total_return)*1.2 FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v7_materialized_avg", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
),
store_avg AS (
    SELECT ctr_store_sk, avg(ctr_total_return) * 1.2 AS threshold
    FROM customer_total_return GROUP BY ctr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, store, customer, store_avg sa
WHERE ctr1.ctr_total_return > sa.threshold
  AND ctr1.ctr_store_sk = sa.ctr_store_sk
  AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD' AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v8_semi_join", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
),
store_avg AS (
    SELECT ctr_store_sk, avg(ctr_total_return) * 1.2 AS threshold
    FROM customer_total_return GROUP BY ctr_store_sk
),
qualifying AS (
    SELECT ctr1.ctr_customer_sk
    FROM customer_total_return ctr1, store_avg sa, store
    WHERE ctr1.ctr_store_sk = sa.ctr_store_sk
      AND ctr1.ctr_total_return > sa.threshold
      AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD'
)
SELECT c_customer_id FROM customer, qualifying q
WHERE c_customer_sk = q.ctr_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v9_no_subquery", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
           sum(SR_FEE) AS ctr_total_return,
           AVG(sum(SR_FEE)) OVER (PARTITION BY sr_store_sk) * 1.2 AS threshold
    FROM store_returns, date_dim, store
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
      AND sr_store_sk = s_store_sk AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > ctr1.threshold AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v10_lateral_join", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer,
     LATERAL (SELECT avg(ctr_total_return)*1.2 AS thresh FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk) t
WHERE ctr1.ctr_total_return > t.thresh
  AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD' AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v11_exists_filter", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
),
store_avg AS (
    SELECT ctr_store_sk, avg(ctr_total_return) * 1.2 AS threshold FROM customer_total_return GROUP BY ctr_store_sk
)
SELECT c_customer_id FROM customer
WHERE EXISTS (
    SELECT 1 FROM customer_total_return ctr1, store, store_avg sa
    WHERE ctr1.ctr_customer_sk = c_customer_sk
      AND ctr1.ctr_store_sk = sa.ctr_store_sk
      AND ctr1.ctr_total_return > sa.threshold
      AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD'
)
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v12_in_filter", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return,
           AVG(sum(SR_FEE)) OVER (PARTITION BY sr_store_sk) * 1.2 AS threshold
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
),
qualifying AS (
    SELECT ctr_customer_sk FROM customer_total_return ctr1, store
    WHERE ctr1.ctr_total_return > ctr1.threshold AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD'
)
SELECT c_customer_id FROM customer WHERE c_customer_sk IN (SELECT ctr_customer_sk FROM qualifying)
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v13_hash_hint", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (SELECT avg(ctr_total_return)*1.2 FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD' AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v14_filtered_customer", """
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(SR_FEE) AS ctr_total_return,
           AVG(sum(SR_FEE)) OVER (PARTITION BY sr_store_sk) * 1.2 AS threshold
    FROM store_returns, date_dim, store
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
      AND sr_store_sk = s_store_sk AND s_state = 'SD'
      AND sr_customer_sk IS NOT NULL
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > ctr1.threshold AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
    ("v15_early_agg", """
WITH store_returns_agg AS (
    SELECT sr_customer_sk, sr_store_sk, sum(SR_FEE) AS total_fee
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
),
with_avg AS (
    SELECT sr_customer_sk, sr_store_sk, total_fee,
           AVG(total_fee) OVER (PARTITION BY sr_store_sk) * 1.2 AS threshold
    FROM store_returns_agg
)
SELECT c_customer_id FROM with_avg wa, store, customer
WHERE wa.total_fee > wa.threshold
  AND s_store_sk = wa.sr_store_sk AND s_state = 'SD'
  AND wa.sr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100;
"""),
]

if __name__ == "__main__":
    print("="*70)
    print("Q1: Store Returns Analysis")
    print("="*70)
    results = test_query(1, Q1_VARS)

    # Find best
    best = max([r for r in results if r["correct"]], key=lambda x: x["speedup"] or 0, default=None)
    if best:
        print(f"\n  BEST: v{best['version']:02d} {best['name']} = {best['speedup']:.2f}x")
