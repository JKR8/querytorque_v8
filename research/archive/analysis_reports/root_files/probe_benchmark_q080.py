"""Interleaved 4x triage benchmark for Q080 probe attacks.

Pattern: warmup_orig, warmup_opt, measure_orig, measure_opt
"""
import sys, time, json
sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from qt_sql.execution.factory import create_executor_from_dsn

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"

ORIGINAL = open("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries/query080_multi_i1.sql").read().strip()

ATTACK_1 = """SET LOCAL jit = 'off';

WITH filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_current_price > 50
      AND i_category IN ('Children', 'Sports')
), filtered_promotion AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_email = 'N'
      AND p_channel_tv = 'N'
      AND p_channel_radio = 'N'
      AND p_channel_press = 'N'
      AND p_channel_event = 'N'
), ssr AS (
    SELECT s_store_id as store_id,
           sum(ss_ext_sales_price) as sales,
           sum(coalesce(sr_return_amt, 0)) as returns,
           sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
    FROM store_sales
    LEFT OUTER JOIN store_returns ON (
        ss_item_sk = sr_item_sk
        AND ss_ticket_number = sr_ticket_number
    )
    INNER JOIN date_dim ON ss_sold_date_sk = d_date_sk
    INNER JOIN store ON ss_store_sk = s_store_sk
    INNER JOIN filtered_items ON ss_item_sk = filtered_items.i_item_sk
    INNER JOIN filtered_promotion ON ss_promo_sk = filtered_promotion.p_promo_sk
    WHERE d_date BETWEEN cast('1998-08-29' as date)
                     AND cast('1998-08-29' as date) + interval '30 day'
      AND ss_wholesale_cost BETWEEN 23 AND 38
    GROUP BY s_store_id
), csr AS (
    SELECT cp_catalog_page_id as catalog_page_id,
           sum(cs_ext_sales_price) as sales,
           sum(coalesce(cr_return_amount, 0)) as returns,
           sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
    FROM catalog_sales
    LEFT OUTER JOIN catalog_returns ON (
        cs_item_sk = cr_item_sk
        AND cs_order_number = cr_order_number
    )
    INNER JOIN date_dim ON cs_sold_date_sk = d_date_sk
    INNER JOIN catalog_page ON cs_catalog_page_sk = cp_catalog_page_sk
    INNER JOIN filtered_items ON cs_item_sk = filtered_items.i_item_sk
    INNER JOIN filtered_promotion ON cs_promo_sk = filtered_promotion.p_promo_sk
    WHERE d_date BETWEEN cast('1998-08-29' as date)
                     AND cast('1998-08-29' as date) + interval '30 day'
      AND cs_wholesale_cost BETWEEN 23 AND 38
    GROUP BY cp_catalog_page_id
), wsr AS (
    SELECT web_site_id,
           sum(ws_ext_sales_price) as sales,
           sum(coalesce(wr_return_amt, 0)) as returns,
           sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
    FROM web_sales
    LEFT OUTER JOIN web_returns ON (
        ws_item_sk = wr_item_sk
        AND ws_order_number = wr_order_number
    )
    INNER JOIN date_dim ON ws_sold_date_sk = d_date_sk
    INNER JOIN web_site ON ws_web_site_sk = web_site_sk
    INNER JOIN filtered_items ON ws_item_sk = filtered_items.i_item_sk
    INNER JOIN filtered_promotion ON ws_promo_sk = filtered_promotion.p_promo_sk
    WHERE d_date BETWEEN cast('1998-08-29' as date)
                     AND cast('1998-08-29' as date) + interval '30 day'
      AND ws_wholesale_cost BETWEEN 23 AND 38
    GROUP BY web_site_id
)
SELECT channel
     , id
     , sum(sales) as sales
     , sum(returns) as returns
     , sum(profit) as profit
FROM (
    SELECT 'store channel' as channel
         , 'store' || store_id as id
         , sales
         , returns
         , profit
    FROM ssr
    UNION ALL
    SELECT 'catalog channel' as channel
         , 'catalog_page' || catalog_page_id as id
         , sales
         , returns
         , profit
    FROM csr
    UNION ALL
    SELECT 'web channel' as channel
         , 'web_site' || web_site_id as id
         , sales
         , returns
         , profit
    FROM wsr
) x
GROUP BY ROLLUP (channel, id)
ORDER BY channel, id
LIMIT 100;"""

ATTACK_2 = """SET LOCAL jit = 'off';
SET LOCAL enable_sort = 'off';

WITH ssr AS (
    SELECT s_store_id as store_id,
           sum(ss_ext_sales_price) as sales,
           sum(coalesce(sr_return_amt, 0)) as returns,
           sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
    FROM store_sales LEFT OUTER JOIN store_returns ON
           (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
        date_dim,
        store,
        item,
        promotion
    WHERE ss_sold_date_sk = d_date_sk
          AND d_date between cast('1998-08-29' as date)
                     AND cast('1998-08-29' as date) + interval '30 day'
          AND ss_store_sk = s_store_sk
          AND ss_item_sk = i_item_sk
          AND i_current_price > 50
          AND ss_promo_sk = p_promo_sk
          AND p_channel_email = 'N'
          AND p_channel_tv = 'N'
          AND p_channel_radio = 'N'
          AND p_channel_press = 'N'
          AND p_channel_event = 'N'
          AND ss_wholesale_cost BETWEEN 23 AND 38
          AND i_category IN ('Children', 'Sports')
    GROUP BY s_store_id
),
csr AS (
    SELECT cp_catalog_page_id as catalog_page_id,
           sum(cs_ext_sales_price) as sales,
           sum(coalesce(cr_return_amount, 0)) as returns,
           sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
    FROM catalog_sales LEFT OUTER JOIN catalog_returns ON
           (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),
        date_dim,
        catalog_page,
        item,
        promotion
    WHERE cs_sold_date_sk = d_date_sk
          AND d_date between cast('1998-08-29' as date)
                     AND cast('1998-08-29' as date) + interval '30 day'
          AND cs_catalog_page_sk = cp_catalog_page_sk
          AND cs_item_sk = i_item_sk
          AND i_current_price > 50
          AND cs_promo_sk = p_promo_sk
          AND p_channel_email = 'N'
          AND p_channel_tv = 'N'
          AND p_channel_radio = 'N'
          AND p_channel_press = 'N'
          AND p_channel_event = 'N'
          AND cs_wholesale_cost BETWEEN 23 AND 38
          AND i_category IN ('Children', 'Sports')
    GROUP BY cp_catalog_page_id
),
wsr AS (
    SELECT web_site_id,
           sum(ws_ext_sales_price) as sales,
           sum(coalesce(wr_return_amt, 0)) as returns,
           sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
    FROM web_sales LEFT OUTER JOIN web_returns ON
           (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),
        date_dim,
        web_site,
        item,
        promotion
    WHERE ws_sold_date_sk = d_date_sk
          AND d_date between cast('1998-08-29' as date)
                     AND cast('1998-08-29' as date) + interval '30 day'
          AND ws_web_site_sk = web_site_sk
          AND ws_item_sk = i_item_sk
          AND i_current_price > 50
          AND ws_promo_sk = p_promo_sk
          AND p_channel_email = 'N'
          AND p_channel_tv = 'N'
          AND p_channel_radio = 'N'
          AND p_channel_press = 'N'
          AND p_channel_event = 'N'
          AND ws_wholesale_cost BETWEEN 23 AND 38
          AND i_category IN ('Children', 'Sports')
    GROUP BY web_site_id
)
SELECT channel
     , id
     , sum(sales) as sales
     , sum(returns) as returns
     , sum(profit) as profit
FROM (
    SELECT 'store channel' as channel
         , 'store' || store_id as id
         , sales
         , returns
         , profit
    FROM ssr
    UNION ALL
    SELECT 'catalog channel' as channel
         , 'catalog_page' || catalog_page_id as id
         , sales
         , returns
         , profit
    FROM csr
    UNION ALL
    SELECT 'web channel' as channel
         , 'web_site' || web_site_id as id
         , sales
         , returns
         , profit
    FROM wsr
) x
GROUP BY ROLLUP (channel, id)
ORDER BY channel, id
LIMIT 100;"""


def timed_execute(executor, sql, timeout_ms=300_000):
    """Execute SQL and return (rows, elapsed_ms)."""
    start = time.perf_counter()
    rows = executor.execute(sql, timeout_ms=timeout_ms)
    elapsed = (time.perf_counter() - start) * 1000
    return rows, elapsed


def benchmark_4x_triage(executor, original_sql, attack_sql, label):
    """4x triage: warmup_orig, warmup_attack, measure_orig, measure_attack."""
    print(f"\n--- {label} ---")

    # Warmup original
    print("  Warmup original...", end="", flush=True)
    _, w_orig = timed_execute(executor, original_sql)
    print(f" {w_orig:.1f}ms")

    # Warmup attack
    print("  Warmup attack...", end="", flush=True)
    _, w_atk = timed_execute(executor, attack_sql)
    print(f" {w_atk:.1f}ms")

    # Measure original
    print("  Measure original...", end="", flush=True)
    orig_rows, m_orig = timed_execute(executor, original_sql)
    print(f" {m_orig:.1f}ms ({len(orig_rows)} rows)")

    # Measure attack
    print("  Measure attack...", end="", flush=True)
    atk_rows, m_atk = timed_execute(executor, attack_sql)
    print(f" {m_atk:.1f}ms ({len(atk_rows)} rows)")

    # Row count check
    rows_match = len(orig_rows) == len(atk_rows)
    speedup = m_orig / m_atk if m_atk > 0 else 0.0

    status = "PASS" if rows_match else "FAIL"
    if rows_match:
        if speedup >= 1.50:
            verdict = "WIN"
        elif speedup >= 1.10:
            verdict = "IMPROVED"
        elif speedup >= 0.95:
            verdict = "NEUTRAL"
        else:
            verdict = "REGRESSION"
    else:
        verdict = "FAIL"

    print(f"\n  Result: {verdict} | {speedup:.2f}x | rows_match={rows_match}")
    print(f"  Timings: orig={m_orig:.1f}ms, attack={m_atk:.1f}ms")
    print(f"  Warmups: orig={w_orig:.1f}ms, attack={w_atk:.1f}ms")

    return {
        "label": label,
        "warmup_orig_ms": round(w_orig, 1),
        "warmup_attack_ms": round(w_atk, 1),
        "measure_orig_ms": round(m_orig, 1),
        "measure_attack_ms": round(m_atk, 1),
        "speedup": round(speedup, 3),
        "orig_rows": len(orig_rows),
        "attack_rows": len(atk_rows),
        "rows_match": rows_match,
        "verdict": verdict,
    }


def main():
    print("Connecting to PG14.3...")
    executor = create_executor_from_dsn(DSN)
    executor.connect()

    try:
        results = []

        r1 = benchmark_4x_triage(executor, ORIGINAL, ATTACK_1, "Attack 1: Materialized dimension CTEs")
        results.append(r1)

        r2 = benchmark_4x_triage(executor, ORIGINAL, ATTACK_2, "Attack 2: enable_sort=off (force hash agg)")
        results.append(r2)

        # Summary
        print("\n" + "=" * 60)
        print("  Q080 PROBE BENCHMARK SUMMARY")
        print("=" * 60)
        for r in results:
            print(f"  {r['label']}: {r['verdict']} {r['speedup']:.2f}x "
                  f"({r['measure_orig_ms']:.1f}ms → {r['measure_attack_ms']:.1f}ms)")

        # Save results
        out = {
            "query_id": "query080_multi_i1",
            "benchmark_method": "4x_triage_interleaved",
            "attacks": results,
        }
        out_path = "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/probe/round_0/q080_benchmark.json"
        with open(out_path, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\n  Saved to {out_path}")

    finally:
        executor.close()


if __name__ == "__main__":
    main()
