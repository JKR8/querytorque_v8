"""Full validation of config ceiling wins — 5x trimmed mean.

Run 5 times, discard min and max, average remaining 3.
"""

import json
import time
import psycopg2
from pathlib import Path

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
BENCHMARK_DIR = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76")
TIMEOUT_MS = 60000

WINS_TO_VALIDATE = [
    {
        "query_id": "query023_multi_i1",
        "combo": "no_hashjoin+max_parallel",
        "config": {
            "enable_hashjoin": "off",
            "max_parallel_workers_per_gather": "8",
        },
    },
    {
        "query_id": "query083_multi_i1",
        "combo": "force_nestloop+ssd_costs",
        "config": {
            "enable_hashjoin": "off",
            "enable_mergejoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        },
    },
    {
        "query_id": "query014_multi_i1",
        "combo": "no_hashjoin+ssd_costs",
        "config": {
            "enable_hashjoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        },
    },
]


def run_timed(conn, sql, config=None):
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{TIMEOUT_MS}'")
        if config:
            for k, v in config.items():
                cur.execute(f"SET LOCAL {k} = %s", (v,))
        t0 = time.perf_counter()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except psycopg2.errors.QueryCanceled:
            conn.rollback()
            return float('inf'), 0
        elapsed_ms = (time.perf_counter() - t0) * 1000
    return elapsed_ms, len(rows)


def trimmed_mean_5x(conn, sql, config=None):
    """5x trimmed mean: run 5 times, discard min/max, avg remaining 3."""
    times = []
    row_count = 0
    for i in range(5):
        conn.autocommit = False
        ms, rc = run_timed(conn, sql, config)
        conn.rollback()
        if ms == float('inf'):
            return float('inf'), 0, []
        times.append(ms)
        row_count = rc
    # Discard min and max
    sorted_times = sorted(times)
    trimmed = sorted_times[1:4]  # middle 3
    avg = sum(trimmed) / len(trimmed)
    return avg, row_count, times


def main():
    conn = psycopg2.connect(DSN)

    results = []

    for win in WINS_TO_VALIDATE:
        qid = win["query_id"]
        sql_path = BENCHMARK_DIR / "queries" / f"{qid}.sql"
        sql = sql_path.read_text().strip()

        print(f"\n{'='*70}")
        print(f"  VALIDATING: {qid} — {win['combo']}")
        print(f"{'='*70}")

        # Baseline: 5x trimmed mean
        print(f"  Baseline (5x trimmed mean)...", flush=True)
        base_avg, base_rows, base_times = trimmed_mean_5x(conn, sql)
        print(f"    avg={base_avg:.1f}ms  rows={base_rows}")
        print(f"    runs: {[f'{t:.1f}' for t in base_times]}")
        print(f"    trimmed: {[f'{t:.1f}' for t in sorted(base_times)[1:4]]}")

        if base_avg == float('inf'):
            print(f"    BASELINE TIMEOUT — skipping")
            continue

        # Config: 5x trimmed mean
        print(f"  Config (5x trimmed mean)...", flush=True)
        cfg_avg, cfg_rows, cfg_times = trimmed_mean_5x(conn, sql, win["config"])
        print(f"    avg={cfg_avg:.1f}ms  rows={cfg_rows}")
        print(f"    runs: {[f'{t:.1f}' for t in cfg_times]}")
        print(f"    trimmed: {[f'{t:.1f}' for t in sorted(cfg_times)[1:4]]}")

        if cfg_avg == float('inf'):
            print(f"    CONFIG TIMEOUT")
            continue

        speedup = base_avg / cfg_avg if cfg_avg > 0 else 0
        rows_match = base_rows == cfg_rows

        marker = "WIN" if speedup >= 1.5 else "IMPROVED" if speedup >= 1.1 else "NEUTRAL" if speedup >= 0.95 else "REGRESSION"

        print(f"\n  RESULT: {speedup:.3f}x [{marker}]")
        print(f"    {base_avg:.1f}ms → {cfg_avg:.1f}ms")
        print(f"    Rows match: {rows_match} ({base_rows} vs {cfg_rows})")

        if not rows_match:
            print(f"    *** ROW COUNT MISMATCH — FAIL ***")

        results.append({
            "query_id": qid,
            "combo": win["combo"],
            "config": win["config"],
            "baseline_avg_ms": round(base_avg, 2),
            "config_avg_ms": round(cfg_avg, 2),
            "speedup": round(speedup, 4),
            "status": marker,
            "rows_match": rows_match,
            "baseline_rows": base_rows,
            "config_rows": cfg_rows,
            "baseline_runs": [round(t, 2) for t in base_times],
            "config_runs": [round(t, 2) for t in cfg_times],
        })

    conn.close()

    # Save
    out_path = Path("research/scripts/config_ceiling_validated.json")
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to {out_path}")

    # Summary
    print(f"\n{'='*70}")
    print(f"  VALIDATED RESULTS")
    print(f"{'='*70}")
    for r in results:
        print(f"  {r['query_id']:30s}  {r['speedup']:.3f}x  {r['combo']:30s}  [{r['status']}]  rows_ok={r['rows_match']}")


if __name__ == "__main__":
    main()
