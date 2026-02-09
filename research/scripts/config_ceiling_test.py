"""Config ceiling test — wall-clock timing of best config combos from plan_explore.

Tests the most promising SET LOCAL config combinations identified from
Layer 1 explore data against live PostgreSQL, using 4x triage timing
(warmup_orig, warmup_cfg, measure_orig, measure_cfg).

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 research/scripts/config_ceiling_test.py
"""

import json
import time
import psycopg2
from pathlib import Path

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
BENCHMARK_DIR = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76")
TIMEOUT_MS = 60000  # 60s statement timeout

# Best config combos per query, ranked by explore cost_ratio (highest first)
# These are the ones most likely to produce real wall-clock speedups
TARGETS = {
    "query064_multi_i1": [
        ("max_parallel+ssd_plus_mem", {
            "max_parallel_workers_per_gather": "8",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
            "work_mem": "256MB",
            "hash_mem_multiplier": "4",
        }),
        ("ssd_plus_mem", {
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
            "work_mem": "256MB",
            "hash_mem_multiplier": "4",
        }),
        ("no_hashjoin+ssd_plus_mem", {
            "enable_hashjoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
            "work_mem": "256MB",
            "hash_mem_multiplier": "4",
        }),
        ("work_mem_256mb+max_parallel", {
            "work_mem": "256MB",
            "max_parallel_workers_per_gather": "8",
        }),
        ("work_mem_256mb+ssd_costs", {
            "work_mem": "256MB",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("work_mem_256mb", {
            "work_mem": "256MB",
        }),
    ],
    "query083_multi_i1": [
        ("ssd_costs", {
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("no_seqscan+ssd_costs", {
            "enable_seqscan": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("no_hashjoin+ssd_costs", {
            "enable_hashjoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("max_parallel", {
            "max_parallel_workers_per_gather": "8",
        }),
        ("no_parallel+ssd_costs", {
            "max_parallel_workers_per_gather": "0",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("force_nestloop+ssd_costs", {
            "enable_hashjoin": "off",
            "enable_mergejoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
    ],
    "query014_multi_i1": [
        ("ssd_costs", {
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("no_hashjoin+ssd_costs", {
            "enable_hashjoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("no_reorder+ssd_costs", {
            "join_collapse_limit": "1",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("force_hash", {
            "enable_nestloop": "off",
            "enable_mergejoin": "off",
        }),
        ("force_nestloop+ssd_costs", {
            "enable_hashjoin": "off",
            "enable_mergejoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
        ("max_parallel+ssd_costs", {
            "max_parallel_workers_per_gather": "8",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        }),
    ],
    "query023_multi_i1": [
        ("work_mem_256mb+max_parallel", {
            "work_mem": "256MB",
            "max_parallel_workers_per_gather": "8",
        }),
        ("max_parallel", {
            "max_parallel_workers_per_gather": "8",
        }),
        ("no_hashjoin+max_parallel", {
            "enable_hashjoin": "off",
            "max_parallel_workers_per_gather": "8",
        }),
        ("no_reorder", {
            "join_collapse_limit": "1",
        }),
        ("work_mem_256mb", {
            "work_mem": "256MB",
        }),
        ("no_seqscan+max_parallel", {
            "enable_seqscan": "off",
            "max_parallel_workers_per_gather": "8",
        }),
    ],
}


def run_timed(conn, sql: str, config: dict = None, timeout_ms: int = TIMEOUT_MS) -> float:
    """Execute SQL with optional SET LOCAL config, return wall-clock ms."""
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
        if config:
            for k, v in config.items():
                cur.execute(f"SET LOCAL {k} = %s", (v,))
        t0 = time.perf_counter()
        try:
            cur.execute(sql)
            _ = cur.fetchall()
        except psycopg2.errors.QueryCanceled:
            conn.rollback()
            return float('inf')
        elapsed_ms = (time.perf_counter() - t0) * 1000
    return elapsed_ms


def triage_4x(conn, sql: str, config: dict = None) -> tuple:
    """4x triage timing: warmup_orig, warmup_cfg, measure_orig, measure_cfg.

    Returns (baseline_ms, config_ms, speedup).
    """
    # 1. Warmup original
    with conn:
        conn.autocommit = False
        run_timed(conn, sql)
        conn.rollback()

    # 2. Warmup config
    with conn:
        conn.autocommit = False
        run_timed(conn, sql, config)
        conn.rollback()

    # 3. Measure original
    with conn:
        conn.autocommit = False
        baseline_ms = run_timed(conn, sql)
        conn.rollback()

    # 4. Measure config
    with conn:
        conn.autocommit = False
        config_ms = run_timed(conn, sql, config)
        conn.rollback()

    if config_ms == float('inf') or baseline_ms == float('inf'):
        return (baseline_ms, config_ms, 0.0)

    speedup = baseline_ms / config_ms if config_ms > 0 else 0.0
    return (baseline_ms, config_ms, speedup)


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False

    results = {}

    for query_id, combos in TARGETS.items():
        sql_path = BENCHMARK_DIR / "queries" / f"{query_id}.sql"
        if not sql_path.exists():
            print(f"  SKIP {query_id} — SQL file not found")
            continue
        sql = sql_path.read_text().strip()

        print(f"\n{'='*70}")
        print(f"  {query_id}")
        print(f"{'='*70}")

        # Baseline: 3-run (discard warmup, avg last 2)
        print(f"  Baseline (3-run)...", end="", flush=True)
        baseline_runs = []
        for i in range(3):
            with conn:
                conn.autocommit = False
                ms = run_timed(conn, sql)
                conn.rollback()
            baseline_runs.append(ms)
        if baseline_runs[0] == float('inf'):
            print(f" TIMEOUT — skipping query")
            continue
        baseline_ms = (baseline_runs[1] + baseline_runs[2]) / 2
        print(f" {baseline_ms:.1f}ms (runs: {[f'{r:.1f}' for r in baseline_runs]})")

        query_results = {
            "query_id": query_id,
            "baseline_ms": baseline_ms,
            "baseline_runs": baseline_runs,
            "combos": [],
        }

        best_speedup = 0.0
        best_combo = None

        for combo_name, config in combos:
            print(f"  {combo_name:35s}", end="", flush=True)

            base_ms, cfg_ms, speedup = triage_4x(conn, sql, config)

            if cfg_ms == float('inf'):
                print(f" TIMEOUT")
                query_results["combos"].append({
                    "combo": combo_name,
                    "status": "timeout",
                })
                continue

            marker = ""
            if speedup >= 1.50:
                marker = " *** WIN ***"
            elif speedup >= 1.10:
                marker = " * improved"
            elif speedup < 0.90:
                marker = " ! regression"

            print(f" {speedup:.3f}x  ({base_ms:.1f}→{cfg_ms:.1f}ms){marker}")

            combo_result = {
                "combo": combo_name,
                "config": config,
                "triage_baseline_ms": base_ms,
                "triage_config_ms": cfg_ms,
                "speedup": round(speedup, 4),
            }
            query_results["combos"].append(combo_result)

            if speedup > best_speedup:
                best_speedup = speedup
                best_combo = combo_name

        if best_combo:
            print(f"\n  CEILING: {best_speedup:.3f}x via {best_combo}")
            query_results["ceiling_speedup"] = round(best_speedup, 4)
            query_results["ceiling_combo"] = best_combo

        results[query_id] = query_results

    conn.close()

    # Save results
    out_path = Path("research/scripts/config_ceiling_results.json")
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to {out_path}")

    # Summary
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    for qid, r in results.items():
        ceiling = r.get("ceiling_speedup", 0)
        combo = r.get("ceiling_combo", "none")
        baseline = r.get("baseline_ms", 0)
        marker = "WIN" if ceiling >= 1.5 else "IMPROVED" if ceiling >= 1.1 else "NEUTRAL" if ceiling >= 0.95 else "REGRESSION"
        print(f"  {qid:30s}  baseline={baseline:>10.1f}ms  ceiling={ceiling:.3f}x ({combo})  [{marker}]")


if __name__ == "__main__":
    main()
