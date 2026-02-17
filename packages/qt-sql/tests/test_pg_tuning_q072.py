#!/usr/bin/env python3
"""4-variant SET LOCAL tuning test on Q072_spj_spj.

Measures all 4 combinations:
  A. Original SQL, no config  (baseline)
  B. Original SQL + SET LOCAL (config-only contribution)
  C. Rewritten SQL, no config (rewrite-only contribution)
  D. Rewritten SQL + SET LOCAL (combined contribution)

Uses 3-run pattern: warmup + 2 measures, average.

EXPLAIN analysis for Q072_spj_spj (original):
  - Nested loop on customer_demographics: 30,768 loops at 0.131ms = ~4s
  - catalog_sales index scan: 365 loops at ~46ms = ~17s
  - random_page_cost = 4 (HDD default, data on SSD)
  - Only 1 parallel worker launched (2 planned)
  - JIT: 103 functions, 63ms overhead
  - work_mem = 4MB (hash ops are tiny, not the bottleneck)

SET LOCAL candidates:
  - random_page_cost = 1.1 (SSD storage)
  - max_parallel_workers_per_gather = 4 (more parallelism)
  - effective_cache_size = 24GB (encourage index scan preference)

Run:
  cd <repo-root>
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.tests.test_pg_tuning_q072
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

from qt_sql.execution.postgres_executor import PostgresExecutor

# ── Config ──
PG_CONN = {
    "host": "127.0.0.1",
    "port": 5433,
    "database": "dsb_sf10",
    "user": "jakc9",
    "password": "jakc9",
}

BENCHMARK_DIR = PROJECT_ROOT / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb"
ORIGINAL_SQL = (BENCHMARK_DIR / "queries/query072_spj_spj.sql").read_text().strip()
REWRITTEN_SQL = (BENCHMARK_DIR / "best/query072_spj_spj.sql").read_text().strip()

# SET LOCAL configs to test (each is a named variant)
CONFIGS = {
    "ssd_tuning": [
        "SET LOCAL random_page_cost = '1.1'",
        "SET LOCAL effective_cache_size = '24GB'",
    ],
    "parallel_boost": [
        "SET LOCAL max_parallel_workers_per_gather = '4'",
        "SET LOCAL parallel_tuple_cost = '0.001'",
        "SET LOCAL parallel_setup_cost = '100'",
    ],
    "ssd_plus_parallel": [
        "SET LOCAL random_page_cost = '1.1'",
        "SET LOCAL effective_cache_size = '24GB'",
        "SET LOCAL max_parallel_workers_per_gather = '4'",
        "SET LOCAL parallel_tuple_cost = '0.001'",
        "SET LOCAL parallel_setup_cost = '100'",
    ],
    "full_kitchen_sink": [
        "SET LOCAL random_page_cost = '1.1'",
        "SET LOCAL effective_cache_size = '24GB'",
        "SET LOCAL max_parallel_workers_per_gather = '4'",
        "SET LOCAL parallel_tuple_cost = '0.001'",
        "SET LOCAL parallel_setup_cost = '100'",
        "SET LOCAL work_mem = '256MB'",
        "SET LOCAL hash_mem_multiplier = '4'",
        "SET LOCAL jit = 'off'",
    ],
}


def time_query(executor: PostgresExecutor, sql: str, label: str) -> tuple[float, int]:
    """Run 3 times (warmup + 2 measures), return (avg_ms, row_count)."""
    # Warmup
    t0 = time.perf_counter()
    rows = executor.execute(sql, timeout_ms=60_000)
    t_warmup = (time.perf_counter() - t0) * 1000

    # Measure 1
    t0 = time.perf_counter()
    rows = executor.execute(sql, timeout_ms=60_000)
    t1 = (time.perf_counter() - t0) * 1000

    # Measure 2
    t0 = time.perf_counter()
    executor.execute(sql, timeout_ms=60_000)
    t2 = (time.perf_counter() - t0) * 1000

    avg = (t1 + t2) / 2
    print(f"  {label:30s}  warmup={t_warmup:8.1f}ms  "
          f"r1={t1:8.1f}ms  r2={t2:8.1f}ms  avg={avg:8.1f}ms  "
          f"({len(rows)} rows)")
    return avg, len(rows)


def time_query_with_config(
    executor: PostgresExecutor, sql: str, config_cmds: list[str], label: str
) -> tuple[float, int]:
    """Run 3 times with SET LOCAL config, return (avg_ms, row_count)."""
    # Warmup
    t0 = time.perf_counter()
    rows = executor.execute_with_config(sql, config_cmds, timeout_ms=60_000)
    t_warmup = (time.perf_counter() - t0) * 1000

    # Measure 1
    t0 = time.perf_counter()
    rows = executor.execute_with_config(sql, config_cmds, timeout_ms=60_000)
    t1 = (time.perf_counter() - t0) * 1000

    # Measure 2
    t0 = time.perf_counter()
    executor.execute_with_config(sql, config_cmds, timeout_ms=60_000)
    t2 = (time.perf_counter() - t0) * 1000

    avg = (t1 + t2) / 2
    print(f"  {label:30s}  warmup={t_warmup:8.1f}ms  "
          f"r1={t1:8.1f}ms  r2={t2:8.1f}ms  avg={avg:8.1f}ms  "
          f"({len(rows)} rows)")
    return avg, len(rows)


def explain_with_config(
    executor: PostgresExecutor, sql: str, config_cmds: list[str], label: str
):
    """Run EXPLAIN ANALYZE with SET LOCAL config to see plan changes."""
    conn = executor._ensure_connected()
    with conn.cursor() as cur:
        try:
            cur.execute("BEGIN")
            cur.execute("SET LOCAL statement_timeout = 60000")
            for cmd in config_cmds:
                cur.execute(cmd)
            cur.execute(f"EXPLAIN (ANALYZE, COSTS, TIMING) {sql}")
            rows = cur.fetchall()
            cur.execute("COMMIT")
            print(f"\n  === EXPLAIN with {label} ===")
            for r in rows[-10:]:  # Just the summary lines
                print(f"    {r[0]}")
        except Exception as e:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            print(f"  EXPLAIN with {label} failed: {e}")


def main():
    print()
    print("=" * 70)
    print("  4-VARIANT SET LOCAL TUNING TEST: Q072_spj_spj")
    print("=" * 70)

    executor = PostgresExecutor(**PG_CONN)
    executor.connect()

    # Show current settings
    settings = executor.get_settings()
    print()
    print("  Current PG settings:")
    for k, v in sorted(settings.items()):
        print(f"    {k} = {v}")

    results = {}

    # ── A. Original SQL, no config (baseline) ──
    print()
    print("-" * 70)
    print("  VARIANT A: Original SQL, no config")
    print("-" * 70)
    t_a, rc_a = time_query(executor, ORIGINAL_SQL, "original")
    results["A_baseline"] = t_a

    # ── B. Original SQL + each config variant ──
    for config_name, config_cmds in CONFIGS.items():
        print()
        print("-" * 70)
        print(f"  VARIANT B: Original SQL + {config_name}")
        print("-" * 70)
        t_b, rc_b = time_query_with_config(
            executor, ORIGINAL_SQL, config_cmds, f"orig+{config_name}"
        )
        results[f"B_{config_name}"] = t_b

        speedup = t_a / t_b if t_b > 0 else 0
        delta = "FASTER" if speedup > 1.05 else ("SLOWER" if speedup < 0.95 else "NEUTRAL")
        print(f"  -> Config-only speedup: {speedup:.2f}x ({delta})")

    # ── C. Rewritten SQL, no config (rewrite-only) ──
    print()
    print("-" * 70)
    print("  VARIANT C: Rewritten SQL, no config")
    print("-" * 70)
    t_c, rc_c = time_query(executor, REWRITTEN_SQL, "rewritten")
    results["C_rewrite_only"] = t_c

    speedup_c = t_a / t_c if t_c > 0 else 0
    print(f"  -> Rewrite-only speedup: {speedup_c:.2f}x")

    # ── D. Rewritten SQL + each config variant ──
    for config_name, config_cmds in CONFIGS.items():
        print()
        print("-" * 70)
        print(f"  VARIANT D: Rewritten SQL + {config_name}")
        print("-" * 70)
        t_d, rc_d = time_query_with_config(
            executor, REWRITTEN_SQL, config_cmds, f"rewr+{config_name}"
        )
        results[f"D_{config_name}"] = t_d

        speedup_d = t_a / t_d if t_d > 0 else 0
        additive = t_c / t_d if t_d > 0 else 0
        print(f"  -> Combined speedup (vs baseline): {speedup_d:.2f}x")
        print(f"  -> Config additive (vs rewrite-only): {additive:.2f}x")

    # ── Show EXPLAIN for most promising config on original ──
    best_config = min(
        ((k, v) for k, v in results.items() if k.startswith("B_")),
        key=lambda x: x[1]
    )
    best_name = best_config[0].replace("B_", "")
    if best_name in CONFIGS:
        explain_with_config(executor, ORIGINAL_SQL, CONFIGS[best_name], best_name)

    # ── Summary ──
    print()
    print("=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"  {'Variant':35s}  {'Time (ms)':>12s}  {'vs Baseline':>12s}")
    print(f"  {'-'*35}  {'-'*12}  {'-'*12}")
    for k, v in sorted(results.items()):
        speedup = t_a / v if v > 0 else 0
        label = k.replace("_", " ")
        print(f"  {label:35s}  {v:12.1f}  {speedup:11.2f}x")
    print("=" * 70)

    # Save results
    out_path = PROJECT_ROOT / "research/pg_tuning_q072_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\n  Results saved to: {out_path}")

    executor.close()


if __name__ == "__main__":
    main()
