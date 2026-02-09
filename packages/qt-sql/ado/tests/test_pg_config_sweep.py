#!/usr/bin/env python3
"""Sweep SET LOCAL configs across ALL 52 DSB queries on PG.

Uses 4x triage pattern (interleaved): warmup orig, warmup config,
measure orig, measure config. Fast screening — ~4 runs per query.

Tests 2 config profiles:
  1. ssd_only: random_page_cost=1.1, effective_cache_size=24GB
  2. ssd_plus_mem: ssd_only + work_mem=256MB, hash_mem_multiplier=4

Run:
  cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.tests.test_pg_config_sweep
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

from qt_sql.execution.postgres_executor import PostgresExecutor

PG_CONN = {
    "host": "127.0.0.1",
    "port": 5433,
    "database": "dsb_sf10",
    "user": "jakc9",
    "password": "jakc9",
}

QUERY_DIR = PROJECT_ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/queries"
TIMEOUT_MS = 120_000  # 2 min per execution

CONFIGS = {
    "ssd_only": [
        "SET LOCAL random_page_cost = '1.1'",
        "SET LOCAL effective_cache_size = '24GB'",
    ],
    "ssd_plus_mem": [
        "SET LOCAL random_page_cost = '1.1'",
        "SET LOCAL effective_cache_size = '24GB'",
        "SET LOCAL work_mem = '256MB'",
        "SET LOCAL hash_mem_multiplier = '4'",
    ],
}


def triage_measure(
    executor: PostgresExecutor,
    sql: str,
    config_cmds: list[str],
) -> tuple[float, float, int, int]:
    """4x triage: warmup orig, warmup config, measure orig, measure config.

    Returns (orig_ms, config_ms, orig_rows, config_rows).
    """
    # Warmup original
    rows_o = executor.execute(sql, timeout_ms=TIMEOUT_MS)

    # Warmup config
    rows_c = executor.execute_with_config(sql, config_cmds, timeout_ms=TIMEOUT_MS)

    # Measure original
    t0 = time.perf_counter()
    rows_o = executor.execute(sql, timeout_ms=TIMEOUT_MS)
    t_orig = (time.perf_counter() - t0) * 1000

    # Measure config
    t0 = time.perf_counter()
    rows_c = executor.execute_with_config(sql, config_cmds, timeout_ms=TIMEOUT_MS)
    t_config = (time.perf_counter() - t0) * 1000

    return t_orig, t_config, len(rows_o), len(rows_c)


def main():
    print()
    print("=" * 80)
    print("  SET LOCAL CONFIG SWEEP: ALL 52 DSB QUERIES")
    print("=" * 80)

    executor = PostgresExecutor(**PG_CONN)
    executor.connect()

    # Show current settings
    settings = executor.get_settings()
    print()
    print("  Current PG settings:")
    for k, v in sorted(settings.items()):
        print(f"    {k} = {v}")
    print()

    query_files = sorted(QUERY_DIR.glob("*.sql"))
    print(f"  Queries: {len(query_files)}")
    print()

    all_results = {}

    for config_name, config_cmds in CONFIGS.items():
        print("=" * 80)
        print(f"  CONFIG: {config_name}")
        print(f"  Commands: {config_cmds}")
        print("=" * 80)
        print()
        print(f"  {'Query':25s}  {'Orig(ms)':>10s}  {'Config(ms)':>10s}  "
              f"{'Speedup':>8s}  {'Rows':>6s}  {'Match':>5s}")
        print(f"  {'-'*25}  {'-'*10}  {'-'*10}  {'-'*8}  {'-'*6}  {'-'*5}")

        wins = 0
        neutral = 0
        regressions = 0
        errors = 0
        speedups = []

        for qf in query_files:
            qid = qf.stem
            sql = qf.read_text().strip()

            try:
                t_orig, t_config, rc_o, rc_c = triage_measure(
                    executor, sql, config_cmds
                )
                rows_match = rc_o == rc_c
                speedup = t_orig / t_config if t_config > 0 else 1.0

                tag = ""
                if speedup >= 1.10:
                    tag = " WIN"
                    wins += 1
                elif speedup < 0.95:
                    tag = " REG"
                    regressions += 1
                else:
                    neutral += 1

                speedups.append(speedup)

                print(f"  {qid:25s}  {t_orig:10.1f}  {t_config:10.1f}  "
                      f"{speedup:7.2f}x  {rc_o:6d}  {'OK' if rows_match else 'FAIL':>5s}"
                      f"{tag}")

                all_results.setdefault(config_name, {})[qid] = {
                    "orig_ms": round(t_orig, 1),
                    "config_ms": round(t_config, 1),
                    "speedup": round(speedup, 3),
                    "rows_match": rows_match,
                    "orig_rows": rc_o,
                }

            except Exception as e:
                err_str = str(e)[:80]
                print(f"  {qid:25s}  ERROR: {err_str}")
                errors += 1
                try:
                    executor.rollback()
                except Exception:
                    # Reconnect if connection is broken
                    try:
                        executor.close()
                    except Exception:
                        pass
                    executor = PostgresExecutor(**PG_CONN)
                    executor.connect()

        print()
        avg_speedup = sum(speedups) / len(speedups) if speedups else 0
        print(f"  {config_name} summary:")
        print(f"    WIN (>=1.10x):    {wins}")
        print(f"    NEUTRAL:          {neutral}")
        print(f"    REGRESSION:       {regressions}")
        print(f"    ERROR:            {errors}")
        print(f"    Avg speedup:      {avg_speedup:.3f}x")
        if speedups:
            print(f"    Min speedup:      {min(speedups):.3f}x")
            print(f"    Max speedup:      {max(speedups):.3f}x")
            print(f"    Median speedup:   {sorted(speedups)[len(speedups)//2]:.3f}x")
        print()

    # Save results
    out_path = PROJECT_ROOT / "research/pg_config_sweep_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(all_results, indent=2))
    print(f"  Results saved to: {out_path}")

    executor.close()


if __name__ == "__main__":
    main()
