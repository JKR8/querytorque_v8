#!/usr/bin/env python3
"""Batch benchmark runner for PostgreSQL SET LOCAL config tuning.

Uses 6-step interleaved pattern for each query:
  1. Warmup original
  2. Warmup rewrite
  3. Warmup rewrite+config
  4. Measure original        → t_orig
  5. Measure rewrite         → t_rewrite
  6. Measure rewrite+config  → t_config

rewrite_speedup = t_orig / t_rewrite
config_speedup  = t_orig / t_config
config_additive = t_rewrite / t_config  (same-run comparison)

For timeout queries: use 300,000ms baseline, only run rewrite and rewrite+config.

Input:  pg_tuning_configs.json + leaderboard_sf10.json + swarm batch SQL
Output: research/pg_config_validation_results.json
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "packages/qt-sql"))
sys.path.insert(0, str(ROOT / "packages/qt-shared"))

CONFIGS_FILE = ROOT / "research/pg_tuning_configs.json"
LEADERBOARD_FILE = ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/leaderboard_sf10.json"
SWARM_BATCH = ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/swarm_batch_20260208_142643"
SWARM_SESSIONS = ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/swarm_sessions"
ORIGINAL_QUERIES = ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/queries"
OUTPUT_FILE = ROOT / "research/pg_config_validation_results.json"

DSN = "postgresql://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
TIMEOUT_MS = 300_000

# Timeout queries — original times out at 300s
TIMEOUT_QUERIES = {"query001_multi", "query014_multi", "query032_multi",
                   "query039_multi", "query081_multi", "query092_multi"}


def _worker_sql_file(query_dir: Path, worker_num: int) -> Path | None:
    """Map worker number to SQL file path."""
    if worker_num == 5:
        f = query_dir / "snipe_worker_sql.sql"
    elif worker_num == 6:
        f = query_dir / "final_worker_sql.sql"
    elif 1 <= worker_num <= 4:
        f = query_dir / f"worker_{worker_num}_sql.sql"
    else:
        return None
    return f if f.exists() else None


def find_optimized_sql(query_id: str, leaderboard_entry: dict) -> str | None:
    """Find the best optimized SQL for a query from the swarm batch."""
    query_dir = SWARM_BATCH / query_id
    if not query_dir.exists():
        query_dir = None

    if query_dir:
        # Try result.json first (authoritative)
        result_file = query_dir / "result.json"
        if result_file.exists():
            result = json.loads(result_file.read_text())
            best_worker = result.get("best_worker")
            if best_worker and best_worker > 0:
                f = _worker_sql_file(query_dir, best_worker)
                if f:
                    return f.read_text().strip()

        # Fallback to leaderboard worker
        worker = leaderboard_entry.get("worker")
        if worker is not None:
            f = _worker_sql_file(query_dir, worker)
            if f:
                return f.read_text().strip()

        # Last resort in batch: try all workers
        for w in [1, 2, 3, 4, 5, 6]:
            f = _worker_sql_file(query_dir, w)
            if f:
                return f.read_text().strip()

    # Check swarm_sessions (Q032/Q092 have winning SQL here)
    session_dir = SWARM_SESSIONS / query_id
    if session_dir.exists():
        for iter_dir in sorted(session_dir.glob("iteration_*")):
            best_w = leaderboard_entry.get("worker")
            if best_w and 1 <= best_w <= 4:
                sql_f = iter_dir / f"worker_{best_w:02d}" / "optimized.sql"
                if sql_f.exists():
                    return sql_f.read_text().strip()
            for worker_dir in sorted(iter_dir.glob("worker_*")):
                sql_f = worker_dir / "optimized.sql"
                if sql_f.exists():
                    return sql_f.read_text().strip()

    return None


def find_original_sql(query_id: str) -> str | None:
    """Find original SQL for a query."""
    sql_file = ORIGINAL_QUERIES / f"{query_id}.sql"
    if sql_file.exists():
        return sql_file.read_text().strip()
    return None


def triage_benchmark(executor, orig_sql: str, opt_sql: str,
                     config_cmds: list[str], is_timeout: bool) -> dict:
    """6-step interleaved benchmark (1-2-3-1-2-3).

    Steps 1-3 = warmup, Steps 4-6 = measure.
    All three variants measured on same PG instance in same run.

    For timeout queries: skip original, warmup+measure rewrite and rewrite+config.

    Returns dict with timing and row counts.
    """
    if is_timeout:
        # Timeout: warmup rewrite, warmup config, measure rewrite, measure config
        executor.execute(opt_sql, timeout_ms=TIMEOUT_MS)  # warmup rewrite
        executor.execute_with_config(opt_sql, config_cmds, timeout_ms=TIMEOUT_MS)  # warmup config

        t0 = time.perf_counter()
        rows_rw = executor.execute(opt_sql, timeout_ms=TIMEOUT_MS)
        t_rewrite = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        rows_cfg = executor.execute_with_config(opt_sql, config_cmds, timeout_ms=TIMEOUT_MS)
        t_config = (time.perf_counter() - t0) * 1000

        config_additive = t_rewrite / t_config if t_config > 0 else 1.0

        return {
            "original_ms": 300_000.0,
            "rewrite_ms": round(t_rewrite, 1),
            "config_ms": round(t_config, 1),
            "rewrite_speedup": round(300_000.0 / t_rewrite, 3) if t_rewrite > 0 else 1.0,
            "config_speedup": round(300_000.0 / t_config, 3) if t_config > 0 else 1.0,
            "config_additive": round(config_additive, 3),
            "orig_rows": None,
            "rewrite_rows": len(rows_rw),
            "config_rows": len(rows_cfg),
            "rows_match_rw": None,
            "rows_match_cfg": len(rows_rw) == len(rows_cfg),
            "timeout_baseline": True,
        }

    # Normal query: full 6-step interleaved
    # Warmup round
    executor.execute(orig_sql, timeout_ms=TIMEOUT_MS)           # 1. warmup original
    executor.execute(opt_sql, timeout_ms=TIMEOUT_MS)            # 2. warmup rewrite
    executor.execute_with_config(                               # 3. warmup rewrite+config
        opt_sql, config_cmds, timeout_ms=TIMEOUT_MS
    )

    # Measure round
    t0 = time.perf_counter()                                    # 4. measure original
    rows_orig = executor.execute(orig_sql, timeout_ms=TIMEOUT_MS)
    t_orig = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()                                    # 5. measure rewrite
    rows_rw = executor.execute(opt_sql, timeout_ms=TIMEOUT_MS)
    t_rewrite = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()                                    # 6. measure rewrite+config
    rows_cfg = executor.execute_with_config(
        opt_sql, config_cmds, timeout_ms=TIMEOUT_MS
    )
    t_config = (time.perf_counter() - t0) * 1000

    rewrite_speedup = t_orig / t_rewrite if t_rewrite > 0 else 1.0
    config_speedup = t_orig / t_config if t_config > 0 else 1.0
    config_additive = t_rewrite / t_config if t_config > 0 else 1.0

    return {
        "original_ms": round(t_orig, 1),
        "rewrite_ms": round(t_rewrite, 1),
        "config_ms": round(t_config, 1),
        "rewrite_speedup": round(rewrite_speedup, 3),
        "config_speedup": round(config_speedup, 3),
        "config_additive": round(config_additive, 3),
        "orig_rows": len(rows_orig),
        "rewrite_rows": len(rows_rw),
        "config_rows": len(rows_cfg),
        "rows_match_rw": len(rows_orig) == len(rows_rw),
        "rows_match_cfg": len(rows_orig) == len(rows_cfg),
        "timeout_baseline": False,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Validate PG config tuning")
    parser.add_argument("--query", type=str, help="Run only this query ID")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be benchmarked without running")
    args = parser.parse_args()

    print("=" * 70)
    print("PostgreSQL Config Tuning — 6-Step Interleaved Benchmark")
    print("=" * 70)
    print(f"Method: 1-2-3-1-2-3 (warmup all 3, measure all 3)")
    print(f"DSN: {DSN}")

    # Load inputs
    configs = json.loads(CONFIGS_FILE.read_text())
    leaderboard = json.loads(LEADERBOARD_FILE.read_text())

    # Build lookup from leaderboard
    lb_lookup = {q["query_id"]: q for q in leaderboard["queries"]}

    # Filter to queries with config
    queries_to_run = []
    for query_id, config in sorted(configs.items()):
        if not config.get("params"):
            continue
        if args.query and query_id != args.query:
            continue
        queries_to_run.append((query_id, config))

    print(f"\nQueries with config: {len(queries_to_run)}")
    print(f"Leaderboard entries: {len(lb_lookup)}")

    if args.dry_run:
        print("\n--- DRY RUN ---")
        for query_id, config in queries_to_run:
            lb_entry = lb_lookup.get(query_id, {})
            opt_sql = find_optimized_sql(query_id, lb_entry)
            orig_sql = find_original_sql(query_id)
            status = lb_entry.get("status", "?")
            is_timeout = query_id in TIMEOUT_QUERIES
            print(f"  {query_id:30s} status={status:12s} "
                  f"timeout={'Y' if is_timeout else 'N'} "
                  f"rules={config.get('rules_triggered', [])} "
                  f"orig={'✓' if orig_sql else '✗'} "
                  f"opt={'✓' if opt_sql else '✗'} "
                  f"cmds={len(config.get('config_cmds', []))}")
        return

    # Initialize executor directly
    from qt_sql.execution.postgres_executor import PostgresExecutor

    # Parse DSN
    # postgresql://jakc9:jakc9@127.0.0.1:5434/dsb_sf10
    executor = PostgresExecutor(
        host="127.0.0.1",
        port=5434,
        database="dsb_sf10",
        user="jakc9",
        password="jakc9",
    )
    executor.connect()
    print(f"\nConnected: {executor.get_version()}")

    results = {}
    errors = []
    total = len(queries_to_run)
    start_time = time.time()

    for i, (query_id, config) in enumerate(queries_to_run, 1):
        lb_entry = lb_lookup.get(query_id, {})
        config_cmds = config.get("config_cmds", [])
        is_timeout = query_id in TIMEOUT_QUERIES

        orig_sql = find_original_sql(query_id)
        if not orig_sql:
            print(f"  [{i}/{total}] {query_id}: SKIP — no original SQL")
            continue

        opt_sql = find_optimized_sql(query_id, lb_entry)
        if not opt_sql:
            opt_sql = orig_sql  # config-only test

        lb_speedup = lb_entry.get("speedup", 0)
        lb_orig_ms = lb_entry.get("original_ms")
        lb_opt_ms = lb_entry.get("optimized_ms")

        elapsed = time.time() - start_time
        print(f"\n  [{i}/{total}] {query_id} "
              f"(lb_speedup={lb_speedup:.2f}, "
              f"timeout={'Y' if is_timeout else 'N'}, "
              f"elapsed={elapsed:.0f}s)")
        print(f"    Config: {config.get('params', {})}")

        try:
            result = triage_benchmark(
                executor, orig_sql, opt_sql, config_cmds, is_timeout
            )
        except Exception as e:
            print(f"    ERROR: {e}")
            results[query_id] = {
                "status": "ERROR",
                "error": str(e),
                "config_params": config["params"],
                "rules": config.get("rules_triggered", []),
            }
            errors.append(query_id)
            # Reconnect in case of connection loss
            try:
                executor.close()
            except Exception:
                pass
            executor = PostgresExecutor(
                host="127.0.0.1", port=5434, database="dsb_sf10",
                user="jakc9", password="jakc9",
            )
            executor.connect()
            continue

        config_additive = result["config_additive"]

        # Safety check
        safety_flag = ""
        if result["config_speedup"] < 0.5:
            safety_flag = " ⚠ >2x REGRESSION"

        print(f"    orig={result['original_ms']:.1f}ms "
              f"rewrite={result['rewrite_ms']:.1f}ms "
              f"config={result['config_ms']:.1f}ms")
        print(f"    rewrite_speedup={result['rewrite_speedup']:.3f}x "
              f"config_speedup={result['config_speedup']:.3f}x "
              f"config_additive={config_additive:.3f}x "
              f"rows_rw={result.get('rows_match_rw')} "
              f"rows_cfg={result.get('rows_match_cfg')}"
              f"{safety_flag}")

        # Classify by config_additive (config lift over rewrite-only)
        if config_additive >= 1.1:
            config_status = "CONFIG_WIN"
        elif config_additive >= 1.05:
            config_status = "CONFIG_IMPROVED"
        elif config_additive >= 0.95:
            config_status = "CONFIG_NEUTRAL"
        else:
            config_status = "CONFIG_REGRESSION"

        results[query_id] = {
            "status": config_status,
            "original_ms": result["original_ms"],
            "rewrite_ms": result["rewrite_ms"],
            "config_ms": result["config_ms"],
            "rewrite_speedup": result["rewrite_speedup"],
            "config_speedup": result["config_speedup"],
            "config_additive": config_additive,
            "orig_rows": result["orig_rows"],
            "rewrite_rows": result["rewrite_rows"],
            "config_rows": result["config_rows"],
            "rows_match_rw": result.get("rows_match_rw"),
            "rows_match_cfg": result.get("rows_match_cfg"),
            "timeout_baseline": result.get("timeout_baseline", False),
            "config_params": config["params"],
            "config_cmds": config.get("config_cmds", []),
            "rules": config.get("rules_triggered", []),
            "lb_original_ms": lb_orig_ms,
            "lb_optimized_ms": lb_opt_ms,
            "lb_speedup": lb_speedup,
        }

    # Close executor
    executor.close()

    # Summary
    total_elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"Summary ({total_elapsed:.0f}s total)")
    print("=" * 70)
    status_counts: dict[str, int] = {}
    for r in results.values():
        s = r.get("status", "UNKNOWN")
        status_counts[s] = status_counts.get(s, 0) + 1
    for status, count in sorted(status_counts.items()):
        print(f"  {status:20s} {count}")
    if errors:
        print(f"\n  Errors: {errors}")

    # Avg config_additive for non-error
    additives = [
        r["config_additive"] for r in results.values()
        if r.get("config_additive") is not None
    ]
    if additives:
        print(f"\n  Avg config_additive: {sum(additives)/len(additives):.3f}x")

    # Top config wins
    winners = [
        (qid, r) for qid, r in results.items()
        if r.get("config_additive") is not None and r["config_additive"] >= 1.05
    ]
    winners.sort(key=lambda x: x[1]["config_additive"], reverse=True)
    if winners:
        print(f"\n  Config winners (additive ≥ 1.05x):")
        for qid, r in winners:
            print(f"    {qid:30s} additive={r['config_additive']:.3f}x "
                  f"config_speedup={r['config_speedup']:.3f}x")

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "method": "4x_triage_1-2-1-2",
        "dsn": DSN,
        "total_benchmarked": len(results),
        "total_elapsed_s": round(total_elapsed, 1),
        "status_counts": status_counts,
        "results": results,
    }
    OUTPUT_FILE.write_text(json.dumps(output, indent=2) + "\n")
    print(f"\nWrote results to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
