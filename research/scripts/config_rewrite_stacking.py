"""Config + SQL Rewrite Stacking Experiment.

For each query, test whether config gains and SQL rewrite gains are:
  (a) Additive/multiplicative (they stack)
  (b) Redundant (rewrite already captures the config gain)
  (c) Conflicting (rewrite needs a different config)

Variants per query:
  V1: Original SQL, no config (baseline)
  V2: Original SQL + winning config
  V3: Best SQL rewrite, no config
  V4: Best SQL rewrite + same winning config
  V5+: Best SQL rewrite + other promising configs (re-scan)

All measured with 5x trimmed mean.

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 research/scripts/config_rewrite_stacking.py
"""

import json
import time
import psycopg2
from pathlib import Path

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
SWARM_DIR = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb/swarm_batch_20260208_142643")
QUERIES_DIR = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb/queries")
TIMEOUT_MS = 300000  # 5 min for Q014

# Config combos proven for the DSB-76 versions (same structure, should transfer)
WINNING_CONFIGS = {
    "query023_multi": {
        "name": "no_hashjoin+max_parallel",
        "config": {
            "enable_hashjoin": "off",
            "max_parallel_workers_per_gather": "8",
        },
    },
    "query083_multi": {
        "name": "force_nestloop+ssd_costs",
        "config": {
            "enable_hashjoin": "off",
            "enable_mergejoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        },
    },
    "query014_multi": {
        "name": "no_hashjoin+ssd_costs",
        "config": {
            "enable_hashjoin": "off",
            "random_page_cost": "1.1",
            "effective_cache_size": "24GB",
        },
    },
}

# Extra configs to re-scan for rewritten SQL (the rewrite changes the plan landscape)
RESCAN_CONFIGS = {
    "work_mem_256mb": {"work_mem": "256MB"},
    "ssd_costs": {"random_page_cost": "1.1", "effective_cache_size": "24GB"},
    "ssd_plus_mem": {
        "random_page_cost": "1.1", "effective_cache_size": "24GB",
        "work_mem": "256MB", "hash_mem_multiplier": "4",
    },
    "max_parallel": {"max_parallel_workers_per_gather": "8"},
    "no_jit": {"jit": "off"},
    "force_hash": {"enable_nestloop": "off", "enable_mergejoin": "off"},
    "no_nestloop": {"enable_nestloop": "off"},
    "no_parallel": {"max_parallel_workers_per_gather": "0"},
    "no_jit+ssd": {"jit": "off", "random_page_cost": "1.1", "effective_cache_size": "24GB"},
    "mem_256mb+max_par": {"work_mem": "256MB", "max_parallel_workers_per_gather": "8"},
}

# Best rewrite worker per query
BEST_WORKERS = {
    "query023_multi": "worker_4_sql.sql",   # 66.33x
    "query083_multi": "worker_2_sql.sql",   # 1.01x (best available)
    "query014_multi": "worker_4_sql.sql",   # 1.05x
}


def run_timed(conn, sql, config=None, timeout_ms=TIMEOUT_MS):
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
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
        except Exception as e:
            conn.rollback()
            return float('inf'), -1  # error
        elapsed_ms = (time.perf_counter() - t0) * 1000
    return elapsed_ms, len(rows)


def trimmed_mean_5x(conn, sql, config=None, timeout_ms=TIMEOUT_MS):
    times = []
    row_count = 0
    for i in range(5):
        conn.autocommit = False
        ms, rc = run_timed(conn, sql, config, timeout_ms)
        conn.rollback()
        if ms == float('inf'):
            return float('inf'), rc, []
        times.append(ms)
        row_count = rc
    sorted_times = sorted(times)
    trimmed = sorted_times[1:4]
    avg = sum(trimmed) / len(trimmed)
    return avg, row_count, times


def test_variant(conn, name, sql, config=None, timeout_ms=TIMEOUT_MS):
    """Test one variant, print result, return dict."""
    print(f"  {name:45s}", end="", flush=True)
    avg, rows, times = trimmed_mean_5x(conn, sql, config, timeout_ms)
    if avg == float('inf'):
        print(f" TIMEOUT (rows={rows})")
        return {"name": name, "avg_ms": None, "rows": rows, "times": [], "status": "timeout"}
    print(f" {avg:>10.1f}ms  rows={rows}  (runs: {[f'{t:.0f}' for t in times]})")
    return {"name": name, "avg_ms": round(avg, 2), "rows": rows, "times": [round(t, 2) for t in times]}


def main():
    conn = psycopg2.connect(DSN)
    all_results = {}

    for query_id in ["query023_multi", "query083_multi", "query014_multi"]:
        print(f"\n{'='*75}")
        print(f"  {query_id}")
        print(f"{'='*75}")

        # Load SQL
        orig_sql = (QUERIES_DIR / f"{query_id}.sql").read_text().strip()
        rewrite_sql = (SWARM_DIR / query_id / BEST_WORKERS[query_id]).read_text().strip()
        win_cfg = WINNING_CONFIGS[query_id]

        query_results = {"query_id": query_id, "variants": []}

        # V1: Original SQL, no config (baseline)
        v1 = test_variant(conn, "V1: Original SQL", orig_sql)
        query_results["variants"].append(v1)
        baseline_ms = v1["avg_ms"]

        if baseline_ms is None:
            print(f"  BASELINE TIMEOUT — skipping query")
            all_results[query_id] = query_results
            continue

        # V2: Original SQL + winning config
        v2 = test_variant(conn, f"V2: Original + {win_cfg['name']}", orig_sql, win_cfg["config"])
        query_results["variants"].append(v2)

        # V3: Best SQL rewrite, no config
        v3 = test_variant(conn, f"V3: Rewrite ({BEST_WORKERS[query_id]})", rewrite_sql)
        query_results["variants"].append(v3)

        # V4: Best SQL rewrite + same winning config
        v4 = test_variant(
            conn,
            f"V4: Rewrite + {win_cfg['name']}",
            rewrite_sql,
            win_cfg["config"],
        )
        query_results["variants"].append(v4)

        # V5+: Re-scan configs on rewritten SQL
        print(f"\n  --- Re-scanning configs on rewritten SQL ---")
        rescan_results = []
        for cfg_name, cfg in RESCAN_CONFIGS.items():
            v = test_variant(conn, f"V5: Rewrite + {cfg_name}", rewrite_sql, cfg)
            rescan_results.append(v)
            query_results["variants"].append(v)

        # Analysis
        print(f"\n  --- ANALYSIS ---")
        v2_ms = v2.get("avg_ms")
        v3_ms = v3.get("avg_ms")
        v4_ms = v4.get("avg_ms")

        if baseline_ms and v2_ms:
            print(f"  Config-only speedup:       {baseline_ms/v2_ms:.3f}x ({baseline_ms:.0f}→{v2_ms:.0f}ms)")
        if baseline_ms and v3_ms:
            print(f"  Rewrite-only speedup:      {baseline_ms/v3_ms:.3f}x ({baseline_ms:.0f}→{v3_ms:.0f}ms)")
        if baseline_ms and v4_ms:
            print(f"  Rewrite+config speedup:    {baseline_ms/v4_ms:.3f}x ({baseline_ms:.0f}→{v4_ms:.0f}ms)")

        if v3_ms and v4_ms:
            stacking = v3_ms / v4_ms
            if stacking > 1.05:
                print(f"  STACKING: Config adds {stacking:.3f}x ON TOP of rewrite  *** GAINS COMPOUND ***")
            elif stacking < 0.95:
                print(f"  CONFLICT: Config HURTS rewrite ({stacking:.3f}x)  *** REWRITE NEEDS DIFFERENT CONFIG ***")
            else:
                print(f"  REDUNDANT: Config adds nothing to rewrite ({stacking:.3f}x)")

        # Find best rescan config for rewrite
        best_rescan = None
        best_rescan_ms = v3_ms if v3_ms else float('inf')
        for r in rescan_results:
            if r.get("avg_ms") and r["avg_ms"] < best_rescan_ms:
                # Check row count matches
                if r["rows"] == v3.get("rows"):
                    best_rescan_ms = r["avg_ms"]
                    best_rescan = r["name"]

        if best_rescan and v3_ms:
            print(f"  Best config for REWRITE:   {best_rescan} → {baseline_ms/best_rescan_ms:.3f}x total "
                  f"({v3_ms/best_rescan_ms:.3f}x added)")

        all_results[query_id] = query_results

    conn.close()

    # Save
    out_path = Path("research/scripts/config_rewrite_stacking_results.json")
    out_path.write_text(json.dumps(all_results, indent=2, default=str))
    print(f"\nResults saved to {out_path}")

    # Final summary
    print(f"\n{'='*75}")
    print(f"  STACKING VERDICT")
    print(f"{'='*75}")
    for qid, r in all_results.items():
        variants = r["variants"]
        v = {v["name"].split(":")[0].strip(): v for v in variants}
        base = v.get("V1", {}).get("avg_ms")
        if not base:
            print(f"  {qid}: TIMEOUT")
            continue
        cfg = v.get("V2", {}).get("avg_ms")
        rw = v.get("V3", {}).get("avg_ms")
        both = v.get("V4", {}).get("avg_ms")
        print(f"  {qid}:")
        if cfg: print(f"    Config only:    {base/cfg:.2f}x")
        if rw:  print(f"    Rewrite only:   {base/rw:.2f}x")
        if both: print(f"    Rewrite+config: {base/both:.2f}x")
        if rw and both:
            delta = rw / both
            if delta > 1.05:
                print(f"    → STACKS: +{delta:.2f}x additional from config")
            elif delta < 0.95:
                print(f"    → CONFLICTS: config hurts rewrite by {delta:.2f}x")
            else:
                print(f"    → REDUNDANT: config adds nothing ({delta:.2f}x)")


if __name__ == "__main__":
    main()
