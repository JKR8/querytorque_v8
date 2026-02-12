#!/usr/bin/env python3
"""Config Tuning Pass 2 — Targeted follow-up on Pass 1 findings.

Key learnings from Pass 1:
- random_page_cost=1.1 dominates cost reduction but often HURTS runtime
- combo_kitchen wins when parallelism or join_collapse actually change the plan
- Need to test configs WITHOUT random_page_cost separately

Strategy:
1. Re-race the 5 real winners with the EXACT winning config (no random_page_cost noise)
2. Try surgical configs on "rpc-only" queries to see if non-rpc changes help
3. For losses: try individual params that actually change plan shape

Usage:
    cd QueryTorque_V8
    python3 research/config_tuning_pass2.py
"""

import json
import os
import re
import sys
import time
import threading
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BENCH_DIR = PROJECT_ROOT / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb"
QUERIES_DIR = BENCH_DIR / "queries"
BEST_DIR = BENCH_DIR / "best"
OUTPUT_DIR = PROJECT_ROOT / "research/config_tuning_results"

DSN = "host=127.0.0.1 port=5434 dbname=dsb_sf10 user=jakc9 password=jakc9"


def get_conn():
    return psycopg2.connect(DSN)


def run_explain(conn, sql, analyze=False, config=None, hint=None, timeout_ms=300000):
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
        if config:
            for k, v in config.items():
                cur.execute(f"SET LOCAL {k} = %s", (v,))
        cmd = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)" if analyze else "EXPLAIN (FORMAT TEXT)"
        full_sql = f"/*+ {hint} */\n{sql}" if hint else sql
        cur.execute(f"{cmd} {full_sql}")
        rows = cur.fetchall()
        plan = "\n".join(r[0] for r in rows)
    conn.rollback()
    cost_m = re.search(r'cost=[\d.]+\.\.([\d.]+)', plan)
    exec_m = re.search(r'Execution Time:\s*([\d.]+)', plan)
    return plan, float(cost_m.group(1)) if cost_m else 0, float(exec_m.group(1)) if exec_m else None


def race(sql, config_b=None, hint_b=None):
    """Race same query: plain vs with config. Both wait at barrier."""
    results = {"a_ms": None, "b_ms": None}
    barrier = threading.Barrier(2, timeout=600)

    def run_plain():
        try:
            c = get_conn()
            with c.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '300000'")
                barrier.wait()
                t0 = time.perf_counter()
                cur.execute(sql)
                cur.fetchall()
                results["a_ms"] = (time.perf_counter() - t0) * 1000
            c.rollback(); c.close()
        except Exception as e:
            results["a_error"] = str(e)

    def run_tuned():
        try:
            c = get_conn()
            with c.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '300000'")
                if config_b:
                    for k, v in config_b.items():
                        cur.execute(f"SET LOCAL {k} = %s", (v,))
                barrier.wait()
                t0 = time.perf_counter()
                if hint_b:
                    cur.execute(f"/*+ {hint_b} */\n{sql}")
                else:
                    cur.execute(sql)
                cur.fetchall()
                results["b_ms"] = (time.perf_counter() - t0) * 1000
            c.rollback(); c.close()
        except Exception as e:
            results["b_error"] = str(e)

    ta = threading.Thread(target=run_plain)
    tb = threading.Thread(target=run_tuned)
    ta.start(); tb.start()
    ta.join(600); tb.join(600)
    return results


# ── Targeted tests ──
# Based on Pass 1 findings, test specific configs for each query

TESTS = [
    # === CONFIRMED WINNERS — revalidate without random_page_cost ===
    {
        "query_id": "query102_spj_spj",
        "tests": [
            ("combo_kitchen", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                               "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001",
                               "effective_cache_size": "48GB", "random_page_cost": "1.1"}),
            ("no_rpc", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                        "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("just_ssd", {"random_page_cost": "1.1"}),
            ("wm512_jit", {"work_mem": "512MB", "jit": "off"}),
            ("par_only", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                          "parallel_tuple_cost": "0.001"}),
        ],
    },
    {
        "query_id": "query087_multi",
        "tests": [
            ("combo_kitchen", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                               "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001",
                               "effective_cache_size": "48GB", "random_page_cost": "1.1"}),
            ("no_rpc", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                        "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("just_ssd", {"random_page_cost": "1.1"}),
            ("wm_par", {"work_mem": "256MB", "max_parallel_workers_per_gather": "4",
                        "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("force_par", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                           "parallel_tuple_cost": "0.001"}),
        ],
    },
    {
        "query_id": "query072_spj_spj",
        "tests": [
            ("jcl_12", {"join_collapse_limit": "12"}),
            ("jcl_16", {"join_collapse_limit": "16"}),
            ("jcl_12_wm", {"join_collapse_limit": "12", "work_mem": "256MB"}),
            ("jcl_12_par", {"join_collapse_limit": "12", "max_parallel_workers_per_gather": "4",
                            "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("jcl_12_full", {"join_collapse_limit": "12", "work_mem": "256MB",
                             "max_parallel_workers_per_gather": "4",
                             "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
        ],
    },
    {
        "query_id": "query091_spj_spj",
        "tests": [
            ("combo_kitchen", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                               "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001",
                               "effective_cache_size": "48GB", "random_page_cost": "1.1"}),
            ("no_rpc", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                        "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("par_only", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                          "parallel_tuple_cost": "0.001"}),
            ("wm256_par", {"work_mem": "256MB", "max_parallel_workers_per_gather": "4"}),
        ],
    },
    {
        "query_id": "query023_multi",
        "tests": [
            ("combo_kitchen", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                               "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001",
                               "effective_cache_size": "48GB", "random_page_cost": "1.1"}),
            ("no_rpc", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                        "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("just_ssd", {"random_page_cost": "1.1"}),
            ("wm512_par4", {"work_mem": "512MB", "max_parallel_workers_per_gather": "4"}),
            ("par_lowcost", {"parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
        ],
    },
    # === LOSSES — try surgical non-rpc configs ===
    {
        "query_id": "query059_multi",
        "tests": [
            ("wm_512", {"work_mem": "512MB"}),
            ("wm_1g", {"work_mem": "1GB"}),
            ("par_low", {"parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("wm512_par", {"work_mem": "512MB", "parallel_setup_cost": "100",
                           "parallel_tuple_cost": "0.001"}),
            ("hmm_8", {"hash_mem_multiplier": "8.0"}),
        ],
    },
    {
        "query_id": "query064_multi",
        "tests": [
            ("wm_512", {"work_mem": "512MB"}),
            ("wm_1g", {"work_mem": "1GB"}),
            ("wm_2g", {"work_mem": "2GB"}),
            ("wm512_par4", {"work_mem": "512MB", "max_parallel_workers_per_gather": "4",
                            "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("wm1g_hmm8", {"work_mem": "1GB", "hash_mem_multiplier": "8.0"}),
        ],
    },
    {
        "query_id": "query101_spj_spj",
        "tests": [
            ("wm_512", {"work_mem": "512MB"}),
            ("par_only", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                          "parallel_tuple_cost": "0.001"}),
            ("wm512_par", {"work_mem": "512MB", "max_parallel_workers_per_gather": "4",
                           "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("cache48", {"effective_cache_size": "48GB"}),
        ],
    },
    # === TIES — try harder ===
    {
        "query_id": "query018_spj_spj",
        "tests": [
            ("wm_512", {"work_mem": "512MB"}),
            ("par_low", {"parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("wm512_par", {"work_mem": "512MB", "parallel_setup_cost": "100",
                           "parallel_tuple_cost": "0.001"}),
            ("no_nest", {"enable_nestloop": "off"}),
        ],
    },
    {
        "query_id": "query072_agg",
        "tests": [
            ("jcl_12", {"join_collapse_limit": "12"}),
            ("jcl_16", {"join_collapse_limit": "16"}),
            ("par_low", {"parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
            ("wm256_jcl", {"work_mem": "256MB", "join_collapse_limit": "12"}),
        ],
    },
]


def main():
    print(f"Config Tuning Pass 2 — {len(TESTS)} queries, targeted configs")
    print(f"Started: {datetime.now().isoformat()}\n")

    all_results = []

    for test_group in TESTS:
        qid = test_group["query_id"]
        opt_path = BEST_DIR / f"{qid}.sql"
        if not opt_path.exists():
            print(f"  SKIP {qid}: no optimized SQL")
            continue

        opt_sql = opt_path.read_text().strip()
        print(f"\n{'='*70}")
        print(f"QUERY: {qid}")
        print(f"{'='*70}")

        # Warmup
        try:
            c = get_conn()
            with c.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '300000'")
                cur.execute(opt_sql)
                cur.fetchall()
            c.rollback(); c.close()
        except:
            pass

        group_results = {"query_id": qid, "tests": []}

        for config_name, config in test_group["tests"]:
            # First check cost via EXPLAIN
            conn = get_conn()
            try:
                _, cost, _ = run_explain(conn, opt_sql, analyze=False, config=config)
                _, base_cost, _ = run_explain(conn, opt_sql, analyze=False)
                cost_ratio = cost / base_cost if base_cost > 0 else 1.0
            except Exception as e:
                print(f"  {config_name:20s} EXPLAIN ERROR: {e}")
                conn.close()
                continue
            conn.close()

            # Race
            r = race(opt_sql, config_b=config)
            a_ms = r.get("a_ms")
            b_ms = r.get("b_ms")

            if a_ms and b_ms:
                gap_pct = (a_ms - b_ms) / a_ms * 100 if a_ms > 0 else 0
                winner = "config" if b_ms < a_ms else "baseline"
                marker = "✓" if winner == "config" and gap_pct > 2 else "—" if abs(gap_pct) < 2 else "✗"

                entry = {
                    "name": config_name,
                    "config": config,
                    "cost_ratio": round(cost_ratio, 4),
                    "baseline_ms": round(a_ms, 1),
                    "tuned_ms": round(b_ms, 1),
                    "gap_pct": round(gap_pct, 1),
                    "winner": winner,
                }
                group_results["tests"].append(entry)

                print(f"  {marker} {config_name:20s} base={a_ms:>8.1f}ms tuned={b_ms:>8.1f}ms gap={gap_pct:>+6.1f}% cost_r={cost_ratio:.4f}")
            else:
                print(f"  ? {config_name:20s} race error: {r.get('a_error','?')}/{r.get('b_error','?')}")

        all_results.append(group_results)

    # Save
    out_path = OUTPUT_DIR / "config_tuning_pass2.json"
    out_path.write_text(json.dumps({
        "results": all_results,
        "completed_at": datetime.now().isoformat(),
    }, indent=2, default=str))

    # Summary
    print(f"\n\n{'='*70}")
    print("PASS 2 SUMMARY")
    print(f"{'='*70}")

    for gr in all_results:
        qid = gr["query_id"]
        tests = gr["tests"]
        config_wins = [t for t in tests if t.get("winner") == "config" and t.get("gap_pct", 0) > 2]
        if config_wins:
            best = max(config_wins, key=lambda t: t["gap_pct"])
            print(f"  {qid:25s} BEST: {best['name']:20s} {best['gap_pct']:>+6.1f}% ({best['baseline_ms']:.0f}→{best['tuned_ms']:.0f}ms)")
        else:
            print(f"  {qid:25s} no config win >2%")

    print(f"\nResults: {out_path}")


if __name__ == "__main__":
    main()
