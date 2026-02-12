#!/usr/bin/env python3
"""Config Tuning Pass 4 — Validate big wins + crack combo mysteries.

Key discoveries from Pass 3 to validate:
1. query064_multi: geqo=off → +38.1% (19 joins, GEQO converges poorly)
2. query050_spj_spj: par4 → +29.6% (parallelism win)
3. query023_multi: par_only → +25.5% (parallelism win)
4. query059_multi: enable_sort=off → +12.9% (sort bottleneck)
5. query102_spj_spj: combo_kitchen +78.8% but NO subset works — need to find minimum combo

Also: validate query030, query091_agg, query069_multi (16%+ wins from Pass 3)

Validation method: 3 races per config, report all 3 gaps.

Usage:
    cd QueryTorque_V8
    python3 research/config_tuning_pass4.py
"""

import json
import re
import time
import threading
from pathlib import Path
from datetime import datetime

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BENCH_DIR = PROJECT_ROOT / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb"
BEST_DIR = BENCH_DIR / "best"
OUTPUT_DIR = PROJECT_ROOT / "research/config_tuning_results"
DSN = "host=127.0.0.1 port=5434 dbname=dsb_sf10 user=jakc9 password=jakc9"


def get_conn():
    return psycopg2.connect(DSN)


def race(sql, config_b=None, hint_b=None, timeout_ms=300000):
    """Race plain vs config-tuned. Both start at barrier."""
    results = {"a_ms": None, "b_ms": None}
    barrier = threading.Barrier(2, timeout=600)

    def run_plain():
        try:
            c = get_conn()
            with c.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
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
                cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
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


def validate_config(qid, sql, config, hint=None, n_races=3, label=""):
    """Run n races and report all gaps."""
    gaps = []
    # Warmup both sides
    try:
        c = get_conn()
        with c.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '300000'")
            cur.execute(sql)
            cur.fetchall()
        c.rollback(); c.close()
    except:
        pass

    for i in range(n_races):
        r = race(sql, config_b=config, hint_b=hint)
        a_ms, b_ms = r.get("a_ms"), r.get("b_ms")
        if a_ms and b_ms:
            gap = (a_ms - b_ms) / a_ms * 100
            gaps.append({"race": i+1, "plain_ms": round(a_ms, 1), "tuned_ms": round(b_ms, 1), "gap_pct": round(gap, 1)})
        else:
            gaps.append({"race": i+1, "error": r.get("a_error") or r.get("b_error")})

    # Summary
    valid_gaps = [g["gap_pct"] for g in gaps if "gap_pct" in g]
    if valid_gaps:
        avg_gap = sum(valid_gaps) / len(valid_gaps)
        min_gap = min(valid_gaps)
        max_gap = max(valid_gaps)
        consistent = all(g > 0 for g in valid_gaps)  # all races show config winning
    else:
        avg_gap = min_gap = max_gap = 0
        consistent = False

    return {
        "query_id": qid,
        "label": label,
        "config": config,
        "hint": hint,
        "races": gaps,
        "avg_gap_pct": round(avg_gap, 1),
        "min_gap_pct": round(min_gap, 1),
        "max_gap_pct": round(max_gap, 1),
        "consistent": consistent,
        "verdict": "WIN" if consistent and avg_gap > 3 else "MARGINAL" if avg_gap > 0 else "LOSS",
    }


# ── Tests to run ──

VALIDATIONS = [
    # === BIG WINS to validate ===
    {
        "query_id": "query064_multi",
        "tests": [
            ("geqo_off", {"geqo": "off"}, None),
            ("geqo_off_par", {"geqo": "off", "max_parallel_workers_per_gather": "4",
                              "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}, None),
            ("geqo_off_wm256", {"geqo": "off", "work_mem": "256MB"}, None),
            ("geqo_thresh_20", {"geqo_threshold": "20"}, None),
        ],
    },
    {
        "query_id": "query050_spj_spj",
        "tests": [
            ("par4", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
            ("par4_wm256", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                            "parallel_tuple_cost": "0.001", "work_mem": "256MB"}, None),
            ("par2", {"max_parallel_workers_per_gather": "2"}, None),
        ],
    },
    {
        "query_id": "query023_multi",
        "tests": [
            ("par4", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
            ("par4_wm512", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                            "parallel_tuple_cost": "0.001", "work_mem": "512MB"}, None),
        ],
    },
    {
        "query_id": "query059_multi",
        "tests": [
            ("no_sort", {"enable_sort": "off"}, None),
            ("no_sort_par", {"enable_sort": "off", "max_parallel_workers_per_gather": "4",
                             "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}, None),
        ],
    },
    # === COMBO MYSTERY: query102 — test all 2-param combos ===
    {
        "query_id": "query102_spj_spj",
        "tests": [
            # The full combo that works
            ("combo_kitchen", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                               "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001",
                               "effective_cache_size": "48GB", "random_page_cost": "1.1"}, None),
            # Test: does effective_cache_size do anything?
            ("cache48_rpc", {"effective_cache_size": "48GB", "random_page_cost": "1.1"}, None),
            # Test: combo without cache
            ("no_cache_combo", {"work_mem": "512MB", "jit": "off", "max_parallel_workers_per_gather": "4",
                                "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001",
                                "random_page_cost": "1.1"}, None),
            # Test: combo without jit
            ("no_jit_combo", {"work_mem": "512MB", "max_parallel_workers_per_gather": "4",
                              "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001",
                              "effective_cache_size": "48GB", "random_page_cost": "1.1"}, None),
            # Test: rpc + par + cache (no work_mem, no jit)
            ("rpc_par_cache", {"random_page_cost": "1.1", "max_parallel_workers_per_gather": "4",
                               "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001",
                               "effective_cache_size": "48GB"}, None),
            # Test: rpc + cache only
            ("rpc_cache", {"random_page_cost": "1.1", "effective_cache_size": "48GB"}, None),
        ],
    },
    # === SECONDARY WINS to validate ===
    {
        "query_id": "query030_multi",
        "tests": [
            ("par4", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
        ],
    },
    {
        "query_id": "query091_agg",
        "tests": [
            ("par4", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
            ("wm256_par", {"work_mem": "256MB", "max_parallel_workers_per_gather": "4",
                           "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}, None),
        ],
    },
    {
        "query_id": "query069_multi",
        "tests": [
            ("wm256_par", {"work_mem": "256MB", "max_parallel_workers_per_gather": "4",
                           "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}, None),
            ("wm256_only", {"work_mem": "256MB"}, None),
        ],
    },
    {
        "query_id": "query087_multi",
        "tests": [
            ("wm256_only", {"work_mem": "256MB"}, None),
            ("wm256_par", {"work_mem": "256MB", "max_parallel_workers_per_gather": "4",
                           "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}, None),
        ],
    },
    {
        "query_id": "query091_spj_spj",
        "tests": [
            ("par4", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
        ],
    },
]


def main():
    print(f"Config Tuning Pass 4 — 3-Race Validation of Big Wins")
    print(f"Started: {datetime.now().isoformat()}\n")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []

    for group in VALIDATIONS:
        qid = group["query_id"]
        opt_path = BEST_DIR / f"{qid}.sql"
        if not opt_path.exists():
            print(f"  SKIP {qid}: no optimized SQL")
            continue
        opt_sql = opt_path.read_text().strip()

        print(f"\n{'='*70}")
        print(f"VALIDATING: {qid}")
        print(f"{'='*70}")

        for name, config, hint in group["tests"]:
            result = validate_config(qid, opt_sql, config, hint, n_races=3, label=name)

            races_str = " | ".join(
                f"R{g['race']}: {g.get('gap_pct', '?'):+.1f}%"
                if 'gap_pct' in g else f"R{g['race']}: ERR"
                for g in result["races"]
            )
            verdict = result["verdict"]
            avg = result["avg_gap_pct"]
            marker = {"WIN": "✓", "MARGINAL": "~", "LOSS": "✗"}.get(verdict, "?")

            print(f"  {marker} {name:25s} avg={avg:+.1f}% [{races_str}] → {verdict}")

            all_results.append(result)

    # Save
    out_path = OUTPUT_DIR / "config_tuning_pass4.json"
    out_path.write_text(json.dumps({
        "results": all_results,
        "completed_at": datetime.now().isoformat(),
    }, indent=2, default=str))

    # Summary
    print(f"\n\n{'='*70}")
    print("PASS 4 VALIDATION SUMMARY")
    print(f"{'='*70}")

    wins = [r for r in all_results if r["verdict"] == "WIN"]
    marginal = [r for r in all_results if r["verdict"] == "MARGINAL"]
    losses = [r for r in all_results if r["verdict"] == "LOSS"]

    print(f"\n  CONFIRMED WINS ({len(wins)}):")
    for r in sorted(wins, key=lambda x: -x["avg_gap_pct"]):
        cfg_str = ", ".join(f"{k}={v}" for k, v in r["config"].items()) if r["config"] else r.get("hint", "?")
        print(f"    {r['query_id']:25s} {r['label']:25s} avg={r['avg_gap_pct']:+.1f}% (min={r['min_gap_pct']:+.1f}% max={r['max_gap_pct']:+.1f}%)")

    print(f"\n  MARGINAL ({len(marginal)}):")
    for r in sorted(marginal, key=lambda x: -x["avg_gap_pct"]):
        print(f"    {r['query_id']:25s} {r['label']:25s} avg={r['avg_gap_pct']:+.1f}%")

    print(f"\n  LOSSES ({len(losses)}):")
    for r in sorted(losses, key=lambda x: x["avg_gap_pct"]):
        print(f"    {r['query_id']:25s} {r['label']:25s} avg={r['avg_gap_pct']:+.1f}%")

    # Final recommendations
    print(f"\n{'='*70}")
    print("FINAL CONFIG RECOMMENDATIONS")
    print(f"{'='*70}")

    # Group by query_id, pick best confirmed win
    from collections import defaultdict
    by_query = defaultdict(list)
    for r in wins:
        by_query[r["query_id"]].append(r)

    for qid in sorted(by_query.keys()):
        best = max(by_query[qid], key=lambda x: x["avg_gap_pct"])
        cfg_str = ", ".join(f"{k}={v}" for k, v in best["config"].items())
        print(f"  {qid:25s} → {cfg_str}  ({best['avg_gap_pct']:+.1f}%)")

    print(f"\nResults: {out_path}")


if __name__ == "__main__":
    main()
