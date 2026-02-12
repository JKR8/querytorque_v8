#!/usr/bin/env python3
"""Config Tuning Batch — Systematic SET LOCAL + pg_hint_plan tuning.

For each of 20 middle-leaderboard queries:
1. EXPLAIN ANALYZE baseline (optimized SQL)
2. Try 15+ config options via EXPLAIN (cost-only, no execution)
3. Pick best config based on plan cost reduction
4. Race: optimized vs config-tuned simultaneously
5. Record results

Usage:
    cd QueryTorque_V8
    python3 research/config_tuning_batch.py
"""

import json
import os
import re
import sys
import time
import threading
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

# ── Paths ──
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BENCH_DIR = PROJECT_ROOT / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb"
QUERIES_DIR = BENCH_DIR / "queries"
BEST_DIR = BENCH_DIR / "best"
OUTPUT_DIR = PROJECT_ROOT / "research/config_tuning_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DSN = "host=127.0.0.1 port=5434 dbname=dsb_sf10 user=jakc9 password=jakc9"

# ── Batch: 20 queries from the middle of the leaderboard ──
# Criteria: runtime > 500ms, have optimized SQL, spans WIN→NEUTRAL range
BATCH = [
    # WINs with room for config improvement
    "query059_multi",     # 4.12x, 6616ms, config neutral
    "query072_spj_spj",   # 3.64x, 1300ms, NO_BOTTLENECK
    "query101_spj_spj",   # 2.91x, 40441ms, NO_BOTTLENECK
    "query065_multi",     # 1.93x, 3250ms, config neutral
    "query087_multi",     # 1.44x, 6124ms, config neutral
    "query102_spj_spj",   # 1.36x, 11290ms, NO_BOTTLENECK
    "query069_multi",     # 1.33x, 1564ms, config neutral
    "query084_agg",       # 1.14x, 729ms, NO_BOTTLENECK
    # IMPROVED
    "query027_spj_spj",   # 1.08x, 5853ms, NO_BOTTLENECK
    "query050_spj_spj",   # 1.08x, 7927ms, NO_BOTTLENECK
    "query023_multi",     # 1.07x, 9229ms, config neutral
    "query064_multi",     # 1.06x, 28671ms, config regression
    # NEUTRAL with decent runtimes
    "query018_spj_spj",   # 1.04x, 4558ms
    "query091_spj_spj",   # 1.02x, 1729ms
    "query030_multi",     # 1.02x, 1174ms
    "query091_agg",       # 1.01x, 1782ms
    "query018_agg",       # 1.01x, 4236ms
    "query072_agg",       # 1.00x, 4802ms
    "query094_multi",     # 1.00x, 1651ms
    "query040_spj_spj",   # 0.99x, 1188ms
]


# ── Config options to try ──
# Each is a dict of SET LOCAL params, named for readability
CONFIG_OPTIONS = {
    # Memory configs
    "wm_128": {"work_mem": "128MB"},
    "wm_256": {"work_mem": "256MB"},
    "wm_512": {"work_mem": "512MB"},
    "wm_1g":  {"work_mem": "1GB"},
    # JIT
    "jit_off": {"jit": "off"},
    # Parallelism
    "par_4":   {"max_parallel_workers_per_gather": "4"},
    "par_low": {"parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"},
    # Index preference
    "ssd":     {"random_page_cost": "1.1"},
    # Cache
    "cache48": {"effective_cache_size": "48GB"},
    # Join tuning
    "no_nest": {"enable_nestloop": "off"},
    "jcl_12":  {"join_collapse_limit": "12"},
    "hmm_4":   {"hash_mem_multiplier": "4.0"},
    # Combos
    "combo_mem_jit":  {"work_mem": "512MB", "jit": "off"},
    "combo_mem_par":  {"work_mem": "256MB", "max_parallel_workers_per_gather": "4",
                       "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"},
    "combo_full":     {"work_mem": "512MB", "jit": "off",
                       "max_parallel_workers_per_gather": "4",
                       "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"},
    "combo_cache_ssd": {"effective_cache_size": "48GB", "random_page_cost": "1.1"},
    "combo_mem_nest":  {"work_mem": "512MB", "enable_nestloop": "off"},
    "combo_mem_jcl":   {"work_mem": "256MB", "join_collapse_limit": "12"},
    "combo_kitchen":   {"work_mem": "512MB", "jit": "off",
                        "effective_cache_size": "48GB", "random_page_cost": "1.1",
                        "max_parallel_workers_per_gather": "4",
                        "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"},
}


def get_conn():
    """Create a new database connection."""
    return psycopg2.connect(DSN)


def run_explain(conn, sql: str, analyze: bool = False,
                config: Optional[Dict[str, str]] = None,
                hint: Optional[str] = None,
                timeout_ms: int = 300000) -> Tuple[str, float]:
    """Run EXPLAIN [ANALYZE] with optional SET LOCAL config and hints.

    Returns (plan_text, total_cost). total_cost is from the top node.
    """
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
        if config:
            for k, v in config.items():
                cur.execute(f"SET LOCAL {k} = %s", (v,))

        explain_cmd = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)" if analyze else "EXPLAIN (FORMAT TEXT)"

        if hint:
            full_sql = f"/*+ {hint} */\n{sql}"
        else:
            full_sql = sql

        cur.execute(f"{explain_cmd} {full_sql}")
        rows = cur.fetchall()
        plan_text = "\n".join(r[0] for r in rows)

    conn.rollback()  # ROLLBACK to release SET LOCAL

    # Extract top-level cost
    cost_match = re.search(r'cost=[\d.]+\.\.([\d.]+)', plan_text)
    total_cost = float(cost_match.group(1)) if cost_match else 0.0

    return plan_text, total_cost


def extract_plan_features(plan: str) -> Dict[str, Any]:
    """Extract key features from EXPLAIN plan text."""
    features = {}

    # Execution time (only from ANALYZE)
    exec_match = re.search(r'Execution Time:\s*([\d.]+)\s*ms', plan)
    features["exec_ms"] = float(exec_match.group(1)) if exec_match else None

    # Planning time
    plan_match = re.search(r'Planning Time:\s*([\d.]+)\s*ms', plan)
    features["plan_ms"] = float(plan_match.group(1)) if plan_match else None

    # Top-level cost
    cost_match = re.search(r'cost=[\d.]+\.\.([\d.]+)', plan)
    features["total_cost"] = float(cost_match.group(1)) if cost_match else 0

    # JIT
    features["has_jit"] = "JIT:" in plan
    jit_time = re.search(r'JIT:.*?Time:\s*([\d.]+)\s*ms', plan, re.DOTALL)
    features["jit_ms"] = float(jit_time.group(1)) if jit_time else 0

    # Parallel
    features["has_parallel"] = "Gather" in plan or "Parallel" in plan

    # Hash spills
    batch_matches = re.findall(r'Batches:\s*(\d+)', plan)
    features["max_batches"] = max((int(b) for b in batch_matches), default=1)
    features["has_spill"] = features["max_batches"] > 1

    # Memory
    mem_matches = re.findall(r'(?:Peak\s+)?Memory(?:\s+Usage)?:\s*(\d+)kB', plan, re.IGNORECASE)
    features["peak_mem_kb"] = max((int(m) for m in mem_matches), default=0)

    # Sort method
    features["has_disk_sort"] = "Sort Method: external" in plan

    # Nested loops
    nl_rows = re.findall(r'Nested Loop.*?rows=(\d+)', plan, re.IGNORECASE | re.DOTALL)
    features["max_nl_rows"] = max((int(r) for r in nl_rows), default=0)

    # Join count
    features["join_count"] = len(re.findall(
        r'(?:Hash|Merge|Nested Loop)\s+(?:Left |Right |Full |Semi |Anti )?Join',
        plan, re.IGNORECASE
    ))

    # Seq scans
    seq_scans = re.findall(r'Seq Scan on (\w+).*?rows=(\d+)', plan, re.IGNORECASE | re.DOTALL)
    features["large_seq_scans"] = [(t, int(r)) for t, r in seq_scans if int(r) > 100000]

    # Buffers
    shared_read = re.findall(r'shared read=(\d+)', plan)
    features["total_shared_read"] = sum(int(r) for r in shared_read)
    temp_read = re.findall(r'temp read=(\d+)', plan)
    features["total_temp_read"] = sum(int(r) for r in temp_read)

    return features


def generate_hint_candidates(plan: str) -> List[Tuple[str, str]]:
    """Generate pg_hint_plan candidates based on plan analysis.

    Returns list of (hint_name, hint_text) tuples.
    """
    hints = []

    # Find nested loops with high rows — try forcing hash join
    nl_joins = re.findall(
        r'Nested Loop.*?(?:rows=(\d+)).*?\n\s+->.*?(?:Scan|Join) (?:on |using )?(\w+).*?\n\s+->.*?(?:Scan|Join) (?:on |using )?(\w+)',
        plan, re.IGNORECASE | re.DOTALL
    )
    for rows, t1, t2 in nl_joins:
        if int(rows) > 1000:
            hints.append(
                (f"hash_{t1}_{t2}", f"HashJoin({t1} {t2})")
            )

    # Find hash joins — try forcing merge join
    hash_joins = re.findall(
        r'Hash Join.*?\n.*?Hash Cond:.*?(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
        plan, re.IGNORECASE | re.DOTALL
    )
    for _, _, t2_alias, _ in hash_joins[:3]:  # limit to 3
        hints.append(
            (f"merge_{t2_alias}", f"MergeJoin({t2_alias})")
        )

    # Find seq scans on tables that might have indexes
    seq_scans = re.findall(r'Seq Scan on (\w+)', plan, re.IGNORECASE)
    fact_tables = {"store_sales", "catalog_sales", "web_sales",
                   "store_returns", "catalog_returns", "web_returns", "inventory"}
    for table in set(seq_scans):
        if table.lower() in fact_tables:
            hints.append(
                (f"idx_{table}", f"IndexScan({table})")
            )
            hints.append(
                (f"bitmapidx_{table}", f"BitmapScan({table})")
            )

    # If many nested loops, try disabling them globally via hint
    nl_count = plan.count("Nested Loop")
    if nl_count >= 3:
        hints.append(("no_nestloop_hint", "Set(enable_nestloop off)"))

    # Parallelism hints
    if "Gather" not in plan and "Parallel" not in plan:
        hints.append(("force_parallel", "Set(max_parallel_workers_per_gather 4) Set(parallel_setup_cost 100) Set(parallel_tuple_cost 0.001)"))

    return hints[:8]  # Cap at 8 hint options


def race_queries(
    sql_a: str,
    sql_b: str,
    config_b: Optional[Dict[str, str]] = None,
    hint_b: Optional[str] = None,
) -> Dict[str, Any]:
    """Run two queries simultaneously on separate connections.

    sql_a = plain optimized query
    sql_b = same query with config/hints applied

    Returns timing for both.
    """
    results = {"a_ms": None, "b_ms": None, "a_error": None, "b_error": None}
    barrier = threading.Barrier(2, timeout=600)

    def run_a():
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '300000'")
                barrier.wait()
                t0 = time.perf_counter()
                cur.execute(sql_a)
                cur.fetchall()
                results["a_ms"] = (time.perf_counter() - t0) * 1000
            conn.rollback()
            conn.close()
        except Exception as e:
            results["a_error"] = str(e)

    def run_b():
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '300000'")
                if config_b:
                    for k, v in config_b.items():
                        cur.execute(f"SET LOCAL {k} = %s", (v,))
                barrier.wait()
                t0 = time.perf_counter()
                if hint_b:
                    cur.execute(f"/*+ {hint_b} */\n{sql_b}")
                else:
                    cur.execute(sql_b)
                cur.fetchall()
                results["b_ms"] = (time.perf_counter() - t0) * 1000
            conn.rollback()
            conn.close()
        except Exception as e:
            results["b_error"] = str(e)

    t_a = threading.Thread(target=run_a)
    t_b = threading.Thread(target=run_b)
    t_a.start()
    t_b.start()
    t_a.join(timeout=600)
    t_b.join(timeout=600)

    return results


def tune_query(query_id: str) -> Dict[str, Any]:
    """Full tuning pipeline for one query.

    1. EXPLAIN ANALYZE baseline
    2. Try 15+ configs via EXPLAIN (cost-only)
    3. Pick best config
    4. Race optimized vs config-tuned
    """
    print(f"\n{'='*70}")
    print(f"TUNING: {query_id}")
    print(f"{'='*70}")

    # Load SQL
    orig_path = QUERIES_DIR / f"{query_id}.sql"
    opt_path = BEST_DIR / f"{query_id}.sql"

    if not orig_path.exists() or not opt_path.exists():
        print(f"  SKIP: missing SQL files")
        return {"query_id": query_id, "status": "SKIP", "reason": "missing SQL"}

    orig_sql = orig_path.read_text().strip()
    opt_sql = opt_path.read_text().strip()

    result = {
        "query_id": query_id,
        "timestamp": datetime.now().isoformat(),
    }

    conn = get_conn()

    # ── Step 1: EXPLAIN ANALYZE baseline (optimized SQL) ──
    print(f"\n  Step 1: EXPLAIN ANALYZE baseline...")
    try:
        baseline_plan, baseline_cost = run_explain(conn, opt_sql, analyze=True)
        baseline_features = extract_plan_features(baseline_plan)
        result["baseline"] = {
            "cost": baseline_cost,
            "exec_ms": baseline_features["exec_ms"],
            "plan_ms": baseline_features["plan_ms"],
            "has_jit": baseline_features["has_jit"],
            "jit_ms": baseline_features["jit_ms"],
            "has_parallel": baseline_features["has_parallel"],
            "has_spill": baseline_features["has_spill"],
            "max_batches": baseline_features["max_batches"],
            "peak_mem_kb": baseline_features["peak_mem_kb"],
            "has_disk_sort": baseline_features["has_disk_sort"],
            "max_nl_rows": baseline_features["max_nl_rows"],
            "join_count": baseline_features["join_count"],
            "large_seq_scans": [(t, r) for t, r in baseline_features["large_seq_scans"]],
            "total_shared_read": baseline_features["total_shared_read"],
            "total_temp_read": baseline_features["total_temp_read"],
        }
        exec_ms = baseline_features["exec_ms"]
        print(f"    Exec: {exec_ms:.1f}ms | Cost: {baseline_cost:.0f}")
        print(f"    JIT: {'YES (' + str(baseline_features['jit_ms']) + 'ms)' if baseline_features['has_jit'] else 'no'}")
        print(f"    Parallel: {'YES' if baseline_features['has_parallel'] else 'no'}")
        print(f"    Spill: {'YES (' + str(baseline_features['max_batches']) + ' batches)' if baseline_features['has_spill'] else 'no'}")
        print(f"    NL max rows: {baseline_features['max_nl_rows']:,}")
        print(f"    Joins: {baseline_features['join_count']}")
        if baseline_features['large_seq_scans']:
            print(f"    Large seq scans: {baseline_features['large_seq_scans']}")
        if baseline_features['total_temp_read']:
            print(f"    Temp read blocks: {baseline_features['total_temp_read']:,}")
    except Exception as e:
        print(f"    ERROR: {e}")
        result["status"] = "BASELINE_ERROR"
        result["error"] = str(e)
        conn.close()
        return result

    # ── Step 2: Try config options (EXPLAIN only, no execution) ──
    print(f"\n  Step 2: Testing {len(CONFIG_OPTIONS)} config options...")
    config_results = []

    for name, config in CONFIG_OPTIONS.items():
        try:
            plan, cost = run_explain(conn, opt_sql, analyze=False, config=config)
            cost_ratio = cost / baseline_cost if baseline_cost > 0 else 1.0
            features = extract_plan_features(plan)

            entry = {
                "name": name,
                "config": config,
                "cost": cost,
                "cost_ratio": round(cost_ratio, 4),
                "has_parallel": features["has_parallel"],
                "join_count": features["join_count"],
            }
            config_results.append(entry)

            marker = ""
            if cost_ratio < 0.90:
                marker = " ★★★"
            elif cost_ratio < 0.95:
                marker = " ★★"
            elif cost_ratio < 0.99:
                marker = " ★"
            elif cost_ratio > 1.05:
                marker = " ⚠"

            print(f"    {name:20s} cost={cost:>12.0f} ratio={cost_ratio:.4f}{marker}")
        except Exception as e:
            print(f"    {name:20s} ERROR: {e}")
            config_results.append({"name": name, "config": config, "error": str(e)})

    # ── Step 3: Generate and test pg_hint_plan candidates ──
    print(f"\n  Step 3: Testing pg_hint_plan candidates...")
    hint_candidates = generate_hint_candidates(baseline_plan)
    hint_results = []

    for hint_name, hint_text in hint_candidates:
        try:
            plan, cost = run_explain(conn, opt_sql, analyze=False, hint=hint_text)
            cost_ratio = cost / baseline_cost if baseline_cost > 0 else 1.0
            features = extract_plan_features(plan)

            entry = {
                "name": hint_name,
                "hint": hint_text,
                "cost": cost,
                "cost_ratio": round(cost_ratio, 4),
            }
            hint_results.append(entry)

            marker = ""
            if cost_ratio < 0.90:
                marker = " ★★★"
            elif cost_ratio < 0.95:
                marker = " ★★"
            elif cost_ratio < 0.99:
                marker = " ★"

            print(f"    {hint_name:25s} cost={cost:>12.0f} ratio={cost_ratio:.4f}{marker}")
        except Exception as e:
            print(f"    {hint_name:25s} ERROR: {e}")

    # ── Step 3b: Test best hint + best config combo ──
    # Find best config and best hint
    valid_configs = [c for c in config_results if "error" not in c]
    valid_hints = [h for h in hint_results if "error" not in h]

    best_config = min(valid_configs, key=lambda x: x["cost_ratio"]) if valid_configs else None
    best_hint = min(valid_hints, key=lambda x: x["cost_ratio"]) if valid_hints else None

    combo_results = []
    if best_config and best_hint and best_hint["cost_ratio"] < 0.99:
        try:
            combo_name = f"{best_config['name']}+{best_hint['name']}"
            plan, cost = run_explain(
                conn, opt_sql, analyze=False,
                config=best_config["config"],
                hint=best_hint["hint"]
            )
            cost_ratio = cost / baseline_cost if baseline_cost > 0 else 1.0
            combo_results.append({
                "name": combo_name,
                "config": best_config["config"],
                "hint": best_hint["hint"],
                "cost": cost,
                "cost_ratio": round(cost_ratio, 4),
            })
            print(f"    COMBO {combo_name}: cost={cost:.0f} ratio={cost_ratio:.4f}")
        except Exception as e:
            print(f"    COMBO ERROR: {e}")

    conn.close()

    # ── Step 4: Pick best overall option ──
    all_options = []
    for c in valid_configs:
        all_options.append({"type": "config", **c})
    for h in valid_hints:
        all_options.append({"type": "hint", **h})
    for combo in combo_results:
        all_options.append({"type": "combo", **combo})

    if not all_options:
        result["status"] = "NO_OPTIONS"
        result["config_results"] = config_results
        result["hint_results"] = hint_results
        return result

    best_option = min(all_options, key=lambda x: x.get("cost_ratio", 1.0))
    result["best_option"] = best_option
    result["config_results"] = config_results
    result["hint_results"] = hint_results
    result["combo_results"] = combo_results
    result["total_options_tried"] = len(config_results) + len(hint_results) + len(combo_results)

    print(f"\n  BEST OPTION: {best_option.get('name', '?')} "
          f"(cost ratio: {best_option.get('cost_ratio', 1.0):.4f})")

    # ── Step 5: Race — optimized vs config-tuned ──
    if best_option.get("cost_ratio", 1.0) >= 1.0:
        print(f"\n  Step 5: SKIP race — no cost improvement found")
        result["status"] = "NO_IMPROVEMENT"
        result["race"] = None
    else:
        print(f"\n  Step 5: Racing optimized vs config-tuned...")
        race_config = best_option.get("config")
        race_hint = best_option.get("hint")

        # Warmup both connections
        try:
            c1 = get_conn()
            with c1.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = '300000'")
                cur.execute(opt_sql)
                cur.fetchall()
            c1.rollback()
            c1.close()
        except:
            pass

        # Race
        race_result = race_queries(opt_sql, opt_sql, config_b=race_config, hint_b=race_hint)

        a_ms = race_result.get("a_ms")
        b_ms = race_result.get("b_ms")
        if a_ms and b_ms:
            gap_ms = a_ms - b_ms
            gap_pct = (gap_ms / a_ms * 100) if a_ms > 0 else 0
            winner = "config" if b_ms < a_ms else "baseline"
            print(f"    Baseline (no config):  {a_ms:.1f}ms")
            print(f"    Config-tuned:          {b_ms:.1f}ms")
            print(f"    Gap: {gap_ms:+.1f}ms ({gap_pct:+.1f}%) → {winner.upper()} WINS")
            race_result["winner"] = winner
            race_result["gap_ms"] = round(gap_ms, 1)
            race_result["gap_pct"] = round(gap_pct, 1)
        else:
            print(f"    Race failed: a_err={race_result.get('a_error')}, b_err={race_result.get('b_error')}")

        result["race"] = race_result
        result["status"] = "RACED"

    return result


def main():
    print(f"Config Tuning Batch — {len(BATCH)} queries")
    print(f"Output: {OUTPUT_DIR}")
    print(f"DB: dsb_sf10 @ 127.0.0.1:5434")
    print(f"Started: {datetime.now().isoformat()}")

    all_results = []
    summary = {"total": 0, "raced": 0, "config_wins": 0,
               "baseline_wins": 0, "no_improvement": 0, "errors": 0}

    for query_id in BATCH:
        try:
            result = tune_query(query_id)
            all_results.append(result)
            summary["total"] += 1

            status = result.get("status", "?")
            if status == "RACED":
                summary["raced"] += 1
                race = result.get("race", {})
                if race.get("winner") == "config":
                    summary["config_wins"] += 1
                elif race.get("winner") == "baseline":
                    summary["baseline_wins"] += 1
            elif status == "NO_IMPROVEMENT":
                summary["no_improvement"] += 1
            elif "ERROR" in status:
                summary["errors"] += 1

            # Save incrementally
            out_path = OUTPUT_DIR / "config_tuning_batch.json"
            out_path.write_text(json.dumps({
                "batch": BATCH,
                "summary": summary,
                "results": all_results,
                "completed_at": datetime.now().isoformat(),
            }, indent=2, default=str))

        except Exception as e:
            print(f"\n  FATAL ERROR for {query_id}: {e}")
            all_results.append({
                "query_id": query_id,
                "status": "FATAL_ERROR",
                "error": str(e),
            })
            summary["errors"] += 1

    # Final summary
    print(f"\n\n{'='*70}")
    print(f"BATCH COMPLETE")
    print(f"{'='*70}")
    print(f"Total: {summary['total']}")
    print(f"Raced: {summary['raced']}")
    print(f"Config wins: {summary['config_wins']}")
    print(f"Baseline wins: {summary['baseline_wins']}")
    print(f"No improvement: {summary['no_improvement']}")
    print(f"Errors: {summary['errors']}")

    # Final save
    out_path = OUTPUT_DIR / "config_tuning_batch.json"
    out_path.write_text(json.dumps({
        "batch": BATCH,
        "summary": summary,
        "results": all_results,
        "completed_at": datetime.now().isoformat(),
    }, indent=2, default=str))
    print(f"\nResults: {out_path}")


if __name__ == "__main__":
    main()
