#!/usr/bin/env python3
"""Config Tuning Pass 3 — EXPLAIN-driven analysis + pg_hint_plan on stubborn queries.

Key learnings from Pass 1+2:
- 5 queries confirmed config wins (parallelism, join_collapse, random_page_cost)
- 5 queries completely resist config tuning — need plan shape changes

Strategy:
1. EXPLAIN ANALYZE stubborn queries to identify bottleneck operators
2. Generate targeted pg_hint_plan hints based on plan structure
3. Try plan shape changes (HashJoin↔MergeJoin↔NestLoop, scan types)
4. For confirmed winners: find MINIMAL config that preserves gain
5. Also test remaining untested queries from the original 20

Usage:
    cd QueryTorque_V8
    python3 research/config_tuning_pass3.py
"""

import json
import re
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


def run_explain(conn, sql, analyze=True, config=None, hint=None, timeout_ms=300000):
    """Run EXPLAIN (optionally ANALYZE) with optional SET LOCAL config and pg_hint_plan hint."""
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
    cost_m = re.search(r'cost=[\d.]+\.\.([.\d]+)', plan)
    exec_m = re.search(r'Execution Time:\s*([\d.]+)', plan)
    return plan, float(cost_m.group(1)) if cost_m else 0, float(exec_m.group(1)) if exec_m else None


def extract_plan_features(plan_text):
    """Extract key features from EXPLAIN plan for hint generation."""
    features = {
        "has_jit": "JIT:" in plan_text,
        "has_parallel": "Parallel" in plan_text or "Gather" in plan_text,
        "has_seq_scan": "Seq Scan" in plan_text,
        "has_index_scan": "Index Scan" in plan_text or "Index Only Scan" in plan_text,
        "has_bitmap_scan": "Bitmap" in plan_text,
        "has_hash_join": "Hash Join" in plan_text,
        "has_merge_join": "Merge Join" in plan_text,
        "has_nested_loop": "Nested Loop" in plan_text,
        "has_sort": "Sort " in plan_text,
        "has_hash_agg": "HashAggregate" in plan_text,
        "has_group_agg": "GroupAggregate" in plan_text,
        "has_materialize": "Materialize" in plan_text,
        "has_subplan": "SubPlan" in plan_text,
        "has_cte_scan": "CTE Scan" in plan_text,
    }

    # Extract table names from scans
    tables = set()
    for m in re.finditer(r'(?:Seq Scan|Index (?:Only )?Scan|Bitmap Heap Scan) on (\w+)', plan_text):
        tables.add(m.group(1))
    features["tables"] = sorted(tables)

    # Extract join pairs (table aliases involved in joins)
    join_nodes = []
    for m in re.finditer(r'(Hash Join|Merge Join|Nested Loop).*?(?:Hash Cond|Merge Cond|Join Filter):\s*\((\w+)\..*?=\s*(\w+)\.', plan_text, re.DOTALL):
        join_nodes.append({"type": m.group(1), "left": m.group(2), "right": m.group(3)})
    features["joins"] = join_nodes

    # Extract seq scan rows (for parallelism hint)
    seq_scans = []
    for m in re.finditer(r'Seq Scan on (\w+).*?rows=(\d+)', plan_text):
        seq_scans.append({"table": m.group(1), "rows": int(m.group(2))})
    features["seq_scans"] = seq_scans

    # Detect spills
    features["has_disk_spill"] = "Batches:" in plan_text and re.search(r'Batches:\s*(\d+)', plan_text) and int(re.search(r'Batches:\s*(\d+)', plan_text).group(1)) > 1

    # Extract execution time
    exec_m = re.search(r'Execution Time:\s*([\d.]+)', plan_text)
    features["exec_time_ms"] = float(exec_m.group(1)) if exec_m else None

    # Count join nodes
    features["join_count"] = len(re.findall(r'(?:Hash Join|Merge Join|Nested Loop)', plan_text))

    return features


def generate_hint_candidates(features, plan_text):
    """Generate pg_hint_plan hint candidates based on EXPLAIN analysis."""
    candidates = []

    # 1. Swap join methods on each join pair
    for j in features.get("joins", []):
        left, right = j["left"], j["right"]
        current = j["type"]
        alternatives = {
            "Hash Join": ["MergeJoin", "NestLoop"],
            "Merge Join": ["HashJoin", "NestLoop"],
            "Nested Loop": ["HashJoin", "MergeJoin"],
        }
        for alt in alternatives.get(current, []):
            candidates.append({
                "name": f"{alt}_{left}_{right}",
                "hint": f"{alt}({left} {right})",
                "reason": f"swap {current}→{alt} on {left}⋈{right}",
            })

    # 2. Force index scan on large seq scans
    for ss in features.get("seq_scans", []):
        if ss["rows"] > 100000:
            candidates.append({
                "name": f"IndexScan_{ss['table']}",
                "hint": f"IndexScan({ss['table']})",
                "reason": f"force index on {ss['table']} ({ss['rows']} rows seq scan)",
            })
            # Also try bitmap
            candidates.append({
                "name": f"BitmapScan_{ss['table']}",
                "hint": f"BitmapScan({ss['table']})",
                "reason": f"force bitmap on {ss['table']} ({ss['rows']} rows seq scan)",
            })

    # 3. Force seq scan on small index scans (if any table is being index scanned needlessly)
    for m in re.finditer(r'Index (?:Only )?Scan.*? on (\w+).*?rows=(\d+)', plan_text):
        table, rows = m.group(1), int(m.group(2))
        if rows > 50000:  # large index scan → maybe seq scan + parallel is better
            candidates.append({
                "name": f"SeqScan_{table}",
                "hint": f"SeqScan({table})",
                "reason": f"force seq on {table} ({rows} rows index scan → try parallel)",
            })

    # 4. Disable/enable specific features
    if features.get("has_materialize"):
        candidates.append({
            "name": "NoMaterialize",
            "hint": "Set(enable_material off)",
            "reason": "disable materialize nodes",
        })

    if features.get("has_hash_agg"):
        candidates.append({
            "name": "GroupAgg",
            "hint": "Set(enable_hashagg off)",
            "reason": "force group agg instead of hash agg",
        })

    if features.get("has_sort"):
        candidates.append({
            "name": "NoSort",
            "hint": "Set(enable_sort off)",
            "reason": "disable sort (force hash-based operations)",
        })

    # 5. Leading hint — try different join orderings for complex queries
    tables = features.get("tables", [])
    if len(tables) >= 4:
        # Try reversed order
        rev = " ".join(reversed(tables[:6]))
        candidates.append({
            "name": "Leading_rev",
            "hint": f"Leading(({rev}))",
            "reason": f"reversed join order: {rev}",
        })

    return candidates


def race(sql_a, sql_b, config_b=None, hint_b=None, timeout_ms=300000):
    """Race two queries: A (plain opt) vs B (opt + config/hint).
    Both wait at barrier for synchronized start. Both run to completion."""
    results = {"a_ms": None, "b_ms": None}
    barrier = threading.Barrier(2, timeout=600)

    def run_a():
        try:
            c = get_conn()
            with c.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
                barrier.wait()
                t0 = time.perf_counter()
                cur.execute(sql_a)
                cur.fetchall()
                results["a_ms"] = (time.perf_counter() - t0) * 1000
            c.rollback(); c.close()
        except Exception as e:
            results["a_error"] = str(e)

    def run_b():
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
                    cur.execute(f"/*+ {hint_b} */\n{sql_a}")
                else:
                    cur.execute(sql_a)
                cur.fetchall()
                results["b_ms"] = (time.perf_counter() - t0) * 1000
            c.rollback(); c.close()
        except Exception as e:
            results["b_error"] = str(e)

    ta = threading.Thread(target=run_a)
    tb = threading.Thread(target=run_b)
    ta.start(); tb.start()
    ta.join(600); tb.join(600)
    return results


# ── Test groups ──

# Group A: STUBBORN QUERIES — need EXPLAIN + hints
STUBBORN_QUERIES = [
    "query059_multi",
    "query064_multi",
    "query101_spj_spj",
    "query018_spj_spj",
    "query072_agg",
]

# Group B: CONFIRMED WINNERS — find minimal config
WINNER_CONFIGS = {
    "query102_spj_spj": [
        ("rpc_only", {"random_page_cost": "1.1"}, None),
        ("rpc_wm", {"random_page_cost": "1.1", "work_mem": "256MB"}, None),
        ("rpc_par", {"random_page_cost": "1.1", "max_parallel_workers_per_gather": "4",
                     "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}, None),
    ],
    "query087_multi": [
        ("par_only", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
        ("wm256_only", {"work_mem": "256MB"}, None),
        ("par2", {"max_parallel_workers_per_gather": "2"}, None),
    ],
    "query091_spj_spj": [
        ("par_only", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
        ("par2", {"max_parallel_workers_per_gather": "2"}, None),
        ("par_no_cost", {"max_parallel_workers_per_gather": "4"}, None),
    ],
    "query072_spj_spj": [
        ("jcl_12_only", {"join_collapse_limit": "12"}, None),
        ("par_only", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
        ("jcl_12_par2", {"join_collapse_limit": "12", "max_parallel_workers_per_gather": "2"}, None),
    ],
    "query023_multi": [
        ("wm512_jit", {"work_mem": "512MB", "jit": "off"}, None),
        ("par_only", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                      "parallel_tuple_cost": "0.001"}, None),
        ("wm256_par", {"work_mem": "256MB", "max_parallel_workers_per_gather": "4"}, None),
    ],
}

# Group C: UNTESTED QUERIES from original 20 not yet explored deeply
UNTESTED_QUERIES = [
    "query030_multi",
    "query091_agg",
    "query018_agg",
    "query094_multi",
    "query040_spj_spj",
    "query065_multi",
    "query069_multi",
    "query084_agg",
    "query027_spj_spj",
    "query050_spj_spj",
]

# Standard configs to try on untested queries
STANDARD_CONFIGS = [
    ("par4", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
              "parallel_tuple_cost": "0.001"}),
    ("wm256_par", {"work_mem": "256MB", "max_parallel_workers_per_gather": "4",
                   "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
    ("jcl_12", {"join_collapse_limit": "12"}),
    ("wm512_jit_off", {"work_mem": "512MB", "jit": "off"}),
    ("rpc_par", {"random_page_cost": "1.1", "max_parallel_workers_per_gather": "4",
                 "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
]


def analyze_stubborn(qid, opt_sql):
    """Deep EXPLAIN analysis + pg_hint_plan exploration for stubborn queries."""
    print(f"\n{'='*70}")
    print(f"STUBBORN: {qid}")
    print(f"{'='*70}")

    # Get baseline EXPLAIN ANALYZE
    conn = get_conn()
    plan, cost, exec_ms = run_explain(conn, opt_sql, analyze=True)
    conn.close()

    features = extract_plan_features(plan)
    print(f"  Baseline: {exec_ms:.1f}ms | cost={cost:.0f}")
    print(f"  Features: JIT={features['has_jit']} parallel={features['has_parallel']} "
          f"joins={features['join_count']} tables={len(features['tables'])}")
    print(f"  Tables: {', '.join(features['tables'][:10])}")

    if features['joins']:
        join_strs = []
        for j in features['joins'][:6]:
            jtype = j["type"]
            jleft = j["left"]
            jright = j["right"]
            join_strs.append(f"{jtype}({jleft}⋈{jright})")
        print(f"  Joins: {' | '.join(join_strs)}")

    # Generate hint candidates
    hint_candidates = generate_hint_candidates(features, plan)
    print(f"  Generated {len(hint_candidates)} hint candidates")

    results = {"query_id": qid, "baseline_ms": exec_ms, "tests": []}

    # Phase 1: EXPLAIN-only screening of all candidates (fast — no ANALYZE)
    promising = []
    for cand in hint_candidates:
        conn = get_conn()
        try:
            _, hint_cost, _ = run_explain(conn, opt_sql, analyze=False, hint=cand["hint"])
            cost_ratio = hint_cost / cost if cost > 0 else 1.0
            if cost_ratio < 0.95:  # >5% cost reduction
                promising.append({**cand, "cost_ratio": cost_ratio, "hint_cost": hint_cost})
                print(f"    PROMISING: {cand['name']:30s} cost_ratio={cost_ratio:.4f} ({cand['reason']})")
            elif cost_ratio > 2.0:
                print(f"    SKIP:      {cand['name']:30s} cost_ratio={cost_ratio:.4f} (too expensive)")
        except Exception as e:
            print(f"    ERROR:     {cand['name']:30s} {str(e)[:60]}")
        conn.close()

    # Phase 2: EXPLAIN ANALYZE the promising ones
    for cand in promising[:8]:  # limit to top 8
        conn = get_conn()
        try:
            _, _, hint_exec_ms = run_explain(conn, opt_sql, analyze=True, hint=cand["hint"])
            if hint_exec_ms:
                gap_pct = (exec_ms - hint_exec_ms) / exec_ms * 100
                marker = "✓" if gap_pct > 5 else "—" if abs(gap_pct) < 5 else "✗"
                print(f"    {marker} ANALYZE {cand['name']:25s} {hint_exec_ms:.1f}ms (gap={gap_pct:+.1f}%)")
                cand["analyze_ms"] = hint_exec_ms
                cand["gap_pct"] = gap_pct
        except Exception as e:
            print(f"    ? ANALYZE {cand['name']:25s} error: {str(e)[:60]}")
        conn.close()

    # Phase 3: Also try config combos that weren't tested
    extra_configs = [
        ("geqo_off", {"geqo": "off"}),
        ("geqo_10", {"geqo_threshold": "10"}),
        ("hmm8", {"hash_mem_multiplier": "8.0"}),
        ("wm1g_par", {"work_mem": "1GB", "max_parallel_workers_per_gather": "4",
                      "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
        ("merge_pref", {"enable_hashjoin": "off"}),
        ("no_sort", {"enable_sort": "off"}),
        ("materialize_off", {"enable_material": "off"}),
    ]

    for name, cfg in extra_configs:
        conn = get_conn()
        try:
            _, cfg_cost, _ = run_explain(conn, opt_sql, analyze=False, config=cfg)
            cost_ratio = cfg_cost / cost if cost > 0 else 1.0
            if abs(cost_ratio - 1.0) > 0.03:  # plan actually changes
                _, _, cfg_exec = run_explain(conn, opt_sql, analyze=True, config=cfg)
                if cfg_exec:
                    gap_pct = (exec_ms - cfg_exec) / exec_ms * 100
                    marker = "✓" if gap_pct > 5 else "—" if abs(gap_pct) < 5 else "✗"
                    print(f"    {marker} CONFIG  {name:25s} {cfg_exec:.1f}ms (gap={gap_pct:+.1f}%, cost_r={cost_ratio:.4f})")
                    results["tests"].append({
                        "name": name, "type": "config", "config": cfg,
                        "exec_ms": cfg_exec, "gap_pct": round(gap_pct, 1),
                        "cost_ratio": round(cost_ratio, 4),
                    })
        except Exception as e:
            print(f"    ? CONFIG  {name:25s} error: {str(e)[:60]}")
        conn.close()

    # Phase 4: Race the best hint candidate
    best_hint = None
    for cand in promising:
        if cand.get("gap_pct", 0) > 5:
            if not best_hint or cand["gap_pct"] > best_hint["gap_pct"]:
                best_hint = cand

    if best_hint:
        print(f"\n  RACE: {best_hint['name']} (expected {best_hint['gap_pct']:+.1f}%)")
        r = race(opt_sql, opt_sql, hint_b=best_hint["hint"])
        a_ms, b_ms = r.get("a_ms"), r.get("b_ms")
        if a_ms and b_ms:
            gap_pct = (a_ms - b_ms) / a_ms * 100
            winner = "hint" if gap_pct > 2 else "baseline" if gap_pct < -2 else "tie"
            print(f"  RACE RESULT: plain={a_ms:.1f}ms hint={b_ms:.1f}ms gap={gap_pct:+.1f}% → {winner}")
            results["race"] = {
                "hint": best_hint["hint"], "name": best_hint["name"],
                "plain_ms": round(a_ms, 1), "hint_ms": round(b_ms, 1),
                "gap_pct": round(gap_pct, 1), "winner": winner,
            }
    else:
        print(f"\n  NO promising hint found for {qid}")

    return results


def validate_winner(qid, opt_sql, configs):
    """Find minimal config that preserves the gain for confirmed winners."""
    print(f"\n{'='*70}")
    print(f"WINNER MINIMAL: {qid}")
    print(f"{'='*70}")

    results = {"query_id": qid, "tests": []}

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

    for name, config, hint in configs:
        r = race(opt_sql, opt_sql, config_b=config, hint_b=hint)
        a_ms, b_ms = r.get("a_ms"), r.get("b_ms")

        if a_ms and b_ms:
            gap_pct = (a_ms - b_ms) / a_ms * 100
            marker = "✓" if gap_pct > 2 else "—" if abs(gap_pct) < 2 else "✗"
            print(f"  {marker} {name:25s} plain={a_ms:.1f}ms tuned={b_ms:.1f}ms gap={gap_pct:+.1f}%")
            results["tests"].append({
                "name": name, "config": config, "hint": hint,
                "plain_ms": round(a_ms, 1), "tuned_ms": round(b_ms, 1),
                "gap_pct": round(gap_pct, 1),
            })
        else:
            print(f"  ? {name:25s} race error: {r.get('a_error','?')}/{r.get('b_error','?')}")

    return results


def screen_untested(qid, opt_sql, configs):
    """Quick screening of untested queries with standard configs."""
    print(f"\n{'='*70}")
    print(f"SCREEN: {qid}")
    print(f"{'='*70}")

    results = {"query_id": qid, "tests": []}

    # Get baseline EXPLAIN ANALYZE
    conn = get_conn()
    plan, cost, exec_ms = run_explain(conn, opt_sql, analyze=True)
    conn.close()

    features = extract_plan_features(plan)
    print(f"  Baseline: {exec_ms:.1f}ms | joins={features['join_count']} "
          f"tables={len(features['tables'])} parallel={features['has_parallel']}")

    results["baseline_ms"] = exec_ms
    results["features"] = {
        "join_count": features["join_count"],
        "has_parallel": features["has_parallel"],
        "has_jit": features["has_jit"],
        "table_count": len(features["tables"]),
    }

    # EXPLAIN-only cost screen first
    for name, cfg in configs:
        conn = get_conn()
        try:
            _, cfg_cost, _ = run_explain(conn, opt_sql, analyze=False, config=cfg)
            cost_ratio = cfg_cost / cost if cost > 0 else 1.0

            # Only race if cost changes significantly
            if abs(cost_ratio - 1.0) > 0.02:
                r = race(opt_sql, opt_sql, config_b=cfg)
                a_ms, b_ms = r.get("a_ms"), r.get("b_ms")
                if a_ms and b_ms:
                    gap_pct = (a_ms - b_ms) / a_ms * 100
                    marker = "✓" if gap_pct > 2 else "—" if abs(gap_pct) < 2 else "✗"
                    print(f"  {marker} {name:20s} plain={a_ms:.1f}ms tuned={b_ms:.1f}ms gap={gap_pct:+.1f}% cost_r={cost_ratio:.4f}")
                    results["tests"].append({
                        "name": name, "config": cfg,
                        "plain_ms": round(a_ms, 1), "tuned_ms": round(b_ms, 1),
                        "gap_pct": round(gap_pct, 1), "cost_ratio": round(cost_ratio, 4),
                    })
                else:
                    print(f"  ? {name:20s} race error")
            else:
                print(f"  — {name:20s} (cost unchanged, skip)")
        except Exception as e:
            print(f"  ? {name:20s} error: {str(e)[:60]}")
        conn.close()

    # Also generate and test hint candidates for untested queries
    hint_candidates = generate_hint_candidates(features, plan)
    promising_hints = []
    for cand in hint_candidates[:10]:
        conn = get_conn()
        try:
            _, hint_cost, _ = run_explain(conn, opt_sql, analyze=False, hint=cand["hint"])
            cost_ratio = hint_cost / cost if cost > 0 else 1.0
            if cost_ratio < 0.90:  # >10% cost reduction
                promising_hints.append({**cand, "cost_ratio": cost_ratio})
        except:
            pass
        conn.close()

    # Race top 3 promising hints
    for cand in promising_hints[:3]:
        r = race(opt_sql, opt_sql, hint_b=cand["hint"])
        a_ms, b_ms = r.get("a_ms"), r.get("b_ms")
        if a_ms and b_ms:
            gap_pct = (a_ms - b_ms) / a_ms * 100
            marker = "✓" if gap_pct > 2 else "—" if abs(gap_pct) < 2 else "✗"
            print(f"  {marker} HINT {cand['name']:15s} plain={a_ms:.1f}ms hint={b_ms:.1f}ms gap={gap_pct:+.1f}% ({cand['reason']})")
            results["tests"].append({
                "name": f"hint_{cand['name']}", "hint": cand["hint"],
                "plain_ms": round(a_ms, 1), "tuned_ms": round(b_ms, 1),
                "gap_pct": round(gap_pct, 1), "cost_ratio": round(cand["cost_ratio"], 4),
            })

    return results


def main():
    print(f"Config Tuning Pass 3 — EXPLAIN + pg_hint_plan deep exploration")
    print(f"Started: {datetime.now().isoformat()}\n")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_results = {"stubborn": [], "winners": [], "untested": []}

    # ── Phase A: Deep analysis of stubborn queries ──
    print(f"\n{'#'*70}")
    print("PHASE A: STUBBORN QUERIES — EXPLAIN + pg_hint_plan")
    print(f"{'#'*70}")

    for qid in STUBBORN_QUERIES:
        opt_path = BEST_DIR / f"{qid}.sql"
        if not opt_path.exists():
            print(f"  SKIP {qid}: no optimized SQL")
            continue
        opt_sql = opt_path.read_text().strip()
        result = analyze_stubborn(qid, opt_sql)
        all_results["stubborn"].append(result)

    # ── Phase B: Minimal config for confirmed winners ──
    print(f"\n{'#'*70}")
    print("PHASE B: WINNERS — MINIMAL CONFIG IDENTIFICATION")
    print(f"{'#'*70}")

    for qid, configs in WINNER_CONFIGS.items():
        opt_path = BEST_DIR / f"{qid}.sql"
        if not opt_path.exists():
            print(f"  SKIP {qid}: no optimized SQL")
            continue
        opt_sql = opt_path.read_text().strip()
        result = validate_winner(qid, opt_sql, configs)
        all_results["winners"].append(result)

    # ── Phase C: Quick screen of untested queries ──
    print(f"\n{'#'*70}")
    print("PHASE C: UNTESTED QUERIES — SCREENING")
    print(f"{'#'*70}")

    for qid in UNTESTED_QUERIES:
        opt_path = BEST_DIR / f"{qid}.sql"
        if not opt_path.exists():
            print(f"  SKIP {qid}: no optimized SQL")
            continue
        opt_sql = opt_path.read_text().strip()
        result = screen_untested(qid, opt_sql, STANDARD_CONFIGS)
        all_results["untested"].append(result)

    # ── Save results ──
    out_path = OUTPUT_DIR / "config_tuning_pass3.json"
    out_path.write_text(json.dumps({
        "results": all_results,
        "completed_at": datetime.now().isoformat(),
    }, indent=2, default=str))

    # ── Summary ──
    print(f"\n\n{'='*70}")
    print("PASS 3 SUMMARY")
    print(f"{'='*70}")

    print("\n  STUBBORN QUERIES:")
    for r in all_results["stubborn"]:
        qid = r["query_id"]
        if r.get("race"):
            race_r = r["race"]
            print(f"    {qid:25s} RACE: {race_r['name']:20s} gap={race_r['gap_pct']:+.1f}% → {race_r['winner']}")
        else:
            tests = r.get("tests", [])
            wins = [t for t in tests if t.get("gap_pct", 0) > 5]
            if wins:
                best = max(wins, key=lambda t: t["gap_pct"])
                print(f"    {qid:25s} CONFIG: {best['name']:20s} gap={best['gap_pct']:+.1f}%")
            else:
                print(f"    {qid:25s} no improvement found")

    print("\n  WINNER MINIMAL CONFIGS:")
    for r in all_results["winners"]:
        qid = r["query_id"]
        tests = r.get("tests", [])
        wins = [t for t in tests if t.get("gap_pct", 0) > 2]
        if wins:
            best = max(wins, key=lambda t: t["gap_pct"])
            print(f"    {qid:25s} MINIMAL: {best['name']:20s} gap={best['gap_pct']:+.1f}%")
        else:
            print(f"    {qid:25s} no minimal config preserves gain")

    print("\n  UNTESTED SCREENING:")
    for r in all_results["untested"]:
        qid = r["query_id"]
        tests = r.get("tests", [])
        wins = [t for t in tests if t.get("gap_pct", 0) > 2]
        if wins:
            best = max(wins, key=lambda t: t["gap_pct"])
            print(f"    {qid:25s} BEST: {best['name']:20s} gap={best['gap_pct']:+.1f}%")
        else:
            print(f"    {qid:25s} no config win")

    print(f"\nResults: {out_path}")


if __name__ == "__main__":
    main()
