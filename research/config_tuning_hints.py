#!/usr/bin/env python3
"""Config Tuning — Dedicated pg_hint_plan Exploration.

Prior findings:
- Hints alone rarely win (PoC: 0/5 queries)
- Hints + config combos can win big (Q102: 2.22x with HashJoin+wm+jit)
- 11 queries resist ALL config tuning — hints are last resort
- EXPLAIN cost is unreliable (r=0.44) — must validate via ANALYZE

Strategy:
1. EXPLAIN ANALYZE each query to get full plan with aliases
2. Parse join pairs with actual aliases (not just table names)
3. Generate systematic hints: swap joins, force scans, Leading, Rows
4. Test via EXPLAIN ANALYZE (wall-clock, not cost)
5. Also test hint + best config combos
6. Race the best candidates

Usage:
    cd QueryTorque_V8
    python3 research/config_tuning_hints.py
"""

import json
import re
import time
import threading
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BENCH_DIR = PROJECT_ROOT / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb"
BEST_DIR = BENCH_DIR / "best"
QUERIES_DIR = BENCH_DIR / "queries"
OUTPUT_DIR = PROJECT_ROOT / "research/config_tuning_results"
DSN = "host=127.0.0.1 port=5434 dbname=dsb_sf10 user=jakc9 password=jakc9"


def get_conn():
    return psycopg2.connect(DSN)


def run_explain_analyze(conn, sql, config=None, hint=None, timeout_ms=300000):
    """Run EXPLAIN ANALYZE and return (plan_text, exec_ms)."""
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
        cur.execute("SET pg_hint_plan.enable_hint = on")
        if config:
            for k, v in config.items():
                cur.execute(f"SET LOCAL {k} = %s", (v,))
        full_sql = f"/*+ {hint} */\n{sql}" if hint else sql
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {full_sql}")
        rows = cur.fetchall()
        plan = "\n".join(r[0] for r in rows)
    conn.rollback()
    exec_m = re.search(r'Execution Time:\s*([\d.]+)', plan)
    return plan, float(exec_m.group(1)) if exec_m else None


def race(sql, config_b=None, hint_b=None, timeout_ms=300000):
    """Race plain vs tuned. Both wait at barrier."""
    results = {"a_ms": None, "b_ms": None}
    barrier = threading.Barrier(2, timeout=600)

    def run_a():
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

    def run_b():
        try:
            c = get_conn()
            with c.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
                cur.execute("SET pg_hint_plan.enable_hint = on")
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

    ta = threading.Thread(target=run_a)
    tb = threading.Thread(target=run_b)
    ta.start(); tb.start()
    ta.join(600); tb.join(600)
    return results


def parse_plan_joins(plan_text):
    """Extract join nodes with aliases from EXPLAIN plan.

    Returns list of dicts: {type, aliases, cond, actual_rows, est_rows}
    """
    joins = []

    # Match join lines with their conditions
    # Pattern: "Join Type (actual rows=N ...)" followed by "Cond: (alias.col = alias.col)"
    lines = plan_text.split('\n')
    for i, line in enumerate(lines):
        # Detect join type
        jm = re.search(r'(Hash Join|Merge Join|Nested Loop)(?:\s+Left|\s+Right|\s+Semi|\s+Anti)?\s*\(', line)
        if not jm:
            continue

        join_type = jm.group(1)

        # Get actual vs estimated rows
        actual_m = re.search(r'actual time=[\d.]+\.\.[\d.]+ rows=(\d+)', line)
        est_m = re.search(r'rows=(\d+)', line)
        actual_rows = int(actual_m.group(1)) if actual_m else None

        # Find join condition in next few lines
        aliases = set()
        for j in range(i+1, min(i+5, len(lines))):
            cond_m = re.search(r'(?:Hash Cond|Merge Cond|Join Filter):\s*\((\w+)\.\w+\s*=\s*(\w+)\.\w+', lines[j])
            if cond_m:
                aliases.add(cond_m.group(1))
                aliases.add(cond_m.group(2))
                break

        joins.append({
            "type": join_type,
            "aliases": sorted(aliases),
            "actual_rows": actual_rows,
            "line": i,
        })

    return joins


def parse_plan_scans(plan_text):
    """Extract scan nodes with table names, aliases, and row counts."""
    scans = []
    lines = plan_text.split('\n')
    for i, line in enumerate(lines):
        # Seq Scan, Index Scan, Bitmap Heap Scan, Index Only Scan
        sm = re.search(
            r'(Seq Scan|Index Scan|Index Only Scan|Bitmap Heap Scan|Parallel Seq Scan|Parallel Index Scan)'
            r'(?:\s+(?:using \w+\s+)?on\s+(\w+))?\s+(\w+)?\s*\(',
            line
        )
        if not sm:
            sm = re.search(
                r'(Seq Scan|Index Scan|Index Only Scan|Bitmap Heap Scan|Parallel Seq Scan)'
                r'\s+on\s+(\w+)\s+(\w+)\s',
                line
            )
        if not sm:
            sm = re.search(
                r'(Seq Scan|Index Scan|Index Only Scan|Bitmap Heap Scan|Parallel Seq Scan)'
                r'\s+on\s+(\w+)\s',
                line
            )
        if sm:
            scan_type = sm.group(1)
            table = sm.group(2) if sm.lastindex >= 2 else None
            alias = sm.group(3) if sm.lastindex >= 3 and sm.group(3) else table

            actual_m = re.search(r'actual time=[\d.]+\.\.[\d.]+ rows=(\d+)', line)
            est_m = re.search(r'rows=(\d+)', line)
            actual_rows = int(actual_m.group(1)) if actual_m else None
            est_rows = int(est_m.group(1)) if est_m else None

            scans.append({
                "type": scan_type,
                "table": table,
                "alias": alias,
                "actual_rows": actual_rows,
                "est_rows": est_rows,
            })

    return scans


def generate_hints(joins, scans, plan_text):
    """Generate comprehensive hint candidates from parsed plan."""
    candidates = []

    # 1. Swap join methods
    alt_joins = {
        "Hash Join": ["MergeJoin", "NestLoop"],
        "Merge Join": ["HashJoin", "NestLoop"],
        "Nested Loop": ["HashJoin", "MergeJoin"],
    }

    for j in joins:
        if len(j["aliases"]) >= 2:
            aliases_str = " ".join(j["aliases"])
            for alt in alt_joins.get(j["type"], []):
                candidates.append({
                    "name": f"{alt}_{'-'.join(j['aliases'][:2])}",
                    "hint": f"{alt}({aliases_str})",
                    "reason": f"swap {j['type']}→{alt} on {aliases_str}",
                    "category": "join_method",
                })

    # 2. Force scan types
    for s in scans:
        if not s.get("table"):
            continue
        alias = s.get("alias") or s["table"]
        actual = s.get("actual_rows", 0) or 0

        if "Seq" in s["type"] and actual > 10000:
            candidates.append({
                "name": f"IdxScan_{alias}",
                "hint": f"IndexScan({alias})",
                "reason": f"force index on {alias} ({actual} rows)",
                "category": "scan_method",
            })
            candidates.append({
                "name": f"BmpScan_{alias}",
                "hint": f"BitmapScan({alias})",
                "reason": f"force bitmap on {alias} ({actual} rows)",
                "category": "scan_method",
            })

        if "Index" in s["type"] and actual > 50000:
            candidates.append({
                "name": f"SeqScan_{alias}",
                "hint": f"SeqScan({alias})",
                "reason": f"force seq on {alias} ({actual} rows) — parallel potential",
                "category": "scan_method",
            })

    # 3. Disable nested loops (common win pattern)
    nl_joins = [j for j in joins if j["type"] == "Nested Loop"]
    if nl_joins:
        # Disable NL on the one with most rows
        for nlj in nl_joins:
            if len(nlj["aliases"]) >= 2:
                aliases_str = " ".join(nlj["aliases"])
                candidates.append({
                    "name": f"NoNL_{'-'.join(nlj['aliases'][:2])}",
                    "hint": f"NoNestLoop({aliases_str})",
                    "reason": f"disable NL on {aliases_str}",
                    "category": "join_method",
                })

    # 4. Force all hash joins (aggressive)
    if any(j["type"] != "Hash Join" for j in joins if len(j["aliases"]) >= 2):
        all_hj = []
        for j in joins:
            if len(j["aliases"]) >= 2 and j["type"] != "Hash Join":
                all_hj.append(f"HashJoin({' '.join(j['aliases'])})")
        if all_hj:
            candidates.append({
                "name": "AllHashJoin",
                "hint": " ".join(all_hj),
                "reason": "force all joins to hash join",
                "category": "join_method",
            })

    # 5. Force all merge joins
    if any(j["type"] != "Merge Join" for j in joins if len(j["aliases"]) >= 2):
        all_mj = []
        for j in joins:
            if len(j["aliases"]) >= 2 and j["type"] != "Merge Join":
                all_mj.append(f"MergeJoin({' '.join(j['aliases'])})")
        if all_mj:
            candidates.append({
                "name": "AllMergeJoin",
                "hint": " ".join(all_mj),
                "reason": "force all joins to merge join",
                "category": "join_method",
            })

    # 6. Cardinality fixes for bad estimates
    for j in joins:
        if j.get("actual_rows") and len(j["aliases"]) >= 2:
            # Check if there's a large estimate error (look at plan text for est rows near join)
            pass  # Would need more parsing to get est vs actual per join

    # 7. Set-based hints (via pg_hint_plan Set directive)
    candidates.append({
        "name": "Set_NL_off",
        "hint": "Set(enable_nestloop off)",
        "reason": "disable all nested loops",
        "category": "set",
    })
    candidates.append({
        "name": "Set_HJ_off",
        "hint": "Set(enable_hashjoin off)",
        "reason": "disable all hash joins",
        "category": "set",
    })
    candidates.append({
        "name": "Set_MJ_off",
        "hint": "Set(enable_mergejoin off)",
        "reason": "disable all merge joins",
        "category": "set",
    })

    return candidates


# ── Query groups ──

# Config-resistant queries (from Pass 4)
CONFIG_RESISTANT = [
    "query064_multi",
    "query101_spj_spj",
    "query018_spj_spj",
    "query018_agg",
    "query072_agg",
    "query094_multi",
    "query040_spj_spj",
    "query027_spj_spj",
    "query065_multi",
    "query084_agg",
    "query072_spj_spj",
]

# Config winners — test if hints add FURTHER gains
CONFIG_WINNERS = {
    "query102_spj_spj": {"random_page_cost": "1.1", "effective_cache_size": "48GB"},
    "query050_spj_spj": {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                          "parallel_tuple_cost": "0.001"},
    "query087_multi": {"work_mem": "256MB"},
    "query091_spj_spj": {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                          "parallel_tuple_cost": "0.001"},
    "query023_multi": {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
                        "parallel_tuple_cost": "0.001", "work_mem": "512MB"},
}

# Standard config combos to test WITH hints
HINT_CONFIGS = [
    ("plain", {}),
    ("par4", {"max_parallel_workers_per_gather": "4", "parallel_setup_cost": "100",
              "parallel_tuple_cost": "0.001"}),
    ("wm256_par", {"work_mem": "256MB", "max_parallel_workers_per_gather": "4",
                   "parallel_setup_cost": "100", "parallel_tuple_cost": "0.001"}),
    ("wm256_jit_off", {"work_mem": "256MB", "jit": "off"}),
]


def explore_query(qid, opt_sql, is_winner=False, winner_config=None):
    """Full hint exploration for a single query."""
    print(f"\n{'='*70}")
    print(f"HINT EXPLORATION: {qid}" + (" [WINNER+HINT]" if is_winner else ""))
    print(f"{'='*70}")

    # Baseline EXPLAIN ANALYZE
    conn = get_conn()
    try:
        plan, baseline_ms = run_explain_analyze(conn, opt_sql)
    except Exception as e:
        print(f"  ERROR: baseline EXPLAIN failed: {e}")
        conn.close()
        return {"query_id": qid, "error": str(e)}
    conn.close()

    if not baseline_ms:
        print(f"  ERROR: no execution time in baseline plan")
        return {"query_id": qid, "error": "no exec time"}

    print(f"  Baseline: {baseline_ms:.1f}ms")

    # Parse plan structure
    joins = parse_plan_joins(plan)
    scans = parse_plan_scans(plan)

    print(f"  Joins: {len(joins)} | Scans: {len(scans)}")
    for j in joins[:8]:
        print(f"    {j['type']:15s} {' ⋈ '.join(j['aliases'][:3])}"
              + (f" (rows={j['actual_rows']})" if j.get('actual_rows') else ""))
    for s in scans[:6]:
        alias = s.get('alias') or s.get('table', '?')
        print(f"    {s['type']:25s} {alias} (rows={s.get('actual_rows', '?')})")

    # Generate hint candidates
    hint_candidates = generate_hints(joins, scans, plan)
    print(f"  Generated {len(hint_candidates)} hint candidates")

    results = {
        "query_id": qid,
        "baseline_ms": baseline_ms,
        "join_count": len(joins),
        "scan_count": len(scans),
        "hints_tested": [],
        "races": [],
    }

    # Phase 1: EXPLAIN ANALYZE each hint (wall-clock screening)
    promising = []

    for cand in hint_candidates:
        conn = get_conn()
        try:
            _, hint_ms = run_explain_analyze(conn, opt_sql, hint=cand["hint"])
            if hint_ms:
                gap = (baseline_ms - hint_ms) / baseline_ms * 100
                entry = {**cand, "hint_ms": hint_ms, "gap_pct": round(gap, 1)}
                results["hints_tested"].append(entry)

                if gap > 3:
                    promising.append(entry)
                    print(f"    ✓ {cand['name']:30s} {hint_ms:.1f}ms (gap={gap:+.1f}%) [{cand['reason']}]")
                elif gap < -20:
                    print(f"    ✗ {cand['name']:30s} {hint_ms:.1f}ms (gap={gap:+.1f}%)")
                else:
                    pass  # silent for neutral
        except Exception as e:
            err_short = str(e)[:60]
            if "cancel" not in err_short.lower():
                print(f"    ? {cand['name']:30s} error: {err_short}")
        conn.close()

    # Phase 2: For winners, test hint + config combos
    if is_winner and winner_config:
        print(f"\n  Testing hints WITH winning config...")
        for cand in hint_candidates[:15]:  # top 15 by generation order
            conn = get_conn()
            try:
                _, combo_ms = run_explain_analyze(conn, opt_sql,
                                                   config=winner_config, hint=cand["hint"])
                if combo_ms:
                    # Compare vs config-only baseline
                    conn2 = get_conn()
                    _, config_only_ms = run_explain_analyze(conn2, opt_sql, config=winner_config)
                    conn2.close()

                    if config_only_ms:
                        gap = (config_only_ms - combo_ms) / config_only_ms * 100
                        if gap > 3:
                            print(f"    ✓ COMBO {cand['name']:25s} {combo_ms:.1f}ms vs cfg={config_only_ms:.1f}ms (gap={gap:+.1f}%)")
                            promising.append({
                                **cand,
                                "name": f"combo_{cand['name']}",
                                "hint_ms": combo_ms,
                                "gap_pct": round(gap, 1),
                                "combo_config": winner_config,
                            })
            except:
                pass
            conn.close()

    # Phase 3: For config-resistant, test hint + standard configs
    if not is_winner:
        best_hint = None
        for h in promising:
            if not best_hint or h["gap_pct"] > best_hint["gap_pct"]:
                best_hint = h

        if best_hint:
            print(f"\n  Testing best hint + config combos...")
            for cfg_name, cfg in HINT_CONFIGS[1:]:  # skip 'plain' (already tested)
                conn = get_conn()
                try:
                    _, combo_ms = run_explain_analyze(conn, opt_sql, config=cfg, hint=best_hint["hint"])
                    if combo_ms:
                        gap = (baseline_ms - combo_ms) / baseline_ms * 100
                        marker = "✓" if gap > best_hint["gap_pct"] else "—"
                        print(f"    {marker} {best_hint['name']}+{cfg_name:15s} {combo_ms:.1f}ms (gap={gap:+.1f}%)")
                        if gap > best_hint["gap_pct"]:
                            promising.append({
                                **best_hint,
                                "name": f"{best_hint['name']}+{cfg_name}",
                                "hint_ms": combo_ms,
                                "gap_pct": round(gap, 1),
                                "combo_config": cfg,
                            })
                except:
                    pass
                conn.close()

    # Phase 4: Race top 3 promising candidates
    promising.sort(key=lambda x: -x["gap_pct"])

    for cand in promising[:3]:
        cfg = cand.get("combo_config")
        hint = cand["hint"]

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

        # 3 races
        race_results = []
        for ri in range(3):
            r = race(opt_sql, config_b=cfg, hint_b=hint)
            a_ms, b_ms = r.get("a_ms"), r.get("b_ms")
            if a_ms and b_ms:
                gap = (a_ms - b_ms) / a_ms * 100
                race_results.append({"race": ri+1, "plain_ms": round(a_ms, 1),
                                      "tuned_ms": round(b_ms, 1), "gap_pct": round(gap, 1)})
            else:
                race_results.append({"race": ri+1, "error": r.get("a_error") or r.get("b_error")})

        valid_gaps = [rr["gap_pct"] for rr in race_results if "gap_pct" in rr]
        avg_gap = sum(valid_gaps) / len(valid_gaps) if valid_gaps else 0
        consistent = all(g > 0 for g in valid_gaps) if valid_gaps else False
        verdict = "WIN" if consistent and avg_gap > 3 else "MARGINAL" if avg_gap > 0 else "LOSS"

        races_str = " | ".join(f"R{rr['race']}: {rr.get('gap_pct', '?'):+.1f}%"
                                if 'gap_pct' in rr else f"R{rr['race']}: ERR"
                                for rr in race_results)
        marker = {"WIN": "✓", "MARGINAL": "~", "LOSS": "✗"}.get(verdict, "?")
        cfg_str = f" +{list(cfg.keys())[0]}..." if cfg else ""
        print(f"\n  {marker} RACE {cand['name']}{cfg_str}: avg={avg_gap:+.1f}% [{races_str}] → {verdict}")

        results["races"].append({
            "name": cand["name"],
            "hint": hint,
            "config": cfg,
            "races": race_results,
            "avg_gap_pct": round(avg_gap, 1),
            "consistent": consistent,
            "verdict": verdict,
        })

    if not promising:
        print(f"\n  NO promising hints found for {qid}")

    return results


def main():
    print(f"Config Tuning — pg_hint_plan Exploration")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"Testing {len(CONFIG_RESISTANT)} config-resistant + {len(CONFIG_WINNERS)} winner queries\n")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_results = {"config_resistant": [], "winner_additive": []}

    # Phase A: Config-resistant queries
    print(f"\n{'#'*70}")
    print("PHASE A: CONFIG-RESISTANT QUERIES — HINT EXPLORATION")
    print(f"{'#'*70}")

    for qid in CONFIG_RESISTANT:
        opt_path = BEST_DIR / f"{qid}.sql"
        if not opt_path.exists():
            print(f"\n  SKIP {qid}: no optimized SQL")
            continue
        opt_sql = opt_path.read_text().strip()
        result = explore_query(qid, opt_sql, is_winner=False)
        all_results["config_resistant"].append(result)

    # Phase B: Winners — test if hints add further gains
    print(f"\n{'#'*70}")
    print("PHASE B: WINNERS — ADDITIVE HINT EXPLORATION")
    print(f"{'#'*70}")

    for qid, best_config in CONFIG_WINNERS.items():
        opt_path = BEST_DIR / f"{qid}.sql"
        if not opt_path.exists():
            print(f"\n  SKIP {qid}: no optimized SQL")
            continue
        opt_sql = opt_path.read_text().strip()
        result = explore_query(qid, opt_sql, is_winner=True, winner_config=best_config)
        all_results["winner_additive"].append(result)

    # Save
    out_path = OUTPUT_DIR / "config_tuning_hints.json"
    out_path.write_text(json.dumps({
        "results": all_results,
        "completed_at": datetime.now().isoformat(),
    }, indent=2, default=str))

    # Summary
    print(f"\n\n{'='*70}")
    print("HINT EXPLORATION SUMMARY")
    print(f"{'='*70}")

    print("\n  CONFIG-RESISTANT QUERIES:")
    for r in all_results["config_resistant"]:
        qid = r.get("query_id", "?")
        races = r.get("races", [])
        wins = [rc for rc in races if rc.get("verdict") == "WIN"]
        if wins:
            best = max(wins, key=lambda x: x["avg_gap_pct"])
            print(f"    {qid:25s} WIN: {best['name']:30s} avg={best['avg_gap_pct']:+.1f}%")
        else:
            tested = len(r.get("hints_tested", []))
            promising = len([h for h in r.get("hints_tested", []) if h.get("gap_pct", 0) > 3])
            print(f"    {qid:25s} no hint win ({tested} tested, {promising} promising)")

    print("\n  WINNER ADDITIVE:")
    for r in all_results["winner_additive"]:
        qid = r.get("query_id", "?")
        races = r.get("races", [])
        wins = [rc for rc in races if rc.get("verdict") == "WIN"]
        if wins:
            best = max(wins, key=lambda x: x["avg_gap_pct"])
            print(f"    {qid:25s} ADDITIVE: {best['name']:30s} avg={best['avg_gap_pct']:+.1f}%")
        else:
            print(f"    {qid:25s} no additive hint gain")

    print(f"\nResults: {out_path}")


if __name__ == "__main__":
    main()
