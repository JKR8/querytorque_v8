#!/usr/bin/env python3
"""Config Tuning — Remaining 6 queries not in best/ directory.

Reads original SQL from ALL_OPTIMIZATIONS/postgres_dsb/{qid}/original.sql
and tests 8 configs + 3 global hints + combos with 3-race validation.

Completes the full 52-query config tuning (46 done + 6 remaining).

Usage:
    cd QueryTorque_V8
    python3 research/config_tuning_remaining6.py
"""

import json
import re
import time
import threading
from pathlib import Path
from datetime import datetime

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALL_OPT_DIR = PROJECT_ROOT / "research/ALL_OPTIMIZATIONS/postgres_dsb"
OUTPUT_DIR = PROJECT_ROOT / "research/config_tuning_results"
DSN = "host=127.0.0.1 port=5434 dbname=dsb_sf10 user=jakc9 password=jakc9"

REMAINING = [
    "query001_multi",
    "query031_multi",
    "query038_multi",
    "query075_multi",
    "query085_agg",
    "query085_spj_spj",
]

CONFIGS = [
    ("par4", {
        "max_parallel_workers_per_gather": "4",
        "parallel_setup_cost": "100",
        "parallel_tuple_cost": "0.001",
    }),
    ("wm256_par", {
        "work_mem": "256MB",
        "max_parallel_workers_per_gather": "4",
        "parallel_setup_cost": "100",
        "parallel_tuple_cost": "0.001",
    }),
    ("wm512_par", {
        "work_mem": "512MB",
        "max_parallel_workers_per_gather": "4",
        "parallel_setup_cost": "100",
        "parallel_tuple_cost": "0.001",
    }),
    ("wm256", {"work_mem": "256MB"}),
    ("rpc_cache", {"random_page_cost": "1.1", "effective_cache_size": "48GB"}),
    ("jcl_12", {"join_collapse_limit": "12"}),
    ("sort_off", {"enable_sort": "off"}),
    ("rpc_par", {
        "random_page_cost": "1.1",
        "max_parallel_workers_per_gather": "4",
        "parallel_setup_cost": "100",
        "parallel_tuple_cost": "0.001",
    }),
]

GLOBAL_HINTS = [
    ("NL_off", "Set(enable_nestloop off)"),
    ("HJ_off", "Set(enable_hashjoin off)"),
    ("MJ_off", "Set(enable_mergejoin off)"),
]


def get_conn():
    return psycopg2.connect(DSN)


def run_explain_analyze(conn, sql, config=None, hint=None, timeout_ms=300000):
    """Run EXPLAIN ANALYZE, return (plan_text, exec_ms)."""
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
    """Race plain vs tuned. Both wait at barrier, both run to completion."""
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


def extract_features(plan_text):
    """Extract key features from EXPLAIN plan."""
    join_count = len(re.findall(r'(?:Hash Join|Merge Join|Nested Loop)', plan_text))
    has_parallel = "Parallel" in plan_text or "Gather" in plan_text
    has_jit = "JIT:" in plan_text
    has_nl = "Nested Loop" in plan_text
    has_hj = "Hash Join" in plan_text
    has_mj = "Merge Join" in plan_text
    has_seq = "Seq Scan" in plan_text

    large_seqs = []
    for m in re.finditer(r'(?:Parallel )?Seq Scan\s+on\s+(\w+).*?rows=(\d+)', plan_text):
        rows = int(m.group(2))
        if rows > 10000:
            large_seqs.append({"table": m.group(1), "rows": rows})

    exec_m = re.search(r'Execution Time:\s*([\d.]+)', plan_text)
    exec_ms = float(exec_m.group(1)) if exec_m else None

    return {
        "join_count": join_count,
        "has_parallel": has_parallel,
        "has_jit": has_jit,
        "has_nl": has_nl,
        "has_hj": has_hj,
        "has_mj": has_mj,
        "has_seq": has_seq,
        "large_seqs": large_seqs,
        "exec_ms": exec_ms,
    }


def tune_query(qid, sql):
    """Full config + hint tuning for a single query."""
    print(f"\n{'='*70}")
    print(f"TUNING: {qid}")
    print(f"{'='*70}")

    result = {"query_id": qid, "candidates": [], "races": []}

    # Phase 1: Baseline EXPLAIN ANALYZE
    conn = get_conn()
    try:
        plan, baseline_ms = run_explain_analyze(conn, sql)
    except Exception as e:
        print(f"  ERROR: baseline failed: {str(e)[:80]}")
        conn.close()
        result["error"] = str(e)
        return result
    conn.close()

    if not baseline_ms:
        print(f"  ERROR: no exec time")
        result["error"] = "no exec time"
        return result

    features = extract_features(plan)
    result["baseline_ms"] = baseline_ms
    result["features"] = {k: v for k, v in features.items() if k != "large_seqs"}
    result["features"]["large_seq_count"] = len(features["large_seqs"])

    print(f"  Baseline: {baseline_ms:.1f}ms | joins={features['join_count']} "
          f"NL={features['has_nl']} HJ={features['has_hj']} MJ={features['has_mj']} "
          f"par={features['has_parallel']} seqs={len(features['large_seqs'])}")

    # Phase 2: EXPLAIN ANALYZE each config
    candidates = []

    for cfg_name, cfg in CONFIGS:
        conn = get_conn()
        try:
            _, cfg_ms = run_explain_analyze(conn, sql, config=cfg)
            if cfg_ms:
                gap = (baseline_ms - cfg_ms) / baseline_ms * 100
                candidates.append({
                    "name": cfg_name, "type": "config", "config": cfg, "hint": None,
                    "analyze_ms": cfg_ms, "gap_pct": round(gap, 1),
                })
                if gap > 3:
                    print(f"    ✓ {cfg_name:20s} {cfg_ms:.1f}ms (gap={gap:+.1f}%)")
        except Exception as e:
            if "cancel" not in str(e).lower():
                print(f"    ? {cfg_name:20s} error: {str(e)[:50]}")
        conn.close()

    # Phase 3: EXPLAIN ANALYZE each global hint
    for hint_name, hint in GLOBAL_HINTS:
        conn = get_conn()
        try:
            _, hint_ms = run_explain_analyze(conn, sql, hint=hint)
            if hint_ms:
                gap = (baseline_ms - hint_ms) / baseline_ms * 100
                candidates.append({
                    "name": hint_name, "type": "hint", "config": None, "hint": hint,
                    "analyze_ms": hint_ms, "gap_pct": round(gap, 1),
                })
                if gap > 3:
                    print(f"    ✓ {hint_name:20s} {hint_ms:.1f}ms (gap={gap:+.1f}%) [hint]")
                elif gap < -50:
                    print(f"    ✗ {hint_name:20s} {hint_ms:.1f}ms (gap={gap:+.1f}%)")
        except Exception as e:
            if "cancel" not in str(e).lower():
                print(f"    ? {hint_name:20s} error: {str(e)[:50]}")
        conn.close()

    # Phase 4: Best hint + best config combos
    best_hint = None
    best_cfg = None
    for c in candidates:
        if c["type"] == "hint" and c["gap_pct"] > 3:
            if not best_hint or c["gap_pct"] > best_hint["gap_pct"]:
                best_hint = c
        if c["type"] == "config" and c["gap_pct"] > 3:
            if not best_cfg or c["gap_pct"] > best_cfg["gap_pct"]:
                best_cfg = c

    if best_hint and best_cfg:
        combo_name = f"{best_hint['name']}+{best_cfg['name']}"
        conn = get_conn()
        try:
            _, combo_ms = run_explain_analyze(conn, sql,
                                               config=best_cfg["config"], hint=best_hint["hint"])
            if combo_ms:
                gap = (baseline_ms - combo_ms) / baseline_ms * 100
                candidates.append({
                    "name": combo_name, "type": "hint+config",
                    "config": best_cfg["config"], "hint": best_hint["hint"],
                    "analyze_ms": combo_ms, "gap_pct": round(gap, 1),
                })
                if gap > 3:
                    print(f"    ✓ {combo_name:20s} {combo_ms:.1f}ms (gap={gap:+.1f}%) [combo]")
        except:
            pass
        conn.close()
    elif best_hint and not best_cfg:
        for cfg_name, cfg in [("par4", CONFIGS[0][1]), ("wm256_par", CONFIGS[1][1])]:
            combo_name = f"{best_hint['name']}+{cfg_name}"
            conn = get_conn()
            try:
                _, combo_ms = run_explain_analyze(conn, sql, config=cfg, hint=best_hint["hint"])
                if combo_ms:
                    gap = (baseline_ms - combo_ms) / baseline_ms * 100
                    candidates.append({
                        "name": combo_name, "type": "hint+config",
                        "config": cfg, "hint": best_hint["hint"],
                        "analyze_ms": combo_ms, "gap_pct": round(gap, 1),
                    })
                    if gap > 3:
                        print(f"    ✓ {combo_name:20s} {combo_ms:.1f}ms (gap={gap:+.1f}%) [combo]")
            except:
                pass
            conn.close()

    result["candidates"] = candidates

    # Phase 5: 3-race validate top candidates with EXPLAIN gap > 3%
    promising = sorted([c for c in candidates if c["gap_pct"] > 3],
                       key=lambda x: -x["gap_pct"])

    # Warmup
    try:
        c = get_conn()
        with c.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '300000'")
            cur.execute(sql)
            cur.fetchall()
        c.rollback(); c.close()
    except:
        pass

    for cand in promising[:4]:
        race_results = []
        for ri in range(3):
            r = race(sql, config_b=cand.get("config"), hint_b=cand.get("hint"))
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

        races_str = " | ".join(
            f"R{rr['race']}: {rr.get('gap_pct', '?'):+.1f}%"
            if 'gap_pct' in rr else f"R{rr['race']}: ERR"
            for rr in race_results
        )
        marker = {"WIN": "✓", "MARGINAL": "~", "LOSS": "✗"}.get(verdict, "?")
        print(f"  {marker} RACE {cand['name']:25s} avg={avg_gap:+.1f}% [{races_str}] → {verdict}")

        result["races"].append({
            "name": cand["name"],
            "type": cand["type"],
            "config": cand.get("config"),
            "hint": cand.get("hint"),
            "analyze_gap_pct": cand["gap_pct"],
            "races": race_results,
            "avg_gap_pct": round(avg_gap, 1),
            "consistent": consistent,
            "verdict": verdict,
        })

    if not promising:
        print(f"  No candidates with EXPLAIN gap > 3%")

    return result


def main():
    print(f"Config Tuning — 6 Remaining Queries (using original SQL)")
    print(f"Testing: {len(CONFIGS)} configs + {len(GLOBAL_HINTS)} hints + combos per query")
    print(f"Started: {datetime.now().isoformat()}\n")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []

    for i, qid in enumerate(REMAINING):
        sql_path = ALL_OPT_DIR / qid / "original.sql"
        if not sql_path.exists():
            print(f"\n[{i+1}/6] SKIP {qid} — no original.sql found")
            continue

        sql = sql_path.read_text().strip()
        print(f"\n[{i+1}/6]", end="")
        result = tune_query(qid, sql)
        all_results.append(result)

        # Save incrementally
        out_path = OUTPUT_DIR / "config_tuning_remaining6.json"
        out_path.write_text(json.dumps({
            "results": all_results,
            "progress": f"{i+1}/6",
            "updated_at": datetime.now().isoformat(),
        }, indent=2, default=str))

    # Summary
    print(f"\n\n{'='*70}")
    print("REMAINING 6 QUERIES SUMMARY")
    print(f"{'='*70}")

    wins = []
    no_win = []

    for r in all_results:
        qid = r.get("query_id", "?")
        race_wins = [rc for rc in r.get("races", []) if rc.get("verdict") == "WIN"]
        if race_wins:
            best = max(race_wins, key=lambda x: x["avg_gap_pct"])
            wins.append((qid, best))
            cfg_str = ""
            if best.get("config"):
                cfg_str += ", ".join(f"{k}={v}" for k, v in list(best["config"].items())[:2])
            if best.get("hint"):
                cfg_str += (" + " if cfg_str else "") + best["hint"]
            print(f"  ✓ {qid:25s} {best['name']:25s} avg={best['avg_gap_pct']:+.1f}% [{best['type']}] {cfg_str}")
        else:
            no_win.append(qid)
            tested = len(r.get("candidates", []))
            promising = len([c for c in r.get("candidates", []) if c.get("gap_pct", 0) > 3])
            print(f"  — {qid:25s} no win ({tested} tested, {promising} promising)")

    print(f"\n  WINS: {len(wins)}/6")
    print(f"  NO WIN: {len(no_win)}/6")
    print(f"\n  COMBINED WITH PRIOR 46: {24 + len(wins)}/52 wins total")
    print(f"\nResults: {OUTPUT_DIR / 'config_tuning_remaining6.json'}")


if __name__ == "__main__":
    main()
