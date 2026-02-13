#!/usr/bin/env python3
"""Build apples-to-apples planner-cost comparison for R-Bot vs QueryTorque.

This script supports two QueryTorque policies:
- rewrite_only: best rewrite candidate per query from ALL_OPTIMIZATIONS
- combined_best: best source per query from combined leaderboard (rewrite/config/none)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import shutil
import time
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any

import psycopg2


ROOT = Path(__file__).resolve().parents[2]
OUTDIR = ROOT / "paper" / "rbot_dsb_full_run_20260213"
QT_ROOT = ROOT / "research" / "ALL_OPTIMIZATIONS" / "postgres_dsb"
BEST_DIR = ROOT / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "postgres_dsb" / "best"
COMBINED_JSON = ROOT / "research" / "config_tuning_results" / "combined_pg_dsb_leaderboard.json"
CONFIG_RECS_JSON = ROOT / "research" / "config_tuning_results" / "config_recommendations.json"
RBOT_CSV = OUTDIR / "dsb_full_run_results_full_dimensions.csv"

STATUS_RANK = {
    "WIN": 5,
    "IMPROVED": 4,
    "RECOVERED": 3,
    "NEUTRAL": 2,
    "REGRESSION": 1,
    "ERROR": 0,
    "FAIL": 0,
    None: -1,
}

DSN = "postgresql://jakc9:jakc9@127.0.0.1:5434/dsb_sf10?sslmode=disable"


def as_float(x: Any) -> float | None:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def classify_pct(pct: float | None, eps: float = 1e-9) -> str:
    if pct is None:
        return "ERROR"
    if pct < -eps:
        return "WIN"
    if pct > eps:
        return "LOSS"
    return "TIE"


def summarize(values: list[float | None]) -> dict[str, Any]:
    vals = [v for v in values if v is not None and math.isfinite(v)]
    n = len(vals)
    wins = sum(1 for v in vals if v < -1e-9)
    ties = sum(1 for v in vals if abs(v) <= 1e-9)
    losses = sum(1 for v in vals if v > 1e-9)
    return {
        "n": n,
        "wins": wins,
        "ties": ties,
        "losses": losses,
        "win_rate_pct": (wins / n * 100.0) if n else None,
        "median_pct_change": median(vals) if vals else None,
        "mean_pct_change": mean(vals) if vals else None,
    }


def connect_with_retry(attempts: int = 20, sleep_s: float = 1.0):
    last = None
    for i in range(1, attempts + 1):
        try:
            conn = psycopg2.connect(DSN)
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            conn.rollback()
            return conn, cur
        except Exception as e:
            last = e
            print(f"[connect] retry {i}/{attempts} failed: {repr(e)}")
            time.sleep(sleep_s)
    raise last  # type: ignore[misc]


def explain_total_cost(
    cur,
    conn,
    sql_text: str,
    config: dict[str, Any] | None = None,
    hint: str | None = None,
    timeout_ms: int = 120000,
) -> float:
    cur.execute("BEGIN")
    try:
        cur.execute("SET LOCAL statement_timeout = %s", (str(timeout_ms),))

        if config:
            for k, v in config.items():
                if not re.match(r"^[a-z_][a-z0-9_]*$", k):
                    raise ValueError(f"invalid setting name: {k}")
                cur.execute(f"SET LOCAL {k} = %s", (str(v),))

        full_sql = sql_text
        if hint:
            # Do not hard-fail if pg_hint_plan GUC is unavailable.
            cur.execute("SAVEPOINT qt_hint")
            try:
                cur.execute("SET LOCAL pg_hint_plan.enable_hint = on")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT qt_hint")
            finally:
                cur.execute("RELEASE SAVEPOINT qt_hint")
            full_sql = f"/*+ {hint} */\n{sql_text}"

        cur.execute("EXPLAIN (FORMAT JSON) " + full_sql)
        val = cur.fetchone()[0]
        if isinstance(val, str):
            val = json.loads(val)
        total_cost = float(val[0]["Plan"]["Total Cost"])
        conn.rollback()
        return total_cost
    except Exception:
        conn.rollback()
        raise


def load_rbot_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with RBOT_CSV.open() as f:
        rd = csv.DictReader(f)
        for row in rd:
            if row.get("status") != "complete":
                continue
            qnum = int(row["query_id"])
            pct = as_float(row.get("pct_change_best"))
            rows.append(
                {
                    "query_num": qnum,
                    "query_id": f"query{qnum:03d}",
                    "instance": row.get("instance", ""),
                    "statement_idx": row.get("statement_idx", ""),
                    "log_file": row.get("log_file", ""),
                    "input_cost": as_float(row.get("input_cost")),
                    "output_cost": as_float(row.get("output_cost_best")),
                    "pct_change": pct,
                    "speedup": as_float(row.get("speedup_best")),
                    "class": classify_pct(pct),
                }
            )
    return rows


def pick_best_allopt_candidate(qdir: Path) -> dict[str, Any]:
    qid = qdir.name
    cands: list[tuple[int, float, str | None, str, Path]] = []
    for adir in sorted(qdir.iterdir()):
        if not adir.is_dir():
            continue
        mp = adir / "meta.json"
        op = adir / "optimized.sql"
        if not (mp.exists() and op.exists()):
            continue
        try:
            meta = json.loads(mp.read_text())
        except Exception:
            continue
        val = meta.get("validation", {}) if isinstance(meta, dict) else {}
        st = val.get("status")
        sp = as_float(val.get("best_speedup"))
        cands.append((STATUS_RANK.get(st, -1), sp if sp is not None else float("-inf"), st, adir.name, op))

    if not cands:
        return {
            "query_id": qid,
            "selected_source": None,
            "validation_status": "ERROR",
            "validation_best_speedup": None,
            "optimized_path": None,
            "error": "no_candidates",
        }

    cands.sort(key=lambda t: (t[0], t[1]), reverse=True)
    _, sp, st, src, op = cands[0]
    return {
        "query_id": qid,
        "selected_source": src,
        "validation_status": st,
        "validation_best_speedup": None if sp in (float("-inf"), float("inf")) else sp,
        "optimized_path": str(op),
        "error": None,
    }


def load_combined_inputs() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    board = json.loads(COMBINED_JSON.read_text())["board"]
    board_by_qid = {row["query_id"]: row for row in board}
    recs = json.loads(CONFIG_RECS_JSON.read_text())["recommendations"]
    rec_by_qid = {row["query_id"]: row for row in recs}
    return board_by_qid, rec_by_qid


def collect_qt_candidates(policy: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    qdirs = [d for d in sorted(QT_ROOT.glob("query*")) if d.is_dir()]

    board_by_qid: dict[str, dict[str, Any]] = {}
    rec_by_qid: dict[str, dict[str, Any]] = {}
    if policy == "combined_best":
        board_by_qid, rec_by_qid = load_combined_inputs()

    for qdir in qdirs:
        qid = qdir.name
        m = re.match(r"query(\d+)_", qid)
        qnum = int(m.group(1)) if m else None
        orig_path = qdir / "original.sql"
        if not orig_path.exists():
            out.append(
                {
                    "query_id": qid,
                    "query_num": qnum,
                    "original_path": None,
                    "optimized_path": None,
                    "selected_source": None,
                    "policy_source": policy,
                    "best_source": None,
                    "validation_status": "ERROR",
                    "validation_best_speedup": None,
                    "config_json": None,
                    "hint": None,
                    "error": "missing_original.sql",
                }
            )
            continue

        if policy == "rewrite_only":
            best = pick_best_allopt_candidate(qdir)
            out.append(
                {
                    "query_id": qid,
                    "query_num": qnum,
                    "original_path": str(orig_path),
                    "optimized_path": best["optimized_path"],
                    "selected_source": best["selected_source"],
                    "policy_source": "rewrite_only",
                    "best_source": "rewrite",
                    "validation_status": best["validation_status"],
                    "validation_best_speedup": best["validation_best_speedup"],
                    "config_json": None,
                    "hint": None,
                    "error": best["error"],
                }
            )
            continue

        # combined_best policy
        board = board_by_qid.get(qid)
        if not board:
            # Fallback if board entry is missing.
            best = pick_best_allopt_candidate(qdir)
            out.append(
                {
                    "query_id": qid,
                    "query_num": qnum,
                    "original_path": str(orig_path),
                    "optimized_path": best["optimized_path"],
                    "selected_source": best["selected_source"],
                    "policy_source": "combined_best_fallback",
                    "best_source": "rewrite",
                    "validation_status": best["validation_status"],
                    "validation_best_speedup": best["validation_best_speedup"],
                    "config_json": None,
                    "hint": None,
                    "error": best["error"],
                }
            )
            continue

        best_source = board.get("best_source")
        rewrite_status = board.get("rewrite_status")
        rewrite_speedup = board.get("rewrite_speedup")
        best_speedup = board.get("best_speedup")

        if best_source == "config":
            rec = rec_by_qid.get(qid)
            best_sql_path = BEST_DIR / f"{qid}.sql"
            if best_sql_path.exists():
                tuned_path = best_sql_path
                cfg_origin = "best_dir_sql"
            else:
                tuned_path = orig_path
                cfg_origin = "original_sql_fallback"

            out.append(
                {
                    "query_id": qid,
                    "query_num": qnum,
                    "original_path": str(orig_path),
                    "optimized_path": str(tuned_path),
                    "selected_source": f"config:{(rec or {}).get('type', 'unknown')}",
                    "policy_source": "combined_best",
                    "best_source": "config",
                    "validation_status": board.get("verdict"),
                    "validation_best_speedup": as_float(best_speedup),
                    "config_json": json.dumps((rec or {}).get("config")) if rec and rec.get("config") else None,
                    "hint": (rec or {}).get("hint"),
                    "error": None if rec else "missing_config_recommendation",
                    "config_sql_origin": cfg_origin,
                    "config_type": board.get("config_type"),
                }
            )
            continue

        if best_source == "none":
            out.append(
                {
                    "query_id": qid,
                    "query_num": qnum,
                    "original_path": str(orig_path),
                    "optimized_path": str(orig_path),
                    "selected_source": "none",
                    "policy_source": "combined_best",
                    "best_source": "none",
                    "validation_status": board.get("verdict"),
                    "validation_best_speedup": as_float(best_speedup),
                    "config_json": None,
                    "hint": None,
                    "error": None,
                }
            )
            continue

        # rewrite best_source
        best_sql_path = BEST_DIR / f"{qid}.sql"
        if best_sql_path.exists():
            out.append(
                {
                    "query_id": qid,
                    "query_num": qnum,
                    "original_path": str(orig_path),
                    "optimized_path": str(best_sql_path),
                    "selected_source": "rewrite:best_dir",
                    "policy_source": "combined_best",
                    "best_source": "rewrite",
                    "validation_status": rewrite_status,
                    "validation_best_speedup": as_float(rewrite_speedup),
                    "config_json": None,
                    "hint": None,
                    "error": None,
                }
            )
        else:
            best = pick_best_allopt_candidate(qdir)
            out.append(
                {
                    "query_id": qid,
                    "query_num": qnum,
                    "original_path": str(orig_path),
                    "optimized_path": best["optimized_path"],
                    "selected_source": f"rewrite:fallback:{best['selected_source']}",
                    "policy_source": "combined_best",
                    "best_source": "rewrite",
                    "validation_status": rewrite_status or best["validation_status"],
                    "validation_best_speedup": as_float(rewrite_speedup) or best["validation_best_speedup"],
                    "config_json": None,
                    "hint": None,
                    "error": best["error"],
                }
            )

    return out


def compute_qt_costs(cur, conn, qt_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    total = len(qt_candidates)
    for i, row in enumerate(qt_candidates, 1):
        rec = dict(row)
        rec.update(
            {
                "baseline_cost": None,
                "optimized_cost": None,
                "pct_change": None,
                "speedup": None,
                "class": "ERROR",
            }
        )

        if rec["original_path"] is None or rec["optimized_path"] is None:
            rows.append(rec)
            continue

        try:
            orig_sql = Path(rec["original_path"]).read_text()
            opt_sql = Path(rec["optimized_path"]).read_text()
            cfg = json.loads(rec["config_json"]) if rec.get("config_json") else None
            hint = rec.get("hint")

            base = explain_total_cost(cur, conn, orig_sql)
            opt = explain_total_cost(cur, conn, opt_sql, config=cfg, hint=hint)
            pct = ((opt - base) / base * 100.0) if base else None
            speed = (base / opt) if (opt and opt != 0) else None
            rec["baseline_cost"] = base
            rec["optimized_cost"] = opt
            rec["pct_change"] = pct
            rec["speedup"] = speed
            rec["class"] = classify_pct(pct)
            rec["error"] = None
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {e}"

        rows.append(rec)
        if i % 10 == 0 or i == total:
            print(f"[qt-cost] {i}/{total}")
    return rows


def agg_best(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by[r["query_num"]].append(r)
    out: list[dict[str, Any]] = []
    for q, arr in sorted(by.items()):
        valid = [x for x in arr if x.get("pct_change") is not None]
        if not valid:
            out.append({"query_num": q, "pct_change": None, "speedup": None, "class": "ERROR"})
            continue
        best = min(valid, key=lambda x: x["pct_change"])
        out.append(
            {
                "query_num": q,
                "pct_change": best["pct_change"],
                "speedup": best.get("speedup"),
                "class": classify_pct(best["pct_change"]),
            }
        )
    return out


def output_paths(policy: str) -> dict[str, Path]:
    suffix = "" if policy == "rewrite_only" else "_COMBINED_BEST"
    return {
        "qt_cost_csv": OUTDIR / f"QUERYTORQUE_DSB_EXPLAIN_COST_REPLAY{suffix}.csv",
        "common_csv": OUTDIR / f"RBT_QT_COMMON37_COST_SIDE_BY_SIDE{suffix}.csv",
        "summary_json": OUTDIR / f"RBT_QT_COST_ALIGNED_SUMMARY{suffix}.json",
        "summary_md": OUTDIR / f"RBT_QT_COST_ALIGNED_SUMMARY{suffix}.md",
    }


def write_outputs(
    policy: str,
    rbot_rows: list[dict[str, Any]],
    qt_rows: list[dict[str, Any]],
    make_canonical: bool,
) -> None:
    paths = output_paths(policy)

    with paths["qt_cost_csv"].open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "query_id",
                "query_num",
                "policy_source",
                "best_source",
                "selected_source",
                "validation_status",
                "validation_best_speedup",
                "baseline_cost",
                "optimized_cost",
                "pct_change",
                "speedup",
                "class",
                "config_json",
                "config_type",
                "config_sql_origin",
                "hint",
                "original_path",
                "optimized_path",
                "error",
            ],
        )
        w.writeheader()
        w.writerows(qt_rows)

    rbot_agg = agg_best(rbot_rows)
    qt_agg = agg_best(qt_rows)
    rmap = {r["query_num"]: r for r in rbot_agg}
    qmap = {r["query_num"]: r for r in qt_agg}
    common = sorted(set(rmap) & set(qmap))
    joined = []
    for q in common:
        joined.append(
            {
                "query_num": q,
                "rbot_pct_change_best": rmap[q]["pct_change"],
                "rbot_speedup_best": rmap[q]["speedup"],
                "rbot_class": rmap[q]["class"],
                "qt_pct_change_best": qmap[q]["pct_change"],
                "qt_speedup_best": qmap[q]["speedup"],
                "qt_class": qmap[q]["class"],
            }
        )

    with paths["common_csv"].open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "query_num",
                "rbot_pct_change_best",
                "rbot_speedup_best",
                "rbot_class",
                "qt_pct_change_best",
                "qt_speedup_best",
                "qt_class",
            ],
        )
        w.writeheader()
        w.writerows(joined)

    rbot_sum = summarize([r["pct_change"] for r in rbot_rows])
    qt_sum = summarize([r["pct_change"] for r in qt_rows])
    common_r = summarize([r["rbot_pct_change_best"] for r in joined])
    common_q = summarize([r["qt_pct_change_best"] for r in joined])

    summary = {
        "qt_policy": policy,
        "metric_definition": "Planner cost from EXPLAIN (FORMAT JSON); pct_change=(optimized-baseline)/baseline*100; WIN if pct<0, TIE if pct=0, LOSS if pct>0",
        "rbot_full_instances_76": rbot_sum,
        "querytorque_rows": qt_sum,
        "common_querynum_best_of_each_37": {"rbot": common_r, "querytorque": common_q},
        "counts": {
            "rbot_rows": len(rbot_rows),
            "qt_rows_total": len(qt_rows),
            "qt_cost_errors": sum(1 for r in qt_rows if r.get("pct_change") is None),
            "common_query_nums": len(common),
            "qt_best_source_counts": dict(
                sorted(
                    ((k, v) for k, v in defaultdict(int, {}).items()),
                    key=lambda x: x[0],
                )
            ),
        },
        "artifacts": {
            "qt_cost_replay_csv": str(paths["qt_cost_csv"]),
            "common37_csv": str(paths["common_csv"]),
        },
    }

    # Fill source counts in a deterministic way.
    source_counts: dict[str, int] = defaultdict(int)
    for r in qt_rows:
        source_counts[str(r.get("best_source"))] += 1
    summary["counts"]["qt_best_source_counts"] = dict(sorted(source_counts.items()))

    paths["summary_json"].write_text(json.dumps(summary, indent=2))

    md: list[str] = []
    md.append("# R-Bot vs QueryTorque (Aligned Planner-Cost Metrics)")
    md.append("")
    md.append(f"QT policy: `{policy}`")
    md.append("All numbers below use identical definitions from PostgreSQL `EXPLAIN (FORMAT JSON)` costs.")
    md.append("")
    md.append(f"| Metric | R-Bot (n={rbot_sum['n']}) | QueryTorque (n={qt_sum['n']}) |")
    md.append("|---|---:|---:|")
    for key, label in [
        ("wins", "Wins (cost down)"),
        ("ties", "Ties"),
        ("losses", "Losses"),
        ("win_rate_pct", "Win rate %"),
        ("median_pct_change", "Median % cost change"),
        ("mean_pct_change", "Mean % cost change"),
    ]:
        rv = rbot_sum[key]
        qv = qt_sum[key]
        rv = f"{rv:.3f}" if isinstance(rv, float) else rv
        qv = f"{qv:.3f}" if isinstance(qv, float) else qv
        md.append(f"| {label} | {rv} | {qv} |")
    md.append("")
    md.append(
        f"| Metric (common 37 query numbers; best-of-each; valid n: R-Bot={common_r['n']}, QueryTorque={common_q['n']}) | R-Bot | QueryTorque |"
    )
    md.append("|---|---:|---:|")
    for key, label in [
        ("wins", "Wins (cost down)"),
        ("ties", "Ties"),
        ("losses", "Losses"),
        ("win_rate_pct", "Win rate %"),
        ("median_pct_change", "Median % cost change"),
        ("mean_pct_change", "Mean % cost change"),
    ]:
        rv = common_r[key]
        qv = common_q[key]
        rv = f"{rv:.3f}" if isinstance(rv, float) else rv
        qv = f"{qv:.3f}" if isinstance(qv, float) else qv
        md.append(f"| {label} | {rv} | {qv} |")
    md.append("")
    md.append(f"QT best_source counts: `{dict(sorted(source_counts.items()))}`")
    md.append("")
    md.append("Artifacts:")
    md.append(f"- `{paths['qt_cost_csv']}`")
    md.append(f"- `{paths['common_csv']}`")
    md.append(f"- `{paths['summary_json']}`")
    paths["summary_md"].write_text("\n".join(md) + "\n")

    if make_canonical:
        shutil.copy2(paths["summary_md"], OUTDIR / "QUERYTORQUE_vs_RBOT_DSB_SUMMARY.md")
        shutil.copy2(paths["summary_json"], OUTDIR / "QUERYTORQUE_vs_RBOT_DSB_SUMMARY.json")
        shutil.copy2(paths["common_csv"], OUTDIR / "QUERYTORQUE_vs_RBOT_DSB_SIDE_BY_SIDE.csv")
        shutil.copy2(paths["qt_cost_csv"], OUTDIR / "QUERYTORQUE_DSB_EXPLAIN_COST_REPLAY.csv")
        print("[canonical] updated QUERYTORQUE_vs_RBOT_* files")

    print("WROTE", paths["qt_cost_csv"])
    print("WROTE", paths["common_csv"])
    print("WROTE", paths["summary_json"])
    print("WROTE", paths["summary_md"])
    print("RBOT", rbot_sum)
    print("QT", qt_sum)
    print("COMMON RBOT", common_r)
    print("COMMON QT", common_q)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build aligned R-Bot vs QT planner-cost summary.")
    p.add_argument(
        "--qt-policy",
        choices=["rewrite_only", "combined_best"],
        default="rewrite_only",
        help="How to choose QT candidate per query.",
    )
    p.add_argument(
        "--canonical",
        action="store_true",
        help="Copy outputs to QUERYTORQUE_vs_RBOT_DSB_* canonical filenames.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    print(f"[start] qt policy = {args.qt_policy}")
    print("[start] loading R-Bot rows")
    rbot_rows = load_rbot_rows()
    print(f"[start] loaded {len(rbot_rows)} R-Bot rows")

    print("[start] selecting QT candidates")
    qt_candidates = collect_qt_candidates(args.qt_policy)
    print(f"[start] selected {len(qt_candidates)} QT candidates")

    print("[db] connecting")
    conn, cur = connect_with_retry()
    try:
        print("[db] computing QT costs via EXPLAIN")
        qt_rows = compute_qt_costs(cur, conn, qt_candidates)
    finally:
        cur.close()
        conn.close()

    write_outputs(args.qt_policy, rbot_rows, qt_rows, make_canonical=args.canonical)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
