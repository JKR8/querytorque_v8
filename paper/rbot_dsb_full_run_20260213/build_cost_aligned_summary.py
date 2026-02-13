#!/usr/bin/env python3
"""Build apples-to-apples planner-cost comparison for R-Bot vs QueryTorque."""

from __future__ import annotations

import csv
import json
import math
import re
import time
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any

import psycopg2


ROOT = Path(__file__).resolve().parents[2]
OUTDIR = ROOT / "paper" / "rbot_dsb_full_run_20260213"
QT_ROOT = ROOT / "research" / "ALL_OPTIMIZATIONS" / "postgres_dsb"
RBOT_CSV = OUTDIR / "dsb_full_run_results_full_dimensions.csv"
QT_COST_CSV = OUTDIR / "QUERYTORQUE_DSB_EXPLAIN_COST_REPLAY.csv"
COMMON_CSV = OUTDIR / "RBT_QT_COMMON37_COST_SIDE_BY_SIDE.csv"
SUMMARY_JSON = OUTDIR / "RBT_QT_COST_ALIGNED_SUMMARY.json"
SUMMARY_MD = OUTDIR / "RBT_QT_COST_ALIGNED_SUMMARY.md"

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
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SET statement_timeout TO '120s'")
            cur.execute("SELECT 1")
            cur.fetchone()
            return conn, cur
        except Exception as e:
            last = e
            print(f"[connect] retry {i}/{attempts} failed: {repr(e)}")
            time.sleep(sleep_s)
    raise last  # type: ignore[misc]


def explain_total_cost(cur, sql_text: str) -> float:
    cur.execute("EXPLAIN (FORMAT JSON) " + sql_text)
    val = cur.fetchone()[0]
    if isinstance(val, str):
        val = json.loads(val)
    return float(val[0]["Plan"]["Total Cost"])


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


def collect_qt_best_candidates() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    qdirs = [d for d in sorted(QT_ROOT.glob("query*")) if d.is_dir()]
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
                    "selected_source": None,
                    "validation_status": "ERROR",
                    "validation_best_speedup": None,
                    "optimized_path": None,
                    "error": "missing_original.sql",
                }
            )
            continue

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
            cands.append(
                (STATUS_RANK.get(st, -1), sp if sp is not None else float("-inf"), st, adir.name, op)
            )

        if not cands:
            out.append(
                {
                    "query_id": qid,
                    "query_num": qnum,
                    "original_path": str(orig_path),
                    "selected_source": None,
                    "validation_status": "ERROR",
                    "validation_best_speedup": None,
                    "optimized_path": None,
                    "error": "no_candidates",
                }
            )
            continue

        cands.sort(key=lambda t: (t[0], t[1]), reverse=True)
        _, sp, st, src, op = cands[0]
        out.append(
            {
                "query_id": qid,
                "query_num": qnum,
                "original_path": str(orig_path),
                "selected_source": src,
                "validation_status": st,
                "validation_best_speedup": None if sp in (float("-inf"), float("inf")) else sp,
                "optimized_path": str(op),
                "error": None,
            }
        )
    return out


def compute_qt_costs(cur, qt_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            base = explain_total_cost(cur, orig_sql)
            opt = explain_total_cost(cur, opt_sql)
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


def write_outputs(rbot_rows: list[dict[str, Any]], qt_rows: list[dict[str, Any]]) -> None:
    with QT_COST_CSV.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "query_id",
                "query_num",
                "selected_source",
                "validation_status",
                "validation_best_speedup",
                "baseline_cost",
                "optimized_cost",
                "pct_change",
                "speedup",
                "class",
                "error",
                "original_path",
                "optimized_path",
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

    with COMMON_CSV.open("w", newline="") as f:
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
        "metric_definition": "Planner cost from EXPLAIN (FORMAT JSON); pct_change=(optimized-baseline)/baseline*100; WIN if pct<0, TIE if pct=0, LOSS if pct>0",
        "rbot_full_instances_76": rbot_sum,
        "querytorque_full_variants_52": qt_sum,
        "common_querynum_best_of_each_37": {"rbot": common_r, "querytorque": common_q},
        "counts": {
            "rbot_rows": len(rbot_rows),
            "qt_rows": len(qt_rows),
            "qt_cost_errors": sum(1 for r in qt_rows if r.get("pct_change") is None),
            "common_query_nums": len(common),
        },
        "artifacts": {
            "qt_cost_replay_csv": str(QT_COST_CSV),
            "common37_csv": str(COMMON_CSV),
        },
    }
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2))

    md: list[str] = []
    md.append("# R-Bot vs QueryTorque (Aligned Planner-Cost Metrics)")
    md.append("")
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
    md.append("Artifacts:")
    md.append(f"- `{QT_COST_CSV}`")
    md.append(f"- `{COMMON_CSV}`")
    md.append(f"- `{SUMMARY_JSON}`")
    SUMMARY_MD.write_text("\n".join(md) + "\n")

    print("WROTE", QT_COST_CSV)
    print("WROTE", COMMON_CSV)
    print("WROTE", SUMMARY_JSON)
    print("WROTE", SUMMARY_MD)
    print("RBOT", rbot_sum)
    print("QT", qt_sum)
    print("COMMON RBOT", common_r)
    print("COMMON QT", common_q)


def main() -> int:
    print("[start] loading R-Bot rows")
    rbot_rows = load_rbot_rows()
    print(f"[start] loaded {len(rbot_rows)} R-Bot rows")

    print("[start] selecting QT candidates")
    qt_candidates = collect_qt_best_candidates()
    print(f"[start] selected {len(qt_candidates)} QT candidates")

    print("[db] connecting")
    conn, cur = connect_with_retry()
    try:
        print("[db] computing QT costs via EXPLAIN")
        qt_rows = compute_qt_costs(cur, qt_candidates)
    finally:
        cur.close()
        conn.close()

    write_outputs(rbot_rows, qt_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
