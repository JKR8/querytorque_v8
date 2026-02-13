#!/usr/bin/env python3
"""Race benchmark: R-Bot vs QueryTorque optimized SQL on PostgreSQL DSB.

For each R-Bot row (76 expected), run two synchronized races:
- Thread A executes R-Bot optimized SQL
- Thread B executes QT optimized SQL (with optional config/hint)

Outputs per-row race timings and an aggregate summary.
"""

from __future__ import annotations

import argparse
import ast
import csv
import json
import re
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg2


ROOT = Path(__file__).resolve().parents[2]
OUTDIR = ROOT / "paper" / "rbot_dsb_full_run_20260213"
RBOT_COST_CSV = OUTDIR / "dsb_full_run_results_full_dimensions.csv"
QT_REPLAY_CSV = OUTDIR / "QUERYTORQUE_DSB_EXPLAIN_COST_REPLAY.csv"
RBOT_LOG_ROOT = Path("/tmp/rbot_logs/full_run_keep081/dsb")
DSN = "postgresql://jakc9:jakc9@127.0.0.1:5434/dsb_sf10?sslmode=disable"


@dataclass
class RowSpec:
    row_id: str
    query_num: int
    query_template: str
    instance: str
    statement_idx: str
    log_file: str
    rbot_optimized_sql: str
    qt_query_id: str
    qt_optimized_sql: str
    qt_best_source: str | None
    qt_config: dict[str, Any] | None
    qt_hint: str | None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run 2x synchronized R-Bot vs QT races over R-Bot rows.")
    p.add_argument("--races", type=int, default=2, help="Races per row (user asked minimum 2).")
    p.add_argument("--timeout-ms", type=int, default=60000, help="Statement timeout per side.")
    p.add_argument("--workers", type=int, default=3, help="Parallel row workers.")
    p.add_argument("--tag", default="runtime_race_full76", help="Output tag.")
    p.add_argument("--limit", type=int, default=0, help="Optional row limit for smoke test.")
    return p.parse_args()


def parse_rbot_rewrite_results(log_path: Path) -> list[dict[str, Any]]:
    marker = "Rewrite Execution Results: "
    text = log_path.read_text(errors="ignore")
    out = []
    idx = 0
    while True:
        i = text.find(marker, idx)
        if i == -1:
            break
        j = text.find("\n", i)
        if j == -1:
            j = len(text)
        payload = text[i + len(marker) : j]
        try:
            obj = ast.literal_eval(payload)
            if isinstance(obj, dict) and "output_sql" in obj:
                out.append(obj)
        except Exception:
            pass
        idx = j + 1
    return out


def load_qt_by_query_num() -> dict[int, dict[str, Any]]:
    rows = list(csv.DictReader(QT_REPLAY_CSV.open()))
    by_num: dict[int, list[dict[str, str]]] = {}
    for r in rows:
        qn = int(r["query_num"])
        by_num.setdefault(qn, []).append(r)

    chosen: dict[int, dict[str, Any]] = {}
    for qn, arr in sorted(by_num.items()):
        with_pct = [x for x in arr if x.get("pct_change")]
        if with_pct:
            best = min(with_pct, key=lambda x: float(x["pct_change"]))
        else:
            best = arr[0]
        op = Path(best["optimized_path"])
        if not op.exists():
            continue
        chosen[qn] = {
            "query_id": best["query_id"],
            "optimized_sql": op.read_text(),
            "best_source": best.get("best_source"),
            "config": json.loads(best["config_json"]) if best.get("config_json") else None,
            "hint": best.get("hint") or None,
        }
    return chosen


def load_row_specs(limit: int = 0) -> list[RowSpec]:
    qt_map = load_qt_by_query_num()
    specs: list[RowSpec] = []

    with RBOT_COST_CSV.open() as f:
        rd = csv.DictReader(f)
        for r in rd:
            if r.get("status") != "complete":
                continue
            qnum = int(r["query_id"])
            qt = qt_map.get(qnum)
            if not qt:
                continue
            log_file = r["log_file"]
            log_path = RBOT_LOG_ROOT / log_file
            if not log_path.exists():
                continue
            candidates = parse_rbot_rewrite_results(log_path)
            if not candidates:
                continue
            target_cost = float(r["output_cost_best"])
            best = min(candidates, key=lambda c: abs(float(c.get("output_cost", 1e300)) - target_cost))
            rbot_sql = best["output_sql"]
            row_id = f"{log_file}"
            specs.append(
                RowSpec(
                    row_id=row_id,
                    query_num=qnum,
                    query_template=r["query_template"],
                    instance=r["instance"],
                    statement_idx=r["statement_idx"],
                    log_file=log_file,
                    rbot_optimized_sql=rbot_sql,
                    qt_query_id=qt["query_id"],
                    qt_optimized_sql=qt["optimized_sql"],
                    qt_best_source=qt["best_source"],
                    qt_config=qt["config"],
                    qt_hint=qt["hint"],
                )
            )
            if limit and len(specs) >= limit:
                break
    return specs


def run_sql_once(
    sql: str,
    timeout_ms: int,
    config: dict[str, Any] | None = None,
    hint: str | None = None,
) -> tuple[float | None, str | None]:
    conn = None
    cur = None
    t0 = time.perf_counter()
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SET LOCAL statement_timeout = %s", (str(timeout_ms),))
        if config:
            for k, v in config.items():
                if not re.match(r"^[a-z_][a-z0-9_]*$", k):
                    raise ValueError(f"invalid setting name: {k}")
                cur.execute(f"SET LOCAL {k} = %s", (str(v),))
        full_sql = sql
        if hint:
            cur.execute("SAVEPOINT qt_hint")
            try:
                cur.execute("SET LOCAL pg_hint_plan.enable_hint = on")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT qt_hint")
            finally:
                cur.execute("RELEASE SAVEPOINT qt_hint")
            full_sql = f"/*+ {hint} */\n{sql}"
        cur.execute(full_sql)
        cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000.0
        conn.rollback()
        return elapsed, None
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return None, f"{type(e).__name__}: {e}"
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def run_one_race(spec: RowSpec, timeout_ms: int) -> dict[str, Any]:
    barrier = threading.Barrier(2)
    out: dict[str, Any] = {
        "rbot_ms": None,
        "qt_ms": None,
        "rbot_err": None,
        "qt_err": None,
    }

    def rbot_side() -> None:
        try:
            barrier.wait(timeout=120)
        except Exception as e:
            out["rbot_err"] = f"BarrierError: {e}"
            return
        ms, err = run_sql_once(spec.rbot_optimized_sql, timeout_ms=timeout_ms)
        out["rbot_ms"] = ms
        out["rbot_err"] = err

    def qt_side() -> None:
        try:
            barrier.wait(timeout=120)
        except Exception as e:
            out["qt_err"] = f"BarrierError: {e}"
            return
        ms, err = run_sql_once(
            spec.qt_optimized_sql,
            timeout_ms=timeout_ms,
            config=spec.qt_config,
            hint=spec.qt_hint,
        )
        out["qt_ms"] = ms
        out["qt_err"] = err

    t1 = threading.Thread(target=rbot_side, daemon=True)
    t2 = threading.Thread(target=qt_side, daemon=True)
    t1.start()
    t2.start()
    t1.join(timeout=600)
    t2.join(timeout=600)
    if t1.is_alive() and not out["rbot_err"]:
        out["rbot_err"] = "ThreadTimeout"
    if t2.is_alive() and not out["qt_err"]:
        out["qt_err"] = "ThreadTimeout"
    return out


def bench_row(spec: RowSpec, races: int, timeout_ms: int) -> dict[str, Any]:
    race_rows = []
    for i in range(1, races + 1):
        r = run_one_race(spec, timeout_ms=timeout_ms)
        race_rows.append((i, r))

    rbot_times = [r["rbot_ms"] for _, r in race_rows if r["rbot_ms"] is not None]
    qt_times = [r["qt_ms"] for _, r in race_rows if r["qt_ms"] is not None]
    rbot_med = statistics.median(rbot_times) if rbot_times else None
    qt_med = statistics.median(qt_times) if qt_times else None

    winner = None
    pct_diff = None
    ratio = None
    if rbot_med is not None and qt_med is not None and rbot_med > 0 and qt_med > 0:
        winner = "querytorque" if qt_med < rbot_med else "rbot"
        pct_diff = (rbot_med - qt_med) / rbot_med * 100.0
        ratio = rbot_med / qt_med

    out = {
        "row_id": spec.row_id,
        "query_num": spec.query_num,
        "query_template": spec.query_template,
        "instance": spec.instance,
        "statement_idx": spec.statement_idx,
        "log_file": spec.log_file,
        "qt_query_id": spec.qt_query_id,
        "qt_best_source": spec.qt_best_source,
        "rbot_median_ms": rbot_med,
        "qt_median_ms": qt_med,
        "winner": winner,
        "qt_faster_pct_vs_rbot": pct_diff,
        "rbot_over_qt_ratio": ratio,
        "rbot_successes": len(rbot_times),
        "qt_successes": len(qt_times),
        "rbot_errors": sum(1 for _, r in race_rows if r.get("rbot_err")),
        "qt_errors": sum(1 for _, r in race_rows if r.get("qt_err")),
    }
    for i, r in race_rows:
        out[f"race{i}_rbot_ms"] = r["rbot_ms"]
        out[f"race{i}_qt_ms"] = r["qt_ms"]
        out[f"race{i}_rbot_err"] = r["rbot_err"]
        out[f"race{i}_qt_err"] = r["qt_err"]
    return out


def main() -> int:
    args = parse_args()
    OUTDIR.mkdir(parents=True, exist_ok=True)
    specs = load_row_specs(limit=args.limit)
    print(f"[setup] loaded specs={len(specs)} races={args.races} workers={args.workers}", flush=True)
    if not specs:
        raise RuntimeError("No row specs loaded.")

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(bench_row, s, args.races, args.timeout_ms): s for s in specs}
        done = 0
        total = len(futs)
        for fut in as_completed(futs):
            done += 1
            s = futs[fut]
            try:
                row = fut.result()
                rows.append(row)
                print(
                    f"[{done}/{total}] {s.log_file} winner={row['winner']} "
                    f"rbot_med={row['rbot_median_ms']} qt_med={row['qt_median_ms']}",
                    flush=True,
                )
            except Exception as e:
                print(f"[{done}/{total}] {s.log_file} ERROR {e}", flush=True)

    rows.sort(key=lambda r: r["log_file"])
    total = len(rows)
    valid = [r for r in rows if r.get("winner")]
    qt_wins = sum(1 for r in valid if r["winner"] == "querytorque")
    rbot_wins = sum(1 for r in valid if r["winner"] == "rbot")
    ties = len(valid) - qt_wins - rbot_wins
    coverage = len(valid) / total * 100.0 if total else 0.0
    pct_diffs = [r["qt_faster_pct_vs_rbot"] for r in valid if r.get("qt_faster_pct_vs_rbot") is not None]

    summary = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "dsn": DSN.replace("jakc9:jakc9", "***:***"),
        "rows_total": total,
        "rows_with_winner": len(valid),
        "coverage_pct": coverage,
        "races_per_row": args.races,
        "timeout_ms": args.timeout_ms,
        "workers": args.workers,
        "qt_wins": qt_wins,
        "rbot_wins": rbot_wins,
        "ties": ties,
        "qt_win_rate_pct": (qt_wins / len(valid) * 100.0) if valid else None,
        "median_qt_faster_pct_vs_rbot": statistics.median(pct_diffs) if pct_diffs else None,
        "mean_qt_faster_pct_vs_rbot": statistics.mean(pct_diffs) if pct_diffs else None,
    }

    base = OUTDIR / f"{args.tag}_r{args.races}_w{args.workers}"
    out_csv = base.with_suffix(".csv")
    out_json = base.with_suffix(".json")
    out_md = base.with_suffix(".md")

    # Collect fieldnames dynamically.
    fieldnames = []
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    out_json.write_text(json.dumps({"summary": summary, "rows": rows}, indent=2))

    md = []
    md.append("# R-Bot vs QT Runtime Race (Full Rows)")
    md.append("")
    md.append("| Metric | Value |")
    md.append("|---|---:|")
    md.append(f"| Rows total | {summary['rows_total']} |")
    md.append(f"| Rows with winner (coverage) | {summary['rows_with_winner']} ({summary['coverage_pct']:.1f}%) |")
    md.append(f"| Races per row | {summary['races_per_row']} |")
    md.append(f"| QT wins | {summary['qt_wins']} |")
    md.append(f"| R-Bot wins | {summary['rbot_wins']} |")
    md.append(f"| Ties | {summary['ties']} |")
    md.append(f"| QT win rate | {summary['qt_win_rate_pct']:.2f}% |" if summary["qt_win_rate_pct"] is not None else "| QT win rate | NA |")
    md.append(
        f"| Median QT faster % vs R-Bot | {summary['median_qt_faster_pct_vs_rbot']:.2f}% |"
        if summary["median_qt_faster_pct_vs_rbot"] is not None
        else "| Median QT faster % vs R-Bot | NA |"
    )
    md.append("")
    md.append(f"Artifacts: `{out_csv}`, `{out_json}`")
    out_md.write_text("\n".join(md) + "\n")

    print("[done] summary", summary, flush=True)
    print("[done] wrote", out_csv, flush=True)
    print("[done] wrote", out_json, flush=True)
    print("[done] wrote", out_md, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
