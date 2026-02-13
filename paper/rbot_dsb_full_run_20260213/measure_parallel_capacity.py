#!/usr/bin/env python3
"""Measure feasible parallel DB executions before runtime degradation."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import psycopg2


ROOT = Path(__file__).resolve().parents[2]
BEST_DIR = ROOT / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "postgres_dsb" / "best"
OUTDIR = ROOT / "paper" / "rbot_dsb_full_run_20260213"
DSN = "postgresql://jakc9:jakc9@127.0.0.1:5434/dsb_sf10?sslmode=disable"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parallel runtime capacity sweep on PostgreSQL DSB.")
    p.add_argument(
        "--queries",
        nargs="+",
        default=["query010_multi", "query023_multi", "query069_multi", "query081_multi"],
        help="Query IDs from benchmarks/postgres_dsb/best (without .sql).",
    )
    p.add_argument(
        "--levels",
        nargs="+",
        type=int,
        default=[1, 2, 4, 6, 8],
        help="Concurrent execution levels to test.",
    )
    p.add_argument("--rounds", type=int, default=4, help="Rounds per (query, level).")
    p.add_argument("--warmup-rounds", type=int, default=1, help="Warmup rounds per query at level=1.")
    p.add_argument("--timeout-ms", type=int, default=300000, help="Per statement timeout.")
    p.add_argument(
        "--degrade-threshold-pct",
        type=float,
        default=10.0,
        help="Max acceptable p50 runtime inflation vs level-1 baseline.",
    )
    p.add_argument(
        "--tag",
        default="parallel_capacity",
        help="Output filename tag under paper/rbot_dsb_full_run_20260213.",
    )
    return p.parse_args()


def load_sql(query_id: str) -> str:
    path = BEST_DIR / f"{query_id}.sql"
    if not path.exists():
        raise FileNotFoundError(f"missing SQL: {path}")
    return path.read_text()


def run_once(sql: str, timeout_ms: int) -> tuple[float | None, str | None]:
    t0 = time.perf_counter()
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SET LOCAL statement_timeout = %s", (str(timeout_ms),))
        cur.execute(sql)
        cur.fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        conn.rollback()
        return elapsed_ms, None
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


def run_concurrent_batch(sql: str, concurrency: int, timeout_ms: int) -> list[dict[str, Any]]:
    barrier = threading.Barrier(concurrency)
    out: list[dict[str, Any]] = [{"elapsed_ms": None, "error": None} for _ in range(concurrency)]

    def worker(slot: int) -> None:
        try:
            barrier.wait(timeout=120)
        except Exception as e:
            out[slot] = {"elapsed_ms": None, "error": f"BarrierError: {e}"}
            return
        elapsed, err = run_once(sql, timeout_ms=timeout_ms)
        out[slot] = {"elapsed_ms": elapsed, "error": err}

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=600)
    return out


def p50(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def summarize_level(
    all_rows: list[dict[str, Any]],
    baseline_p50_by_query: dict[str, float],
) -> dict[str, Any]:
    by_query: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        by_query[row["query_id"]].append(row)

    per_query = {}
    degradations = []
    total_errors = 0
    for qid, rows in sorted(by_query.items()):
        ok = [r["elapsed_ms"] for r in rows if r["elapsed_ms"] is not None]
        errs = [r["error"] for r in rows if r["error"]]
        total_errors += len(errs)
        med = p50(ok)
        base = baseline_p50_by_query.get(qid)
        deg = None
        if med is not None and base and base > 0:
            deg = (med / base - 1.0) * 100.0
            degradations.append(deg)
        per_query[qid] = {
            "p50_ms": med,
            "n_ok": len(ok),
            "n_err": len(errs),
            "degrade_pct_vs_l1": deg,
        }

    return {
        "per_query": per_query,
        "total_errors": total_errors,
        "worst_degrade_pct": max(degradations) if degradations else None,
        "median_degrade_pct": float(statistics.median(degradations)) if degradations else None,
    }


def main() -> int:
    args = parse_args()
    OUTDIR.mkdir(parents=True, exist_ok=True)

    sql_by_query = {qid: load_sql(qid) for qid in args.queries}
    print(f"[setup] queries={args.queries}")
    print(f"[setup] levels={args.levels} rounds={args.rounds}")

    # Warmup
    for qid, sql in sql_by_query.items():
        for _ in range(args.warmup_rounds):
            _, _ = run_once(sql, timeout_ms=args.timeout_ms)
        print(f"[warmup] {qid} done")

    rows: list[dict[str, Any]] = []
    for level in args.levels:
        print(f"[level] {level} start")
        for qid, sql in sql_by_query.items():
            for r in range(1, args.rounds + 1):
                batch = run_concurrent_batch(sql, concurrency=level, timeout_ms=args.timeout_ms)
                for slot, rec in enumerate(batch):
                    rows.append(
                        {
                            "level": level,
                            "query_id": qid,
                            "round": r,
                            "slot": slot,
                            "elapsed_ms": rec["elapsed_ms"],
                            "error": rec["error"],
                        }
                    )
            print(f"  [query] {qid} done")
        print(f"[level] {level} done")

    # Build baseline from level=1
    baseline_rows = [r for r in rows if r["level"] == 1]
    baseline_by_query: dict[str, float] = {}
    for qid in args.queries:
        vals = [r["elapsed_ms"] for r in baseline_rows if r["query_id"] == qid and r["elapsed_ms"] is not None]
        med = p50(vals)
        if med is None:
            raise RuntimeError(f"no baseline timings for {qid}")
        baseline_by_query[qid] = med

    level_summaries = {}
    safe_levels = []
    for level in args.levels:
        subset = [r for r in rows if r["level"] == level]
        s = summarize_level(subset, baseline_by_query)
        level_summaries[str(level)] = s
        worst = s["worst_degrade_pct"]
        if level == 1:
            safe_levels.append(level)
        elif s["total_errors"] == 0 and worst is not None and worst <= args.degrade_threshold_pct:
            safe_levels.append(level)

    max_safe = max(safe_levels) if safe_levels else 1

    summary = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "dsn": DSN.replace("jakc9:jakc9", "***:***"),
        "queries": args.queries,
        "levels": args.levels,
        "rounds": args.rounds,
        "warmup_rounds": args.warmup_rounds,
        "degrade_threshold_pct": args.degrade_threshold_pct,
        "baseline_p50_ms": baseline_by_query,
        "level_summaries": level_summaries,
        "max_safe_parallelism": max_safe,
    }

    base = OUTDIR / f"{args.tag}_r{args.rounds}_q{len(args.queries)}"
    summary_json = base.with_suffix(".json")
    details_csv = base.with_suffix(".csv")
    report_md = base.with_suffix(".md")

    summary_json.write_text(json.dumps(summary, indent=2))

    with details_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["level", "query_id", "round", "slot", "elapsed_ms", "error"])
        w.writeheader()
        w.writerows(rows)

    md: list[str] = []
    md.append("# Parallel Capacity Sweep")
    md.append("")
    md.append(f"Queries: `{args.queries}`")
    md.append(f"Levels: `{args.levels}`")
    md.append(f"Rounds: `{args.rounds}`")
    md.append(f"Threshold: `{args.degrade_threshold_pct}%` worst-query p50 inflation vs level-1")
    md.append("")
    md.append(f"**Max safe parallelism: {max_safe}**")
    md.append("")
    md.append("| Level | Errors | Worst Degrade % | Median Degrade % |")
    md.append("|---|---:|---:|---:|")
    for level in args.levels:
        s = level_summaries[str(level)]
        wd = s["worst_degrade_pct"]
        mdg = s["median_degrade_pct"]
        wd = f"{wd:.2f}" if isinstance(wd, (int, float)) else "NA"
        mdg = f"{mdg:.2f}" if isinstance(mdg, (int, float)) else "NA"
        md.append(f"| {level} | {s['total_errors']} | {wd} | {mdg} |")
    md.append("")
    md.append("Baseline p50 (ms):")
    for qid in args.queries:
        md.append(f"- `{qid}`: `{baseline_by_query[qid]:.2f}`")
    md.append("")
    md.append(f"Artifacts: `{summary_json}`, `{details_csv}`")

    report_md.write_text("\n".join(md) + "\n")

    print(f"[done] max_safe_parallelism={max_safe}")
    print(f"[done] wrote {summary_json}")
    print(f"[done] wrote {details_csv}")
    print(f"[done] wrote {report_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
