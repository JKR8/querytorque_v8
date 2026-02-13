#!/usr/bin/env python3
"""Rerun unresolved rows for the QT vs R-Bot runtime race and merge outputs."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import race_benchmark_qt_vs_rbot as race


DEFAULT_TARGETS = ["query014_0.log", "query014_1.log", "query092_0.log", "query092_1.log"]
DEFAULT_QT92_SQL = (
    race.ROOT
    / "research"
    / "ALL_OPTIMIZATIONS"
    / "postgres_dsb"
    / "query092_multi"
    / "swarm2_final"
    / "optimized.sql"
)
CANON_CSV = race.OUTDIR / "QUERYTORQUE_vs_RBOT_RUNTIME_RACE.csv"
CANON_JSON = race.OUTDIR / "QUERYTORQUE_vs_RBOT_RUNTIME_RACE.json"
CANON_MD = race.OUTDIR / "QUERYTORQUE_vs_RBOT_RUNTIME_RACE.md"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rerun unresolved rows and merge into canonical runtime race outputs.")
    p.add_argument("--races", type=int, default=2)
    p.add_argument("--timeout-ms", type=int, default=300000)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--tag", default="runtime_race_full76_patched_missing")
    p.add_argument("--qt92-sql", default=str(DEFAULT_QT92_SQL))
    p.add_argument("--targets", nargs="*", default=DEFAULT_TARGETS)
    return p.parse_args()


def recompute_summary(rows: list[dict[str, Any]], races: int, timeout_ms: int, workers: int) -> dict[str, Any]:
    total = len(rows)
    valid = [r for r in rows if r.get("winner")]
    qt_wins = sum(1 for r in valid if r["winner"] == "querytorque")
    rbot_wins = sum(1 for r in valid if r["winner"] == "rbot")
    ties = len(valid) - qt_wins - rbot_wins
    coverage = len(valid) / total * 100.0 if total else 0.0
    pct_diffs = [r["qt_faster_pct_vs_rbot"] for r in valid if r.get("qt_faster_pct_vs_rbot") is not None]
    return {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "dsn": race.DSN.replace("jakc9:jakc9", "***:***"),
        "rows_total": total,
        "rows_with_winner": len(valid),
        "coverage_pct": coverage,
        "races_per_row": races,
        "timeout_ms": timeout_ms,
        "workers": workers,
        "qt_wins": qt_wins,
        "rbot_wins": rbot_wins,
        "ties": ties,
        "qt_win_rate_pct": (qt_wins / len(valid) * 100.0) if valid else None,
        "median_qt_faster_pct_vs_rbot": statistics.median(pct_diffs) if pct_diffs else None,
        "mean_qt_faster_pct_vs_rbot": statistics.mean(pct_diffs) if pct_diffs else None,
    }


def write_outputs(base: Path, rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    rows_sorted = sorted(rows, key=lambda r: r["log_file"])

    fieldnames: list[str] = []
    for r in rows_sorted:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    out_csv = base.with_suffix(".csv")
    out_json = base.with_suffix(".json")
    out_md = base.with_suffix(".md")

    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_sorted)

    out_json.write_text(json.dumps({"summary": summary, "rows": rows_sorted}, indent=2))

    md = []
    md.append("# R-Bot vs QT Runtime Race (Merged)")
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


def main() -> int:
    args = parse_args()
    if not CANON_JSON.exists():
        raise RuntimeError(f"Missing canonical JSON: {CANON_JSON}")

    data = json.loads(CANON_JSON.read_text())
    existing_rows: list[dict[str, Any]] = data.get("rows", [])
    by_log = {r["log_file"]: r for r in existing_rows}

    specs = race.load_row_specs(limit=0)
    spec_by_log = {s.log_file: s for s in specs}
    qt92_sql = Path(args.qt92_sql).read_text()

    rerun_specs = []
    for log_file in args.targets:
        s = spec_by_log.get(log_file)
        if not s:
            print(f"[skip] spec not found: {log_file}", flush=True)
            continue
        if s.query_num == 92:
            s.qt_optimized_sql = qt92_sql
        rerun_specs.append(s)

    if not rerun_specs:
        raise RuntimeError("No specs selected for rerun.")

    print(
        f"[setup] rerunning logs={len(rerun_specs)} races={args.races} timeout_ms={args.timeout_ms} workers={args.workers}",
        flush=True,
    )

    rerun_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(race.bench_row, s, args.races, args.timeout_ms): s for s in rerun_specs}
        done = 0
        total = len(futs)
        for fut in as_completed(futs):
            done += 1
            s = futs[fut]
            row = fut.result()
            rerun_rows.append(row)
            print(
                f"[{done}/{total}] {s.log_file} winner={row['winner']} "
                f"rbot_med={row['rbot_median_ms']} qt_med={row['qt_median_ms']}",
                flush=True,
            )

    for r in rerun_rows:
        by_log[r["log_file"]] = r

    merged_rows = list(by_log.values())
    summary = recompute_summary(
        merged_rows,
        races=args.races,
        timeout_ms=args.timeout_ms,
        workers=args.workers,
    )

    base = race.OUTDIR / f"{args.tag}_r{args.races}_w{args.workers}"
    write_outputs(base, merged_rows, summary)
    write_outputs(CANON_CSV.with_suffix(""), merged_rows, summary)

    print("[done] summary", summary, flush=True)
    print("[done] wrote", base.with_suffix(".csv"), flush=True)
    print("[done] wrote", base.with_suffix(".json"), flush=True)
    print("[done] wrote", base.with_suffix(".md"), flush=True)
    print("[done] updated", CANON_CSV, flush=True)
    print("[done] updated", CANON_JSON, flush=True)
    print("[done] updated", CANON_MD, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
