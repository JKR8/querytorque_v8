#!/usr/bin/env python3
"""Runtime benchmark: R-Bot vs QueryTorque on PostgreSQL DSB."""

from __future__ import annotations

import argparse
import ast
import csv
import json
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg2


ROOT = Path(__file__).resolve().parents[2]
OUTDIR = ROOT / "paper" / "rbot_dsb_full_run_20260213"
RBOT_COST_CSV = OUTDIR / "dsb_full_run_results_full_dimensions.csv"
QT_REPLAY_CSV = OUTDIR / "QUERYTORQUE_DSB_EXPLAIN_COST_REPLAY.csv"
RBOT_SQL_ROOT = Path("/mnt/d/rbot/dsb")
RBOT_LOG_ROOT = Path("/tmp/rbot_logs/full_run_keep081/dsb")
DSN = "postgresql://jakc9:jakc9@127.0.0.1:5434/dsb_sf10?sslmode=disable"


@dataclass
class RuntimeSpec:
    system: str
    query_num: int
    query_id: str
    baseline_sql: str
    optimized_sql: str
    config: dict[str, Any] | None = None
    hint: str | None = None
    meta: dict[str, Any] | None = None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark DB runtimes for QT vs R-Bot.")
    p.add_argument("--runs", type=int, default=2, help="Measured runs per SQL.")
    p.add_argument("--warmup-runs", type=int, default=1, help="Warmup runs per SQL.")
    p.add_argument("--timeout-ms", type=int, default=300000, help="Statement timeout.")
    p.add_argument("--tag", default="runtime_side_by_side", help="Output file tag.")
    p.add_argument("--limit-query-nums", nargs="*", type=int, default=None, help="Optional query_num filter.")
    return p.parse_args()


def split_sql_statements(sql_text: str) -> list[str]:
    parts = [p.strip() for p in sql_text.split(";") if p.strip()]
    return [p + ";" for p in parts]


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


def load_rbot_specs(query_nums_filter: set[int] | None) -> dict[int, RuntimeSpec]:
    rows = []
    with RBOT_COST_CSV.open() as f:
        rd = csv.DictReader(f)
        for r in rd:
            if r.get("status") != "complete":
                continue
            qnum = int(r["query_id"])
            if query_nums_filter and qnum not in query_nums_filter:
                continue
            pct = float(r["pct_change_best"])
            rows.append((qnum, pct, r))

    # Best by minimal pct_change per query_num.
    best_by_qnum: dict[int, dict[str, str]] = {}
    for qnum, _, r in sorted(rows, key=lambda x: (x[0], x[1])):
        if qnum not in best_by_qnum:
            best_by_qnum[qnum] = r

    specs: dict[int, RuntimeSpec] = {}
    for qnum, r in sorted(best_by_qnum.items()):
        inst = int(r["instance"])
        stmt_idx = r["statement_idx"]
        rbot_sql_path = RBOT_SQL_ROOT / f"query{qnum:03d}" / f"query{qnum:03d}_{inst}.sql"
        if not rbot_sql_path.exists():
            continue
        raw_baseline = rbot_sql_path.read_text()
        stmts = split_sql_statements(raw_baseline)
        if stmt_idx != "":
            idx = int(stmt_idx)
            if idx < 0 or idx >= len(stmts):
                continue
            baseline_sql = stmts[idx]
        else:
            baseline_sql = stmts[0] if len(stmts) == 1 else raw_baseline

        log_file = r["log_file"]
        log_path = RBOT_LOG_ROOT / log_file
        if not log_path.exists():
            continue
        candidates = parse_rbot_rewrite_results(log_path)
        if not candidates:
            continue
        target_cost = float(r["output_cost_best"])
        best = min(candidates, key=lambda c: abs(float(c.get("output_cost", 1e300)) - target_cost))
        optimized_sql = best["output_sql"]

        specs[qnum] = RuntimeSpec(
            system="rbot",
            query_num=qnum,
            query_id=f"query{qnum:03d}",
            baseline_sql=baseline_sql,
            optimized_sql=optimized_sql,
            config=None,
            hint=None,
            meta={"log_file": log_file, "instance": inst, "statement_idx": stmt_idx},
        )
    return specs


def load_qt_specs(query_nums_filter: set[int] | None) -> dict[int, RuntimeSpec]:
    rows = list(csv.DictReader(QT_REPLAY_CSV.open()))

    # Best by minimal pct_change per query_num among executable rows.
    by_qnum: dict[int, list[dict[str, str]]] = {}
    for r in rows:
        if not r.get("pct_change"):
            continue
        qnum = int(r["query_num"])
        if query_nums_filter and qnum not in query_nums_filter:
            continue
        by_qnum.setdefault(qnum, []).append(r)

    specs: dict[int, RuntimeSpec] = {}
    for qnum, arr in sorted(by_qnum.items()):
        best = min(arr, key=lambda r: float(r["pct_change"]))
        orig_path = Path(best["original_path"])
        opt_path = Path(best["optimized_path"])
        if not (orig_path.exists() and opt_path.exists()):
            continue
        config = json.loads(best["config_json"]) if best.get("config_json") else None
        hint = best.get("hint") or None
        specs[qnum] = RuntimeSpec(
            system="querytorque",
            query_num=qnum,
            query_id=best["query_id"],
            baseline_sql=orig_path.read_text(),
            optimized_sql=opt_path.read_text(),
            config=config,
            hint=hint,
            meta={
                "best_source": best.get("best_source"),
                "selected_source": best.get("selected_source"),
                "policy_source": best.get("policy_source"),
            },
        )
    return specs


class PgRunner:
    def __init__(self, dsn: str, timeout_ms: int):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.timeout_ms = timeout_ms

    def close(self) -> None:
        self.cur.close()
        self.conn.close()

    def run_once(
        self,
        sql: str,
        config: dict[str, Any] | None = None,
        hint: str | None = None,
    ) -> tuple[float | None, str | None]:
        try:
            self.cur.execute("BEGIN")
            self.cur.execute("SET LOCAL statement_timeout = %s", (str(self.timeout_ms),))
            if config:
                for k, v in config.items():
                    self.cur.execute(f"SET LOCAL {k} = %s", (str(v),))
            full_sql = sql
            if hint:
                self.cur.execute("SAVEPOINT qt_hint")
                try:
                    self.cur.execute("SET LOCAL pg_hint_plan.enable_hint = on")
                except Exception:
                    self.cur.execute("ROLLBACK TO SAVEPOINT qt_hint")
                finally:
                    self.cur.execute("RELEASE SAVEPOINT qt_hint")
                full_sql = f"/*+ {hint} */\n{sql}"
            t0 = time.perf_counter()
            self.cur.execute(full_sql)
            self.cur.fetchall()
            elapsed = (time.perf_counter() - t0) * 1000.0
            self.conn.rollback()
            return elapsed, None
        except Exception as e:
            self.conn.rollback()
            return None, f"{type(e).__name__}: {e}"


def bench_sql(
    runner: PgRunner,
    sql: str,
    config: dict[str, Any] | None,
    hint: str | None,
    warmup_runs: int,
    runs: int,
) -> dict[str, Any]:
    for _ in range(warmup_runs):
        runner.run_once(sql, config=config, hint=hint)
    vals = []
    errs = []
    for _ in range(runs):
        t, e = runner.run_once(sql, config=config, hint=hint)
        if t is not None:
            vals.append(t)
        if e:
            errs.append(e)
    med = statistics.median(vals) if vals else None
    return {"times_ms": vals, "median_ms": med, "errors": errs}


def main() -> int:
    args = parse_args()
    OUTDIR.mkdir(parents=True, exist_ok=True)

    qnum_filter = set(args.limit_query_nums) if args.limit_query_nums else None
    rbot_specs = load_rbot_specs(qnum_filter)
    qt_specs = load_qt_specs(qnum_filter)
    common_nums = sorted(set(rbot_specs) & set(qt_specs))
    if not common_nums:
        raise RuntimeError("No common query_num runtime specs found.")

    print(f"[setup] common query nums: {len(common_nums)}")
    print(f"[setup] runs={args.runs} warmups={args.warmup_runs}")

    runner = PgRunner(DSN, timeout_ms=args.timeout_ms)
    try:
        result_rows = []
        for i, qnum in enumerate(common_nums, 1):
            r_spec = rbot_specs[qnum]
            q_spec = qt_specs[qnum]
            print(f"[{i}/{len(common_nums)}] query_num={qnum}")

            r_base = bench_sql(
                runner,
                r_spec.baseline_sql,
                config=None,
                hint=None,
                warmup_runs=args.warmup_runs,
                runs=args.runs,
            )
            r_opt = bench_sql(
                runner,
                r_spec.optimized_sql,
                config=None,
                hint=None,
                warmup_runs=args.warmup_runs,
                runs=args.runs,
            )
            q_base = bench_sql(
                runner,
                q_spec.baseline_sql,
                config=None,
                hint=None,
                warmup_runs=args.warmup_runs,
                runs=args.runs,
            )
            q_opt = bench_sql(
                runner,
                q_spec.optimized_sql,
                config=q_spec.config,
                hint=q_spec.hint,
                warmup_runs=args.warmup_runs,
                runs=args.runs,
            )

            r_speedup = None
            if r_base["median_ms"] and r_opt["median_ms"] and r_opt["median_ms"] > 0:
                r_speedup = r_base["median_ms"] / r_opt["median_ms"]
            q_speedup = None
            if q_base["median_ms"] and q_opt["median_ms"] and q_opt["median_ms"] > 0:
                q_speedup = q_base["median_ms"] / q_opt["median_ms"]

            winner = None
            if r_opt["median_ms"] is not None and q_opt["median_ms"] is not None:
                winner = "rbot" if r_opt["median_ms"] < q_opt["median_ms"] else "querytorque"

            result_rows.append(
                {
                    "query_num": qnum,
                    "rbot_query_id": r_spec.query_id,
                    "qt_query_id": q_spec.query_id,
                    "rbot_baseline_ms": r_base["median_ms"],
                    "rbot_optimized_ms": r_opt["median_ms"],
                    "rbot_speedup": r_speedup,
                    "qt_baseline_ms": q_base["median_ms"],
                    "qt_optimized_ms": q_opt["median_ms"],
                    "qt_speedup": q_speedup,
                    "optimized_runtime_winner": winner,
                    "rbot_errors": len(r_base["errors"]) + len(r_opt["errors"]),
                    "qt_errors": len(q_base["errors"]) + len(q_opt["errors"]),
                    "qt_best_source": (q_spec.meta or {}).get("best_source"),
                }
            )
    finally:
        runner.close()

    # Summary.
    valid_r = [r["rbot_speedup"] for r in result_rows if r["rbot_speedup"] is not None]
    valid_q = [r["qt_speedup"] for r in result_rows if r["qt_speedup"] is not None]
    win_r = sum(1 for r in result_rows if r["optimized_runtime_winner"] == "rbot")
    win_q = sum(1 for r in result_rows if r["optimized_runtime_winner"] == "querytorque")
    ties = len(result_rows) - win_r - win_q

    summary = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "dsn": DSN.replace("jakc9:jakc9", "***:***"),
        "runs": args.runs,
        "warmup_runs": args.warmup_runs,
        "query_nums_compared": len(result_rows),
        "rbot_runtime_speedup_median": statistics.median(valid_r) if valid_r else None,
        "qt_runtime_speedup_median": statistics.median(valid_q) if valid_q else None,
        "rbot_optimized_faster_count": win_r,
        "qt_optimized_faster_count": win_q,
        "optimized_tie_count": ties,
    }

    base = OUTDIR / f"{args.tag}_runs{args.runs}"
    out_csv = base.with_suffix(".csv")
    out_json = base.with_suffix(".json")
    out_md = base.with_suffix(".md")

    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "query_num",
                "rbot_query_id",
                "qt_query_id",
                "rbot_baseline_ms",
                "rbot_optimized_ms",
                "rbot_speedup",
                "qt_baseline_ms",
                "qt_optimized_ms",
                "qt_speedup",
                "optimized_runtime_winner",
                "rbot_errors",
                "qt_errors",
                "qt_best_source",
            ],
        )
        w.writeheader()
        w.writerows(result_rows)

    out_json.write_text(json.dumps({"summary": summary, "rows": result_rows}, indent=2))

    md = []
    md.append("# Runtime Side-by-Side (DB Execution)")
    md.append("")
    md.append(f"- Runs: `{args.runs}` (plus `{args.warmup_runs}` warmup)")
    md.append(f"- Query nums compared: `{len(result_rows)}`")
    md.append("")
    md.append("| Metric | Value |")
    md.append("|---|---:|")
    md.append(f"| R-Bot median speedup (baseline/optimized) | {summary['rbot_runtime_speedup_median']:.3f} |" if summary["rbot_runtime_speedup_median"] else "| R-Bot median speedup (baseline/optimized) | NA |")
    md.append(f"| QT median speedup (baseline/optimized) | {summary['qt_runtime_speedup_median']:.3f} |" if summary["qt_runtime_speedup_median"] else "| QT median speedup (baseline/optimized) | NA |")
    md.append(f"| R-Bot faster optimized runtime count | {win_r} |")
    md.append(f"| QT faster optimized runtime count | {win_q} |")
    md.append(f"| Optimized runtime ties | {ties} |")
    md.append("")
    md.append(f"Artifacts: `{out_csv}`, `{out_json}`")
    out_md.write_text("\n".join(md) + "\n")

    print("[done]", summary)
    print("[done] wrote", out_csv)
    print("[done] wrote", out_json)
    print("[done] wrote", out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
