#!/usr/bin/env python3
"""TPC-DS Full Benchmark - All 99 queries: baseline + best optimized on SF10.

For each query, runs the original and the single best optimized version.
Best source is picked from: DSR1, Kimi, V2 (based on prior speedup data).
Only benchmarks optimized queries where prior best > 1.05x.

Usage:
    python3 benchmark_sf10_20260206.py                  # full run, all 99
    python3 benchmark_sf10_20260206.py --only 23,88,9   # specific queries
    python3 benchmark_sf10_20260206.py --resume         # pick up where left off
    python3 benchmark_sf10_20260206.py --runs 5         # 5-run trimmed mean

Validation (default 3 runs): discard 1st (warmup), average last 2
Validation (5 runs): trimmed mean (remove min/max, average 3)

Output: research/tpcds_benchmark/results_sf10_20260206.json/.csv
"""
import argparse
import csv
import duckdb
import json
import os
import signal
import sys
import time
from pathlib import Path

# ─── Config ──────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent                     # research/tpcds_benchmark/
RESEARCH = SCRIPT_DIR.parent                           # research/
PROJECT_ROOT = RESEARCH.parent                         # project root
QUERIES_DIR = RESEARCH / "pipeline" / "state_0" / "queries"
MASTER_CSV = RESEARCH / "CONSOLIDATED_BENCHMARKS" / "DuckDB_TPC-DS_Master_v3_20260206.csv"
DB_DEFAULT = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"
OUTPUT_JSON = SCRIPT_DIR / "results_sf10_20260206.json"
OUTPUT_CSV = SCRIPT_DIR / "results_sf10_20260206.csv"

# Optimized SQL file locations per source
def _dsr1_path(q: int) -> Path:
    return RESEARCH / "state" / "responses" / f"q{q}_optimized.sql"

def _kimi_path(q: int) -> Path:
    if q <= 30:
        return RESEARCH / "CONSOLIDATED_BENCHMARKS" / "kimi_q1-q30_optimization" / f"q{q}" / "output_optimized.sql"
    return RESEARCH / "CONSOLIDATED_BENCHMARKS" / "kimi_q31-q99_optimization" / f"q{q}" / "output_optimized.sql"

def _v2_path(q: int) -> Path:
    return RESEARCH / "CONSOLIDATED_BENCHMARKS" / "benchmark_output_v2" / f"q{q}" / "final_optimized.sql"


# ─── Source Selection ────────────────────────────────────────────────────────

def build_source_map() -> dict[int, tuple[str, Path, float]]:
    """Read master CSV and pick best optimized source per query.

    Returns {query_num: (source_name, sql_path, prior_speedup)} for all 99 queries.
    Only includes queries where prior best speedup > 1.05x for optimization testing.
    Queries below threshold still get original-only timing.
    """
    source_map = {}

    # Parse master CSV for prior speedups
    prior_data = {}
    if MASTER_CSV.exists():
        with open(MASTER_CSV) as f:
            for row in csv.DictReader(f):
                try:
                    qnum = int(row["Query_Num"])
                except (ValueError, KeyError):
                    continue
                prior_data[qnum] = row

    for q in range(1, 100):
        candidates = []  # (speedup, source_name, path)
        row = prior_data.get(q, {})

        # DSR1
        p = _dsr1_path(q)
        if p.exists():
            sp = float(row.get("DSR1_Speedup") or 0)
            status = row.get("DSR1_Status", "")
            if status not in ("error", ""):
                candidates.append((sp if sp > 0 else 1.0, "dsr1", p))

        # Kimi
        p = _kimi_path(q)
        if p.exists():
            sp = float(row.get("Kimi_Speedup") or 0)
            status = row.get("Kimi_Status", "")
            if status == "pass":
                candidates.append((sp if sp > 0 else 1.0, "kimi", p))

        # V2
        p = _v2_path(q)
        if p.exists():
            valid = row.get("V2_Syntax_Valid") == "True"
            if valid:
                candidates.append((1.0, "v2", p))

        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            best_sp, best_src, best_path = candidates[0]
            # Only benchmark optimized if prior speedup > 1.05x
            if best_sp > 1.05:
                source_map[q] = (best_src, best_path, best_sp)
            else:
                source_map[q] = ("skip", None, best_sp)
        else:
            source_map[q] = ("none", None, 0)

    return source_map


# ─── Query Execution ─────────────────────────────────────────────────────────

class QueryTimeout(Exception):
    pass

def _alarm_handler(signum, frame):
    raise QueryTimeout("Query timed out")


def split_statements(sql_text: str) -> list[str]:
    """Split SQL file into executable statements, stripping comments."""
    lines = []
    for line in sql_text.split("\n"):
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    joined = "\n".join(lines).strip()
    parts = joined.split(";")
    return [p.strip() for p in parts if p.strip() and len(p.strip()) > 20]


def run_statements(con, stmts: list[str], timeout_s: int) -> tuple[float, int, str | None]:
    """Run a list of SQL statements sequentially. Returns (total_ms, total_rows, error)."""
    total_ms = 0
    total_rows = 0
    try:
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(timeout_s)
        for sql in stmts:
            start = time.perf_counter()
            result = con.execute(sql).fetchall()
            total_ms += (time.perf_counter() - start) * 1000
            total_rows += len(result)
        signal.alarm(0)
        return total_ms, total_rows, None
    except QueryTimeout:
        signal.alarm(0)
        return timeout_s * 1000, 0, "TIMEOUT"
    except Exception as e:
        signal.alarm(0)
        return 0, 0, str(e)[:200]


def compute_mean(times: list[float], runs: int) -> float:
    """Compute validated mean based on number of runs."""
    if runs >= 5 and len(times) >= 5:
        # 5-run trimmed mean: remove min/max, average remaining
        s = sorted(times)
        trimmed = s[1:-1]
        return sum(trimmed) / len(trimmed)
    elif len(times) >= 3:
        # 3-run: discard warmup (1st), average rest
        return sum(times[1:]) / len(times[1:])
    elif len(times) == 2:
        return times[1]
    elif len(times) == 1:
        return times[0]
    return 0


def benchmark_query(con, query_num: int, source_map: dict, runs: int, timeout_s: int) -> dict:
    """Run original + optimized for one query. Return result dict."""
    result = {
        "query": query_num,
        "original_mean_ms": None,
        "original_times": [],
        "original_rows": None,
        "original_error": None,
        "optimized_mean_ms": None,
        "optimized_times": [],
        "optimized_rows": None,
        "optimized_error": None,
        "optimized_source": None,
        "speedup": None,
        "rows_match": None,
    }

    # ── Original ──────────────────────────────────────────────────────────
    orig_file = QUERIES_DIR / f"q{query_num}.sql"
    if not orig_file.exists():
        result["original_error"] = "FILE_NOT_FOUND"
        return result

    orig_stmts = split_statements(orig_file.read_text())

    for i in range(runs):
        ms, rows, err = run_statements(con, orig_stmts, timeout_s)
        if err:
            result["original_error"] = err
            break
        result["original_times"].append(round(ms, 2))
        result["original_rows"] = rows

    if result["original_error"]:
        return result

    result["original_mean_ms"] = round(compute_mean(result["original_times"], runs), 2)

    # ── Optimized ─────────────────────────────────────────────────────────
    source_name, source_path, prior_sp = source_map.get(query_num, ("none", None, 0))
    result["optimized_source"] = source_name
    result["prior_speedup"] = prior_sp

    if source_name in ("skip", "none") or source_path is None or not source_path.exists():
        result["optimized_error"] = "SKIPPED" if source_name == "skip" else "NO_OPTIMIZED_SQL"
        return result

    opt_sql = source_path.read_text()
    opt_stmts = split_statements(opt_sql)
    if not opt_stmts:
        result["optimized_error"] = "EMPTY_SQL"
        return result

    for i in range(runs):
        ms, rows, err = run_statements(con, opt_stmts, timeout_s)
        if err:
            result["optimized_error"] = err
            break
        result["optimized_times"].append(round(ms, 2))
        result["optimized_rows"] = rows

    if result["optimized_error"]:
        return result

    result["optimized_mean_ms"] = round(compute_mean(result["optimized_times"], runs), 2)

    # ── Comparison ────────────────────────────────────────────────────────
    if result["optimized_mean_ms"] and result["optimized_mean_ms"] > 0:
        result["speedup"] = round(result["original_mean_ms"] / result["optimized_mean_ms"], 4)
    result["rows_match"] = (result["original_rows"] == result["optimized_rows"])

    return result


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TPC-DS SF10 Full Benchmark (original + optimized)")
    parser.add_argument("--runs", type=int, default=3, help="Runs per query (3=warmup discard, 5=trimmed mean)")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout per query in seconds")
    parser.add_argument("--db", type=str, default=DB_DEFAULT)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=99)
    parser.add_argument("--only", type=str, default=None, help="Comma-separated query numbers")
    parser.add_argument("--resume", action="store_true", help="Skip already-completed queries")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}")
        sys.exit(1)

    query_nums = ([int(x.strip()) for x in args.only.split(",")] if args.only
                  else list(range(args.start, args.end + 1)))

    # Build source map
    source_map = build_source_map()

    # Resume support
    existing = {}
    if args.resume and OUTPUT_JSON.exists():
        with open(OUTPUT_JSON) as f:
            data = json.load(f)
            existing = {r["query"]: r for r in data.get("results", [])}
        print(f"Resuming: {len(existing)} queries already completed")

    validation = "5-run trimmed mean" if args.runs >= 5 else f"{args.runs}-run (discard warmup, avg rest)"
    print("=" * 80)
    print("TPC-DS SF10 FULL BENCHMARK — Original + Best Optimized")
    print(f"Database:    {args.db}")
    print(f"Queries:     {len(query_nums)} (Q{min(query_nums)}–Q{max(query_nums)})")
    print(f"Runs:        {args.runs} per query per variant")
    print(f"Validation:  {validation}")
    print(f"Timeout:     {args.timeout}s per query")
    print("=" * 80)

    # Show source distribution
    src_counts = {}
    for q in query_nums:
        s = source_map.get(q, ("none", None))[0]
        src_counts[s] = src_counts.get(s, 0) + 1
    print(f"Sources: {', '.join(f'{k}={v}' for k, v in sorted(src_counts.items()))}")
    print()

    con = duckdb.connect(args.db, read_only=True)

    results = []
    wins, passes, regressions, errors = 0, 0, 0, 0

    for i, qnum in enumerate(query_nums):
        # Resume: skip completed
        if qnum in existing and existing[qnum].get("speedup") is not None:
            r = existing[qnum]
            results.append(r)
            sp = r["speedup"]
            tag = "WIN" if sp >= 1.1 else "PASS" if sp >= 0.95 else "REG"
            print(f"[{i+1:3d}/{len(query_nums)}] Q{qnum:02d}: CACHED {sp:.2f}x {tag} ({r['optimized_source']})")
            continue

        src_name, _, prior_sp = source_map.get(qnum, ("none", None, 0))
        print(f"[{i+1:3d}/{len(query_nums)}] Q{qnum:02d} ({src_name}): ", end="", flush=True)

        r = benchmark_query(con, qnum, source_map, args.runs, args.timeout)
        results.append(r)

        # Print result
        if r.get("original_error"):
            errors += 1
            print(f"ORIG_ERROR: {r['original_error'][:50]}")
        elif r.get("optimized_error") == "SKIPPED":
            orig_ms = r["original_mean_ms"]
            o_times = ",".join(f"{t:.0f}" for t in r["original_times"])
            print(f"baseline={orig_ms:.0f}ms [{o_times}] (no opt >1.05x)")
        elif r.get("optimized_error"):
            orig_ms = r["original_mean_ms"]
            print(f"baseline={orig_ms:.0f}ms | OPT_ERROR: {r['optimized_error'][:40]}")
        elif r.get("speedup"):
            sp = r["speedup"]
            orig = r["original_mean_ms"]
            opt = r["optimized_mean_ms"]
            rows_ok = "✓" if r["rows_match"] else "✗ROWS"

            if sp >= 1.1:
                tag = "WIN"
                wins += 1
            elif sp >= 0.95:
                tag = "PASS"
                passes += 1
            else:
                tag = "REGRESSION"
                regressions += 1

            o_times = ",".join(f"{t:.0f}" for t in r["original_times"])
            p_times = ",".join(f"{t:.0f}" for t in r["optimized_times"])
            print(f"{sp:.2f}x {tag:>10} | orig={orig:.0f}ms [{o_times}] → opt={opt:.0f}ms [{p_times}] {rows_ok}")
        else:
            print(f"NO_DATA")

        # Incremental save every 5 queries
        if (i + 1) % 5 == 0 or (i + 1) == len(query_nums):
            _save(results, args, validation)

    con.close()

    _save(results, args, validation)
    _summary(results, wins, passes, regressions, errors, query_nums)


def _save(results: list, args, validation: str):
    sorted_r = sorted(results, key=lambda r: r["query"])
    out = {
        "benchmark": "TPC-DS SF10 Full Benchmark",
        "database": args.db,
        "runs_per_query": args.runs,
        "timeout_s": args.timeout,
        "validation": validation,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": sorted_r,
    }
    with open(OUTPUT_JSON, "w") as f:
        json.dump(out, f, indent=2)

    with open(OUTPUT_CSV, "w") as f:
        f.write("query,original_mean_ms,optimized_mean_ms,speedup,source,rows_match,"
                "orig_run1,orig_run2,orig_run3,opt_run1,opt_run2,opt_run3,error\n")
        for r in sorted_r:
            ot = r.get("original_times", [])
            pt = r.get("optimized_times", [])
            f.write(",".join([
                str(r["query"]),
                f"{r['original_mean_ms']:.2f}" if r.get("original_mean_ms") else "",
                f"{r['optimized_mean_ms']:.2f}" if r.get("optimized_mean_ms") else "",
                f"{r['speedup']:.4f}" if r.get("speedup") else "",
                r.get("optimized_source") or "",
                str(r.get("rows_match", "")),
                f"{ot[0]:.2f}" if len(ot) > 0 else "",
                f"{ot[1]:.2f}" if len(ot) > 1 else "",
                f"{ot[2]:.2f}" if len(ot) > 2 else "",
                f"{pt[0]:.2f}" if len(pt) > 0 else "",
                f"{pt[1]:.2f}" if len(pt) > 1 else "",
                f"{pt[2]:.2f}" if len(pt) > 2 else "",
                r.get("original_error") or r.get("optimized_error") or "",
            ]) + "\n")


def _summary(results, wins, passes, regressions, errors, query_nums):
    print("\n" + "=" * 80)
    print("FINAL RESULTS — TPC-DS SF10")
    print("=" * 80)

    valid = [r for r in results if r.get("speedup")]
    total_orig = sum(r["original_mean_ms"] for r in valid)
    total_opt = sum(r["optimized_mean_ms"] for r in valid)

    print(f"\nQueries:     {len(query_nums)} total")
    print(f"WIN (≥1.1x): {wins}")
    print(f"PASS:         {passes}")
    print(f"REGRESSION:   {regressions}")
    print(f"ERROR:        {errors}")
    if total_opt > 0:
        print(f"\nAggregate:   {total_orig/1000:.1f}s → {total_opt/1000:.1f}s ({total_orig/total_opt:.2f}x)")

    # Top 10 wins
    winners = sorted([r for r in valid if r["speedup"] >= 1.1], key=lambda r: r["speedup"], reverse=True)
    if winners:
        print(f"\n{'─' * 80}")
        print(f"TOP WINS")
        print(f"{'─' * 80}")
        for r in winners[:15]:
            saved = r["original_mean_ms"] - r["optimized_mean_ms"]
            rows_ok = "✓" if r["rows_match"] else "✗"
            print(f"  Q{r['query']:>2}: {r['speedup']:>5.2f}x  "
                  f"({r['original_mean_ms']:>8.1f} → {r['optimized_mean_ms']:>8.1f}ms, "
                  f"saved {saved:>7.1f}ms) [{r['optimized_source']}] {rows_ok}")

    # Regressions
    regs = sorted([r for r in valid if r["speedup"] < 0.95], key=lambda r: r["speedup"])
    if regs:
        print(f"\n{'─' * 80}")
        print(f"REGRESSIONS")
        print(f"{'─' * 80}")
        for r in regs[:10]:
            rows_ok = "✓" if r["rows_match"] else "✗"
            print(f"  Q{r['query']:>2}: {r['speedup']:>5.2f}x  "
                  f"({r['original_mean_ms']:>8.1f} → {r['optimized_mean_ms']:>8.1f}ms) "
                  f"[{r['optimized_source']}] {rows_ok}")

    # Biggest time savings
    by_savings = sorted(valid, key=lambda r: r["original_mean_ms"] - r["optimized_mean_ms"], reverse=True)
    print(f"\n{'─' * 80}")
    print(f"BIGGEST TIME SAVINGS (absolute ms)")
    print(f"{'─' * 80}")
    for r in by_savings[:10]:
        saved = r["original_mean_ms"] - r["optimized_mean_ms"]
        print(f"  Q{r['query']:>2}: {saved:>+8.1f}ms  ({r['original_mean_ms']:.0f} → {r['optimized_mean_ms']:.0f}ms, {r['speedup']:.2f}x)")

    print(f"\nResults saved to: {OUTPUT_JSON}")
    print(f"CSV saved to:     {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
