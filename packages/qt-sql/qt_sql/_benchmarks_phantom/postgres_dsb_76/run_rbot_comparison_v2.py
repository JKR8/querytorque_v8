#!/usr/bin/env python3
"""Run V2 swarm on ALL 76 DSB queries for R-Bot paper comparison.

Methodology matches R-Bot (VLDB 2025):
  - PostgreSQL 14.3, DSB SF10, 300s timeout
  - 76 queries: 38 templates × 2 parameterized instances (seeds 1001/1002)
  - Per-query improvement measured as speedup ratio
  - Results reported as: improvement rate, avg latency, median, p90

V2 swarm features (vs V1):
  - Analyst-as-interpreter briefing (structured, not free-form)
  - Engine gap profiles (offensive hunting guide)
  - Per-worker SET LOCAL config tuning
  - 4 specialized workers per query

Usage (from project root):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb_76/run_rbot_comparison_v2.py

    # Resume from checkpoint (skips completed queries):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb_76/run_rbot_comparison_v2.py --resume

    # Fan-out only (1 iteration, faster):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb_76/run_rbot_comparison_v2.py --fan-out-only

    # Run specific queries:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb_76/run_rbot_comparison_v2.py \
        --queries query001_multi_i1 query013_agg_i2
"""

import argparse
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(".env")

from ado.pipeline import Pipeline
from ado.schemas import OptimizationMode


# ── Configuration ─────────────────────────────────────────────────────────
BENCHMARK_DIR = "packages/qt-sql/ado/benchmarks/postgres_dsb_76"
QUERIES_DIR = Path(BENCHMARK_DIR) / "queries"
MAX_ITERATIONS = 2   # 1 fan-out + 1 snipe (balance speed vs coverage)
TARGET_SPEEDUP = 2.0
WIN_THRESHOLD = 1.10  # >=1.10x counts as "improved" for R-Bot comparison

# R-Bot paper numbers (Table 5, DSB 10x, GPT-4)
RBOT_DSB_IMPROVED = 18
RBOT_DSB_TOTAL = 76
RBOT_DSB_IMPROVEMENT_RATE = 23.7  # percent
RBOT_DSB_AVG_LATENCY_ORIG = 37.76  # seconds
RBOT_DSB_AVG_LATENCY_OPT = 25.35   # seconds
RBOT_DSB_MEDIAN_ORIG = 5.28
RBOT_DSB_MEDIAN_OPT = 4.58
RBOT_DSB_P90_ORIG = 300.00
RBOT_DSB_P90_OPT = 17.17


def discover_queries(queries_dir: Path) -> list[str]:
    """Auto-discover all query SQL files, sorted by name."""
    sql_files = sorted(queries_dir.glob("*.sql"))
    return [f.stem for f in sql_files]


def _fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


def compute_rbot_metrics(results: list[dict]) -> dict:
    """Compute R-Bot-comparable aggregate metrics.

    Uses actual timing from validation (original_ms, optimized_ms) when
    available.  Falls back to speedup-only improvement rate if timing absent.
    """
    # Improvement rate: only needs speedup
    non_error = [r for r in results if r.get("speedup", 0) > 0]
    n_improved = sum(1 for r in non_error if r["speedup"] >= WIN_THRESHOLD)
    n_total = len(results)

    metrics = {
        "n_total": n_total,
        "n_non_error": len(non_error),
        "n_improved": n_improved,
        "improvement_rate_pct": round(100 * n_improved / max(n_total, 1), 1),
    }

    # Absolute latency from timing data
    with_timing = [
        r for r in results
        if r.get("original_ms") and r["original_ms"] > 0
    ]

    if with_timing:
        orig_times = [r["original_ms"] / 1000.0 for r in with_timing]
        opt_times = []
        for r in with_timing:
            if r.get("optimized_ms") and r["optimized_ms"] > 0:
                opt_times.append(r["optimized_ms"] / 1000.0)
            elif r.get("speedup", 0) > 0:
                opt_times.append((r["original_ms"] / r["speedup"]) / 1000.0)
            else:
                opt_times.append(r["original_ms"] / 1000.0)

        metrics.update({
            "n_with_timing": len(with_timing),
            "avg_latency_orig_s": round(statistics.mean(orig_times), 2),
            "avg_latency_opt_s": round(statistics.mean(opt_times), 2),
            "median_latency_orig_s": round(statistics.median(orig_times), 2),
            "median_latency_opt_s": round(statistics.median(opt_times), 2),
            "p90_latency_orig_s": round(
                sorted(orig_times)[int(0.9 * len(orig_times))], 2
            ),
            "p90_latency_opt_s": round(
                sorted(opt_times)[int(0.9 * len(opt_times))], 2
            ),
            "latency_reduction_pct": round(
                100 * (1 - statistics.mean(opt_times) / statistics.mean(orig_times)), 1
            ),
        })

    return metrics


def print_comparison_table(metrics: dict):
    """Print side-by-side comparison with R-Bot paper numbers."""
    print(f"\n{'='*70}")
    print(f"  R-BOT COMPARISON TABLE (DSB 10x, PostgreSQL 14.3)")
    print(f"{'='*70}")

    fmt = "  {:<30} {:>15} {:>15}"
    print(fmt.format("Metric", "R-Bot (GPT-4)", "QueryTorque V2"))
    print(f"  {'─'*30} {'─'*15} {'─'*15}")

    print(fmt.format(
        "Queries evaluated",
        f"{RBOT_DSB_TOTAL}",
        f"{metrics.get('n_total', '?')}",
    ))
    print(fmt.format(
        "Queries improved",
        f"{RBOT_DSB_IMPROVED}/{RBOT_DSB_TOTAL} ({RBOT_DSB_IMPROVEMENT_RATE}%)",
        f"{metrics.get('n_improved', '?')}/{metrics.get('n_total', '?')} "
        f"({metrics.get('improvement_rate_pct', '?')}%)",
    ))

    if metrics.get("avg_latency_orig_s"):
        print(fmt.format(
            "Avg latency (orig)",
            f"{RBOT_DSB_AVG_LATENCY_ORIG:.2f}s",
            f"{metrics['avg_latency_orig_s']}s",
        ))
        print(fmt.format(
            "Avg latency (opt)",
            f"{RBOT_DSB_AVG_LATENCY_OPT:.2f}s",
            f"{metrics['avg_latency_opt_s']}s",
        ))
        print(fmt.format(
            "Latency reduction",
            f"{100*(1 - RBOT_DSB_AVG_LATENCY_OPT/RBOT_DSB_AVG_LATENCY_ORIG):.1f}%",
            f"{metrics['latency_reduction_pct']}%",
        ))
        print(fmt.format(
            "Median latency (orig)",
            f"{RBOT_DSB_MEDIAN_ORIG:.2f}s",
            f"{metrics['median_latency_orig_s']}s",
        ))
        print(fmt.format(
            "Median latency (opt)",
            f"{RBOT_DSB_MEDIAN_OPT:.2f}s",
            f"{metrics['median_latency_opt_s']}s",
        ))
        print(fmt.format(
            "p90 latency (orig)",
            f"{RBOT_DSB_P90_ORIG:.2f}s",
            f"{metrics['p90_latency_orig_s']}s",
        ))
        print(fmt.format(
            "p90 latency (opt)",
            f"{RBOT_DSB_P90_OPT:.2f}s",
            f"{metrics['p90_latency_opt_s']}s",
        ))

    print(f"{'='*70}\n")


def print_template_rollup(results: list[dict]):
    """Print per-template summary (average across instances)."""
    from collections import defaultdict
    import re

    template_results = defaultdict(list)
    for r in results:
        m = re.match(r"(query\d+_\w+?)_i\d+", r["query_id"])
        if m:
            template_results[m.group(1)].append(r)

    print(f"\n  PER-TEMPLATE ROLLUP (avg across instances):")
    print(f"  {'Template':<25} {'i1':>8} {'i2':>8} {'Avg':>8} {'Status':>10}")
    print(f"  {'─'*25} {'─'*8} {'─'*8} {'─'*8} {'─'*10}")

    for tmpl in sorted(template_results):
        instances = template_results[tmpl]
        speedups = [r.get("speedup", 0) for r in instances]
        avg_sp = statistics.mean(speedups) if speedups else 0
        i1 = next((r.get("speedup", 0) for r in instances if r["query_id"].endswith("_i1")), 0)
        i2 = next((r.get("speedup", 0) for r in instances if r["query_id"].endswith("_i2")), 0)
        status = "WIN" if avg_sp >= WIN_THRESHOLD else ("NEUTRAL" if avg_sp >= 0.95 else "REGR")
        print(f"  {tmpl:<25} {i1:>7.2f}x {i2:>7.2f}x {avg_sp:>7.2f}x {status:>10}")


def main():
    parser = argparse.ArgumentParser(
        description="V2 Swarm — R-Bot comparison benchmark (76 DSB queries)"
    )
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint, skip completed queries")
    parser.add_argument("--queries", nargs="+", default=None,
                        help="Specific query IDs to run (default: all 76)")
    parser.add_argument("--max-iterations", type=int, default=MAX_ITERATIONS,
                        help=f"Max iterations per query (default: {MAX_ITERATIONS})")
    parser.add_argument("--fan-out-only", action="store_true",
                        help="Skip snipe iterations (fan-out only, faster)")
    args = parser.parse_args()

    max_iter = 1 if args.fan_out_only else args.max_iterations

    # Discover queries
    if args.queries:
        query_ids = args.queries
    else:
        query_ids = discover_queries(QUERIES_DIR)

    if not query_ids:
        print(f"ERROR: No queries found in {QUERIES_DIR}")
        sys.exit(1)

    # Output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(BENCHMARK_DIR) / f"rbot_comparison_v2_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Checkpoint file for resume
    checkpoint_path = out_dir / "checkpoint.json"
    completed = {}
    if args.resume:
        # Find most recent rbot_comparison_v2_* directory
        existing = sorted(Path(BENCHMARK_DIR).glob("rbot_comparison_v2_*"))
        if existing:
            latest = existing[-1]
            cp = latest / "checkpoint.json"
            if cp.exists():
                completed = json.loads(cp.read_text())
                out_dir = latest  # Reuse the same output dir
                checkpoint_path = cp
                print(f"  Resuming from {latest.name} ({len(completed)} queries done)")

    t_batch = time.time()
    n_total = len(query_ids)
    n_skip = sum(1 for q in query_ids if q in completed)

    print(f"\n{'#'*70}")
    print(f"  V2 SWARM — R-Bot Comparison Benchmark (76 DSB queries)")
    print(f"  Queries: {n_total} ({n_total - n_skip} to run, {n_skip} already done)")
    print(f"  Mode: V2 analyst-as-interpreter, 4 workers, per-worker SET LOCAL")
    print(f"  Max iterations: {max_iter} (fan-out{'+ snipe' if max_iter > 1 else ' only'})")
    print(f"  Target: {TARGET_SPEEDUP:.1f}x  Win threshold: {WIN_THRESHOLD:.2f}x  Timeout: 300s")
    print(f"  Validation: 5x trimmed mean")
    print(f"  Output: {out_dir}")
    print(f"{'#'*70}\n")

    pipeline = Pipeline(BENCHMARK_DIR)
    results = list(completed.values())  # Start with previously completed

    for i, query_id in enumerate(query_ids, 1):
        if query_id in completed:
            sp = completed[query_id].get("speedup", 0)
            print(f"  [{i}/{n_total}] SKIP {query_id} (done: {sp:.2f}x)")
            continue

        sql_path = QUERIES_DIR / f"{query_id}.sql"
        if not sql_path.exists():
            print(f"  [{i}/{n_total}] SKIP {query_id} — file not found")
            result = {
                "query_id": query_id, "status": "SKIP", "speedup": 0.0,
                "original_ms": None, "optimized_ms": None, "elapsed_s": 0.0,
            }
            results.append(result)
            completed[query_id] = result
            continue

        sql = sql_path.read_text()
        t_query = time.time()

        n_wins = len([r for r in results if r.get("speedup", 0) >= WIN_THRESHOLD])
        print(f"\n{'='*60}")
        print(f"  [{i}/{n_total}] {query_id}")
        print(f"  Batch: {_fmt(time.time() - t_batch)} elapsed, {n_wins} wins so far")
        print(f"{'='*60}")

        try:
            session_result = pipeline.run_optimization_session(
                query_id=query_id,
                sql=sql,
                mode=OptimizationMode.SWARM,
                max_iterations=max_iter,
                target_speedup=TARGET_SPEEDUP,
            )
            elapsed = time.time() - t_query

            # Extract timing and metadata from session result
            original_ms = None
            optimized_ms = None
            worker_id = None
            transforms = []
            set_local_cmds = None

            for it_data in (session_result.iterations or []):
                for wr in it_data.get("worker_results", []):
                    sp = wr.get("speedup", 0)
                    if sp == session_result.best_speedup and sp > 0:
                        worker_id = wr.get("worker_id")
                        transforms = wr.get("transforms", [])
                        set_local_cmds = wr.get("set_local_commands")
                        original_ms = wr.get("original_ms")
                        optimized_ms = wr.get("optimized_ms")

            result = {
                "query_id": query_id,
                "status": session_result.status,
                "speedup": round(session_result.best_speedup, 3),
                "original_ms": original_ms,
                "optimized_ms": optimized_ms,
                "best_worker": worker_id,
                "transforms": transforms,
                "set_local_commands": set_local_cmds,
                "n_iterations": session_result.n_iterations,
                "n_api_calls": session_result.n_api_calls,
                "elapsed_s": round(elapsed, 1),
            }
            results.append(result)
            completed[query_id] = result

            status_icon = "★" if session_result.best_speedup >= WIN_THRESHOLD else "·"
            print(f"  {status_icon} {session_result.best_speedup:.2f}x "
                  f"({session_result.status}) [{_fmt(elapsed)}]")

        except Exception as e:
            elapsed = time.time() - t_query
            print(f"  ✗ CRASH: {e}")
            result = {
                "query_id": query_id, "status": "CRASH", "speedup": 0.0,
                "original_ms": None, "optimized_ms": None,
                "error": str(e), "elapsed_s": round(elapsed, 1),
            }
            results.append(result)
            completed[query_id] = result

        # Save checkpoint after each query
        checkpoint_path.write_text(json.dumps(completed, indent=2))

    # ── Final Summary ─────────────────────────────────────────────────────
    total_elapsed = time.time() - t_batch

    # Categorize results
    wins = [r for r in results if r.get("speedup", 0) >= WIN_THRESHOLD]
    neutrals = [r for r in results if 0.95 <= r.get("speedup", 0) < WIN_THRESHOLD]
    regressions = [r for r in results if 0 < r.get("speedup", 0) < 0.95]
    errors = [r for r in results if r.get("status") in ("ERROR", "CRASH", "SKIP")]

    print(f"\n\n{'#'*70}")
    print(f"  BATCH COMPLETE — {_fmt(total_elapsed)}")
    print(f"{'#'*70}\n")

    # Per-query table
    header = f"  {'Query':<30} {'Status':<10} {'Speedup':>8} {'Worker':>7} {'Time':>8}"
    print(header)
    print(f"  {'─'*30} {'─'*10} {'─'*8} {'─'*7} {'─'*8}")

    for r in sorted(results, key=lambda x: x.get("speedup", 0), reverse=True):
        marker = "★" if r.get("speedup", 0) >= WIN_THRESHOLD else " "
        wid = f"W{r.get('best_worker', '?')}" if r.get("best_worker") else "—"
        print(f" {marker}{r['query_id']:<30} {r.get('status', '?'):<10} "
              f"{r.get('speedup', 0):>7.2f}x {wid:>7} {_fmt(r.get('elapsed_s', 0)):>8}")

    # Template rollup
    print_template_rollup(results)

    # Aggregate summary
    print(f"\n  Summary:")
    print(f"    Wins (>={WIN_THRESHOLD:.2f}x): {len(wins)}/{len(results)}")
    print(f"    Neutral:    {len(neutrals)}/{len(results)}")
    print(f"    Regression: {len(regressions)}/{len(results)}")
    print(f"    Error/Skip: {len(errors)}/{len(results)}")

    if wins:
        speedups = [r["speedup"] for r in wins]
        print(f"    Avg winning speedup: {statistics.mean(speedups):.2f}x")
        print(f"    Max speedup: {max(speedups):.2f}x")

    # R-Bot comparison metrics
    metrics = compute_rbot_metrics(results)
    if metrics:
        print_comparison_table(metrics)

    # ── Save Results ──────────────────────────────────────────────────────
    output = {
        "benchmark": "dsb",
        "engine": "postgresql",
        "pg_version": "14.3",
        "scale_factor": 10,
        "method": "v2_swarm",
        "n_queries": 76,
        "instances_per_template": 2,
        "validation_method": "5x-trimmed-mean",
        "max_iterations": max_iter,
        "target_speedup": TARGET_SPEEDUP,
        "win_threshold": WIN_THRESHOLD,
        "timestamp": datetime.now().isoformat(),
        "total_elapsed_s": round(total_elapsed, 1),
        "summary": {
            "total": len(results),
            "wins": len(wins),
            "neutral": len(neutrals),
            "regression": len(regressions),
            "errors": len(errors),
            "improvement_rate_pct": metrics.get("improvement_rate_pct", 0),
        },
        "rbot_comparison": {
            "rbot_paper": {
                "model": "GPT-4",
                "improved": RBOT_DSB_IMPROVED,
                "total": RBOT_DSB_TOTAL,
                "improvement_rate_pct": RBOT_DSB_IMPROVEMENT_RATE,
                "avg_latency_orig_s": RBOT_DSB_AVG_LATENCY_ORIG,
                "avg_latency_opt_s": RBOT_DSB_AVG_LATENCY_OPT,
                "median_orig_s": RBOT_DSB_MEDIAN_ORIG,
                "median_opt_s": RBOT_DSB_MEDIAN_OPT,
                "p90_orig_s": RBOT_DSB_P90_ORIG,
                "p90_opt_s": RBOT_DSB_P90_OPT,
            },
            "querytorque_v2": metrics,
        },
        "queries": sorted(results, key=lambda x: x.get("speedup", 0), reverse=True),
    }

    results_path = out_dir / "results.json"
    results_path.write_text(json.dumps(output, indent=2))
    print(f"  Results saved: {results_path}")

    # Also save a summary.txt for quick reference
    summary_path = out_dir / "summary.txt"
    with open(summary_path, "w") as f:
        f.write(f"V2 Swarm — R-Bot Comparison Benchmark\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"Engine: PostgreSQL 14.3, DSB SF10\n")
        f.write(f"Method: V2 swarm (analyst-as-interpreter, 4 workers, SET LOCAL)\n")
        f.write(f"Queries: 76 (38 templates × 2 instances)\n")
        f.write(f"Validation: 5x trimmed mean\n")
        f.write(f"Max iterations: {max_iter}\n\n")
        f.write(f"Results: {len(wins)} WIN / {len(neutrals)} NEUTRAL / "
                f"{len(regressions)} REGRESSION / {len(errors)} ERROR\n")
        f.write(f"Improvement rate: {metrics.get('improvement_rate_pct', '?')}% "
                f"(R-Bot: {RBOT_DSB_IMPROVEMENT_RATE}%)\n")
        f.write(f"Total time: {_fmt(total_elapsed)}\n\n")
        f.write(f"Top winners:\n")
        for r in sorted(results, key=lambda x: x.get("speedup", 0), reverse=True)[:15]:
            if r.get("speedup", 0) >= WIN_THRESHOLD:
                f.write(f"  {r['query_id']}: {r['speedup']:.2f}x "
                        f"(W{r.get('best_worker', '?')})\n")
    print(f"  Summary saved: {summary_path}")


if __name__ == "__main__":
    main()
