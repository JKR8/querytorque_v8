#!/usr/bin/env python3
"""Build leaderboard_sf10.json from V2 swarm results.

Reads the results/checkpoint from the most recent rbot_comparison_v2_* run
and produces a leaderboard in the same format as the 52-query benchmark.

Also generates an HTML leaderboard for easy viewing.

Usage (from project root):
    python3 packages/qt-sql/ado/benchmarks/postgres_dsb_76/build_leaderboard.py

    # From a specific run directory:
    python3 packages/qt-sql/ado/benchmarks/postgres_dsb_76/build_leaderboard.py \
        --run-dir packages/qt-sql/ado/benchmarks/postgres_dsb_76/rbot_comparison_v2_20260209_...
"""

import argparse
import json
import statistics
from datetime import datetime
from pathlib import Path

BENCHMARK_DIR = Path(__file__).parent
WIN_THRESHOLD = 1.10


def load_results(run_dir: Path) -> list[dict]:
    """Load results from a run directory (results.json or checkpoint.json)."""
    results_path = run_dir / "results.json"
    if results_path.exists():
        data = json.loads(results_path.read_text())
        return data.get("queries", [])

    checkpoint_path = run_dir / "checkpoint.json"
    if checkpoint_path.exists():
        completed = json.loads(checkpoint_path.read_text())
        return list(completed.values())

    return []


def classify_status(speedup: float) -> str:
    if speedup >= WIN_THRESHOLD:
        return "WIN"
    elif speedup >= 1.05:
        return "IMPROVED"
    elif speedup >= 0.95:
        return "NEUTRAL"
    elif speedup > 0:
        return "REGRESSION"
    else:
        return "ERROR"


def build_leaderboard(results: list[dict], source: str) -> dict:
    """Build leaderboard JSON from results."""
    queries = []
    for r in sorted(results, key=lambda x: x.get("speedup", 0), reverse=True):
        speedup = r.get("speedup", 0)
        status = r.get("status", classify_status(speedup))

        # Normalize status to leaderboard categories
        if status in ("CRASH", "SKIP"):
            status = "ERROR"
        elif status not in ("WIN", "IMPROVED", "NEUTRAL", "REGRESSION", "ERROR"):
            status = classify_status(speedup)

        entry = {
            "query_id": r["query_id"],
            "status": status,
            "speedup": round(speedup, 2),
            "original_ms": r.get("original_ms"),
            "optimized_ms": r.get("optimized_ms"),
            "worker": r.get("best_worker"),
            "transforms": r.get("transforms", []),
            "set_local_commands": r.get("set_local_commands"),
            "n_iterations": r.get("n_iterations"),
            "elapsed_s": r.get("elapsed_s"),
        }
        queries.append(entry)

    # Compute summary
    wins = [q for q in queries if q["status"] == "WIN"]
    improved = [q for q in queries if q["status"] == "IMPROVED"]
    neutrals = [q for q in queries if q["status"] == "NEUTRAL"]
    regressions = [q for q in queries if q["status"] == "REGRESSION"]
    errors = [q for q in queries if q["status"] == "ERROR"]

    all_speedups = [q["speedup"] for q in queries if q["speedup"] > 0]
    win_speedups = [q["speedup"] for q in wins]

    leaderboard = {
        "benchmark": "dsb",
        "engine": "postgresql",
        "pg_version": "14.3",
        "scale_factor": 10,
        "n_queries": 76,
        "instances_per_template": 2,
        "validation_method": "5x-trimmed-mean",
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "summary": {
            "total": len(queries),
            "wins": len(wins),
            "improved": len(improved),
            "neutral": len(neutrals),
            "regression": len(regressions),
            "errors": len(errors),
            "avg_speedup": round(statistics.mean(all_speedups), 4) if all_speedups else 0,
            "median_speedup": round(statistics.median(all_speedups), 4) if all_speedups else 0,
            "max_speedup": round(max(all_speedups), 2) if all_speedups else 0,
            "avg_win_speedup": round(statistics.mean(win_speedups), 2) if win_speedups else 0,
        },
        "rbot_comparison": {
            "rbot_improved": 18,
            "rbot_total": 76,
            "rbot_rate_pct": 23.7,
            "qt_improved": len(wins),
            "qt_total": len(queries),
            "qt_rate_pct": round(100 * len(wins) / max(len(queries), 1), 1),
        },
        "queries": queries,
    }

    return leaderboard


def build_html(leaderboard: dict) -> str:
    """Generate HTML leaderboard table."""
    summary = leaderboard["summary"]
    rbot = leaderboard["rbot_comparison"]

    status_colors = {
        "WIN": "#22c55e",
        "IMPROVED": "#86efac",
        "NEUTRAL": "#fbbf24",
        "REGRESSION": "#ef4444",
        "ERROR": "#9ca3af",
    }

    rows = ""
    for i, q in enumerate(leaderboard["queries"], 1):
        color = status_colors.get(q["status"], "#fff")
        transforms = ", ".join(q.get("transforms", [])) or "—"
        worker = f"W{q['worker']}" if q.get("worker") else "—"
        orig = f"{q['original_ms']:.0f}" if q.get("original_ms") else "—"
        opt = f"{q['optimized_ms']:.0f}" if q.get("optimized_ms") else "—"
        rows += f"""
        <tr>
            <td>{i}</td>
            <td>{q['query_id']}</td>
            <td style="color:{color};font-weight:bold">{q['status']}</td>
            <td style="text-align:right">{q['speedup']:.2f}x</td>
            <td style="text-align:right">{orig}</td>
            <td style="text-align:right">{opt}</td>
            <td>{worker}</td>
            <td style="font-size:0.85em">{transforms}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>DSB 76-Query Leaderboard — PostgreSQL SF10</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2em; background: #0f172a; color: #e2e8f0; }}
h1 {{ color: #f8fafc; }}
.summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 1em; margin: 1.5em 0; }}
.card {{ background: #1e293b; padding: 1em; border-radius: 8px; text-align: center; }}
.card .value {{ font-size: 2em; font-weight: bold; }}
.card .label {{ font-size: 0.85em; color: #94a3b8; }}
.comparison {{ background: #1e293b; padding: 1.5em; border-radius: 8px; margin: 1.5em 0; }}
.comparison table {{ width: 100%; border-collapse: collapse; }}
.comparison th, .comparison td {{ padding: 0.5em 1em; text-align: left; }}
.comparison th {{ color: #94a3b8; border-bottom: 1px solid #334155; }}
table.leaderboard {{ width: 100%; border-collapse: collapse; }}
table.leaderboard th {{ background: #1e293b; padding: 0.6em; text-align: left; position: sticky; top: 0; }}
table.leaderboard td {{ padding: 0.5em; border-bottom: 1px solid #1e293b; }}
table.leaderboard tr:hover {{ background: #1e293b; }}
</style>
</head><body>
<h1>DSB 76-Query Leaderboard — PostgreSQL 14.3, SF10</h1>
<p>V2 Swarm — R-Bot comparison benchmark. Generated {leaderboard['updated_at']}</p>

<div class="summary">
    <div class="card"><div class="value" style="color:#22c55e">{summary['wins']}</div><div class="label">WINS</div></div>
    <div class="card"><div class="value" style="color:#86efac">{summary['improved']}</div><div class="label">IMPROVED</div></div>
    <div class="card"><div class="value" style="color:#fbbf24">{summary['neutral']}</div><div class="label">NEUTRAL</div></div>
    <div class="card"><div class="value" style="color:#ef4444">{summary['regression']}</div><div class="label">REGRESSION</div></div>
    <div class="card"><div class="value" style="color:#9ca3af">{summary['errors']}</div><div class="label">ERROR</div></div>
    <div class="card"><div class="value">{summary['max_speedup']:.1f}x</div><div class="label">MAX SPEEDUP</div></div>
</div>

<div class="comparison">
<h3>R-Bot Comparison (DSB 10x)</h3>
<table>
<tr><th>Metric</th><th>R-Bot (GPT-4)</th><th>QueryTorque V2</th></tr>
<tr><td>Queries improved</td><td>{rbot['rbot_improved']}/{rbot['rbot_total']} ({rbot['rbot_rate_pct']}%)</td><td>{rbot['qt_improved']}/{rbot['qt_total']} ({rbot['qt_rate_pct']}%)</td></tr>
<tr><td>Avg winning speedup</td><td>—</td><td>{summary['avg_win_speedup']:.2f}x</td></tr>
</table>
</div>

<table class="leaderboard">
<thead><tr><th>#</th><th>Query</th><th>Status</th><th>Speedup</th><th>Orig (ms)</th><th>Opt (ms)</th><th>Worker</th><th>Transforms</th></tr></thead>
<tbody>{rows}
</tbody></table>
</body></html>"""

    return html


def find_latest_run(benchmark_dir: Path) -> Path | None:
    """Find the most recent rbot_comparison_v2_* directory."""
    runs = sorted(benchmark_dir.glob("rbot_comparison_v2_*"))
    return runs[-1] if runs else None


def main():
    parser = argparse.ArgumentParser(description="Build leaderboard from V2 swarm results")
    parser.add_argument("--run-dir", type=str, default=None,
                        help="Specific run directory (default: latest)")
    args = parser.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
    else:
        run_dir = find_latest_run(BENCHMARK_DIR)

    if not run_dir or not run_dir.exists():
        print("ERROR: No run directory found. Run the benchmark first.")
        print(f"  Looked in: {BENCHMARK_DIR}/rbot_comparison_v2_*")
        return

    print(f"  Loading results from: {run_dir.name}")
    results = load_results(run_dir)
    if not results:
        print("ERROR: No results found in run directory")
        return

    print(f"  Found {len(results)} query results")

    # Build leaderboard
    leaderboard = build_leaderboard(results, source=run_dir.name)

    # Save JSON
    lb_path = BENCHMARK_DIR / "leaderboard_sf10.json"
    lb_path.write_text(json.dumps(leaderboard, indent=2))
    print(f"  Saved: {lb_path}")

    # Save HTML
    html = build_html(leaderboard)
    html_path = BENCHMARK_DIR / "leaderboard_sf10.html"
    html_path.write_text(html)
    print(f"  Saved: {html_path}")

    # Print summary
    s = leaderboard["summary"]
    r = leaderboard["rbot_comparison"]
    print(f"\n  Summary: {s['wins']} WIN / {s['improved']} IMPROVED / "
          f"{s['neutral']} NEUTRAL / {s['regression']} REGR / {s['errors']} ERR")
    print(f"  Improvement rate: {r['qt_rate_pct']}% (R-Bot: {r['rbot_rate_pct']}%)")
    if s["max_speedup"] > 0:
        print(f"  Max speedup: {s['max_speedup']:.2f}x, Avg win: {s['avg_win_speedup']:.2f}x")


if __name__ == "__main__":
    main()
