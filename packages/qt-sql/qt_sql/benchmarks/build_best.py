#!/usr/bin/env python3
"""Build the best/ folder for a benchmark.

Aggregates the best optimized SQL from all sources (state_0 pipeline,
analyst mode, etc.) into a single best/ directory per benchmark.

Usage:
    python build_best.py duckdb_tpcds
    python build_best.py postgres_dsb
    python build_best.py --all
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

BENCHMARKS_DIR = Path(__file__).parent


def load_leaderboard(benchmark_dir: Path) -> dict:
    """Load leaderboard.json and normalize to dict keyed by query_id.

    Handles standard format {queries: [...]}, bare list, or legacy dict.
    """
    lb_path = benchmark_dir / "leaderboard.json"
    if not lb_path.exists():
        print(f"  No leaderboard.json in {benchmark_dir.name}")
        return {}

    with open(lb_path) as f:
        data = json.load(f)

    if isinstance(data, dict) and "queries" in data:
        return {entry["query_id"]: entry for entry in data["queries"]}
    elif isinstance(data, list):
        return {entry["query_id"]: entry for entry in data}
    elif isinstance(data, dict):
        for k, v in data.items():
            if "query_id" not in v:
                v["query_id"] = k
        return data
    return {}


def load_analyst_winners(benchmark_dir: Path) -> dict:
    """Load analyst_winners/ overrides (SQL + validation)."""
    winners_dir = benchmark_dir / "analyst_winners"
    if not winners_dir.exists():
        return {}

    winners = {}
    for sql_file in sorted(winners_dir.glob("*_optimized.sql")):
        query_id = sql_file.stem.replace("_optimized", "")
        val_file = winners_dir / f"{query_id}_validation.json"

        entry = {"optimized_sql": sql_file.read_text().strip()}
        if val_file.exists():
            with open(val_file) as f:
                entry["validation"] = json.load(f)
        winners[query_id] = entry

    return winners


def build_best(benchmark_name: str):
    """Build best/ folder for a single benchmark."""
    benchmark_dir = BENCHMARKS_DIR / benchmark_name
    if not benchmark_dir.exists():
        print(f"Benchmark '{benchmark_name}' not found")
        return

    print(f"\nBuilding best/ for {benchmark_name}")
    print("=" * 50)

    # Load all sources
    leaderboard = load_leaderboard(benchmark_dir)
    analyst_winners = load_analyst_winners(benchmark_dir)

    if not leaderboard:
        print("  No leaderboard data — skipping")
        return

    # Load config for metadata
    config_path = benchmark_dir / "config.json"
    config = {}
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)

    # Create best/ directory
    best_dir = benchmark_dir / "best"
    best_dir.mkdir(exist_ok=True)

    # Process each query — keep the best optimization
    manifest = {
        "benchmark": benchmark_name,
        "engine": config.get("engine", "unknown"),
        "scale_factor": config.get("scale_factor", "unknown"),
        "built_at": datetime.now().isoformat(),
        "summary": {
            "total_queries": 0,
            "optimized": 0,  # queries with speedup >= 1.0
            "wins": 0,       # speedup >= 1.10
            "improved": 0,   # speedup 1.0 - 1.10
            "avg_speedup": 0,
        },
        "queries": {},
    }

    all_speedups = []

    for query_id, entry in sorted(leaderboard.items()):
        speedup = entry.get("speedup", 0)
        status = entry.get("status", "unknown")
        source = entry.get("source", "state_0")
        optimized_sql = entry.get("optimized_sql", "")

        # Check if analyst_winners has a better version
        # Analyst winners key format: "query_4" vs leaderboard "q4"
        analyst_key_variants = [
            query_id,
            query_id.replace("q", "query_"),
            query_id.replace("query", "q"),
        ]
        for ak in analyst_key_variants:
            if ak in analyst_winners:
                aw = analyst_winners[ak]
                aw_speedup = aw.get("validation", {}).get("speedup", 0)
                if aw_speedup > speedup:
                    optimized_sql = aw["optimized_sql"]
                    speedup = aw_speedup
                    status = aw.get("validation", {}).get("status", status)
                    source = "analyst_mode"
                break

        manifest["summary"]["total_queries"] += 1

        # Only save SQL for queries with actual speedup
        if speedup >= 1.0 and optimized_sql:
            # Save optimized SQL
            sql_path = best_dir / f"{query_id}.sql"
            sql_path.write_text(optimized_sql.strip() + "\n")

            # Save validation metadata
            meta = {
                "query_id": query_id,
                "status": status,
                "speedup": speedup,
                "source": source,
                "original_ms": entry.get("original_ms"),
                "optimized_ms": entry.get("optimized_ms"),
                "rows_match": entry.get("rows_match"),
            }
            meta_path = best_dir / f"{query_id}.json"
            with open(meta_path, "w") as f:
                json.dump(meta, f, indent=2)

            manifest["summary"]["optimized"] += 1
            if speedup >= 1.10:
                manifest["summary"]["wins"] += 1
            else:
                manifest["summary"]["improved"] += 1
            all_speedups.append(speedup)

        # Always track in manifest queries dict
        manifest["queries"][query_id] = {
            "speedup": speedup,
            "status": status,
            "source": source,
            "has_sql": bool(speedup >= 1.0 and optimized_sql),
        }

    # Compute average speedup (only for optimized queries)
    if all_speedups:
        manifest["summary"]["avg_speedup"] = round(
            sum(all_speedups) / len(all_speedups), 4
        )

    # Save manifest
    manifest_path = best_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    # Print summary
    s = manifest["summary"]
    print(f"  Total queries: {s['total_queries']}")
    print(f"  Optimized (>=1.0x): {s['optimized']}")
    print(f"    Wins (>=1.10x): {s['wins']}")
    print(f"    Improved (1.0-1.10x): {s['improved']}")
    print(f"  Avg speedup: {s['avg_speedup']}x")
    print(f"  Output: {best_dir}/")

    # List top 5 winners
    top = sorted(
        [(qid, info) for qid, info in manifest["queries"].items()
         if info["speedup"] >= 1.10],
        key=lambda x: x[1]["speedup"],
        reverse=True,
    )[:5]
    if top:
        print(f"\n  Top 5 winners:")
        for qid, info in top:
            print(f"    {qid}: {info['speedup']}x ({info['source']})")


def main():
    if len(sys.argv) < 2:
        print("Usage: python build_best.py <benchmark_name|--all>")
        sys.exit(1)

    if sys.argv[1] == "--all":
        # Build for all benchmarks that have a config.json
        for d in sorted(BENCHMARKS_DIR.iterdir()):
            if d.is_dir() and (d / "config.json").exists():
                build_best(d.name)
    else:
        build_best(sys.argv[1])


if __name__ == "__main__":
    main()
