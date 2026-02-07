#!/usr/bin/env python3
"""One-time migration: convert all leaderboard.json files to standard format.

Standard format:
{
  "benchmark": "duckdb_tpcds",
  "engine": "duckdb",
  "scale_factor": 10,
  "updated_at": "2026-02-07T...",
  "summary": { total, wins, improved, neutral, regression, errors, avg_speedup },
  "queries": [
    { query_id, status, speedup, original_ms, optimized_ms,
      original_times, optimized_times, rows_match, source, transforms,
      original_sql, optimized_sql, error }
  ]
}
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

BENCHMARKS_DIR = Path(__file__).parent

# Canonical query fields (in order)
CANONICAL_FIELDS = [
    "query_id", "status", "speedup", "original_ms", "optimized_ms",
    "original_times", "optimized_times", "rows_match",
    "source", "transforms", "original_sql", "optimized_sql", "error",
]


def classify_status(speedup: float, error: str = None) -> str:
    """Derive status from speedup."""
    if error:
        return "ERROR"
    if speedup >= 1.10:
        return "WIN"
    if speedup >= 1.0:
        return "IMPROVED"
    if speedup >= 0.95:
        return "NEUTRAL"
    return "REGRESSION"


def compute_summary(queries: list) -> dict:
    """Compute summary stats from query list."""
    total = len(queries)
    wins = sum(1 for q in queries if q.get("status") == "WIN")
    improved = sum(1 for q in queries if q.get("status") == "IMPROVED")
    neutral = sum(1 for q in queries if q.get("status") == "NEUTRAL")
    regression = sum(1 for q in queries if q.get("status") == "REGRESSION")
    errors = sum(1 for q in queries if q.get("status") in ("ERROR", "error"))
    speedups = [q["speedup"] for q in queries if q.get("speedup", 0) > 0]
    avg_speedup = round(sum(speedups) / len(speedups), 4) if speedups else 0

    return {
        "total": total,
        "wins": wins,
        "improved": improved,
        "neutral": neutral,
        "regression": regression,
        "errors": errors,
        "avg_speedup": avg_speedup,
    }


def normalize_transforms(raw) -> list:
    """Normalize transforms to list of strings."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw:
        return [t.strip() for t in raw.split(",") if t.strip()]
    return []


def normalize_query(entry: dict) -> dict:
    """Normalize a single query entry to canonical fields."""
    # Normalize status casing
    status = entry.get("status", "unknown").upper()
    if status in ("WRONG_RESULTS", "PARSE_ERROR"):
        status = "ERROR"

    # Normalize transforms
    transforms = normalize_transforms(
        entry.get("transforms") or entry.get("transforms_applied")
        or entry.get("transform") or []
    )

    return {
        "query_id": entry.get("query_id", entry.get("query", "")),
        "status": status,
        "speedup": entry.get("speedup", 0),
        "original_ms": entry.get("original_ms", 0),
        "optimized_ms": entry.get("optimized_ms", 0),
        "original_times": entry.get("original_times", entry.get("original_runs_ms", [])),
        "optimized_times": entry.get("optimized_times", entry.get("optimized_runs_ms", [])),
        "rows_match": entry.get("rows_match"),
        "source": entry.get("source", "state_0"),
        "transforms": transforms,
        "original_sql": entry.get("original_sql", ""),
        "optimized_sql": entry.get("optimized_sql", ""),
        "error": entry.get("error") or None,
    }


def migrate_benchmark(benchmark_name: str):
    """Migrate a single benchmark's leaderboard.json."""
    benchmark_dir = BENCHMARKS_DIR / benchmark_name
    lb_path = benchmark_dir / "leaderboard.json"

    if not lb_path.exists():
        print(f"  {benchmark_name}: no leaderboard.json — skip")
        return

    config_path = benchmark_dir / "config.json"
    config = {}
    if config_path.exists():
        config = json.loads(config_path.read_text())

    raw = json.loads(lb_path.read_text())

    # Detect format
    if isinstance(raw, dict) and "queries" in raw:
        fmt = "wrapped_list"
        raw_queries = raw["queries"]
    elif isinstance(raw, list):
        fmt = "bare_list"
        raw_queries = raw
    elif isinstance(raw, dict):
        fmt = "dict_keyed"
        raw_queries = list(raw.values())
    else:
        print(f"  {benchmark_name}: unknown format — skip")
        return

    print(f"  {benchmark_name}: format={fmt}, entries={len(raw_queries)}")

    # Normalize all queries
    queries = [normalize_query(q) for q in raw_queries]

    # Merge analyst_winners/ overrides (better speedup wins)
    winners_dir = benchmark_dir / "analyst_winners"
    if winners_dir.exists():
        queries_by_id = {q["query_id"]: q for q in queries}
        for sql_file in sorted(winners_dir.glob("*_optimized.sql")):
            raw_id = sql_file.stem.replace("_optimized", "")
            val_file = winners_dir / f"{raw_id}_validation.json"
            if not val_file.exists():
                continue
            val = json.loads(val_file.read_text())
            # Map analyst_winners key to leaderboard query_id
            # analyst uses "query_4" → leaderboard uses "q4"
            lb_id = raw_id.replace("query_", "q")
            optimized_sql = sql_file.read_text().strip()

            if lb_id in queries_by_id:
                existing = queries_by_id[lb_id]
                if val.get("speedup", 0) > existing.get("speedup", 0):
                    existing["speedup"] = val["speedup"]
                    existing["status"] = val.get("status", "WIN")
                    existing["original_ms"] = val.get("original_ms", existing["original_ms"])
                    existing["optimized_ms"] = val.get("optimized_ms", existing["optimized_ms"])
                    existing["original_times"] = val.get("original_runs_ms", existing["original_times"])
                    existing["optimized_times"] = val.get("optimized_runs_ms", existing["optimized_times"])
                    existing["rows_match"] = val.get("rows_match", existing["rows_match"])
                    existing["source"] = "analyst_mode"
                    existing["optimized_sql"] = optimized_sql
                    print(f"    Merged analyst winner: {lb_id} → {val['speedup']}x")
            else:
                # New query (e.g., q23a split from q23)
                new_entry = {
                    "query_id": lb_id,
                    "status": val.get("status", "WIN"),
                    "speedup": val.get("speedup", 0),
                    "original_ms": val.get("original_ms", 0),
                    "optimized_ms": val.get("optimized_ms", 0),
                    "original_times": val.get("original_runs_ms", []),
                    "optimized_times": val.get("optimized_runs_ms", []),
                    "rows_match": val.get("rows_match"),
                    "source": "analyst_mode",
                    "transforms": [],
                    "original_sql": "",
                    "optimized_sql": optimized_sql,
                    "error": None,
                }
                # Try to load original SQL from queries/ dir
                for ext in [f"{raw_id}.sql", f"{lb_id}.sql"]:
                    orig_path = benchmark_dir / "queries" / ext
                    if orig_path.exists():
                        new_entry["original_sql"] = orig_path.read_text().strip()
                        break
                queries.append(new_entry)
                print(f"    Added new analyst winner: {lb_id} → {val['speedup']}x")

    # Sort by speedup descending (winners first)
    queries.sort(key=lambda q: q["speedup"], reverse=True)

    # Build standard wrapper
    standard = {
        "benchmark": config.get("benchmark", benchmark_name),
        "engine": config.get("engine", "unknown"),
        "scale_factor": config.get("scale_factor", "unknown"),
        "updated_at": datetime.now().isoformat(),
        "summary": compute_summary(queries),
        "queries": queries,
    }

    # Backup original
    backup = lb_path.with_suffix(".json.bak")
    shutil.copy2(lb_path, backup)
    print(f"    Backup: {backup.name}")

    # Write standardized
    lb_path.write_text(json.dumps(standard, indent=2))

    s = standard["summary"]
    print(f"    => {s['total']}Q: {s['wins']}W {s['improved']}I {s['neutral']}N "
          f"{s['regression']}R {s['errors']}E | avg {s['avg_speedup']}x")


def main():
    print("Migrating all benchmark leaderboards to standard format\n")
    for d in sorted(BENCHMARKS_DIR.iterdir()):
        if d.is_dir() and (d / "config.json").exists():
            migrate_benchmark(d.name)

    print("\nDone. Originals backed up as .json.bak")


if __name__ == "__main__":
    main()
