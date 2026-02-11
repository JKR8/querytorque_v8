#!/usr/bin/env python3
"""Build the GOLD folder: SQL pairs + metadata for every query in both leaderboards.

Creates:
  research/GOLD/
  ├── duckdb_tpcds/
  │   ├── all/{qN}/original.sql, optimized.sql, metadata.json   (every query)
  │   ├── wins/{qN}/...              (WIN queries only, sorted by speedup)
  │   ├── improved/{qN}/...          (IMPROVED queries)
  │   ├── neutral/{qN}/...           (NEUTRAL queries)
  │   └── regression/{qN}/...        (REGRESSION queries)
  ├── pg_dsb/
  │   └── (same structure)
  ├── GOLD_LEADERBOARD_DUCKDB_TPCDS.csv
  ├── GOLD_LEADERBOARD_PG_DSB.csv
  └── README.md
"""

import csv
import json
import os
import shutil
from pathlib import Path

ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
GOLD_DIR = ROOT / "research" / "GOLD"
BENCH_DUCK = ROOT / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "duckdb_tpcds"
BENCH_PG = ROOT / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "postgres_dsb"

STATUS_BUCKET = {
    "WIN": "wins",
    "IMPROVED": "improved",
    "NEUTRAL": "neutral",
    "REGRESSION": "regression",
    "NO_DATA": "no_data",
    "ERROR": "errors",
}


def write_pair(qdir, orig_path, opt_path, meta):
    """Write a base:opt pair into a query directory."""
    qdir.mkdir(parents=True, exist_ok=True)
    if orig_path and orig_path.exists():
        shutil.copy2(orig_path, qdir / "original.sql")
    if opt_path and opt_path.exists():
        shutil.copy2(opt_path, qdir / "optimized.sql")
    with open(qdir / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)


def build_duckdb():
    out = GOLD_DIR / "duckdb_tpcds"

    # Read gold leaderboard CSV
    lb_path = ROOT / "research" / "GOLD_LEADERBOARD_DUCKDB_TPCDS.csv"
    with open(lb_path) as f:
        rows = list(csv.DictReader(f))

    originals_dir = BENCH_DUCK / "queries"
    best_dir = BENCH_DUCK / "best"

    index_entries = []
    stats = {"total": 0, "with_pair": 0, "orig_only": 0, "no_sql": 0}
    by_status = {}

    for row in rows:
        query = row["Query"]         # "Q88"
        qkey = query.lower()         # "q88"
        status = row["Status"]
        speedup = float(row["Best_Speedup"]) if row["Best_Speedup"] else None
        stats["total"] += 1

        # Locate files
        num = qkey[1:]  # "88"
        orig = originals_dir / f"query_{num}.sql"
        opt_sql = best_dir / f"{qkey}.sql"
        opt_json = best_dir / f"{qkey}.json"

        has_orig = orig.exists()
        has_opt = opt_sql.exists()
        if has_opt:
            stats["with_pair"] += 1
        elif has_orig:
            stats["orig_only"] += 1
        else:
            stats["no_sql"] += 1

        meta = {
            "query_id": qkey,
            "query_name": query,
            "engine": "duckdb",
            "benchmark": "TPC-DS",
            "scale_factor": 10,
            "status": status,
            "speedup": speedup,
            "source": row["Best_Source"],
            "transforms": [t.strip() for t in row["Transform"].split(",")] if row["Transform"] else [],
            "original_ms": float(row["Orig_ms"]) if row["Orig_ms"] else None,
            "optimized_ms": float(row["Opt_ms"]) if row["Opt_ms"] else None,
            "notes": row.get("Notes", ""),
        }

        # Enrich from best/ metadata
        if opt_json.exists():
            with open(opt_json) as f:
                best_meta = json.load(f)
            meta["sql_source"] = best_meta.get("source", "unknown")
            meta["sql_speedup"] = best_meta.get("speedup")
            if meta["sql_source"] != meta["source"]:
                meta["sql_source_mismatch"] = True

        index_entries.append(meta)

        # Write to all/ (flat)
        write_pair(out / "all" / qkey, orig if has_orig else None, opt_sql if has_opt else None, meta)

        # Write to status bucket
        bucket = STATUS_BUCKET.get(status, status.lower())
        if has_orig or has_opt:
            write_pair(out / bucket / qkey, orig if has_orig else None, opt_sql if has_opt else None, meta)
            by_status.setdefault(bucket, []).append(meta)

    # Write index (sorted by speedup desc)
    index_entries.sort(key=lambda x: x["speedup"] if x["speedup"] else 0, reverse=True)
    index = {
        "engine": "duckdb",
        "benchmark": "TPC-DS SF10",
        "total": stats["total"],
        "paired": stats["with_pair"],
        "summary": {k: len(v) for k, v in by_status.items()},
        "queries": index_entries,
    }
    with open(out / "index.json", "w") as f:
        json.dump(index, f, indent=2)

    print(f"DuckDB TPC-DS: {stats['total']} queries, {stats['with_pair']} paired, {stats['orig_only']} orig-only, {stats['no_sql']} missing")
    return stats, index


def build_pg():
    out = GOLD_DIR / "pg_dsb"

    lb_path = ROOT / "research" / "GOLD_LEADERBOARD_PG_DSB.csv"
    with open(lb_path) as f:
        rows = list(csv.DictReader(f))

    originals_dir = BENCH_PG / "queries"
    best_dir = BENCH_PG / "best"

    index_entries = []
    stats = {"total": 0, "with_pair": 0, "orig_only": 0, "no_sql": 0}
    by_status = {}

    for row in rows:
        query = row["Query"]  # "query092_multi"
        status = row["Status"]
        speedup = float(row["Best_Speedup"]) if row["Best_Speedup"] else None

        # Skip _orig suffix entries (duplicate regression records)
        if query.endswith("_orig"):
            continue

        stats["total"] += 1

        orig = originals_dir / f"{query}.sql"
        opt_sql = best_dir / f"{query}.sql"
        opt_json = best_dir / f"{query}.json"

        has_orig = orig.exists()
        has_opt = opt_sql.exists()
        if has_opt:
            stats["with_pair"] += 1
        elif has_orig:
            stats["orig_only"] += 1
        else:
            stats["no_sql"] += 1

        meta = {
            "query_id": query,
            "engine": "postgresql",
            "benchmark": "DSB",
            "scale_factor": 10,
            "status": status,
            "speedup": speedup,
            "source": row["Best_Source"],
            "transforms": [t.strip() for t in row["Transforms"].split(",")] if row.get("Transforms") else [],
            "original_ms": float(row["Orig_ms"]) if row["Orig_ms"] else None,
            "optimized_ms": float(row["Opt_ms"]) if row["Opt_ms"] else None,
            "notes": row.get("Notes", ""),
        }

        if opt_json.exists():
            with open(opt_json) as f:
                best_meta = json.load(f)
            meta["sql_source"] = best_meta.get("source", "unknown")
            meta["sql_speedup"] = best_meta.get("speedup")

        index_entries.append(meta)

        # Write to all/
        write_pair(out / "all" / query, orig if has_orig else None, opt_sql if has_opt else None, meta)

        # Write to status bucket
        bucket = STATUS_BUCKET.get(status, status.lower())
        if has_orig or has_opt:
            write_pair(out / bucket / query, orig if has_orig else None, opt_sql if has_opt else None, meta)
            by_status.setdefault(bucket, []).append(meta)

    index_entries.sort(key=lambda x: x["speedup"] if x["speedup"] else 0, reverse=True)
    index = {
        "engine": "postgresql",
        "benchmark": "DSB SF10",
        "total": stats["total"],
        "paired": stats["with_pair"],
        "summary": {k: len(v) for k, v in by_status.items()},
        "queries": index_entries,
    }
    with open(out / "index.json", "w") as f:
        json.dump(index, f, indent=2)

    print(f"PG DSB: {stats['total']} queries, {stats['with_pair']} paired, {stats['orig_only']} orig-only, {stats['no_sql']} missing")
    return stats, index


def write_readme(duck_stats, duck_index, pg_stats, pg_index):
    """Write a comprehensive README with tables."""
    lines = [
        "# GOLD: Best-of-All-Time Optimization Results",
        "",
        "> Built: 2026-02-11",
        "> Every optimization we ever did, paired as original:optimized",
        "",
        "## Contents",
        "",
        "### DuckDB TPC-DS (SF10)",
        f"- **{duck_stats['total']} queries** total",
        f"- **{duck_stats['with_pair']} paired** (original + optimized)",
        f"- Status: {duck_index['summary']}",
        "- Sources: Kimi K2.5, V2 Evolutionary, 3-Worker Retry, 4-Worker Retry, DSR1",
        "",
        "### PostgreSQL DSB (SF10)",
        f"- **{pg_stats['total']} queries** total",
        f"- **{pg_stats['with_pair']} paired** (original + optimized)",
        f"- Status: {pg_index['summary']}",
        "- Sources: V2 Swarm (6 workers), Config Tuning, pg_hint_plan, Regression Retry",
        "",
        "## Directory Structure",
        "",
        "```",
        "GOLD/",
        "├── duckdb_tpcds/",
        "│   ├── index.json            # Master index (sorted by speedup)",
        "│   ├── all/{qN}/             # Every query flat",
        "│   │   ├── original.sql",
        "│   │   ├── optimized.sql",
        "│   │   └── metadata.json",
        "│   ├── wins/{qN}/            # WIN queries (>1.10x)",
        "│   ├── improved/{qN}/        # IMPROVED (1.05x-1.10x)",
        "│   ├── neutral/{qN}/         # NEUTRAL (0.95x-1.05x)",
        "│   └── regression/{qN}/      # REGRESSION (<0.95x)",
        "├── pg_dsb/",
        "│   └── (same structure)",
        "├── GOLD_LEADERBOARD_DUCKDB_TPCDS.csv",
        "├── GOLD_LEADERBOARD_PG_DSB.csv",
        "└── README.md",
        "```",
        "",
        "## DuckDB TPC-DS Full Leaderboard",
        "",
        "| # | Query | Speedup | Status | Transform | Source | Orig (ms) | Opt (ms) |",
        "|---|-------|---------|--------|-----------|--------|-----------|----------|",
    ]

    for i, q in enumerate(duck_index["queries"], 1):
        sp = f"{q['speedup']:.2f}x" if q["speedup"] else "N/A"
        xform = ", ".join(q["transforms"]) if q["transforms"] else ""
        orig = f"{q['original_ms']:.0f}" if q["original_ms"] else ""
        opt = f"{q['optimized_ms']:.0f}" if q["optimized_ms"] else ""
        lines.append(f"| {i} | {q['query_name']} | {sp} | {q['status']} | {xform} | {q['source']} | {orig} | {opt} |")

    lines.extend([
        "",
        "## PostgreSQL DSB Full Leaderboard",
        "",
        "| # | Query | Speedup | Status | Transform | Source | Orig (ms) | Opt (ms) |",
        "|---|-------|---------|--------|-----------|--------|-----------|----------|",
    ])

    for i, q in enumerate(pg_index["queries"], 1):
        sp = f"{q['speedup']:.2f}x" if q["speedup"] else "N/A"
        xform = ", ".join(q["transforms"]) if q["transforms"] else ""
        orig = f"{q['original_ms']:.0f}" if q["original_ms"] else ""
        opt = f"{q['optimized_ms']:.0f}" if q["optimized_ms"] else ""
        lines.append(f"| {i} | {q['query_id']} | {sp} | {q['status']} | {xform} | {q['source']} | {orig} | {opt} |")

    lines.extend([
        "",
        "## Validation Rules",
        "",
        "All speedups validated using one of:",
        "1. **3x runs**: Run 3 times, discard 1st (warmup), average last 2",
        "2. **5x trimmed mean**: Run 5 times, remove min/max, average remaining 3",
        "3. **4x triage (1-2-1-2)**: Interleaved warmup+measure for drift control",
        "",
        "**Single-run timing comparisons are never used.**",
        "",
    ])

    with open(GOLD_DIR / "README.md", "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    # Clean and rebuild
    if GOLD_DIR.exists():
        shutil.rmtree(GOLD_DIR)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # Copy leaderboard CSVs
    for csv_name in ["GOLD_LEADERBOARD_DUCKDB_TPCDS.csv", "GOLD_LEADERBOARD_PG_DSB.csv"]:
        src = ROOT / "research" / csv_name
        if src.exists():
            shutil.copy2(src, GOLD_DIR / csv_name)

    duck_stats, duck_index = build_duckdb()
    pg_stats, pg_index = build_pg()
    write_readme(duck_stats, duck_index, pg_stats, pg_index)

    total = duck_stats["with_pair"] + pg_stats["with_pair"]
    print(f"\n{'='*60}")
    print(f"  GOLD collection built: {total} paired optimizations")
    print(f"  Location: {GOLD_DIR}")
    print(f"{'='*60}")
