#!/usr/bin/env python3
"""Sync canonical benchmark artifacts from leaderboard status.

This enforces `leaderboard.json` as canonical status and regenerates:
1) `best/manifest.json` (status-aligned metadata)
2) `pairs.json` (canonical base -> optimized pairs)

By policy, queries with status `error` are excluded from canonical pairs.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def _qid_number(qid: str) -> int:
    if not qid.startswith("q"):
        raise ValueError(f"Unexpected query id format: {qid}")
    return int(qid[1:])


def _query_sql_path(benchmark_dir: Path, query_id: str) -> Path:
    return benchmark_dir / "queries" / f"query_{_qid_number(query_id)}.sql"


def _best_sql_path(benchmark_dir: Path, query_id: str) -> Path:
    return benchmark_dir / "best" / f"{query_id}.sql"


def sync_benchmark(benchmark_dir: Path) -> dict[str, int]:
    lb_path = benchmark_dir / "leaderboard.json"
    if not lb_path.exists():
        raise FileNotFoundError(f"Missing leaderboard: {lb_path}")

    leaderboard = _load_json(lb_path)
    entries = leaderboard.get("queries", [])
    if not isinstance(entries, list):
        raise ValueError(f"Expected list at {lb_path}:queries")

    normalized = sorted(entries, key=lambda e: _qid_number(str(e["query_id"])))
    status_counts: dict[str, int] = {}
    for e in normalized:
        status = str(e.get("status", "unknown")).lower()
        status_counts[status] = status_counts.get(status, 0) + 1

    pairs = []
    per_query_manifest: dict[str, dict[str, Any]] = {}
    non_error_queries = 0
    with_sql = 0
    missing_sql = 0

    for e in normalized:
        query_id = str(e["query_id"])
        status = str(e.get("status", "unknown")).lower()
        source = str(e.get("source", ""))
        speedup = e.get("speedup")
        rows_match = e.get("rows_match")

        query_sql_path = _query_sql_path(benchmark_dir, query_id)
        best_sql_path = _best_sql_path(benchmark_dir, query_id)
        has_best_sql = best_sql_path.exists()

        # Canonical policy: status error => no canonical optimized SQL.
        has_sql = status != "error" and has_best_sql

        if status != "error":
            non_error_queries += 1
            if has_sql:
                with_sql += 1
            else:
                missing_sql += 1

        if has_sql:
            if not query_sql_path.exists():
                raise FileNotFoundError(f"Missing base SQL for {query_id}: {query_sql_path}")
            original_sql = query_sql_path.read_text(encoding="utf-8").strip()
            optimized_sql = best_sql_path.read_text(encoding="utf-8").strip()
            pairs.append(
                {
                    "source": source or "leaderboard",
                    "query": query_id,
                    "status": status,
                    "speedup": speedup,
                    "rows_match": rows_match,
                    "original": original_sql,
                    "optimized": optimized_sql,
                }
            )

        per_query_manifest[query_id] = {
            "status": status,
            "speedup": speedup,
            "source": source,
            "rows_match": rows_match,
            "has_sql": has_sql,
        }

    manifest = {
        "benchmark": leaderboard.get("benchmark", benchmark_dir.name),
        "engine": leaderboard.get("engine", "unknown"),
        "scale_factor": leaderboard.get("scale_factor", "unknown"),
        "built_at": datetime.now(timezone.utc).isoformat(),
        "source": f"{lb_path.name} (canonical status)",
        "summary": {
            "total_queries": len(normalized),
            "non_error_queries": non_error_queries,
            "error_queries": status_counts.get("error", 0),
            "with_sql": with_sql,
            "missing_sql": missing_sql,
            "status_counts": status_counts,
        },
        "queries": per_query_manifest,
    }

    _write_json(benchmark_dir / "best" / "manifest.json", manifest)
    _write_json(benchmark_dir / "pairs.json", pairs)

    return {
        "total_queries": len(normalized),
        "non_error_queries": non_error_queries,
        "error_queries": status_counts.get("error", 0),
        "with_sql": with_sql,
        "missing_sql": missing_sql,
        "pairs": len(pairs),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync canonical artifacts from leaderboard status")
    parser.add_argument("benchmark", help="benchmark directory name under benchmarks/, e.g. duckdb_tpcds")
    args = parser.parse_args()

    base = Path(__file__).resolve().parent
    benchmark_dir = base / args.benchmark
    if not benchmark_dir.exists():
        raise FileNotFoundError(f"Benchmark directory not found: {benchmark_dir}")

    result = sync_benchmark(benchmark_dir)
    print(
        "Synced {benchmark}: total={total_queries} non_error={non_error_queries} "
        "errors={error_queries} with_sql={with_sql} missing_sql={missing_sql} pairs={pairs}".format(
            benchmark=args.benchmark, **result
        )
    )


if __name__ == "__main__":
    main()
