#!/usr/bin/env python3
"""
Relabel all queries in leaderboard.json using enhanced SQL-diff inference.

Fixes placeholder labels (snipe_worker, final_worker, []), combo labels
(shared_dimension_multi_channel, composite_decorrelate_union, etc.),
and other non-standard transform names.

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.benchmarks.duckdb_tpcds.relabel_leaderboard
"""

from __future__ import annotations

import json
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Import the enhanced inference
from qt_sql.sql_rewriter import infer_transforms_from_sql_diff
from qt_sql.build_blackboard import (
    KNOWN_TRANSFORMS,
    normalize_query_id,
    extract_transforms_from_response,
    load_json,
    load_text,
)

BENCH_DIR = Path(__file__).resolve().parent
LEADERBOARD = BENCH_DIR / "leaderboard.json"
SWARM_DIR = BENCH_DIR / "swarm_batch_20260208_102033"

# Labels that need relabeling
PLACEHOLDER_LABELS = {"snipe_worker", "final_worker", "iter_worker_5", "iter_worker_6"}
COMBO_LABELS = {
    "shared_dimension_multi_channel",
    "composite_decorrelate_union",
    "history_steered",
    "deferred_window_aggregation",
}


def find_sql_pair(qid: str) -> tuple:
    """Find original + optimized SQL for a query from swarm batch or other sources."""
    # Try swarm batch first
    # Convert qid (q88) to dir name (query_88)
    num = qid[1:]  # strip 'q'
    query_dir = SWARM_DIR / f"query_{num}"

    original_sql = None
    optimized_sql = None

    if query_dir.is_dir():
        original_sql = load_text(query_dir / "original.sql") or None

        # Try each benchmark iteration to find the best worker's SQL
        for bench_file in ["benchmark_iter2.json", "benchmark_iter1.json", "benchmark_iter0.json"]:
            bench_data = load_json(query_dir / bench_file)
            if not bench_data:
                continue

            workers = bench_data.get("workers", [])
            # Find best passing worker
            best = None
            for w in workers:
                if w.get("rows_match") and w.get("status") == "pass":
                    if best is None or w.get("speedup", 0) > best.get("speedup", 0):
                        best = w

            if best:
                wid = best.get("worker_id", 0)
                if "iter2" in bench_file:
                    sql_file = "final_worker_sql.sql"
                elif "iter1" in bench_file:
                    sql_file = "snipe_worker_sql.sql"
                else:
                    sql_file = f"worker_{wid}_sql.sql"

                opt_text = load_text(query_dir / sql_file)
                if opt_text:
                    optimized_sql = opt_text
                    break

    # Try global blackboard as fallback
    if not original_sql or not optimized_sql:
        knowledge_path = BENCH_DIR.parent.parent / "knowledge" / "duckdb_tpcds.json"
        if knowledge_path.exists():
            knowledge = load_json(knowledge_path)
            if knowledge:
                queries = knowledge.get("queries", {})
                entry = queries.get(qid, {})
                if not original_sql:
                    original_sql = entry.get("original_sql")
                if not optimized_sql:
                    optimized_sql = entry.get("optimized_sql")

    return original_sql, optimized_sql


def needs_relabeling(transforms: list) -> bool:
    """Check if a query's transforms need relabeling."""
    if not transforms:
        return True

    for t in transforms:
        if t in PLACEHOLDER_LABELS:
            return True
        if t in COMBO_LABELS:
            return True  # Combo labels should be decomposed

    return False


def filter_known_transforms(transforms: list) -> list:
    """Keep only transforms that are in the known set, filtering out combo/placeholder labels."""
    return [t for t in transforms if t in KNOWN_TRANSFORMS]


def relabel_one(qid: str, current_transforms: list) -> list:
    """Relabel a single query's transforms."""
    # First: keep any valid individual transforms from current labels
    valid_existing = filter_known_transforms(current_transforms)

    # If all current transforms are valid known transforms, keep them
    if valid_existing and len(valid_existing) == len(current_transforms):
        return current_transforms  # Nothing to fix

    # Try SQL-diff inference
    original_sql, optimized_sql = find_sql_pair(qid)
    if original_sql and optimized_sql:
        inferred = infer_transforms_from_sql_diff(original_sql, optimized_sql)
        if inferred:
            # Merge: valid existing + newly inferred (dedup)
            merged = list(valid_existing)
            for t in inferred:
                if t not in merged:
                    merged.append(t)
            return merged

    # If inference failed but we have some valid existing, return those
    if valid_existing:
        return valid_existing

    return current_transforms  # Can't improve


def main():
    # Load leaderboard
    with open(LEADERBOARD) as f:
        leaderboard = json.load(f)

    # Backup
    backup_path = BENCH_DIR / f"leaderboard_pre_relabel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    shutil.copy2(LEADERBOARD, backup_path)
    print(f"Backed up to {backup_path.name}")

    queries = leaderboard.get("queries", [])
    print(f"Leaderboard: {len(queries)} queries")

    # Track changes
    changes = []
    unchanged = 0
    failed = 0

    for entry in queries:
        qid = entry.get("query_id", "")
        old_transforms = entry.get("transforms", [])

        if not needs_relabeling(old_transforms):
            unchanged += 1
            continue

        new_transforms = relabel_one(qid, old_transforms)

        if new_transforms != old_transforms:
            changes.append({
                "qid": qid,
                "old": old_transforms,
                "new": new_transforms,
                "speedup": entry.get("speedup", 0),
            })
            entry["transforms"] = new_transforms
        else:
            failed += 1

    # Write updated leaderboard
    leaderboard["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(LEADERBOARD, "w") as f:
        json.dump(leaderboard, f, indent=2)

    # Print summary
    print(f"\n{'=' * 70}")
    print("RELABELING SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Changed:   {len(changes)}")
    print(f"  Unchanged: {unchanged}")
    print(f"  Failed:    {failed}")

    if changes:
        print(f"\n--- Changes ---")
        for c in sorted(changes, key=lambda x: x["speedup"], reverse=True):
            print(f"  {c['qid']:>5s} ({c['speedup']:.2f}x): {c['old']} -> {c['new']}")

    # Verify: count remaining placeholder/combo labels
    remaining_bad = 0
    for entry in queries:
        for t in entry.get("transforms", []):
            if t in PLACEHOLDER_LABELS or t in COMBO_LABELS:
                remaining_bad += 1
                break

    empty_transforms = sum(1 for e in queries if not e.get("transforms", []))

    print(f"\n--- Post-relabel stats ---")
    print(f"  Queries with placeholder/combo labels: {remaining_bad}")
    print(f"  Queries with empty transforms: {empty_transforms}")
    print(f"  Total queries: {len(queries)}")

    print(f"\nDone. Updated {LEADERBOARD.name}")


if __name__ == "__main__":
    main()
