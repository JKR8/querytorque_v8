#!/usr/bin/env python3
"""Build strategy leaderboard: per-(archetype, transform) success rates.

Reads duckdb_tpcds.json (all_attempts from Kimi, Retry3W, Retry4W, Swarm),
classifies each query by structural archetype, and computes success/regression
rates per (archetype, transform) cell.

Output: strategy_leaderboard.json — consumed by the V2 analyst prompt to
guide transform selection based on observed data.

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.benchmarks.duckdb_tpcds.build_strategy_leaderboard
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
KNOWLEDGE_FILE = SCRIPT_DIR / "knowledge" / "duckdb_tpcds.json"
QUERIES_DIR = SCRIPT_DIR / "queries"
OUTPUT_FILE = SCRIPT_DIR / "strategy_leaderboard.json"

# Thresholds
WIN_THRESHOLD = 1.1        # speedup >= 1.1x = success
REGRESSION_THRESHOLD = 0.95  # speedup < 0.95x = regression
ELIMINATION_MIN_ATTEMPTS = 5
ELIMINATION_MAX_SUCCESS_RATE = 0.15


def _normalize_query_id(qid: str) -> str:
    """Normalize q1 -> query_1, q23a -> query_23a, etc."""
    m = re.match(r"^q(\d+\w*)$", qid)
    if m:
        return f"query_{m.group(1)}"
    return qid


def _load_query_sql(query_id: str, original_sql_from_knowledge: str | None) -> str | None:
    """Load query SQL, preferring the file on disk over the knowledge blob."""
    # Try file first (cleaner, no optimized SQL mixed in)
    sql_path = QUERIES_DIR / f"{query_id}.sql"
    if sql_path.exists():
        return sql_path.read_text().strip()
    # Fall back to knowledge JSON's original_sql
    if original_sql_from_knowledge:
        return original_sql_from_knowledge.strip()
    return None


def build_leaderboard() -> Dict:
    """Build the complete strategy leaderboard."""
    from ado.faiss_builder import extract_tags, classify_category

    # Load knowledge
    if not KNOWLEDGE_FILE.exists():
        print(f"ERROR: {KNOWLEDGE_FILE} not found")
        sys.exit(1)

    knowledge = json.loads(KNOWLEDGE_FILE.read_text())
    queries = knowledge.get("queries", {})
    print(f"Loaded {len(queries)} queries from {KNOWLEDGE_FILE.name}")

    # Step 1: Classify each query by archetype
    query_archetypes: Dict[str, str] = {}
    query_sql_cache: Dict[str, str] = {}
    classify_errors = 0

    for qid, qdata in queries.items():
        norm_id = _normalize_query_id(qid)
        sql = _load_query_sql(norm_id, qdata.get("original_sql"))
        if not sql:
            print(f"  WARN: No SQL for {qid}, skipping")
            classify_errors += 1
            continue

        query_sql_cache[qid] = sql
        tags = extract_tags(sql, dialect="duckdb")
        archetype = classify_category(tags)
        query_archetypes[norm_id] = archetype

    print(f"Classified {len(query_archetypes)} queries ({classify_errors} errors)")

    # Step 2: Aggregate per (archetype, transform) cell
    # cell_key = (archetype, transform)
    cell_attempts: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    cell_successes: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    cell_regressions: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    cell_queries_won: Dict[Tuple[str, str], Set[str]] = defaultdict(set)

    # Also track per-archetype totals
    arch_queries: Dict[str, Set[str]] = defaultdict(set)
    arch_attempts: Dict[str, int] = defaultdict(int)
    arch_wins: Dict[str, int] = defaultdict(int)
    arch_speedups: Dict[str, List[float]] = defaultdict(list)

    total_attempts = 0
    skipped_no_rows = 0
    skipped_no_transforms = 0

    for qid, qdata in queries.items():
        norm_id = _normalize_query_id(qid)
        archetype = query_archetypes.get(norm_id)
        if not archetype:
            continue

        arch_queries[archetype].add(norm_id)

        for attempt in qdata.get("all_attempts", []):
            # Skip if rows_match is explicitly false
            if attempt.get("rows_match") is False:
                skipped_no_rows += 1
                continue

            transforms = attempt.get("transforms", [])
            if not transforms:
                skipped_no_transforms += 1
                continue

            speedup = attempt.get("speedup")
            if speedup is None:
                continue

            total_attempts += 1
            arch_attempts[archetype] += 1
            arch_speedups[archetype].append(speedup)
            if speedup >= WIN_THRESHOLD:
                arch_wins[archetype] += 1

            is_success = speedup >= WIN_THRESHOLD
            is_regression = speedup < REGRESSION_THRESHOLD

            for transform in transforms:
                transform = transform.strip().lower()
                if not transform:
                    continue

                key = (archetype, transform)
                cell_attempts[key].append(speedup)

                if is_success:
                    cell_successes[key].append(speedup)
                    cell_queries_won[key].add(norm_id)
                if is_regression:
                    cell_regressions[key].append(speedup)

    print(f"\nProcessed {total_attempts} valid attempts")
    print(f"  Skipped: {skipped_no_rows} (rows_match=false), {skipped_no_transforms} (no transforms)")

    # Step 3: Build archetype summary
    archetype_summary = {}
    for arch in sorted(arch_queries.keys()):
        q_list = sorted(arch_queries[arch])
        speedups = arch_speedups.get(arch, [])
        wins = arch_wins.get(arch, 0)
        n_attempts = arch_attempts.get(arch, 0)
        archetype_summary[arch] = {
            "query_count": len(q_list),
            "total_attempts": n_attempts,
            "queries": q_list,
            "win_rate": round(wins / n_attempts, 3) if n_attempts else 0,
            "avg_speedup": round(sum(speedups) / len(speedups), 3) if speedups else 0,
        }

    print(f"\nArchetype distribution:")
    for arch, s in sorted(archetype_summary.items(), key=lambda x: -x[1]["query_count"]):
        print(f"  {arch}: {s['query_count']} queries, {s['total_attempts']} attempts, "
              f"win_rate={s['win_rate']:.1%}, avg={s['avg_speedup']:.2f}x")

    # Step 4: Build transform_by_archetype
    transform_by_archetype: Dict[str, Dict] = defaultdict(dict)

    all_cells = set(cell_attempts.keys())
    for (arch, transform) in sorted(all_cells):
        speedups = cell_attempts[(arch, transform)]
        successes = cell_successes.get((arch, transform), [])
        regressions = cell_regressions.get((arch, transform), [])
        queries_won = sorted(cell_queries_won.get((arch, transform), set()))

        n = len(speedups)
        n_success = len(successes)
        n_regression = len(regressions)

        transform_by_archetype[arch][transform] = {
            "attempts": n,
            "successes": n_success,
            "success_rate": round(n_success / n, 3) if n else 0,
            "avg_speedup_when_successful": round(sum(successes) / n_success, 3) if successes else 0,
            "avg_speedup_all": round(sum(speedups) / n, 3) if speedups else 0,
            "regressions": n_regression,
            "regression_rate": round(n_regression / n, 3) if n else 0,
            "queries_won": queries_won,
        }

    # Step 5: Build elimination table
    elimination_table: Dict[str, Dict] = {}
    for (arch, transform), speedups in cell_attempts.items():
        n = len(speedups)
        if n < ELIMINATION_MIN_ATTEMPTS:
            continue
        successes = cell_successes.get((arch, transform), [])
        regressions = cell_regressions.get((arch, transform), [])
        success_rate = len(successes) / n if n else 0
        if success_rate < ELIMINATION_MAX_SUCCESS_RATE:
            if arch not in elimination_table:
                elimination_table[arch] = {"avoid": [], "reason": {}}
            avg_all = sum(speedups) / n
            elimination_table[arch]["avoid"].append(transform)
            elimination_table[arch]["reason"][transform] = (
                f"{len(successes)}/{n} successes, "
                f"{len(regressions)} regressions "
                f"(avg {avg_all:.2f}x)"
            )

    # Sort avoid lists
    for arch in elimination_table:
        elimination_table[arch]["avoid"].sort()

    # Step 6: Build top_transforms_by_archetype (ranked by success_rate)
    top_transforms: Dict[str, List] = {}
    for arch, transforms in transform_by_archetype.items():
        ranked = sorted(
            transforms.items(),
            key=lambda x: (-x[1]["success_rate"], -x[1]["avg_speedup_all"]),
        )
        top_transforms[arch] = [
            {
                "transform": t,
                "success_rate": data["success_rate"],
                "avg_speedup": data["avg_speedup_all"],
                "attempts": data["attempts"],
            }
            for t, data in ranked
            if data["attempts"] >= 3  # minimum signal
        ]

    # Step 7: Assemble output
    leaderboard = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_attempts": total_attempts,
        "total_queries": len(query_archetypes),
        "query_archetypes": dict(sorted(query_archetypes.items())),
        "archetype_summary": archetype_summary,
        "transform_by_archetype": dict(sorted(transform_by_archetype.items())),
        "elimination_table": dict(sorted(elimination_table.items())),
        "top_transforms_by_archetype": dict(sorted(top_transforms.items())),
    }

    OUTPUT_FILE.write_text(json.dumps(leaderboard, indent=2))
    print(f"\nWrote {OUTPUT_FILE}")
    print(f"  {len(query_archetypes)} queries, {total_attempts} attempts")
    print(f"  {len(elimination_table)} archetypes with eliminations")

    return leaderboard


if __name__ == "__main__":
    build_leaderboard()
