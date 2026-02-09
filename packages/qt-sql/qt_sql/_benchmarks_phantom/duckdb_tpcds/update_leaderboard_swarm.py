#!/usr/bin/env python3
"""
Merge swarm benchmark results into the DuckDB TPC-DS leaderboard.

For each query:
- Parse ALL swarm benchmark files (iter0, iter1, iter2) across all queries
- Find the best valid worker result (highest speedup with rows_match=True, status="pass")
- Compare against existing leaderboard entry and keep the better one
- Extract transform names from assignments.json for the winning worker
- Recompute summary statistics
- Write updated leaderboard.json (after backing up old one)
"""

import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from glob import glob

BENCH_DIR = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql/ado/benchmarks/duckdb_tpcds")
SWARM_DIR = BENCH_DIR / "swarm_batch_20260208_102033"
LEADERBOARD = BENCH_DIR / "leaderboard.json"
BACKUP = BENCH_DIR / "leaderboard_pre_swarm.json"


def normalize_query_id(raw_id: str) -> str:
    """Convert 'query_88' -> 'q88', 'query_23a' -> 'q23a', etc."""
    m = re.match(r"query_(\d+[ab]?)", raw_id)
    if m:
        return f"q{m.group(1)}"
    if raw_id.startswith("q"):
        return raw_id
    return raw_id


def load_assignments(query_dir: Path) -> dict:
    """Load assignments.json and return {worker_id: {strategy, examples}}."""
    apath = query_dir / "assignments.json"
    if not apath.exists():
        return {}
    with open(apath) as f:
        assignments = json.load(f)
    return {a["worker_id"]: a for a in assignments}


def get_transforms_for_worker(query_dir: Path, worker_id: int) -> list:
    """Extract transform/example names for a given worker.

    For workers 1-4: use assignments.json examples.
    For workers 5-6 (snipe/final): infer from SQL diff using structural analysis.
    """
    assignments = load_assignments(query_dir)
    if worker_id in assignments:
        a = assignments[worker_id]
        examples = a.get("examples", [])
        if examples:
            return examples

    # For later-iteration workers (id=5,6 = snipe/final), infer from SQL diff
    original_sql = ""
    optimized_sql = ""

    # Load original SQL
    orig_path = query_dir / "original.sql"
    if orig_path.exists():
        with open(orig_path) as f:
            original_sql = f.read().strip()

    # Load optimized SQL for this worker
    if worker_id == 5:
        sql_path = query_dir / "snipe_worker_sql.sql"
    elif worker_id == 6:
        sql_path = query_dir / "final_worker_sql.sql"
    else:
        sql_path = query_dir / f"worker_{worker_id}_sql.sql"

    if sql_path.exists():
        with open(sql_path) as f:
            optimized_sql = f.read().strip()

    # Try SQL-diff inference
    if original_sql and optimized_sql:
        try:
            import sys
            # Ensure ado module is importable
            project_root = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
            for p in [
                str(project_root / "packages" / "qt-shared"),
                str(project_root / "packages" / "qt-sql"),
                str(project_root),
            ]:
                if p not in sys.path:
                    sys.path.insert(0, p)
            from ado.sql_rewriter import infer_transforms_from_sql_diff
            inferred = infer_transforms_from_sql_diff(original_sql, optimized_sql)
            if inferred:
                return inferred
        except Exception:
            pass

    # Last resort fallback (should rarely hit now)
    if worker_id == 5:
        return ["snipe_worker"]
    elif worker_id == 6:
        return ["final_worker"]
    return [f"iter_worker_{worker_id}"]


def parse_swarm_results() -> dict:
    """
    Parse all swarm query directories and return best result per normalized query_id.
    """
    results = {}

    query_dirs = sorted(SWARM_DIR.glob("query_*"))
    for query_dir in query_dirs:
        if not query_dir.is_dir():
            continue

        iter_files = sorted(query_dir.glob("benchmark_iter*.json"))
        if not iter_files:
            continue

        dir_name = query_dir.name

        best_for_query = None

        for iter_file in iter_files:
            try:
                with open(iter_file) as f:
                    bench = json.load(f)
            except (json.JSONDecodeError, IOError):
                continue

            raw_qid = bench.get("query_id", dir_name)
            qid = normalize_query_id(raw_qid)
            baseline_ms = bench.get("baseline_trimmed_mean_ms", 0)

            iter_match = re.search(r"iter(\d+)", iter_file.name)
            iteration = int(iter_match.group(1)) if iter_match else 0

            for worker in bench.get("workers", []):
                if not worker.get("rows_match", False):
                    continue
                if worker.get("status", "") != "pass":
                    continue

                speedup = worker.get("speedup", 0)
                if speedup <= 0:
                    continue

                opt_ms = worker.get("trimmed_mean_ms", 0)
                wid = worker.get("worker_id", 0)

                if best_for_query is None or speedup > best_for_query["speedup"]:
                    best_for_query = {
                        "qid": qid,
                        "speedup": speedup,
                        "original_ms": baseline_ms,
                        "optimized_ms": opt_ms,
                        "worker_id": wid,
                        "rows_match": True,
                        "iteration": iteration,
                        "query_dir": query_dir,
                    }

        if best_for_query:
            qid = best_for_query["qid"]
            best_for_query["transforms"] = get_transforms_for_worker(
                best_for_query["query_dir"], best_for_query["worker_id"]
            )
            if qid not in results or best_for_query["speedup"] > results[qid]["speedup"]:
                results[qid] = best_for_query

    return results


def classify_status(speedup: float) -> str:
    if speedup >= 1.1:
        return "WIN"
    elif speedup >= 1.05:
        return "IMPROVED"
    elif speedup >= 0.95:
        return "NEUTRAL"
    else:
        return "REGRESSION"


def main():
    # 1. Back up old leaderboard
    if LEADERBOARD.exists():
        shutil.copy2(LEADERBOARD, BACKUP)
        print(f"Backed up old leaderboard to {BACKUP.name}")

    # 2. Load existing leaderboard
    with open(LEADERBOARD) as f:
        leaderboard = json.load(f)

    existing_queries = {q["query_id"]: q for q in leaderboard["queries"]}
    print(f"Existing leaderboard: {len(existing_queries)} queries")

    # 3. Parse swarm results
    swarm_results = parse_swarm_results()
    print(f"Swarm results: {len(swarm_results)} queries with valid results")

    # 4. Merge: for each query, keep whichever is better
    changes = {
        "swarm_better": [],
        "existing_better": [],
        "new_entries": [],
    }

    merged_queries = {}

    # Start with existing entries
    for qid, entry in existing_queries.items():
        merged_queries[qid] = entry.copy()

    # Overlay swarm results where they are better
    for qid, swarm in swarm_results.items():
        swarm_speedup = swarm["speedup"]
        swarm_entry = {
            "query_id": qid,
            "status": classify_status(swarm_speedup),
            "speedup": round(swarm_speedup, 4),
            "original_ms": round(swarm["original_ms"], 2),
            "optimized_ms": round(swarm["optimized_ms"], 2),
            "transforms": swarm["transforms"],
            "source": "Swarm",
            "rows_match": True,
        }

        if qid in existing_queries:
            existing_speedup = existing_queries[qid].get("speedup", 0)
            if swarm_speedup > existing_speedup:
                old_source = existing_queries[qid].get("source", "?")
                changes["swarm_better"].append({
                    "qid": qid,
                    "old_speedup": existing_speedup,
                    "new_speedup": round(swarm_speedup, 4),
                    "old_source": old_source,
                    "worker_id": swarm["worker_id"],
                    "iteration": swarm["iteration"],
                })
                merged_queries[qid] = swarm_entry
            else:
                changes["existing_better"].append({
                    "qid": qid,
                    "existing_speedup": existing_speedup,
                    "swarm_speedup": round(swarm_speedup, 4),
                })
        else:
            changes["new_entries"].append({
                "qid": qid,
                "speedup": round(swarm_speedup, 4),
            })
            merged_queries[qid] = swarm_entry

    # 5. Reclassify all entries and compute summary
    wins = improved = neutral = regression = errors = 0
    total_speedup = 0

    for qid, entry in merged_queries.items():
        sp = entry.get("speedup", 1.0)
        entry["status"] = classify_status(sp)
        total_speedup += sp

        if sp >= 1.1:
            wins += 1
        elif sp >= 1.05:
            improved += 1
        elif sp >= 0.95:
            neutral += 1
        else:
            regression += 1

    total = len(merged_queries)
    avg_speedup = round(total_speedup / total, 4) if total > 0 else 0

    # 6. Sort by speedup descending
    sorted_queries = sorted(merged_queries.values(), key=lambda q: q.get("speedup", 0), reverse=True)

    # 7. Build updated leaderboard
    updated = {
        "benchmark": "tpcds",
        "engine": "duckdb",
        "scale_factor": 10,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Consolidated: V2 Standard + Evo + Retry3W/4W + Swarm (2026-02-08)",
        "summary": {
            "total": total,
            "wins": wins,
            "improved": improved,
            "neutral": neutral,
            "regression": regression,
            "errors": errors,
            "avg_speedup": avg_speedup,
        },
        "queries": sorted_queries,
    }

    # 8. Write
    with open(LEADERBOARD, "w") as f:
        json.dump(updated, f, indent=2)
    print(f"\nWrote updated leaderboard to {LEADERBOARD.name}")

    # 9. Print summary
    old_summary = leaderboard.get("summary", {})
    print("\n" + "=" * 70)
    print("LEADERBOARD UPDATE SUMMARY")
    print("=" * 70)

    print(f"\nTotal queries: {old_summary.get('total', '?')} -> {total}")
    print(f"Wins (>=1.1x):        {old_summary.get('wins', '?')} -> {wins}")
    print(f"Improved (1.05-1.1x): {old_summary.get('improved', '?')} -> {improved}")
    print(f"Neutral (0.95-1.05x): {old_summary.get('neutral', '?')} -> {neutral}")
    print(f"Regression (<0.95x):  {old_summary.get('regression', '?')} -> {regression}")
    print(f"Avg speedup:          {old_summary.get('avg_speedup', '?')} -> {avg_speedup}")

    print(f"\n--- Swarm beat existing ({len(changes['swarm_better'])} queries) ---")
    for c in sorted(changes["swarm_better"], key=lambda x: x["new_speedup"], reverse=True):
        print(f"  {c['qid']:>5s}: {c['old_speedup']:.2f}x ({c['old_source']}) -> {c['new_speedup']:.2f}x (Swarm W{c['worker_id']} iter{c['iteration']})")

    print(f"\n--- New entries from swarm ({len(changes['new_entries'])} queries) ---")
    for c in sorted(changes["new_entries"], key=lambda x: x["speedup"], reverse=True):
        print(f"  {c['qid']:>5s}: {c['speedup']:.2f}x")

    print(f"\n--- Existing was better ({len(changes['existing_better'])} queries) ---")
    for c in sorted(changes["existing_better"], key=lambda x: x["existing_speedup"], reverse=True):
        print(f"  {c['qid']:>5s}: existing {c['existing_speedup']:.2f}x vs swarm {c['swarm_speedup']:.2f}x")

    # Show top 15 in final leaderboard
    print(f"\n--- Top 15 in updated leaderboard ---")
    for i, q in enumerate(sorted_queries[:15]):
        print(f"  {i+1:2d}. {q['query_id']:>5s}: {q['speedup']:.2f}x [{q['source']}] {q.get('transforms', [])}")

    # Count queries with no valid swarm result
    swarm_query_dirs = set(d.name for d in SWARM_DIR.glob("query_*") if d.is_dir())
    swarm_valid_ids = set()
    for qid in swarm_results:
        # reverse map: q88 -> query_88
        num = qid[1:]
        swarm_valid_ids.add(f"query_{num}")
    no_valid = swarm_query_dirs - swarm_valid_ids
    if no_valid:
        print(f"\n--- Swarm queries with NO valid result ({len(no_valid)}) ---")
        for d in sorted(no_valid):
            print(f"  {d}")

    print(f"\nDone.")


if __name__ == "__main__":
    main()
