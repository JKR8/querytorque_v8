#!/usr/bin/env python3
"""Validate all collected LLM responses using PROPER 3x validation methodology.

VALIDATION METHOD (per query):
  1. Warmup run (discard timing)
  2. Measure run 1
  3. Measure run 2
  4. Average measures 1 and 2

This eliminates JIT/cache warmup effects and provides stable timing.
See: packages/qt-sql/qt_sql/validation/benchmarker.py lines 129-213
"""

import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List

PROJECT_ROOT = Path(__file__).parent.parent.parent  # research/scripts -> research -> PROJECT_ROOT
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-sql"))

from qt_sql.validation.sql_validator import SQLValidator
from qt_sql.validation.schemas import ValidationStatus

# Config
import argparse
_parser = argparse.ArgumentParser()
_parser.add_argument("--db", default="/mnt/d/TPC-DS/tpcds_sf5.duckdb")
_args, _ = _parser.parse_known_args()
DB_PATH = _args.db
_parser.add_argument("--dir", default="retry_neutrals")
_args2, _ = _parser.parse_known_args()
COLLECT_DIR = PROJECT_ROOT / _args2.dir

# Auto-detect queries from directory
def get_queries_from_dir(collect_dir: Path) -> list[str]:
    """Get list of query IDs from the collect directory."""
    queries = []
    if collect_dir.exists():
        for d in collect_dir.iterdir():
            if d.is_dir() and d.name.startswith("q") and d.name[1:].isdigit():
                queries.append(d.name)
    return sorted(queries, key=lambda x: int(x[1:]))

QUERIES = get_queries_from_dir(COLLECT_DIR)

# Default speedups (1.0 if not known)
ORIGINAL_SPEEDUPS = {}

WORKER_EXAMPLES = {
    1: ["decorrelate", "pushdown", "early_filter"],
    2: ["date_cte_isolate", "dimension_cte_isolate", "multi_date_range_cte"],
    3: ["prefetch_fact_join", "multi_dimension_prefetch", "materialize_cte"],
    4: ["single_pass_aggregation", "or_to_union", "intersect_to_exists", "union_cte_split"],
}


@dataclass
class ValidationResult:
    query_id: str
    worker_id: int
    examples: List[str]
    status: str
    speedup: float
    error: Optional[str]
    original_time_ms: float
    optimized_time_ms: float


def categorize_error(error_msg: str) -> str:
    if not error_msg:
        return "none"
    err = error_msg.lower()
    if "syntax" in err or "parse" in err:
        return "syntax"
    if "timeout" in err or "canceled" in err:
        return "timeout"
    if "mismatch" in err or "different" in err or "row" in err:
        return "semantic"
    return "other"


def validate_one(query_id: str, worker_id: int) -> Optional[ValidationResult]:
    """Validate one query/worker combination."""
    query_dir = COLLECT_DIR / query_id
    original_file = query_dir / "original.sql"
    optimized_file = query_dir / f"w{worker_id}_optimized.sql"

    if not original_file.exists() or not optimized_file.exists():
        return None

    original_sql = original_file.read_text()
    optimized_sql = optimized_file.read_text()

    try:
        validator = SQLValidator(database=DB_PATH)
        result = validator.validate(original_sql, optimized_sql)

        status = "pass" if result.status == ValidationStatus.PASS else "fail"
        error = result.errors[0] if result.errors else None

        return ValidationResult(
            query_id=query_id,
            worker_id=worker_id,
            examples=WORKER_EXAMPLES[worker_id],
            status=status,
            speedup=result.speedup,
            error=error,
            original_time_ms=getattr(result, 'original_time', 0),
            optimized_time_ms=getattr(result, 'optimized_time', 0),
        )
    except Exception as e:
        return ValidationResult(
            query_id=query_id,
            worker_id=worker_id,
            examples=WORKER_EXAMPLES[worker_id],
            status="error",
            speedup=0.0,
            error=str(e),
            original_time_ms=0,
            optimized_time_ms=0,
        )


def main():
    print(f"Validating collected responses from {COLLECT_DIR}")
    print(f"Database: {DB_PATH}")
    print(f"Queries: {len(QUERIES)} × 4 workers = {len(QUERIES) * 4} validations")
    print()

    # Build work list
    work = []
    for qid in QUERIES:
        for wid in [1, 2, 3, 4]:
            work.append((qid, wid))

    # Run validations in parallel
    results: List[ValidationResult] = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(validate_one, qid, wid): (qid, wid) for qid, wid in work}

        for future in as_completed(futures):
            qid, wid = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    icon = "✓" if result.status == "pass" and result.speedup > 1.0 else "·"
                    print(f"  {icon} {qid}/W{wid}: {result.status} {result.speedup:.2f}x")
            except Exception as e:
                print(f"  ✗ {qid}/W{wid}: {e}")

    elapsed = time.time() - start
    print(f"\nValidation completed in {elapsed:.1f}s")

    # Aggregate by query - pick best worker
    query_results = {}
    for r in results:
        if r.query_id not in query_results:
            query_results[r.query_id] = []
        query_results[r.query_id].append(r)

    # Summary
    print("\n" + "=" * 70)
    print("RESULTS BY QUERY (Best Worker)")
    print("=" * 70)

    improved = 0
    fixed = 0
    summary_rows = []

    for qid in QUERIES:
        workers = query_results.get(qid, [])
        orig = ORIGINAL_SPEEDUPS.get(qid, 0)

        # Find best passing result
        passing = [w for w in workers if w.status == "pass" and w.speedup > 1.0]
        if passing:
            best = max(passing, key=lambda w: w.speedup)
            delta = best.speedup - orig
            if delta > 0:
                improved += 1
            if best.speedup >= 1.0:
                fixed += 1
            print(f"  {qid}: {orig:.2f}x → {best.speedup:.2f}x (W{best.worker_id}) Δ={delta:+.2f}x ✓")
        else:
            # No passing result - show best attempt
            best = max(workers, key=lambda w: w.speedup) if workers else None
            if best:
                err_cat = categorize_error(best.error)
                print(f"  {qid}: {orig:.2f}x → {best.speedup:.2f}x (W{best.worker_id}) [{best.status}/{err_cat}]")
            else:
                print(f"  {qid}: no data")

        # Build summary row
        w_data = {w.worker_id: w for w in workers}
        row = {
            "query_id": qid,
            "original_speedup": orig,
            "best_speedup": best.speedup if best else 0,
            "best_worker": best.worker_id if best else None,
            "best_status": best.status if best else "missing",
            "improvement": (best.speedup - orig) if best else 0,
        }
        for wid in [1, 2, 3, 4]:
            w = w_data.get(wid)
            row[f"w{wid}_speedup"] = w.speedup if w else None
            row[f"w{wid}_status"] = w.status if w else None
            row[f"w{wid}_error"] = categorize_error(w.error) if w and w.error else None
        summary_rows.append(row)

    print("\n" + "=" * 70)
    print(f"SUMMARY: {improved}/{len(QUERIES)} improved, {fixed}/{len(QUERIES)} fixed (≥1.0x)")
    print("=" * 70)

    # Save CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = COLLECT_DIR / f"validation_{timestamp}.csv"

    with open(csv_file, "w", newline="") as f:
        fieldnames = ["query_id", "original_speedup", "best_speedup", "best_worker", "best_status", "improvement",
                      "w1_speedup", "w1_status", "w1_error",
                      "w2_speedup", "w2_status", "w2_error",
                      "w3_speedup", "w3_status", "w3_error",
                      "w4_speedup", "w4_status", "w4_error"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"\nResults saved to: {csv_file}")

    # Save detailed JSON
    json_file = COLLECT_DIR / f"validation_{timestamp}.json"
    with open(json_file, "w") as f:
        json.dump({
            "timestamp": timestamp,
            "summary": {
                "total_queries": len(QUERIES),
                "improved": improved,
                "fixed": fixed,
                "validation_time_s": elapsed,
            },
            "results": [asdict(r) for r in results],
        }, f, indent=2)

    print(f"Details saved to: {json_file}")


if __name__ == "__main__":
    main()
