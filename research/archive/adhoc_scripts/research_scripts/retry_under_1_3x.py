#!/usr/bin/env python3
"""
4-Worker Fan-Out Retry for ALL queries under 1.3x speedup.

Targets 44 queries: 15 IMPROVED (<1.3x), 14 NEUTRAL, 15 REGRESSION.
Reuses the proven 4-worker strategy with 13 gold examples.

Two-phase workflow:
  Phase 1: python research/scripts/retry_under_1_3x.py --db /mnt/d/TPC-DS/tpcds_sf10.duckdb --collect-only
  Phase 2: python research/scripts/validate_collected.py --db /mnt/d/TPC-DS/tpcds_sf10.duckdb --dir retry_under_1_3x

Worker Strategy:
- W1: decorrelate, pushdown, early_filter (Subquery transforms)
- W2: date_cte_isolate, dimension_cte_isolate, multi_date_range_cte (CTE isolation)
- W3: prefetch_fact_join, multi_dimension_prefetch, materialize_cte (Fact prefetch)
- W4: single_pass_aggregation, or_to_union, intersect_to_exists, union_cte_split (Consolidation + set ops)
"""

import argparse
import csv
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-sql"))

from qt_sql.optimization.dag_v2 import DagV2Pipeline
from qt_sql.optimization.dag_v3 import build_prompt_with_examples, load_example, GoldExample
from qt_sql.optimization.adaptive_rewriter_v5 import _get_plan_context, _create_llm_client
from qt_sql.validation.sql_validator import SQLValidator
from qt_sql.validation.schemas import ValidationStatus

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# 44 queries under 1.3x speedup (from v3 leaderboard, ordered by current speedup)
PENDING_RETRIES = [
    # IMPROVED but under 1.3x (15 queries)
    "q98", "q83", "q12", "q62", "q66",
    "q84", "q17", "q3", "q82", "q19",
    "q18", "q41", "q92", "q20", "q76",
    # NEUTRAL (14 queries)
    "q45", "q33", "q79", "q4", "q8",
    "q49", "q57", "q60", "q27", "q13",
    "q64", "q48", "q25", "q71",
    # REGRESSION (15 queries)
    "q55", "q75", "q56", "q86", "q50",
    "q91", "q24", "q87", "q34", "q70",
    "q89", "q53", "q61", "q32", "q94",
]

# Current best speedups from v3 leaderboard (what we're trying to beat)
ORIGINAL_SPEEDUPS = {
    # IMPROVED (<1.3x)
    "q98": 1.26, "q83": 1.24, "q12": 1.23, "q62": 1.23, "q66": 1.23,
    "q84": 1.22, "q17": 1.19, "q3": 1.19, "q82": 1.18, "q19": 1.16,
    "q18": 1.14, "q41": 1.14, "q92": 1.14, "q20": 1.13, "q76": 1.10,
    # NEUTRAL
    "q45": 1.08, "q33": 1.05, "q79": 1.05, "q4": 1.03, "q8": 1.03,
    "q49": 1.02, "q57": 1.02, "q60": 1.02, "q27": 1.02, "q13": 1.01,
    "q64": 1.01, "q48": 1.00, "q25": 0.98, "q71": 0.96,
    # REGRESSION
    "q55": 0.94, "q75": 0.94, "q56": 0.92, "q86": 0.92, "q50": 0.91,
    "q91": 0.89, "q24": 0.87, "q87": 0.86, "q34": 0.80, "q70": 0.75,
    "q89": 0.60, "q53": 0.51, "q61": 0.40, "q32": 0.27, "q94": 0.24,
}

# 4-Worker Gold Example Strategy (13 examples total)
WORKER_EXAMPLES = {
    1: ["decorrelate", "pushdown", "early_filter"],      # Subquery transforms
    2: ["date_cte_isolate", "dimension_cte_isolate", "multi_date_range_cte"],  # CTE isolation
    3: ["prefetch_fact_join", "multi_dimension_prefetch", "materialize_cte"],  # Fact prefetch
    4: ["single_pass_aggregation", "or_to_union", "intersect_to_exists", "union_cte_split"],  # Consolidation + set ops
}

# Query file patterns
QUERY_PATTERNS = [
    "packages/qt-sql/tests/fixtures/tpcds/query_{num:02d}.sql",
    "packages/qt-sql/tests/fixtures/tpcds/query_{num}.sql",
]


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class WorkerResult:
    worker_id: int
    query_id: str
    examples_used: List[str]
    status: str  # 'pass', 'fail', 'error', 'collected'
    speedup: float
    error_message: Optional[str]
    error_category: Optional[str]
    original_time_ms: float
    optimized_time_ms: float
    optimized_sql: Optional[str]
    prompt_tokens: int
    response_tokens: int
    llm_time_ms: float


@dataclass
class QueryResult:
    query_id: str
    best_worker: Optional[int]
    best_speedup: float
    best_status: str
    all_workers: List[WorkerResult]
    original_speedup: float
    improvement: float
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def find_query_file(query_id: str) -> Optional[Path]:
    num = int(query_id.replace("q", ""))
    for pattern in QUERY_PATTERNS:
        path = PROJECT_ROOT / pattern.format(num=num)
        if path.exists():
            return path
    return None


def load_query_sql(query_id: str) -> Optional[str]:
    path = find_query_file(query_id)
    if path:
        return path.read_text()
    return None


def categorize_error(error_msg: str) -> str:
    if not error_msg:
        return "unknown"
    error_lower = error_msg.lower()
    if "syntax" in error_lower or "parse" in error_lower:
        return "syntax"
    if "timeout" in error_lower or "canceled" in error_lower:
        return "timeout"
    if "mismatch" in error_lower or "different" in error_lower or "row" in error_lower:
        return "semantic"
    if "connect" in error_lower or "network" in error_lower:
        return "execution"
    return "other"


# ============================================================================
# WORKER FUNCTION
# ============================================================================

def run_worker(
    worker_id: int,
    query_id: str,
    sql: str,
    db_path: str,
    example_ids: List[str],
    output_dir: Path,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    collect_only: bool = False,
) -> WorkerResult:
    """Run a single optimization worker."""
    start_time = time.time()

    query_dir = output_dir / query_id
    query_dir.mkdir(parents=True, exist_ok=True)

    # Load gold examples
    examples: List[GoldExample] = []
    for ex_id in example_ids:
        ex = load_example(ex_id)
        if ex:
            examples.append(ex)

    logger.info(f"  W{worker_id}: Loaded {len(examples)} examples: {[e.id for e in examples]}")

    try:
        plan_summary, plan_text, plan_ctx = _get_plan_context(db_path, sql)

        pipeline = DagV2Pipeline(sql, plan_context=plan_ctx)
        base_prompt = pipeline.get_prompt()
        full_prompt = build_prompt_with_examples(
            base_prompt, examples, plan_summary,
            history_section="",
            include_constraints=False
        )

        prompt_tokens = len(full_prompt) // 4

        # Save prompt BEFORE API call
        (query_dir / f"w{worker_id}_prompt.txt").write_text(full_prompt)

        # Call LLM
        llm_start = time.time()
        llm_client = _create_llm_client(provider, model)
        response_text = llm_client.analyze(full_prompt)
        llm_time_ms = (time.time() - llm_start) * 1000

        response_tokens = len(response_text) // 4

        # Save response
        (query_dir / f"w{worker_id}_response.txt").write_text(response_text)

        # Apply response
        optimized_sql = pipeline.apply_response(response_text)

        # Save optimized SQL
        (query_dir / f"w{worker_id}_optimized.sql").write_text(optimized_sql)

        if collect_only:
            return WorkerResult(
                worker_id=worker_id, query_id=query_id,
                examples_used=example_ids, status="collected",
                speedup=0.0, error_message=None, error_category=None,
                original_time_ms=0, optimized_time_ms=0,
                optimized_sql=optimized_sql,
                prompt_tokens=prompt_tokens, response_tokens=response_tokens,
                llm_time_ms=llm_time_ms,
            )

        # Validate
        validator = SQLValidator(database=db_path)
        result = validator.validate(sql, optimized_sql)

        status = "pass" if result.status == ValidationStatus.PASS else "fail"
        error_msg = result.errors[0] if result.errors else None
        error_cat = categorize_error(error_msg) if error_msg else None

        (query_dir / f"w{worker_id}_validation.json").write_text(
            json.dumps({"worker_id": worker_id, "status": status,
                        "speedup": result.speedup, "error": error_msg,
                        "error_category": error_cat, "examples_used": example_ids}, indent=2))

        return WorkerResult(
            worker_id=worker_id, query_id=query_id,
            examples_used=example_ids, status=status,
            speedup=result.speedup, error_message=error_msg,
            error_category=error_cat,
            original_time_ms=result.original_time if hasattr(result, 'original_time') else 0,
            optimized_time_ms=result.optimized_time if hasattr(result, 'optimized_time') else 0,
            optimized_sql=optimized_sql if status == "pass" else None,
            prompt_tokens=prompt_tokens, response_tokens=response_tokens,
            llm_time_ms=llm_time_ms,
        )

    except Exception as e:
        logger.error(f"  W{worker_id}: Error - {e}")
        return WorkerResult(
            worker_id=worker_id, query_id=query_id,
            examples_used=example_ids, status="error",
            speedup=0.0, error_message=str(e),
            error_category=categorize_error(str(e)),
            original_time_ms=0, optimized_time_ms=0, optimized_sql=None,
            prompt_tokens=0, response_tokens=0,
            llm_time_ms=(time.time() - start_time) * 1000,
        )


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="4-Worker Retry: All queries under 1.3x")
    parser.add_argument("--db", required=True, help="Path to DuckDB database (e.g. /mnt/d/TPC-DS/tpcds_sf10.duckdb)")
    parser.add_argument("--queries", help="Comma-separated query IDs (default: all 44 under 1.3x)")
    parser.add_argument("--output-dir", default="retry_under_1_3x", help="Output directory")
    parser.add_argument("--provider", help="LLM provider override (default: from .env)")
    parser.add_argument("--model", help="LLM model override (default: from .env)")
    parser.add_argument("--collect-only", action="store_true", help="Phase 1: collect LLM responses only, skip validation")
    args = parser.parse_args()

    if args.queries:
        query_ids = [q.strip() for q in args.queries.split(",")]
    else:
        query_ids = PENDING_RETRIES

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = output_dir / f"retry_4worker_{timestamp}.csv"
    details_file = output_dir / f"retry_4worker_{timestamp}_details.json"

    logger.info(f"\n{'#'*60}")
    logger.info(f"# 4-WORKER RETRY: QUERIES UNDER 1.3x")
    logger.info(f"# Queries: {len(query_ids)} ({len(query_ids) * 4} API calls)")
    logger.info(f"# Database: {args.db}")
    logger.info(f"# Output: {output_dir}")
    logger.info(f"# Mode: {'COLLECT ONLY' if args.collect_only else 'COLLECT + VALIDATE'}")
    logger.info(f"{'#'*60}")
    logger.info(f"\nWorker Strategy:")
    for wid, examples in WORKER_EXAMPLES.items():
        logger.info(f"  W{wid}: {', '.join(examples)}")

    logger.info(f"\nTarget queries ({len(query_ids)}):")
    for qid in query_ids:
        orig = ORIGINAL_SPEEDUPS.get(qid, 0)
        cat = "REGRESSION" if orig < 0.95 else "NEUTRAL" if orig < 1.10 else "IMPROVED"
        logger.info(f"  {qid}: {orig:.2f}x ({cat})")

    all_results: List[QueryResult] = []
    start_time = time.time()

    if args.collect_only:
        # Phase 1: Blast all API calls in parallel
        total_calls = len(query_ids) * 4
        logger.info(f"\nPhase 1: Sending {total_calls} API calls in parallel...")

        worker_results_by_query: Dict[str, List[WorkerResult]] = {qid: [] for qid in query_ids}

        with ThreadPoolExecutor(max_workers=90) as pool:
            futures = {}
            for qid in query_ids:
                sql = load_query_sql(qid)
                if not sql:
                    logger.error(f"  Query file not found: {qid}")
                    continue

                query_dir = output_dir / qid
                query_dir.mkdir(parents=True, exist_ok=True)
                (query_dir / "original.sql").write_text(sql)

                for worker_id, example_ids in WORKER_EXAMPLES.items():
                    # Skip if already collected
                    response_file = query_dir / f"w{worker_id}_response.txt"
                    if response_file.exists():
                        logger.info(f"  {qid}/W{worker_id}: already collected, skipping")
                        optimized_sql_file = query_dir / f"w{worker_id}_optimized.sql"
                        worker_results_by_query[qid].append(WorkerResult(
                            worker_id=worker_id, query_id=qid,
                            examples_used=example_ids, status="collected",
                            speedup=0.0, error_message=None, error_category=None,
                            original_time_ms=0, optimized_time_ms=0,
                            optimized_sql=optimized_sql_file.read_text() if optimized_sql_file.exists() else None,
                            prompt_tokens=0, response_tokens=0, llm_time_ms=0,
                        ))
                        continue

                    future = pool.submit(
                        run_worker, worker_id, qid, sql, args.db,
                        example_ids, output_dir, args.provider, args.model, True,
                    )
                    futures[future] = (qid, worker_id)

            for future in as_completed(futures):
                qid, wid = futures[future]
                try:
                    result = future.result()
                    worker_results_by_query[qid].append(result)
                    logger.info(f"  {qid}/W{wid}: collected ({result.llm_time_ms:.0f}ms)")
                except Exception as e:
                    logger.error(f"  {qid}/W{wid}: {e}")

        for qid in query_ids:
            workers = worker_results_by_query.get(qid, [])
            all_results.append(QueryResult(
                query_id=qid, best_worker=None, best_speedup=0.0,
                best_status="collected", all_workers=workers,
                original_speedup=ORIGINAL_SPEEDUPS.get(qid, 0.0),
                improvement=0.0,
            ))
    else:
        # Sequential with validation
        for i, qid in enumerate(query_ids, 1):
            logger.info(f"\n[{i}/{len(query_ids)}] Processing {qid}...")
            sql = load_query_sql(qid)
            if not sql:
                logger.error(f"  Query file not found: {qid}")
                continue

            original_speedup = ORIGINAL_SPEEDUPS.get(qid, 0.0)

            query_dir = output_dir / qid
            query_dir.mkdir(parents=True, exist_ok=True)
            (query_dir / "original.sql").write_text(sql)

            logger.info(f"  {qid.upper()} (current best: {original_speedup:.2f}x)")

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {}
                for worker_id, example_ids in WORKER_EXAMPLES.items():
                    future = pool.submit(
                        run_worker, worker_id, qid, sql, args.db,
                        example_ids, output_dir, args.provider, args.model, False,
                    )
                    futures[future] = worker_id

                worker_results = []
                for future in as_completed(futures):
                    wid = futures[future]
                    try:
                        result = future.result()
                        worker_results.append(result)
                        icon = "+" if result.status == "pass" else "-"
                        logger.info(f"  W{wid} [{icon}]: {result.status} | {result.speedup:.2f}x | err={result.error_category or 'none'}")
                    except Exception as e:
                        logger.error(f"  W{wid}: {e}")

            valid_results = [r for r in worker_results if r.status == "pass" and r.speedup > 1.0]
            if valid_results:
                best = max(valid_results, key=lambda r: r.speedup)
                all_results.append(QueryResult(
                    query_id=qid, best_worker=best.worker_id,
                    best_speedup=best.speedup, best_status="pass",
                    all_workers=worker_results, original_speedup=original_speedup,
                    improvement=best.speedup - original_speedup,
                ))
            else:
                best = max(worker_results, key=lambda r: r.speedup) if worker_results else None
                all_results.append(QueryResult(
                    query_id=qid,
                    best_worker=best.worker_id if best else None,
                    best_speedup=best.speedup if best else 0.0,
                    best_status=best.status if best else "error",
                    all_workers=worker_results, original_speedup=original_speedup,
                    improvement=(best.speedup - original_speedup) if best else 0.0,
                ))

    total_time = time.time() - start_time

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")

    improved = [r for r in all_results if r.improvement > 0]
    wins = [r for r in all_results if r.best_speedup >= 1.5]

    logger.info(f"Total queries: {len(all_results)}")
    logger.info(f"Improved: {len(improved)} ({100*len(improved)/max(len(all_results),1):.1f}%)")
    logger.info(f"WIN (>= 1.5x): {len(wins)}")
    logger.info(f"Total time: {total_time/60:.1f} minutes")

    if not args.collect_only:
        logger.info("\nTop Improvements:")
        top = sorted(all_results, key=lambda r: r.improvement, reverse=True)[:10]
        for r in top:
            if r.improvement > 0:
                logger.info(f"  {r.query_id}: {r.original_speedup:.2f}x -> {r.best_speedup:.2f}x (+{r.improvement:.2f}x) [W{r.best_worker}]")

    # Write CSV
    with open(results_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "query_id", "original_speedup", "best_speedup", "improvement",
            "best_worker", "best_status",
            "w1_speedup", "w1_status", "w1_error",
            "w2_speedup", "w2_status", "w2_error",
            "w3_speedup", "w3_status", "w3_error",
            "w4_speedup", "w4_status", "w4_error",
        ])
        for r in all_results:
            w_data = {w.worker_id: w for w in r.all_workers}
            row = [r.query_id, f"{r.original_speedup:.3f}", f"{r.best_speedup:.3f}",
                   f"{r.improvement:.3f}", r.best_worker, r.best_status]
            for wid in [1, 2, 3, 4]:
                w = w_data.get(wid)
                if w:
                    row.extend([f"{w.speedup:.3f}", w.status, w.error_category or ""])
                else:
                    row.extend(["", "", ""])
            writer.writerow(row)

    logger.info(f"\nResults saved to: {results_file}")

    # Write detailed JSON
    details = {
        "timestamp": timestamp,
        "config": {
            "database": args.db,
            "queries": query_ids,
            "worker_examples": {str(k): v for k, v in WORKER_EXAMPLES.items()},
        },
        "summary": {
            "total_queries": len(all_results),
            "improved": len(improved),
            "wins": len(wins),
            "total_time_minutes": total_time / 60,
        },
        "results": [asdict(r) for r in all_results],
    }

    with open(details_file, "w") as f:
        json.dump(details, f, indent=2, default=str)

    logger.info(f"Details saved to: {details_file}")


if __name__ == "__main__":
    main()
