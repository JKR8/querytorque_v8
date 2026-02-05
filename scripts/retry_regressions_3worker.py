#!/usr/bin/env python3
"""
3-Worker Fan-Out Retry for Regression Queries

Runs 3 workers in parallel per query, each with different gold example sets
to ensure coverage of all 8 verified optimization patterns.

Worker Strategy:
- W1: decorrelate, pushdown, early_filter (Subquery/filter transforms)
- W2: date_cte_isolate, materialize_cte, union_cte_split (CTE optimizations)
- W3: or_to_union, intersect_to_exists (Set operation transforms)

Usage:
    python scripts/retry_regressions_3worker.py --db /path/to/tpcds.duckdb
    python scripts/retry_regressions_3worker.py --db /path/to/tpcds.duckdb --queries q16,q94,q34
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
PROJECT_ROOT = Path(__file__).parent.parent
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

# 25 pending retry queries with regression (speedup <= 0.99)
PENDING_RETRIES = [
    "q16", "q94", "q34", "q9", "q26", "q4", "q5", "q42", "q58", "q91",
    "q12", "q29", "q63", "q43", "q38", "q48", "q37", "q82", "q96", "q22",
    "q25", "q53", "q7", "q73", "q75"
]

# 3-Worker Gold Example Strategy
WORKER_EXAMPLES = {
    1: ["decorrelate", "pushdown", "early_filter"],      # Subquery/filter transforms
    2: ["date_cte_isolate", "materialize_cte", "union_cte_split"],  # CTE optimizations
    3: ["or_to_union", "intersect_to_exists"],           # Set operation transforms
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
    """Result from a single worker."""
    worker_id: int
    query_id: str
    examples_used: List[str]
    status: str  # 'pass', 'fail', 'error'
    speedup: float
    error_message: Optional[str]
    error_category: Optional[str]  # 'syntax', 'semantic', 'timeout', 'execution'
    original_time_ms: float
    optimized_time_ms: float
    optimized_sql: Optional[str]
    prompt_tokens: int
    response_tokens: int
    llm_time_ms: float


@dataclass
class QueryResult:
    """Consolidated result for a query across all workers."""
    query_id: str
    best_worker: Optional[int]
    best_speedup: float
    best_status: str
    all_workers: List[WorkerResult]
    original_speedup: float  # From previous run
    improvement: float  # best_speedup - original_speedup
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def find_query_file(query_id: str) -> Optional[Path]:
    """Find SQL file for query ID (e.g., 'q16' -> query_16.sql)."""
    num = int(query_id.replace("q", ""))
    for pattern in QUERY_PATTERNS:
        path = PROJECT_ROOT / pattern.format(num=num)
        if path.exists():
            return path
    return None


def load_query_sql(query_id: str) -> Optional[str]:
    """Load SQL for a query ID."""
    path = find_query_file(query_id)
    if path:
        return path.read_text()
    return None


def categorize_error(error_msg: str) -> str:
    """Categorize error message."""
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
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> WorkerResult:
    """Run a single optimization worker."""
    start_time = time.time()

    # Load gold examples
    examples: List[GoldExample] = []
    for ex_id in example_ids:
        ex = load_example(ex_id)
        if ex:
            examples.append(ex)

    logger.info(f"  W{worker_id}: Loaded {len(examples)} examples: {[e.id for e in examples]}")

    try:
        # Get execution plan using v2 benchmark function (includes misestimates, joins)
        plan_summary, plan_text, plan_ctx = _get_plan_context(db_path, sql)

        # Build prompt (NO constraints - matches v2 benchmark format)
        pipeline = DagV2Pipeline(sql, plan_context=plan_ctx)
        base_prompt = pipeline.get_prompt()
        full_prompt = build_prompt_with_examples(
            base_prompt, examples, plan_summary,
            history_section="",
            include_constraints=False  # v2 benchmark format has NO constraints
        )

        prompt_tokens = len(full_prompt) // 4  # Rough estimate

        # Call LLM
        llm_start = time.time()
        llm_client = _create_llm_client(provider, model)
        response_text = llm_client.analyze(full_prompt)
        llm_time_ms = (time.time() - llm_start) * 1000

        response_tokens = len(response_text) // 4  # Rough estimate

        # Apply response
        optimized_sql = pipeline.apply_response(response_text)

        # Validate
        validator = SQLValidator(database=db_path)
        result = validator.validate(sql, optimized_sql)

        status = "pass" if result.status == ValidationStatus.PASS else "fail"
        error_msg = result.errors[0] if result.errors else None
        error_cat = categorize_error(error_msg) if error_msg else None

        return WorkerResult(
            worker_id=worker_id,
            query_id=query_id,
            examples_used=example_ids,
            status=status,
            speedup=result.speedup,
            error_message=error_msg,
            error_category=error_cat,
            original_time_ms=result.original_time if hasattr(result, 'original_time') else 0,
            optimized_time_ms=result.optimized_time if hasattr(result, 'optimized_time') else 0,
            optimized_sql=optimized_sql if status == "pass" else None,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            llm_time_ms=llm_time_ms,
        )

    except Exception as e:
        logger.error(f"  W{worker_id}: Error - {e}")
        return WorkerResult(
            worker_id=worker_id,
            query_id=query_id,
            examples_used=example_ids,
            status="error",
            speedup=0.0,
            error_message=str(e),
            error_category=categorize_error(str(e)),
            original_time_ms=0,
            optimized_time_ms=0,
            optimized_sql=None,
            prompt_tokens=0,
            response_tokens=0,
            llm_time_ms=(time.time() - start_time) * 1000,
        )


def run_query_3workers(
    query_id: str,
    db_path: str,
    original_speedup: float = 0.0,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> QueryResult:
    """Run 3 workers in parallel for a query."""
    sql = load_query_sql(query_id)
    if not sql:
        logger.error(f"Query file not found: {query_id}")
        return QueryResult(
            query_id=query_id,
            best_worker=None,
            best_speedup=0.0,
            best_status="error",
            all_workers=[],
            original_speedup=original_speedup,
            improvement=0.0,
        )

    logger.info(f"\n{'='*60}")
    logger.info(f"Processing {query_id.upper()} (original speedup: {original_speedup:.2f}x)")
    logger.info(f"{'='*60}")

    # Run 3 workers in parallel
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {}
        for worker_id, example_ids in WORKER_EXAMPLES.items():
            future = pool.submit(
                run_worker,
                worker_id,
                query_id,
                sql,
                db_path,
                example_ids,
                provider,
                model,
            )
            futures[future] = worker_id

        # Collect results
        worker_results = []
        for future in as_completed(futures):
            worker_id = futures[future]
            try:
                result = future.result()
                worker_results.append(result)

                status_icon = "✓" if result.status == "pass" else "✗"
                logger.info(f"  W{worker_id} [{status_icon}]: {result.status} | speedup={result.speedup:.2f}x | err={result.error_category or 'none'}")
            except Exception as e:
                logger.error(f"  W{worker_id}: Exception - {e}")

    # Find best result
    valid_results = [r for r in worker_results if r.status == "pass" and r.speedup > 1.0]

    if valid_results:
        best = max(valid_results, key=lambda r: r.speedup)
        return QueryResult(
            query_id=query_id,
            best_worker=best.worker_id,
            best_speedup=best.speedup,
            best_status="pass",
            all_workers=worker_results,
            original_speedup=original_speedup,
            improvement=best.speedup - original_speedup,
        )
    else:
        # No valid improvements - return best attempt
        best = max(worker_results, key=lambda r: r.speedup) if worker_results else None
        return QueryResult(
            query_id=query_id,
            best_worker=best.worker_id if best else None,
            best_speedup=best.speedup if best else 0.0,
            best_status=best.status if best else "error",
            all_workers=worker_results,
            original_speedup=original_speedup,
            improvement=(best.speedup - original_speedup) if best else 0.0,
        )


# ============================================================================
# MAIN SCRIPT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="3-Worker Fan-Out Retry for Regression Queries")
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument("--queries", help="Comma-separated query IDs (default: all 25 pending)")
    parser.add_argument("--output-dir", default="retry_results", help="Output directory")
    parser.add_argument("--provider", help="LLM provider override")
    parser.add_argument("--model", help="LLM model override")
    args = parser.parse_args()

    # Parse queries
    if args.queries:
        query_ids = [q.strip() for q in args.queries.split(",")]
    else:
        query_ids = PENDING_RETRIES

    # Original speedups (from your data)
    ORIGINAL_SPEEDUPS = {
        "q16": 0.06, "q94": 0.25, "q34": 0.32, "q9": 0.47, "q26": 0.60,
        "q4": 0.85, "q5": 0.93, "q42": 0.93, "q58": 0.94, "q91": 0.94,
        "q12": 0.95, "q29": 0.95, "q63": 0.95, "q43": 0.96, "q38": 0.96,
        "q48": 0.96, "q37": 0.97, "q82": 0.97, "q96": 0.97, "q22": 0.98,
        "q25": 0.98, "q53": 0.98, "q7": 0.99, "q73": 0.99, "q75": 0.99,
    }

    # Setup output
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = output_dir / f"retry_3worker_{timestamp}.csv"
    details_file = output_dir / f"retry_3worker_{timestamp}_details.json"

    logger.info(f"\n{'#'*60}")
    logger.info(f"# 3-WORKER FAN-OUT RETRY")
    logger.info(f"# Queries: {len(query_ids)}")
    logger.info(f"# Database: {args.db}")
    logger.info(f"# Output: {output_dir}")
    logger.info(f"{'#'*60}")
    logger.info(f"\nWorker Strategy:")
    for wid, examples in WORKER_EXAMPLES.items():
        logger.info(f"  W{wid}: {', '.join(examples)}")

    # Process queries
    all_results: List[QueryResult] = []
    start_time = time.time()

    for i, qid in enumerate(query_ids, 1):
        logger.info(f"\n[{i}/{len(query_ids)}] Processing {qid}...")
        original_speedup = ORIGINAL_SPEEDUPS.get(qid, 0.0)

        result = run_query_3workers(
            qid,
            args.db,
            original_speedup,
            args.provider,
            args.model,
        )
        all_results.append(result)

    total_time = time.time() - start_time

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")

    improved = [r for r in all_results if r.improvement > 0]
    fixed = [r for r in all_results if r.best_speedup >= 1.0]

    logger.info(f"Total queries: {len(all_results)}")
    logger.info(f"Improved: {len(improved)} ({100*len(improved)/len(all_results):.1f}%)")
    logger.info(f"Fixed (>= 1.0x): {len(fixed)} ({100*len(fixed)/len(all_results):.1f}%)")
    logger.info(f"Total time: {total_time/60:.1f} minutes")

    # Top improvements
    logger.info("\nTop Improvements:")
    top = sorted(all_results, key=lambda r: r.improvement, reverse=True)[:10]
    for r in top:
        if r.improvement > 0:
            logger.info(f"  {r.query_id}: {r.original_speedup:.2f}x → {r.best_speedup:.2f}x (+{r.improvement:.2f}x) [W{r.best_worker}]")

    # Write CSV
    with open(results_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "query_id", "original_speedup", "best_speedup", "improvement",
            "best_worker", "best_status",
            "w1_speedup", "w1_status", "w1_error",
            "w2_speedup", "w2_status", "w2_error",
            "w3_speedup", "w3_status", "w3_error",
        ])

        for r in all_results:
            w_data = {w.worker_id: w for w in r.all_workers}
            row = [
                r.query_id,
                f"{r.original_speedup:.3f}",
                f"{r.best_speedup:.3f}",
                f"{r.improvement:.3f}",
                r.best_worker,
                r.best_status,
            ]
            for wid in [1, 2, 3]:
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
            "worker_examples": WORKER_EXAMPLES,
        },
        "summary": {
            "total_queries": len(all_results),
            "improved": len(improved),
            "fixed": len(fixed),
            "total_time_minutes": total_time / 60,
        },
        "results": [asdict(r) for r in all_results],
    }

    with open(details_file, "w") as f:
        json.dump(details, f, indent=2, default=str)

    logger.info(f"Details saved to: {details_file}")

    # Save winning SQL files
    sql_dir = output_dir / "winning_sql"
    sql_dir.mkdir(exist_ok=True)

    for r in all_results:
        if r.best_status == "pass" and r.best_speedup > 1.0:
            for w in r.all_workers:
                if w.worker_id == r.best_worker and w.optimized_sql:
                    sql_file = sql_dir / f"{r.query_id}_optimized_{r.best_speedup:.2f}x.sql"
                    sql_file.write_text(w.optimized_sql)
                    logger.info(f"Saved: {sql_file.name}")


if __name__ == "__main__":
    main()
