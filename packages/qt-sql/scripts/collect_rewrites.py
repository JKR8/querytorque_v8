#!/usr/bin/env python3
"""
Collect SQL rewrites from 5 parallel API calls per query - NO validation.

Simply collects the LLM outputs for analysis.
"""

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from qt_sql.optimization.dag_v2 import DagV2Pipeline
from qt_sql.optimization.dag_v3 import (
    build_prompt_with_examples,
    load_all_examples,
    load_example,
)
from qt_sql.optimization.query_recommender import get_query_recommendations
from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization
from qt_sql.execution.database_utils import run_explain_analyze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
QUERIES_DIR = Path("/mnt/d/TPC-DS/queries_duckdb_converted")


def create_llm_client(provider=None, model=None):
    """Create LLM client."""
    from qt_shared.llm import create_llm_client as _create
    client = _create(provider=provider, model=model)
    if not client:
        raise RuntimeError("No LLM provider configured")
    return client


def load_query(query_num: int) -> str:
    """Load SQL query by number."""
    path = QUERIES_DIR / f"query_{query_num}.sql"
    if path.exists():
        return path.read_text()
    raise FileNotFoundError(f"Query {query_num} not found")


def get_plan_context(db_path: str, sql: str):
    """Get plan summary and context."""
    result = run_explain_analyze(db_path, sql) or {}
    plan_json = result.get("plan_json")
    plan_text = result.get("plan_text") or "(plan not available)"

    if not plan_json:
        return "(plan not available)", plan_text, None

    ctx = analyze_plan_for_optimization(plan_json, sql)
    return format_plan_summary(ctx), plan_text, ctx


def format_plan_summary(ctx) -> str:
    """Compact plan summary."""
    lines = []

    scan_counts = {}
    scan_by_table = {}
    for scan in ctx.table_scans:
        scan_counts[scan.table] = scan_counts.get(scan.table, 0) + 1
        scan_by_table.setdefault(scan.table, []).append(scan)

    for table in scan_by_table:
        scan_by_table[table].sort(key=lambda s: (s.rows_scanned, s.rows_out), reverse=True)

    top_ops = ctx.get_top_operators(5)
    if top_ops:
        lines.append("Operators by cost:")
        for op in top_ops:
            label = op["operator"]
            lines.append(f"- {label}: {op['cost_pct']}% cost, {op['rows']:,} rows")
        lines.append("")

    if scan_by_table:
        lines.append("Scans:")
        for table, scans in sorted(scan_by_table.items(), key=lambda kv: kv[1][0].rows_scanned, reverse=True)[:8]:
            s = scans[0]
            count = scan_counts[table]
            if s.has_filter:
                lines.append(f"- {table} x{count}: {s.rows_scanned:,} → {s.rows_out:,} rows (filtered)")
            else:
                lines.append(f"- {table} x{count}: {s.rows_scanned:,} rows (no filter)")
        lines.append("")

    return "\n".join(lines).strip() or "(plan not available)"


def get_ml_examples(query_id: str, batch_num: int):
    """Get ML-prioritized examples for a specific batch (1-4)."""
    ml_recs = get_query_recommendations(query_id, top_n=12)
    all_examples = load_all_examples()
    example_by_id = {ex.id: ex for ex in all_examples}

    # Build prioritized list
    prioritized = []
    for rec_id in ml_recs:
        if rec_id in example_by_id:
            prioritized.append(example_by_id[rec_id])
        else:
            for ex_id, ex in example_by_id.items():
                if rec_id in ex_id or ex_id.startswith(rec_id):
                    if ex not in prioritized:
                        prioritized.append(ex)
                        break

    # Pad with remaining
    for ex in all_examples:
        if ex not in prioritized:
            prioritized.append(ex)

    # Return batch of 3
    start = (batch_num - 1) * 3
    return prioritized[start:start + 3]


def call_worker(
    worker_id: int,
    sql: str,
    query_id: str,
    base_prompt: str,
    plan_summary: str,
    plan_text: str,
    output_dir: Path,
    provider: str = None,
    model: str = None,
):
    """Single worker - makes API call, saves prompt + response + SQL."""
    try:
        # Get examples for this worker
        if worker_id <= 4:
            examples = get_ml_examples(query_id, worker_id)
            prompt = build_prompt_with_examples(base_prompt, examples, plan_summary, "")
        else:
            # Worker 5: Full SQL adversarial mode
            prompt = f"""You are a SQL optimizer. Rewrite the ENTIRE query for maximum performance.

## Adversarial Explore Mode
Be creative and aggressive. Try radical structural rewrites.

## Original Query
```sql
{sql}
```

## Full Execution Plan
```
{plan_text}
```

## Instructions
1. Analyze bottlenecks
2. Rewrite for maximum performance
3. Try: decorrelate, OR→UNION ALL, push filters, materialize CTEs, reorder joins

Return ONLY the complete optimized SQL. No JSON. No explanation.
"""

        # Save prompt
        prompt_file = output_dir / f"worker_{worker_id}_prompt.txt"
        prompt_file.write_text(prompt)

        # Make API call
        client = create_llm_client(provider, model)
        start = time.time()
        response = client.analyze(prompt)
        duration = time.time() - start

        # Save response
        response_file = output_dir / f"worker_{worker_id}_response.txt"
        response_file.write_text(response)

        # Extract SQL
        if worker_id <= 4:
            # DAG JSON mode - use pipeline to extract
            pipeline = DagV2Pipeline(sql)
            try:
                optimized_sql = pipeline.apply_response(response)
            except Exception as e:
                optimized_sql = f"-- ASSEMBLY FAILED: {e}\n-- Raw response saved in worker_{worker_id}_response.txt"
        else:
            # Full SQL mode - extract from response
            optimized_sql = response.strip()
            if optimized_sql.startswith('```'):
                lines = optimized_sql.split('\n')
                if lines[0].startswith('```'):
                    lines = lines[1:]
                if lines and lines[-1].strip() == '```':
                    lines = lines[:-1]
                optimized_sql = '\n'.join(lines)

        # Save SQL
        sql_file = output_dir / f"worker_{worker_id}_optimized.sql"
        sql_file.write_text(optimized_sql)

        return {
            "worker_id": worker_id,
            "status": "success",
            "duration_s": round(duration, 1),
            "response_chars": len(response),
            "examples": [e.id for e in examples] if worker_id <= 4 else ["adversarial"],
        }

    except Exception as e:
        logger.error(f"Worker {worker_id} failed: {e}")
        return {
            "worker_id": worker_id,
            "status": "error",
            "error": str(e),
        }


def process_query(query_num: int, output_base: Path, provider: str = None, model: str = None):
    """Process a single query with 5 parallel workers."""
    query_id = f"q{query_num}"
    output_dir = output_base / query_id
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Processing Q{query_num}")

    # Load query
    sql = load_query(query_num)
    (output_dir / "original.sql").write_text(sql)

    # Get plan context
    plan_summary, plan_text, plan_context = get_plan_context(SAMPLE_DB, sql)
    (output_dir / "plan_summary.txt").write_text(plan_summary)

    # Build base prompt
    base_prompt = DagV2Pipeline(sql, plan_context=plan_context).get_prompt()
    (output_dir / "base_prompt.txt").write_text(base_prompt)

    # Log ML recommendations
    ml_recs = get_query_recommendations(query_id, top_n=12)
    logger.info(f"  ML recommendations: {ml_recs[:6]}")

    # Run 5 workers in parallel
    results = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = []
        for worker_id in range(1, 6):
            futures.append(pool.submit(
                call_worker,
                worker_id, sql, query_id,
                base_prompt, plan_summary, plan_text,
                output_dir, provider, model
            ))

        for future in as_completed(futures):
            results.append(future.result())

    total_time = round(time.time() - start, 1)

    # Sort by worker_id
    results.sort(key=lambda r: r["worker_id"])

    # Save summary
    summary = {
        "query_num": query_num,
        "query_id": query_id,
        "ml_recommendations": ml_recs,
        "total_time_s": total_time,
        "workers": results,
        "timestamp": datetime.now().isoformat(),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    # Log results
    success_count = sum(1 for r in results if r["status"] == "success")
    logger.info(f"  Completed: {success_count}/5 workers in {total_time}s")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Collect SQL rewrites (no validation)")
    parser.add_argument("--output-dir", "-o", default="./rewrite_collection", help="Output directory")
    parser.add_argument("--start", type=int, default=1, help="Start query number")
    parser.add_argument("--end", type=int, default=99, help="End query number")
    parser.add_argument("--queries", help="Specific queries (comma-separated)")
    parser.add_argument("--provider", help="LLM provider")
    parser.add_argument("--model", help="LLM model")

    args = parser.parse_args()

    output_base = Path(args.output_dir)
    output_base.mkdir(parents=True, exist_ok=True)

    # Determine queries
    if args.queries:
        query_nums = [int(q.strip()) for q in args.queries.split(",")]
    else:
        query_nums = list(range(args.start, args.end + 1))

    logger.info(f"Collecting rewrites for {len(query_nums)} queries")
    logger.info(f"Output: {output_base}")

    all_summaries = []
    for query_num in query_nums:
        try:
            summary = process_query(query_num, output_base, args.provider, args.model)
            all_summaries.append(summary)
        except Exception as e:
            logger.error(f"Q{query_num} failed: {e}")
            all_summaries.append({"query_num": query_num, "status": "error", "error": str(e)})

    # Save overall summary
    overall = {
        "total_queries": len(query_nums),
        "timestamp": datetime.now().isoformat(),
        "queries": all_summaries,
    }
    (output_base / "collection_summary.json").write_text(json.dumps(overall, indent=2))

    logger.info(f"Done. Results in {output_base}")


if __name__ == "__main__":
    main()
