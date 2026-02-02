#!/usr/bin/env python3
"""
Batch SQL Optimization with Kimi K2.5 via OpenRouter

Fire all queries in parallel, collect optimized SQL.

Usage:
    python batch_optimize_kimi.py                     # Queries 1-30
    python batch_optimize_kimi.py --start 1 --end 10  # Queries 1-10
"""

import argparse
import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List

# Add packages to path
REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-sql"))
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-shared"))

from openai import OpenAI

# Configuration
OPENROUTER_API_BASE = "https://openrouter.ai/api/v1"
MODEL_NAME = "moonshotai/kimi-k2.5"

# Paths
QUERIES_DIR = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
OUTPUT_BASE = REPO_ROOT / "research" / "experiments" / "optimizations"


@dataclass
class OptimizationResult:
    """Result of optimizing a single query."""
    query_num: int
    status: str  # success, failed, error
    original_sql: str
    optimized_sql: Optional[str] = None
    raw_response: Optional[str] = None
    error: Optional[str] = None
    latency_ms: float = 0
    tokens_in: int = 0
    tokens_out: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def get_api_key() -> str:
    """Get OpenRouter API key."""
    key = os.getenv("OPENROUTER_API_KEY")
    if not key:
        key_file = REPO_ROOT / "openrouter.txt"
        if key_file.exists():
            key = key_file.read_text().strip()
    if not key:
        print("ERROR: Set OPENROUTER_API_KEY or create openrouter.txt")
        sys.exit(1)
    return key


def load_query(query_num: int) -> str:
    """Load query SQL by number."""
    sql_file = QUERIES_DIR / f"query_{query_num}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"Query file not found: {sql_file}")
    return sql_file.read_text()


def optimize_query(
    query_num: int,
    client: OpenAI,
    output_dir: Path,
) -> OptimizationResult:
    """Optimize a single query with DAG mode."""
    from qt_sql.optimization.dag_v2 import DagV2Pipeline, get_dag_v2_examples

    query_dir = output_dir / f"q{query_num}"
    query_dir.mkdir(parents=True, exist_ok=True)

    result = OptimizationResult(
        query_num=query_num,
        status="pending",
        original_sql="",
    )

    try:
        # Load query
        sql = load_query(query_num)
        result.original_sql = sql
        (query_dir / "original.sql").write_text(sql)

        # Build DAG pipeline and prompt
        pipeline = DagV2Pipeline(sql)

        # Build few-shot examples
        examples = get_dag_v2_examples()
        few_shot_parts = []
        for ex in examples[:2]:
            few_shot_parts.append(f"### Example: {ex['opportunity']}")
            few_shot_parts.append(f"Input:\n{ex['input_slice']}")
            few_shot_parts.append(f"Output:\n```json\n{json.dumps(ex['output'], indent=2)}\n```")
            if 'key_insight' in ex:
                few_shot_parts.append(f"Key insight: {ex['key_insight']}")
            few_shot_parts.append("")

        few_shot = "\n".join(few_shot_parts)
        base_prompt = pipeline.get_prompt()
        full_prompt = f"## Examples\n\n{few_shot}\n\n---\n\n## Your Task\n\n{base_prompt}"

        # Save inputs
        (query_dir / "input_prompt.txt").write_text(full_prompt)
        (query_dir / "input_dag.txt").write_text(pipeline.get_dag_summary())

        # Call API
        start = time.perf_counter()
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": full_prompt}],
            temperature=0.1,
            max_tokens=8192,
            timeout=600,
        )
        latency = (time.perf_counter() - start) * 1000

        content = response.choices[0].message.content
        tokens_in = response.usage.prompt_tokens if response.usage else 0
        tokens_out = response.usage.completion_tokens if response.usage else 0

        result.raw_response = content
        result.latency_ms = latency
        result.tokens_in = tokens_in
        result.tokens_out = tokens_out

        # Save raw response
        (query_dir / "output_raw.txt").write_text(content)

        # Apply response to get optimized SQL
        optimized_sql = pipeline.apply_response(content)
        result.optimized_sql = optimized_sql
        result.status = "success"

        (query_dir / "output_optimized.sql").write_text(optimized_sql)

    except FileNotFoundError as e:
        result.status = "error"
        result.error = str(e)

    except Exception as e:
        result.status = "failed"
        result.error = f"{type(e).__name__}: {str(e)}"
        (query_dir / "error.txt").write_text(f"{result.error}\n\n{traceback.format_exc()}")

    # Save result
    (query_dir / "result.json").write_text(json.dumps(result.to_dict(), indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description="Batch SQL Optimization")
    parser.add_argument("--start", "-s", type=int, default=1)
    parser.add_argument("--end", "-e", type=int, default=30)
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = OUTPUT_BASE / f"kimi_q{args.start}-q{args.end}_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Batch Optimization: Q{args.start}-Q{args.end}")
    print(f"Model: {MODEL_NAME}")
    print(f"Output: {output_dir}")
    print(f"{'='*60}\n")

    api_key = get_api_key()
    client = OpenAI(
        api_key=api_key,
        base_url=OPENROUTER_API_BASE,
        default_headers={
            "HTTP-Referer": "https://querytorque.com",
            "X-Title": "QueryTorque Batch"
        }
    )

    query_nums = list(range(args.start, args.end + 1))
    results: List[OptimizationResult] = []

    print(f"Firing {len(query_nums)} parallel requests...\n")

    # Fire all in parallel
    with ThreadPoolExecutor(max_workers=len(query_nums)) as executor:
        futures = {
            executor.submit(optimize_query, q, client, output_dir): q
            for q in query_nums
        }

        for future in as_completed(futures):
            query_num = futures[future]
            try:
                result = future.result()
                results.append(result)
                status_char = "✓" if result.status == "success" else "✗"
                print(f"  Q{query_num}: {status_char} ({result.latency_ms:.0f}ms)")
            except Exception as e:
                print(f"  Q{query_num}: EXCEPTION ({e})")

    # Summary
    results.sort(key=lambda r: r.query_num)
    success = [r for r in results if r.status == "success"]
    failed = [r for r in results if r.status != "success"]

    print(f"\n{'='*60}")
    print(f"Results: {len(success)}/{len(results)} successful")
    print(f"Total tokens: {sum(r.tokens_in for r in results):,} in, {sum(r.tokens_out for r in results):,} out")

    if failed:
        print(f"\nFailed queries:")
        for r in failed:
            print(f"  Q{r.query_num}: {r.error[:60] if r.error else 'unknown'}")

    # Save summary
    (output_dir / "summary.json").write_text(json.dumps({
        "timestamp": timestamp,
        "success": len(success),
        "failed": len(failed),
        "results": [r.to_dict() for r in results],
    }, indent=2))

    # Create benchmark folder
    benchmark_dir = output_dir / "benchmark_ready"
    benchmark_dir.mkdir(exist_ok=True)
    for r in results:
        if r.status == "success" and r.optimized_sql:
            (benchmark_dir / f"q{r.query_num}_original.sql").write_text(r.original_sql)
            (benchmark_dir / f"q{r.query_num}_optimized.sql").write_text(r.optimized_sql)

    print(f"\nBenchmark-ready: {benchmark_dir}")


if __name__ == "__main__":
    main()
