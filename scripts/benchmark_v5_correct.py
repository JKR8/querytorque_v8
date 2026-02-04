#!/usr/bin/env python3
"""
V5 Benchmark - All 99 queries using ACTUAL v5 DSPy implementation

Uses the existing optimize_v5_dspy() function which implements:
- Workers 1-4: Coverage with DSPy demos (from 11 gold examples)
- Worker 5: Explore mode (no examples, adversarial with full plan)

Usage:
    export DEEPSEEK_API_KEY=your_key_here
    .venv/bin/python scripts/benchmark_v5_correct.py
"""

import sys
import csv
import json
import time
from pathlib import Path
from datetime import datetime

# Add packages to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))

from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_dspy
from qt_sql.validation.schemas import ValidationStatus


def load_query(query_num: int) -> str:
    """Load TPC-DS query."""
    queries_dir = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
    patterns = [
        f"query_{query_num}.sql",
        f"query{query_num:02d}.sql",
        f"query{query_num}.sql",
    ]
    for pattern in patterns:
        path = queries_dir / pattern
        if path.exists():
            return path.read_text()
    raise FileNotFoundError(f"Query {query_num} not found")


def main():
    print("="*70)
    print("V5 Benchmark - 99 Queries - Official v5 DSPy Implementation")
    print("="*70)
    print()
    print("Version: DSPy (not JSON)")
    print("Workers per query: 5 (4 coverage + 1 explore)")
    print("Total API calls: ~495 (99 × 5)")
    print()

    # Create output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = project_root / "research" / "experiments" / "v5_official" / f"run_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "results.csv"
    sample_db = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"

    print(f"Output: {output_dir}")
    print(f"Sample DB: {sample_db}")
    print()

    # CSV setup
    fieldnames = [
        "query",
        "status",
        "elapsed",
        "speedup",
        "worker_id",
        "error",
    ]

    start_benchmark = time.time()
    results = []

    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for q in range(1, 100):
            print(f"\n{'='*70}")
            print(f"Query {q}")
            print(f"{'='*70}")

            query_start = time.time()

            try:
                sql = load_query(q)

                # Call official v5 DSPy implementation
                result = optimize_v5_dspy(
                    sql=sql,
                    sample_db=sample_db,
                    max_workers=5,
                    provider="deepseek",
                )

                elapsed = time.time() - query_start

                row = {
                    "query": q,
                    "status": result.status.value,
                    "elapsed": round(elapsed, 2),
                    "speedup": round(result.speedup, 2),
                    "worker_id": result.worker_id,
                    "error": result.error or "",
                }

                results.append(row)
                writer.writerow(row)
                f.flush()

                status = "✅" if result.status == ValidationStatus.PASS else "❌"
                print(f"{status} Q{q}: Worker {result.worker_id}, {result.speedup:.2f}x, {elapsed:.1f}s")

                # Save query artifacts
                query_dir = output_dir / f"q{q}"
                query_dir.mkdir(exist_ok=True)
                (query_dir / "original.sql").write_text(sql)
                (query_dir / "optimized.sql").write_text(result.optimized_sql)
                (query_dir / "prompt.txt").write_text(result.prompt)
                (query_dir / "response.txt").write_text(result.response)

                metadata = {
                    "query": q,
                    "worker_id": result.worker_id,
                    "status": result.status.value,
                    "speedup": result.speedup,
                    "error": result.error,
                    "elapsed": elapsed,
                }
                (query_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

            except Exception as e:
                elapsed = time.time() - query_start
                print(f"❌ Q{q} failed: {e}")

                row = {
                    "query": q,
                    "status": "error",
                    "elapsed": round(elapsed, 2),
                    "speedup": 0.0,
                    "worker_id": 0,
                    "error": str(e),
                }
                results.append(row)
                writer.writerow(row)
                f.flush()

    elapsed_total = time.time() - start_benchmark

    # Final summary
    print("\n" + "="*70)
    print("BENCHMARK COMPLETE")
    print("="*70)
    print()

    successful = [r for r in results if r["status"] == "pass"]
    failed = [r for r in results if r["status"] != "pass"]

    print(f"Total queries: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print()

    if successful:
        best = max(successful, key=lambda x: x["speedup"])
        avg_speedup = sum(r["speedup"] for r in successful) / len(successful)

        print(f"Best speedup: {best['speedup']:.2f}x (Q{best['query']})")
        print(f"Average speedup: {avg_speedup:.2f}x")
        print()

    print(f"Total time: {elapsed_total/3600:.1f}h")
    print(f"Results: {csv_path}")
    print()

    # Save summary
    summary = {
        "total_queries": len(results),
        "successful": len(successful),
        "failed": len(failed),
        "best_speedup": max([r["speedup"] for r in successful], default=0.0),
        "avg_speedup": sum(r["speedup"] for r in successful) / len(successful) if successful else 0.0,
        "elapsed_hours": round(elapsed_total / 3600, 2),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
