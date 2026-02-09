#!/usr/bin/env python3
"""Generate ADO prompts for all DSB queries on PostgreSQL.

Runs Phase 1 (Parse) → Phase 2 (FAISS) → Phase 3 (Build prompt)
for each query and saves prompts to state_0/prompts/.

Usage:
    python3 generate_prompts.py               # Generate all 52
    python3 generate_prompts.py query001_multi query065_multi  # Specific queries
"""

import json
import sys
import time
from pathlib import Path

# Add qt-sql package to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from qt_sql.pipeline import Pipeline


def main():
    benchmark_dir = Path(__file__).resolve().parent
    p = Pipeline(benchmark_dir=str(benchmark_dir))

    # Create output dirs
    state_dir = benchmark_dir / "state_0"
    prompts_dir = state_dir / "prompts"
    metadata_dir = state_dir / "metadata"
    for d in [prompts_dir, metadata_dir, state_dir / "responses", state_dir / "validation"]:
        d.mkdir(parents=True, exist_ok=True)

    # Load queries
    requested_ids = sys.argv[1:] if len(sys.argv) > 1 else None
    queries = p._load_queries(query_ids=requested_ids)
    print(f"Generating prompts for {len(queries)} DSB queries (PostgreSQL)")
    print(f"Output: {prompts_dir}")
    print()

    dialect = "postgres"
    engine = "postgres"
    results = []
    errors = []

    for i, (qid, sql) in enumerate(sorted(queries.items()), 1):
        try:
            t0 = time.time()

            # Phase 1: Parse
            dag, costs, _explain = p._parse_dag(sql, dialect=dialect)

            # Phase 2: Find FAISS examples
            examples = p._find_examples(sql, engine=engine, k=3)
            example_ids = [e.get("id", "?") for e in examples]

            # Phase 3: Build prompt
            prompt = p.prompter.build_prompt(
                query_id=qid,
                full_sql=sql,
                dag=dag,
                costs=costs,
                examples=examples,
                dialect=dialect,
            )

            elapsed = time.time() - t0

            # Save prompt
            (prompts_dir / f"{qid}.txt").write_text(prompt)

            # Save metadata
            meta_data = {
                "query_id": qid,
                "dag_nodes": len(dag.nodes),
                "dag_edges": len(dag.edges),
                "examples_matched": example_ids,
                "prompt_length": len(prompt),
                "generation_time_s": round(elapsed, 2),
            }
            (metadata_dir / f"{qid}.json").write_text(
                json.dumps(meta_data, indent=2)
            )

            results.append(meta_data)

            print(
                f"  [{i:2d}/{len(queries)}] ✓ {qid:30s} "
                f"nodes={len(dag.nodes):2d}  "
                f"examples={example_ids}  "
                f"prompt={len(prompt):,d}ch  ({elapsed:.1f}s)"
            )

        except Exception as e:
            errors.append({"query_id": qid, "error": str(e)})
            print(f"  [{i:2d}/{len(queries)}] ✗ {qid:30s}  ERROR: {e}")

    # Save summary
    summary = {
        "total_queries": len(queries),
        "prompts_generated": len(results),
        "errors": len(errors),
        "examples_used": {},
        "avg_prompt_length": 0,
    }

    for r in results:
        for ex in r["examples_matched"]:
            summary["examples_used"][ex] = summary["examples_used"].get(ex, 0) + 1

    if results:
        summary["avg_prompt_length"] = round(
            sum(r["prompt_length"] for r in results) / len(results)
        )

    summary["errors_detail"] = errors

    (state_dir / "prompt_generation_summary.json").write_text(
        json.dumps(summary, indent=2)
    )

    # Print summary
    print()
    print("=" * 70)
    print(f"DONE: {len(results)}/{len(queries)} prompts generated, {len(errors)} errors")
    print(f"Avg prompt length: {summary['avg_prompt_length']:,} chars")
    print()
    print("Example frequency:")
    for ex, count in sorted(
        summary["examples_used"].items(), key=lambda x: -x[1]
    ):
        print(f"  {ex:40s} {count:3d} queries")

    if errors:
        print()
        print("ERRORS:")
        for e in errors:
            print(f"  {e['query_id']}: {e['error']}")


if __name__ == "__main__":
    main()
