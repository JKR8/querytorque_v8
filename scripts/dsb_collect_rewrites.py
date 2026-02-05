#!/usr/bin/env python3
"""DSB Rewrite Collection Script - Round-based approach.

Round 1: Collect all LLM rewrite responses (this script)
Round 2: Benchmark all responses (separate script)
Round 3: Generate feedback from results (separate script)
Round 4: Repeat with feedback

Usage:
    source .venv/bin/activate
    python scripts/dsb_collect_rewrites.py --round 1

Providers:
    --provider deepseek --model deepseek-reasoner (default)
    --provider openrouter --model moonshotai/kimi-k2.5
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "qt-sql"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "qt-shared"))

from ado.knowledge import KnowledgeRetriever
from ado.prompt_builder import PromptBuilder, GoldExample
from ado.context import ContextBuilder


def load_dsb_queries(queries_dir: Path) -> dict[str, str]:
    """Load all DSB queries from directory."""
    queries = {}
    for sql_file in sorted(queries_dir.glob("*.sql")):
        query_id = sql_file.stem
        queries[query_id] = sql_file.read_text().strip()
    return queries


def load_general_learnings(learnings_file: Path | None) -> str:
    """Load general learnings that apply to all queries."""
    if not learnings_file or not learnings_file.exists():
        return ""

    return learnings_file.read_text().strip()


def process_single_query(
    query_id: str,
    sql: str,
    output_dir: Path,
    round_num: int,
    sample_db: str,
    general_learnings: str,
    feedback_by_query: dict,
    generator,
    use_llm: bool,
    print_lock: Lock,
    progress: dict,
) -> dict:
    """Process a single query (for parallel execution)."""
    from ado.knowledge import KnowledgeRetriever
    from ado.prompt_builder import PromptBuilder, GoldExample
    from ado.context import ContextBuilder

    start_time = time.time()

    # Thread-local instances
    knowledge = KnowledgeRetriever()
    prompt_builder = PromptBuilder()
    context_builder = ContextBuilder(engine="postgres")

    query_output_dir = output_dir / query_id
    query_output_dir.mkdir(exist_ok=True)

    # 1. Get context (EXPLAIN plan)
    try:
        context = context_builder.build(
            query_id=query_id,
            sql=sql,
            sample_db=sample_db,
        )
        execution_plan = context.plan_summary
    except Exception as e:
        execution_plan = ""

    # 2. Retrieve examples using DSB mapping
    retrieval = knowledge.retrieve(sql, k_examples=3, query_id=query_id)
    examples_used = [ex.id for ex in retrieval.gold_examples]

    # Convert to prompt format
    prompt_examples = [
        GoldExample(
            id=ex.id,
            name=ex.name,
            description=ex.description,
            verified_speedup=ex.verified_speedup,
            example=ex.example,
        )
        for ex in retrieval.gold_examples
    ]

    # 3. Build history from learnings and feedback
    history_parts = []
    if general_learnings:
        history_parts.append(f"## General Learnings (Apply to All Queries)\n{general_learnings}")
    if query_id in feedback_by_query:
        history_parts.append(f"## Previous Attempt Feedback for {query_id}\n{feedback_by_query[query_id]}")
    history = "\n\n".join(history_parts)

    # 4. Build prompt
    prompt = prompt_builder.build(
        original_sql=sql,
        execution_plan=execution_plan,
        history=history,
        use_specific_examples=prompt_examples,
    )

    # Save prompt
    (query_output_dir / "prompt.txt").write_text(prompt)
    (query_output_dir / "original.sql").write_text(sql)
    (query_output_dir / "examples_used.json").write_text(
        json.dumps(examples_used, indent=2)
    )

    # 5. Call LLM if available
    response = None
    optimized_sql = None
    transforms = []
    error_msg = None

    if use_llm:
        try:
            candidates = generator.generate(
                sql=sql,
                prompt=prompt,
                examples_used=examples_used,
                n=1,
                dialect="postgres",
            )
            if candidates:
                cand = candidates[0]
                response = cand.response
                optimized_sql = cand.optimized_sql
                transforms = cand.transforms
        except Exception as e:
            error_msg = str(e)
            response = f"ERROR: {e}"

    # Save response
    if response:
        (query_output_dir / "response.txt").write_text(response)
    if optimized_sql:
        (query_output_dir / "optimized.sql").write_text(optimized_sql)

    duration = time.time() - start_time

    # Save metadata
    metadata = {
        "query_id": query_id,
        "round": round_num,
        "timestamp": datetime.now().isoformat(),
        "duration_sec": round(duration, 1),
        "examples_used": examples_used,
        "transforms": transforms,
        "has_general_learnings": bool(general_learnings),
        "has_query_feedback": query_id in feedback_by_query,
        "has_response": response is not None and not response.startswith("ERROR"),
        "has_optimized_sql": optimized_sql is not None,
        "error": error_msg,
    }
    (query_output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2)
    )

    # Progress logging
    with print_lock:
        progress["done"] += 1
        status = "OK" if metadata["has_optimized_sql"] else ("ERR" if error_msg else "SKIP")
        transforms_str = ",".join(transforms[:2]) if transforms else "-"
        print(f"[{progress['done']:2d}/{progress['total']}] {query_id:20s} {status:4s} {duration:5.1f}s  {transforms_str}")

    return metadata


def collect_rewrites(
    queries: dict[str, str],
    output_dir: Path,
    round_num: int,
    sample_db: str,
    feedback_dir: Path | None = None,
    general_learnings_file: Path | None = None,
    provider: str = "deepseek",
    model: str = None,
    max_workers: int = 10,
) -> dict:
    """Collect LLM rewrite responses for all queries IN PARALLEL.

    Args:
        queries: Dict of {query_id: sql}
        output_dir: Directory to save outputs
        round_num: Current round number
        sample_db: Database connection string for EXPLAIN
        feedback_dir: Previous round's feedback (if any)
        general_learnings_file: File with learnings that apply to all queries
        provider: LLM provider
        model: LLM model
        max_workers: Max parallel workers

    Returns:
        Summary dict with collection stats
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load general learnings (apply to ALL queries)
    general_learnings = load_general_learnings(general_learnings_file)
    if general_learnings:
        print(f"Loaded general learnings from {general_learnings_file}")

    # Load query-specific feedback from previous round
    feedback_by_query = {}
    if feedback_dir and feedback_dir.exists():
        for f in feedback_dir.glob("*.yaml"):
            query_id = f.stem.split("_feedback")[0]
            feedback_by_query[query_id] = f.read_text()
        print(f"Loaded {len(feedback_by_query)} query-specific feedback files")

    # Import LLM client
    try:
        from ado.generate import CandidateGenerator
        generator = CandidateGenerator(provider=provider, model=model)
        use_llm = True
        print(f"LLM: {provider} / {model or 'default'}")
    except Exception as e:
        print(f"Warning: Could not initialize LLM client: {e}")
        print("Will generate prompts only (no LLM responses)")
        generator = None
        use_llm = False

    total = len(queries)
    print_lock = Lock()
    progress = {"done": 0, "total": total}

    print(f"\nStarting parallel collection: {total} queries, {max_workers} workers")
    print(f"Output: {output_dir}")
    print("-" * 60)
    print(f"{'#':>4}  {'Query':<20} {'Status':<4} {'Time':>6}  Transforms")
    print("-" * 60)

    start_time = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_single_query,
                query_id,
                sql,
                output_dir,
                round_num,
                sample_db,
                general_learnings,
                feedback_by_query,
                generator,
                use_llm,
                print_lock,
                progress,
            ): query_id
            for query_id, sql in queries.items()
        }

        for future in as_completed(futures):
            query_id = futures[future]
            try:
                metadata = future.result()
                results.append(metadata)
            except Exception as e:
                print(f"FATAL ERROR {query_id}: {e}")
                results.append({
                    "query_id": query_id,
                    "error": str(e),
                    "has_response": False,
                    "has_optimized_sql": False,
                })

    total_duration = time.time() - start_time

    # Save summary
    summary = {
        "round": round_num,
        "timestamp": datetime.now().isoformat(),
        "total_queries": total,
        "successful_responses": sum(1 for r in results if r.get("has_response")),
        "with_optimized_sql": sum(1 for r in results if r.get("has_optimized_sql")),
        "with_feedback": sum(1 for r in results if r.get("has_query_feedback")),
        "total_duration_sec": round(total_duration, 1),
        "provider": provider,
        "model": model,
        "max_workers": max_workers,
        "queries": results,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    print("-" * 60)
    print(f"DONE in {total_duration:.1f}s ({total_duration/60:.1f} min)")
    print(f"  Successful: {summary['successful_responses']}/{total}")
    print(f"  With SQL:   {summary['with_optimized_sql']}/{total}")
    print(f"  Output:     {output_dir}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Collect DSB query rewrites")
    parser.add_argument(
        "--round", "-r",
        type=int,
        default=1,
        help="Round number (default: 1)",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output directory (default: research/ado/rounds/round_XX)",
    )
    parser.add_argument(
        "--queries", "-q",
        type=Path,
        default=Path("/tmp/dsb_queries"),
        help="Directory containing DSB query SQL files",
    )
    parser.add_argument(
        "--feedback", "-f",
        type=Path,
        default=None,
        help="Directory containing query-specific feedback from previous round",
    )
    parser.add_argument(
        "--learnings", "-l",
        type=Path,
        default=None,
        help="File containing general learnings that apply to all queries",
    )
    parser.add_argument(
        "--sample-db",
        type=str,
        default="postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10",
        help="Database connection string for EXPLAIN",
    )
    parser.add_argument(
        "--provider",
        type=str,
        default="deepseek",
        choices=["deepseek", "openrouter"],
        help="LLM provider (default: deepseek)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="LLM model (default: deepseek-reasoner or kimi-k2.5)",
    )
    parser.add_argument(
        "--prompts-only",
        action="store_true",
        help="Only generate prompts, don't call LLM",
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=10,
        help="Max parallel workers (default: 10)",
    )

    args = parser.parse_args()

    # Set output directory
    if args.output is None:
        args.output = Path(f"research/ado/rounds/round_{args.round:02d}")

    # Load queries
    if not args.queries.exists():
        print(f"Error: Queries directory not found: {args.queries}")
        sys.exit(1)

    queries = load_dsb_queries(args.queries)
    print(f"Loaded {len(queries)} DSB queries from {args.queries}")

    # Collect rewrites
    collect_rewrites(
        queries=queries,
        output_dir=args.output,
        round_num=args.round,
        sample_db=args.sample_db,
        feedback_dir=args.feedback,
        general_learnings_file=args.learnings,
        provider=args.provider if not args.prompts_only else None,
        model=args.model,
        max_workers=args.workers,
    )


if __name__ == "__main__":
    main()
