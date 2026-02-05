#!/usr/bin/env python3
"""
ADO (Autonomous Data Optimization) loop for v5 JSON optimization.

Runs a batch of queries in parallel, captures wins/failures, and writes a YAML
summary suitable for turning into GOLD examples and prompt constraints.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml

# Add packages to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-sql"))
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-shared"))

from qt_sql.optimization.adaptive_rewriter_v5 import (
    _build_base_prompt,
    _create_llm_client,
    _get_plan_context,
    _split_example_batches,
    VERIFIED_TRANSFORMS,
)
from qt_sql.optimization.dag_v3 import get_matching_examples, load_example
from qt_sql.optimization.query_recommender import (
    get_recommendations_for_sql,
    get_similar_queries_for_sql,
)
from qt_sql.validation.sql_validator import SQLValidator
from qt_sql.validation.schemas import ValidationStatus
from qt_sql.optimization.knowledge_base import detect_opportunities
from qt_sql.analyzers.ast_detector import detect_opportunities as detect_ast_opps


DEFAULT_QUERIES_DIR = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
DEFAULT_SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf5.duckdb"


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _load_query(query_num: int, queries_dir: Path) -> str:
    patterns = [
        f"query_{query_num}.sql",
        f"query{query_num:02d}.sql",
        f"query{query_num}.sql",
    ]
    for pattern in patterns:
        path = queries_dir / pattern
        if path.exists():
            return path.read_text()
    raise FileNotFoundError(f"Query {query_num} not found in {queries_dir}")


def _select_query_ids(args: argparse.Namespace) -> list[int]:
    if args.queries:
        return [int(q.strip()) for q in args.queries.split(",") if q.strip()]
    if args.query_file:
        path = Path(args.query_file)
        raw = path.read_text().strip().splitlines()
        return [int(line.strip()) for line in raw if line.strip()]
    if args.query_count:
        rng = random.Random(args.seed)
        return rng.sample(range(1, 100), k=args.query_count)
    return list(range(1, 11))


def _extract_transforms(response_text: str) -> list[str]:
    """Best-effort extraction of rewrite transform IDs from JSON response."""
    if not response_text:
        return []
    json_text = None
    if "```json" in response_text:
        parts = response_text.split("```json", 1)[1]
        json_text = parts.split("```", 1)[0].strip()
    if json_text is None:
        # Try to find raw JSON object containing rewrite_sets
        start = response_text.find("{")
        end = response_text.rfind("}")
        if start >= 0 and end > start:
            candidate = response_text[start:end + 1]
            if "rewrite_sets" in candidate:
                json_text = candidate
    if json_text is None:
        return []
    try:
        data = json.loads(json_text)
    except Exception:
        return []
    transforms = []
    for rs in data.get("rewrite_sets", []):
        t = rs.get("transform")
        if t:
            transforms.append(t)
    return transforms


def _prioritized_examples(sql: str, total_needed: int) -> list:
    """Prioritize examples by FAISS similarity first, then KB matching."""
    ordered: list = []
    seen = set()

    # FAISS-based recs (verified only)
    recs = get_recommendations_for_sql(sql, top_n=max(3, total_needed))
    for ex_id in recs:
        if ex_id in VERIFIED_TRANSFORMS and ex_id not in seen:
            ex = load_example(ex_id)
            if ex:
                ordered.append(ex)
                seen.add(ex_id)

    # Pad with KB-matched examples
    for ex in get_matching_examples(sql):
        if ex.id not in seen:
            ordered.append(ex)
            seen.add(ex.id)
        if len(ordered) >= total_needed:
            break

    return ordered


def _run_worker(
    worker_id: int,
    sql: str,
    base_prompt: str,
    plan_summary: str,
    examples: list,
    sample_db: str,
    explore: bool,
    plan_text: Optional[str],
    provider: Optional[str],
    model: Optional[str],
) -> dict[str, Any]:
    llm_client = _create_llm_client(provider, model)

    history = ""
    if explore:
        history = (
            "## Explore Mode\n"
            "Be adversarial. Exploit transforms the DB engine is unlikely to do automatically.\n"
            "Prioritize structural rewrites that reduce scans/aggregation work.\n"
        )
        if plan_text:
            history += f"\n## Plan (Full EXPLAIN)\n{plan_text}\n"

    from qt_sql.optimization.dag_v3 import build_prompt_with_examples
    from qt_sql.optimization.dag_v2 import DagV2Pipeline

    full_prompt = build_prompt_with_examples(base_prompt, examples, plan_summary, history)
    response_text = llm_client.analyze(full_prompt)

    pipeline = DagV2Pipeline(sql)
    optimized_sql = pipeline.apply_response(response_text)

    validator = SQLValidator(database=sample_db)
    result = validator.validate(sql, optimized_sql)

    error = result.errors[0] if result.errors else None

    return {
        "worker_id": worker_id,
        "optimized_sql": optimized_sql,
        "status": result.status.value,
        "speedup": result.speedup,
        "error": error,
        "prompt": full_prompt,
        "response": response_text,
        "examples_used": [ex.id for ex in examples],
        "transforms": _extract_transforms(response_text),
    }


def _summarize_error(error: Optional[str]) -> str:
    if not error:
        return ""
    line = error.splitlines()[0].strip()
    if len(line) > 200:
        return line[:200] + "..."
    return line


def _suggest_constraint(error: Optional[str]) -> Optional[dict]:
    if not error:
        return None
    if "Row count mismatch" in error:
        return {
            "id": "ROW_COUNT_PRESERVATION",
            "prompt_instruction": "Do NOT change row counts. Preserve join cardinality and filters; do not add or drop rows.",
        }
    if "Value mismatch" in error:
        return {
            "id": "VALUE_PRESERVATION",
            "prompt_instruction": "Do NOT change output values. Preserve projections, aggregations, and join logic exactly.",
        }
    if "LIMIT without ORDER BY" in error:
        return {
            "id": "LIMIT_ORDER_PRESERVATION",
            "prompt_instruction": "Do NOT alter LIMIT/ORDER BY semantics. Keep ordering and limit behavior identical.",
        }
    return None


def _save_worker_artifacts(query_dir: Path, worker: dict[str, Any]) -> dict[str, str]:
    gen_dir = query_dir / f"worker_{worker['worker_id']:02d}"
    gen_dir.mkdir(exist_ok=True)

    (gen_dir / "optimized.sql").write_text(worker["optimized_sql"])
    (gen_dir / "prompt.txt").write_text(worker["prompt"])
    (gen_dir / "response.txt").write_text(worker["response"])
    validation = {
        "worker_id": worker["worker_id"],
        "status": worker["status"],
        "speedup": worker["speedup"],
        "error": worker["error"],
        "examples_used": worker["examples_used"],
        "transforms": worker["transforms"],
    }
    (gen_dir / "validation.json").write_text(json.dumps(validation, indent=2))

    return {
        "optimized_sql": str(gen_dir / "optimized.sql"),
        "prompt": str(gen_dir / "prompt.txt"),
        "response": str(gen_dir / "response.txt"),
        "validation": str(gen_dir / "validation.json"),
    }


def _process_query(
    query_num: int,
    run_dir: Path,
    queries_dir: Path,
    sample_db: str,
    workers_per_query: int,
    examples_per_prompt: int,
    provider: Optional[str],
    model: Optional[str],
) -> dict[str, Any]:
    query_id = f"q{query_num}"
    query_dir = run_dir / query_id
    query_dir.mkdir(exist_ok=True)

    sql = _load_query(query_num, queries_dir)
    (query_dir / "original.sql").write_text(sql)

    plan_summary, plan_text, plan_context = _get_plan_context(sample_db, sql)
    base_prompt = _build_base_prompt(sql, plan_context)

    # AST + KB opportunities
    kb_hits = detect_opportunities(sql)
    kb_ids = [hit.pattern.id.value for hit in kb_hits]
    ast_hits = detect_ast_opps(sql, dialect="duckdb")
    ast_ids = [getattr(i, "rule_id", str(i)) for i in ast_hits]

    # FAISS recommendations (if available)
    faiss_recs = get_recommendations_for_sql(sql, top_n=examples_per_prompt * 2)
    similar_queries = get_similar_queries_for_sql(sql, k=5)
    similar_summary = [asdict(sq) for sq in similar_queries]

    # Example selection: FAISS first, then KB
    needed = workers_per_query * examples_per_prompt
    example_pool = _prioritized_examples(sql, needed)
    batches = _split_example_batches(example_pool, batch_size=examples_per_prompt)

    while len(batches) < workers_per_query:
        batches.append([])

    tasks = []
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers_per_query) as pool:
        for i in range(workers_per_query):
            batch = batches[i] if i < len(batches) else []
            explore = (i == workers_per_query - 1)
            tasks.append(pool.submit(
                _run_worker,
                i + 1,
                sql,
                base_prompt,
                plan_summary,
                batch,
                sample_db,
                explore,
                plan_text,
                provider,
                model,
            ))
        for t in as_completed(tasks):
            results.append(t.result())

    # Persist worker artifacts and build summary
    workers = []
    for r in sorted(results, key=lambda x: x["worker_id"]):
        artifacts = _save_worker_artifacts(query_dir, r)
        workers.append({
            "worker_id": r["worker_id"],
            "status": r["status"],
            "speedup": r["speedup"],
            "error_summary": _summarize_error(r["error"]),
            "examples_used": r["examples_used"],
            "transforms": r["transforms"],
            "artifacts": artifacts,
        })

    # Choose winner: best speedup among PASS
    winners = [w for w in workers if w["status"] == ValidationStatus.PASS.value]
    winner = None
    if winners:
        winner = max(winners, key=lambda w: w["speedup"])

    return {
        "query_id": query_id,
        "kb_opportunities": kb_ids,
        "ast_opportunities": ast_ids,
        "faiss_recommendations": faiss_recs,
        "similar_queries": similar_summary,
        "workers": workers,
        "winner": winner,
        "original_sql_path": str(query_dir / "original.sql"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ADO loop for v5 JSON optimization")
    parser.add_argument("--queries", help="Comma-separated list of query numbers (e.g., 1,15,39)")
    parser.add_argument("--query-file", help="File with one query number per line")
    parser.add_argument("--query-count", type=int, default=10, help="Random query count (default: 10)")
    parser.add_argument("--seed", type=int, default=7, help="Random seed for sampling")
    parser.add_argument("--queries-dir", default=str(DEFAULT_QUERIES_DIR), help="TPC-DS queries directory")
    parser.add_argument("--sample-db", default=DEFAULT_SAMPLE_DB, help="DuckDB file for validation (sf5 recommended)")
    parser.add_argument("--workers", type=int, default=10, help="Workers per query (default: 10)")
    parser.add_argument("--examples-per-prompt", type=int, default=3, help="Gold examples per prompt (default: 3)")
    parser.add_argument("--provider", help="LLM provider (QT_LLM_PROVIDER fallback)")
    parser.add_argument("--model", help="LLM model override")
    parser.add_argument("--output-dir", help="Output directory (default: research/brain/runs/run_YYYYMMDD_HHMMSS)")

    args = parser.parse_args()

    queries_dir = Path(args.queries_dir)
    query_ids = _select_query_ids(args)

    run_id = f"run_{_now_ts()}"
    output_dir = Path(args.output_dir) if args.output_dir else PROJECT_ROOT / "research" / "brain" / "runs" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    run_summary: dict[str, Any] = {
        "run": {
            "id": run_id,
            "started_at": datetime.now().isoformat(),
            "sample_db": args.sample_db,
            "queries_dir": str(queries_dir),
            "query_count": len(query_ids),
            "workers_per_query": args.workers,
            "examples_per_prompt": args.examples_per_prompt,
            "provider": args.provider,
            "model": args.model,
        },
        "queries": [],
        "wins": [],
        "failures": [],
        "constraints_suggestions": [],
    }

    # Process queries in parallel (outer loop)
    with ThreadPoolExecutor(max_workers=min(len(query_ids), 10)) as pool:
        futures = {
            pool.submit(
                _process_query,
                q,
                output_dir,
                queries_dir,
                args.sample_db,
                args.workers,
                args.examples_per_prompt,
                args.provider,
                args.model,
            ): q
            for q in query_ids
        }
        for fut in as_completed(futures):
            result = fut.result()
            run_summary["queries"].append(result)

    # Build wins + failures summary
    for q in run_summary["queries"]:
        if q["winner"]:
            run_summary["wins"].append({
                "query_id": q["query_id"],
                "worker_id": q["winner"]["worker_id"],
                "speedup": q["winner"]["speedup"],
                "transforms": q["winner"]["transforms"],
                "pair": {
                    "original_sql": q["original_sql_path"],
                    "optimized_sql": q["winner"]["artifacts"]["optimized_sql"],
                },
            })

        for w in q["workers"]:
            if w["status"] != ValidationStatus.PASS.value:
                suggestion = _suggest_constraint(w.get("error_summary"))
                if suggestion:
                    run_summary["constraints_suggestions"].append({
                        "query_id": q["query_id"],
                        "worker_id": w["worker_id"],
                        "constraint": suggestion,
                        "evidence": w["error_summary"],
                        "example": {
                            "original_sql": q["original_sql_path"],
                            "optimized_sql": w["artifacts"]["optimized_sql"],
                            "validation": w["artifacts"]["validation"],
                        },
                    })
                run_summary["failures"].append({
                    "query_id": q["query_id"],
                    "worker_id": w["worker_id"],
                    "status": w["status"],
                    "error_summary": w["error_summary"],
                    "example": {
                        "original_sql": q["original_sql_path"],
                        "optimized_sql": w["artifacts"]["optimized_sql"],
                        "validation": w["artifacts"]["validation"],
                    },
                })

    # Sort for readability
    run_summary["queries"].sort(key=lambda x: x["query_id"])
    run_summary["wins"].sort(key=lambda x: x["query_id"])
    run_summary["failures"].sort(key=lambda x: (x["query_id"], x["worker_id"]))

    yaml_path = output_dir / "brain.yaml"
    yaml_path.write_text(yaml.safe_dump(run_summary, sort_keys=False))

    print(f"Saved brain summary: {yaml_path}")
    print(f"Run directory: {output_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
