#!/usr/bin/env python3
"""Batch swarm generation — massively parallel LLM calls, NO validation.

Phase 1 (prep):     Build all fan-out prompts (DAG + FAISS, deterministic, no API)
Phase 2 (analyst):  Fire ~101 analyst calls (max 100 concurrent), save to disk immediately
Phase 2.5 (parse):  Parse analyst responses → build 4 worker prompts per query
Phase 3 (workers):  Fire ~404 worker calls (max 100 concurrent), save to disk immediately

Every API response is written to disk THE INSTANT it returns, before any processing.
Resume-safe: skips queries/workers that already have a response file on disk.

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.batch_swarm
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

# ---------------------------------------------------------------------------
# Bootstrap — must run from project root
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
os.chdir(PROJECT_ROOT)
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from ado.pipeline import Pipeline
from ado.generate import CandidateGenerator
from ado.prompts import build_fan_out_prompt, parse_fan_out_response

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MAX_CONCURRENT = 100
BENCHMARK_DIR = "packages/qt-sql/ado/benchmarks/duckdb_tpcds"
DIALECT = "duckdb"
ENGINE = "duckdb"

# Set to None to run all queries, or a list to filter
TARGET_QUERIES = [
    "query_67", "query_64", "query_75", "query_87", "query_57",
    "query_70", "query_50", "query_13", "query_48", "query_79",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / "swarm_batch.log"),
    ],
)
logger = logging.getLogger("batch_swarm")

# ---------------------------------------------------------------------------
# Progress tracker (thread-safe)
# ---------------------------------------------------------------------------
_lock = Lock()
_progress: dict = {}

def _save_progress(output_dir: Path):
    with _lock:
        (output_dir / "progress.json").write_text(json.dumps(_progress, indent=2))

def _inc(key: str, output_dir: Path):
    with _lock:
        _progress[key] = _progress.get(key, 0) + 1
    _save_progress(output_dir)


# ===================================================================
# PHASE 1 — Build all fan-out prompts (deterministic, no API calls)
# ===================================================================
def phase1_prep(pipeline: Pipeline, queries: dict[str, str], output_dir: Path):
    """Build fan-out prompt for every query.  Returns prep_data dict."""
    logger.info("=" * 70)
    logger.info("PHASE 1  Build fan-out prompts (DAG + FAISS, no API calls)")
    logger.info("=" * 70)

    all_available = pipeline._list_gold_examples(ENGINE)
    prep: dict[str, dict] = {}

    for i, (qid, sql) in enumerate(queries.items(), 1):
        qdir = output_dir / qid
        qdir.mkdir(parents=True, exist_ok=True)

        try:
            dag, costs, _explain = pipeline._parse_dag(sql, dialect=DIALECT, query_id=qid)
            faiss_examples = pipeline._find_examples(sql, engine=ENGINE, k=12)
            regression_warnings = pipeline._find_regression_warnings(
                sql, engine=ENGINE, k=2,
            )

            prompt = build_fan_out_prompt(
                query_id=qid, sql=sql, dag=dag, costs=costs,
                faiss_examples=faiss_examples,
                all_available_examples=all_available,
                dialect=DIALECT,
            )

            # Save everything
            (qdir / "original.sql").write_text(sql)
            (qdir / "fan_out_prompt.txt").write_text(prompt)
            (qdir / "faiss_examples.json").write_text(json.dumps(
                [{"id": e.get("id", "?"), "speedup": e.get("verified_speedup", e.get("speedup", "?")),
                  "description": e.get("description", "")[:200]}
                 for e in faiss_examples],
                indent=2,
            ))
            (qdir / "regression_warnings.json").write_text(json.dumps(
                [{"id": e.get("id", "?"), "speedup": e.get("verified_speedup", "?"),
                  "mechanism": e.get("regression_mechanism", "")}
                 for e in regression_warnings],
                indent=2,
            ))

            prep[qid] = dict(
                dag=dag, costs=costs, sql=sql,
                faiss_examples=faiss_examples,
                regression_warnings=regression_warnings,
                fan_out_prompt=prompt,
            )
            logger.info(f"  [{i}/{len(queries)}] {qid}  prompt ready  ({len(prompt):,} chars)")

        except Exception as e:
            (qdir / "prep_error.txt").write_text(f"{type(e).__name__}: {e}")
            logger.error(f"  [{i}/{len(queries)}] {qid}  PREP FAILED: {e}")

    logger.info(f"Phase 1 done: {len(prep)}/{len(queries)} prompts")
    return prep


# ===================================================================
# PHASE 2 — Analyst fan-out calls (massively parallel)
# ===================================================================
def phase2_analyst(generator: CandidateGenerator, prep: dict, output_dir: Path):
    """Fire all analyst calls. Returns {qid: raw_response}."""
    n = len(prep)
    logger.info("=" * 70)
    logger.info(f"PHASE 2  Firing {n} analyst calls  (max {MAX_CONCURRENT} concurrent)")
    logger.info("=" * 70)

    _progress["p2_total"] = n
    _progress["p2_done"] = 0
    _progress["p2_errors"] = 0
    _progress["p2_skipped"] = 0
    _save_progress(output_dir)

    def call(qid: str) -> tuple[str, str | None]:
        qdir = output_dir / qid
        resp_path = qdir / "fan_out_response.txt"

        # Resume support — skip if already saved
        if resp_path.exists() and resp_path.stat().st_size > 100:
            _inc("p2_skipped", output_dir)
            _inc("p2_done", output_dir)
            return qid, resp_path.read_text()

        try:
            t0 = time.time()
            response = generator._analyze(prep[qid]["fan_out_prompt"])
            elapsed = time.time() - t0

            # IMMEDIATELY save raw response + metadata
            resp_path.write_text(response)
            (qdir / "fan_out_meta.json").write_text(json.dumps({
                "elapsed_s": round(elapsed, 1),
                "response_chars": len(response),
                "status": "ok",
            }, indent=2))

            done = _progress.get("p2_done", 0) + 1
            logger.info(f"  [{done}/{n}] {qid}  analyst OK  ({elapsed:.1f}s, {len(response):,} chars)")
            _inc("p2_done", output_dir)
            return qid, response

        except Exception as e:
            import traceback
            (qdir / "fan_out_error.txt").write_text(
                f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
            )
            logger.error(f"  {qid}  analyst FAILED: {e}")
            _inc("p2_errors", output_dir)
            _inc("p2_done", output_dir)
            return qid, None

    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {pool.submit(call, qid): qid for qid in prep}
        for future in as_completed(futures):
            qid = futures[future]
            try:
                qid, resp = future.result()
                if resp:
                    results[qid] = resp
            except Exception as e:
                logger.error(f"  {qid}  future exception: {e}")

    logger.info(
        f"Phase 2 done: {len(results)}/{n} responses  "
        f"({_progress.get('p2_errors', 0)} errors, "
        f"{_progress.get('p2_skipped', 0)} skipped/resumed)"
    )
    return results


# ===================================================================
# PHASE 2.5 — Parse analyst responses, build worker prompts
# ===================================================================
def phase2_5_parse(
    pipeline: Pipeline, prep: dict,
    analyst_responses: dict[str, str], output_dir: Path,
) -> list[tuple]:
    """Parse fan-out responses and build per-worker prompts.

    Returns list of (qid, worker_id, prompt_text) tuples.
    """
    logger.info("=" * 70)
    logger.info("PHASE 2.5  Parse analyst responses → worker prompts")
    logger.info("=" * 70)

    global_learnings = pipeline.learner.build_learning_summary() or None
    jobs: list[tuple] = []

    for qid, response in analyst_responses.items():
        qdir = output_dir / qid
        data = prep.get(qid)
        if not data:
            logger.warning(f"  {qid}  no prep data, skipping")
            continue

        try:
            assignments = parse_fan_out_response(response)

            # Save parsed assignments
            (qdir / "assignments.json").write_text(json.dumps(
                [{"worker_id": a.worker_id, "strategy": a.strategy,
                  "examples": a.examples, "hint": a.hint}
                 for a in assignments],
                indent=2,
            ))

            regression_warnings = data.get("regression_warnings") or \
                pipeline._find_regression_warnings(data["sql"], engine=ENGINE, k=2)

            for a in assignments:
                examples = pipeline._load_examples_by_id(a.examples, ENGINE)

                base_prompt = pipeline.prompter.build_prompt(
                    query_id=f"{qid}_w{a.worker_id}",
                    full_sql=data["sql"],
                    dag=data["dag"],
                    costs=data["costs"],
                    history=None,
                    examples=examples,
                    expert_analysis=None,
                    global_learnings=global_learnings,
                    regression_warnings=regression_warnings,
                    dialect=DIALECT,
                    semantic_intents=pipeline.get_semantic_intents(qid),
                    engine_version=pipeline._engine_version,
                )

                header = (
                    f"## Optimization Strategy: {a.strategy}\n\n"
                    f"**Your approach**: {a.hint}\n\n"
                    f"**Focus**: Apply the examples below in service of this "
                    f"strategy. Prioritize this specific approach over generic "
                    f"optimizations.\n\n---\n\n"
                )
                worker_prompt = header + base_prompt

                # Save prompt
                (qdir / f"worker_{a.worker_id}_prompt.txt").write_text(worker_prompt)
                jobs.append((qid, a.worker_id, worker_prompt))

            logger.info(f"  {qid}  {len(assignments)} worker prompts built")

        except Exception as e:
            import traceback
            (qdir / "parse_error.txt").write_text(
                f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
            )
            logger.error(f"  {qid}  parse FAILED: {e}")

    logger.info(f"Phase 2.5 done: {len(jobs)} worker prompts ready")
    return jobs


# ===================================================================
# PHASE 3 — Worker generation calls (massively parallel)
# ===================================================================
def phase3_workers(
    generator: CandidateGenerator,
    jobs: list[tuple],
    output_dir: Path,
):
    """Fire all worker LLM calls. Save raw responses to disk immediately."""
    n = len(jobs)
    logger.info("=" * 70)
    logger.info(f"PHASE 3  Firing {n} worker calls  (max {MAX_CONCURRENT} concurrent)")
    logger.info("=" * 70)

    _progress["p3_total"] = n
    _progress["p3_done"] = 0
    _progress["p3_errors"] = 0
    _progress["p3_skipped"] = 0
    _save_progress(output_dir)

    def call(job: tuple) -> tuple[str, int, str]:
        qid, wid, prompt = job
        qdir = output_dir / qid
        resp_path = qdir / f"worker_{wid}_response.txt"

        # Resume support
        if resp_path.exists() and resp_path.stat().st_size > 100:
            _inc("p3_skipped", output_dir)
            _inc("p3_done", output_dir)
            return qid, wid, "skipped"

        try:
            t0 = time.time()
            response = generator._analyze(prompt)
            elapsed = time.time() - t0

            # IMMEDIATELY save raw response + metadata
            resp_path.write_text(response)
            (qdir / f"worker_{wid}_meta.json").write_text(json.dumps({
                "elapsed_s": round(elapsed, 1),
                "response_chars": len(response),
                "status": "ok",
            }, indent=2))

            done = _progress.get("p3_done", 0) + 1
            logger.info(f"  [{done}/{n}] {qid}/w{wid}  OK  ({elapsed:.1f}s, {len(response):,} chars)")
            _inc("p3_done", output_dir)
            return qid, wid, "ok"

        except Exception as e:
            import traceback
            (qdir / f"worker_{wid}_error.txt").write_text(
                f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
            )
            logger.error(f"  {qid}/w{wid}  FAILED: {e}")
            _inc("p3_errors", output_dir)
            _inc("p3_done", output_dir)
            return qid, wid, "error"

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {pool.submit(call, j): j for j in jobs}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                job = futures[future]
                logger.error(f"  {job[0]}/w{job[1]}  future exception: {e}")

    logger.info(
        f"Phase 3 done: {_progress.get('p3_done', 0)}/{n} responses  "
        f"({_progress.get('p3_errors', 0)} errors, "
        f"{_progress.get('p3_skipped', 0)} skipped/resumed)"
    )


# ===================================================================
# Main
# ===================================================================
def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(BENCHMARK_DIR) / f"swarm_batch_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    _progress["started"] = timestamp
    _progress["output_dir"] = str(output_dir)
    _save_progress(output_dir)

    logger.info(f"Output → {output_dir}")

    # Pipeline (shared, for DAG parsing / FAISS / prompt building)
    pipeline = Pipeline(BENCHMARK_DIR, provider="deepseek", model="deepseek-reasoner")

    # LLM generator (thread-safe — each _analyze() call is independent)
    generator = CandidateGenerator(provider="deepseek", model="deepseek-reasoner")

    # Load queries (filtered if TARGET_QUERIES is set)
    queries_dir = Path(BENCHMARK_DIR) / "queries"
    queries = {
        f.stem: f.read_text()
        for f in sorted(queries_dir.glob("query_*.sql"))
        if TARGET_QUERIES is None or f.stem in TARGET_QUERIES
    }
    logger.info(f"Loaded {len(queries)} queries")

    t_start = time.time()

    # Phase 1: Build all fan-out prompts (no API calls)
    prep = phase1_prep(pipeline, queries, output_dir)

    # Phase 2: Fire all analyst calls (max 100 concurrent)
    analyst_responses = phase2_analyst(generator, prep, output_dir)

    # Phase 2.5: Parse → build worker prompts
    worker_jobs = phase2_5_parse(pipeline, prep, analyst_responses, output_dir)

    # Phase 3: Fire all worker calls (max 100 concurrent)
    phase3_workers(generator, worker_jobs, output_dir)

    elapsed = time.time() - t_start
    _progress["completed"] = True
    _progress["elapsed_seconds"] = round(elapsed, 1)
    _save_progress(output_dir)

    logger.info("=" * 70)
    logger.info("DONE")
    logger.info(f"  Output:   {output_dir}")
    logger.info(f"  Analyst:  {_progress.get('p2_done', 0)} calls ({_progress.get('p2_errors', 0)} errors)")
    logger.info(f"  Workers:  {_progress.get('p3_done', 0)} calls ({_progress.get('p3_errors', 0)} errors)")
    logger.info(f"  Elapsed:  {elapsed/60:.1f} min")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
