#!/usr/bin/env python3
"""Swarm full prep — generate ALL prompts, ZERO API calls.

Runs every deterministic step of the swarm pipeline for all benchmark
queries and stops just before the analyst LLM call.

Supports DuckDB TPC-DS (default) and PostgreSQL DSB via --benchmark-dir.

Phase 0: Cache EXPLAIN ANALYZE (DuckDB: run live; PG: load from explains/).
Phase 1: Build fan-out prompts (DAG + FAISS + regression warnings).

Per query this produces:
  {query_id}/
  ├── original.sql
  ├── dag.json
  ├── faiss_examples.json
  ├── regression_warnings.json
  ├── fan_out_prompt.txt      (ready to send to analyst LLM)
  └── meta.json

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.swarm_prep
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.swarm_prep \
        --benchmark-dir packages/qt-sql/ado/benchmarks/postgres_dsb
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Bootstrap ────────────────────────────────────────────────────────
PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
os.chdir(PROJECT_ROOT)
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from ado.pipeline import Pipeline
from ado.prompts import build_fan_out_prompt
from ado.schemas import BenchmarkConfig

# ── Config (defaults, overridden in main() via --benchmark-dir) ──────
BENCHMARK_DIR = Path("packages/qt-sql/ado/benchmarks/duckdb_tpcds")
DIALECT = "duckdb"
ENGINE = "duckdb"
DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
    force=True,
)
log = logging.getLogger("swarm_prep")


# =====================================================================
# Phase 0 — Cache EXPLAIN ANALYZE for every query from SF10
# =====================================================================

def phase0_explain_cache(queries: dict[str, str]) -> dict[str, dict]:
    """Cache EXPLAIN ANALYZE for every query.

    DuckDB: runs EXPLAIN ANALYZE live against the database.
    PostgreSQL: loads pre-collected explains from explains/ directory.

    Returns {query_id: explain_result} dict.
    """
    log.info("=" * 70)
    log.info(f"  PHASE 0: Cache EXPLAIN ANALYZE ({ENGINE})")
    log.info(f"  Database: {DB_PATH}")
    log.info("=" * 70)

    explains_dir = BENCHMARK_DIR / "explains" / "sf10"
    if not explains_dir.exists():
        explains_dir = BENCHMARK_DIR / "explains" / "sf5"
    explains_dir.mkdir(parents=True, exist_ok=True)

    # ── PostgreSQL: load from pre-collected explains ──
    if ENGINE in ("postgresql", "postgres"):
        results = {}
        ok = 0
        missing = 0
        for i, (qid, sql) in enumerate(queries.items(), 1):
            cache_path = explains_dir / f"{qid}.json"
            if cache_path.exists():
                try:
                    data = json.loads(cache_path.read_text())
                    results[qid] = data
                    ok += 1
                    exec_ms = data.get("execution_time_ms", 0) or 0
                    log.info(f"  [{i:3d}/{len(queries)}] {qid:20s}  LOADED  "
                             f"({exec_ms:.1f}ms)")
                except Exception as e:
                    log.error(f"  [{i:3d}/{len(queries)}] {qid:20s}  JSON ERROR: {e}")
                    results[qid] = {}
            else:
                missing += 1
                log.warning(f"  [{i:3d}/{len(queries)}] {qid:20s}  NO EXPLAIN CACHED")
                results[qid] = {}
        log.info(f"  Phase 0 done: {ok}/{len(queries)} loaded from cache, "
                 f"{missing} missing")
        return results

    # ── DuckDB: run EXPLAIN ANALYZE live ──
    import duckdb

    conn = duckdb.connect(DB_PATH, read_only=True)
    db_version = duckdb.__version__
    log.info(f"  DuckDB version: {db_version}")

    results = {}
    ok = 0
    errors = 0

    for i, (qid, sql) in enumerate(queries.items(), 1):
        cache_path = explains_dir / f"{qid}.json"

        try:
            explain_sql = f"EXPLAIN ANALYZE {sql}"
            explain_result = conn.execute(explain_sql).fetchall()

            try:
                conn.execute("PRAGMA enable_profiling='json'")
                conn.execute("PRAGMA enable_progress_bar=false")
                result_rows = conn.execute(sql).fetchall()
                profile_json_raw = conn.execute("PRAGMA last_profiling_output").fetchone()
                conn.execute("PRAGMA disable_profiling")

                if profile_json_raw and profile_json_raw[0]:
                    plan_json = json.loads(profile_json_raw[0])
                else:
                    plan_json = {}
            except Exception:
                plan_json = {}
                result_rows = []
                conn.execute("PRAGMA disable_profiling")

            plan_text = ""
            if explain_result:
                plan_text = "\n".join(str(row[1]) if len(row) > 1 else str(row[0])
                                      for row in explain_result)

            row_count = len(result_rows) if result_rows else 0
            exec_time_ms = plan_json.get("latency", 0.0) if isinstance(plan_json, dict) else 0.0

            explain_data = {
                "execution_time_ms": exec_time_ms,
                "plan_text": plan_text,
                "plan_json": plan_json,
                "actual_rows": row_count,
                "provenance": {
                    "database": DB_PATH,
                    "scale_factor": 10,
                    "engine": "duckdb",
                    "engine_version": db_version,
                    "cached_at": datetime.now().isoformat(),
                },
            }

            cache_path.write_text(json.dumps(explain_data, indent=2, default=str))
            results[qid] = explain_data
            ok += 1
            log.info(f"  [{i:3d}/{len(queries)}] {qid:12s}  OK  "
                      f"({exec_time_ms:.1f}ms, {row_count} rows)")

        except Exception as e:
            errors += 1
            log.error(f"  [{i:3d}/{len(queries)}] {qid:12s}  ERROR  {e}")
            cache_path.write_text(json.dumps({
                "execution_time_ms": None,
                "plan_text": None,
                "plan_json": {},
                "actual_rows": 0,
                "provenance": {
                    "database": DB_PATH,
                    "scale_factor": 10,
                    "engine": "duckdb",
                    "engine_version": db_version,
                    "cached_at": datetime.now().isoformat(),
                    "error": str(e),
                },
            }, indent=2))
            results[qid] = {}

    conn.close()
    log.info(f"  Phase 0 done: {ok}/{len(queries)} cached, {errors} errors")
    return results


# =====================================================================
# Phase 1 — Build all fan-out prompts (deterministic, no API calls)
# =====================================================================

def phase1_prompts(pipeline: Pipeline, queries: dict[str, str], output_dir: Path) -> dict:
    """Build fan-out prompt for every query."""
    log.info("=" * 70)
    log.info("  PHASE 1: Build fan-out prompts (DAG + FAISS, no API calls)")
    log.info("=" * 70)

    all_available = pipeline._list_gold_examples(ENGINE)
    log.info(f"  Gold catalog: {len(all_available)} examples")

    results = {"ok": 0, "error": 0, "queries": {}}

    for i, (qid, sql) in enumerate(queries.items(), 1):
        t0 = time.time()
        qdir = output_dir / qid
        qdir.mkdir(parents=True, exist_ok=True)

        try:
            (qdir / "original.sql").write_text(sql)

            dag, costs, explain_result = pipeline._parse_dag(
                sql, dialect=DIALECT, query_id=qid,
            )
            nodes = list(dag.nodes.keys()) if isinstance(dag.nodes, dict) else []
            (qdir / "dag.json").write_text(json.dumps({
                "n_nodes": len(dag.nodes),
                "n_edges": len(dag.edges),
                "node_ids": nodes,
                "has_explain": explain_result is not None,
            }, indent=2))

            faiss_examples = pipeline._find_examples(sql, engine=ENGINE, k=12)
            (qdir / "faiss_examples.json").write_text(json.dumps(
                [{"id": e.get("id", "?"),
                  "speedup": e.get("verified_speedup", e.get("speedup", "?")),
                  "description": e.get("description", "")[:200]}
                 for e in faiss_examples],
                indent=2,
            ))

            regression_warnings = pipeline._find_regression_warnings(sql, engine=ENGINE, k=2)
            (qdir / "regression_warnings.json").write_text(json.dumps(
                [{"id": e.get("id", "?"),
                  "speedup": e.get("verified_speedup", "?"),
                  "mechanism": e.get("regression_mechanism", "")}
                 for e in regression_warnings],
                indent=2,
            ))

            prompt = build_fan_out_prompt(
                query_id=qid,
                sql=sql,
                dag=dag,
                costs=costs,
                faiss_examples=faiss_examples,
                all_available_examples=all_available,
                dialect=DIALECT,
            )
            (qdir / "fan_out_prompt.txt").write_text(prompt)

            elapsed = time.time() - t0
            (qdir / "meta.json").write_text(json.dumps({
                "query_id": qid,
                "prompt_chars": len(prompt),
                "n_faiss_examples": len(faiss_examples),
                "n_regressions": len(regression_warnings),
                "n_dag_nodes": len(dag.nodes),
                "elapsed_s": round(elapsed, 2),
                "status": "ok",
            }, indent=2))

            results["ok"] += 1
            results["queries"][qid] = {
                "status": "ok",
                "prompt_chars": len(prompt),
                "n_faiss": len(faiss_examples),
                "elapsed_s": round(elapsed, 2),
            }
            log.info(f"  [{i:3d}/{len(queries)}] {qid:12s}  OK  "
                      f"({len(prompt):,} chars, {len(faiss_examples)} ex, {elapsed:.1f}s)")

        except Exception as e:
            import traceback
            elapsed = time.time() - t0
            (qdir / "error.txt").write_text(f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}")
            results["error"] += 1
            results["queries"][qid] = {"status": "error", "error": str(e)}
            log.error(f"  [{i:3d}/{len(queries)}] {qid:12s}  ERROR  {e}")

    log.info(f"  Phase 1 done: {results['ok']}/{len(queries)} prompts ready, "
              f"{results['error']} errors")
    return results


# =====================================================================
# Main
# =====================================================================

def main():
    global BENCHMARK_DIR, DIALECT, ENGINE, DB_PATH

    parser = argparse.ArgumentParser(description="Swarm prep — all prompts, zero API calls")
    parser.add_argument("--benchmark-dir", type=str,
                        help="Benchmark directory (default: duckdb_tpcds)")
    args = parser.parse_args()

    # ── Load config ──
    if args.benchmark_dir:
        BENCHMARK_DIR = Path(args.benchmark_dir)
    cfg_path = BENCHMARK_DIR / "config.json"
    cfg = None
    if cfg_path.exists():
        cfg = BenchmarkConfig.from_file(cfg_path)
        ENGINE = cfg.engine
        DIALECT = cfg.engine if cfg.engine != "postgresql" else "postgres"
        DB_PATH = cfg.db_path_or_dsn
    else:
        log.warning(f"No config.json in {BENCHMARK_DIR}, using defaults")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = BENCHMARK_DIR / f"swarm_batch_{ts}"
    output_dir.mkdir(parents=True, exist_ok=True)

    log_file = logging.FileHandler(output_dir / "prep.log")
    log_file.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%H:%M:%S"))
    log.addHandler(log_file)

    log.info("=" * 70)
    log.info("  SWARM PREP — All prompts, zero API calls")
    log.info(f"  Engine:   {ENGINE}")
    log.info(f"  Dialect:  {DIALECT}")
    log.info(f"  Database: {DB_PATH}")
    log.info(f"  Output:   {output_dir}")
    log.info("=" * 70)

    # Load all queries — works for both query_*.sql and query0XX_*.sql
    queries_dir = BENCHMARK_DIR / "queries"
    queries = {
        f.stem: f.read_text()
        for f in sorted(queries_dir.glob("query*.sql"))
    }
    log.info(f"  Loaded {len(queries)} queries")

    t_total = time.time()

    # Phase 0: Cache EXPLAIN ANALYZE
    phase0_explain_cache(queries)

    # Phase 1: Build all fan-out prompts
    pipeline = Pipeline(str(BENCHMARK_DIR), provider="deepseek", model="deepseek-reasoner")
    results = phase1_prompts(pipeline, queries, output_dir)

    total_elapsed = time.time() - t_total

    # Manifest
    prompt_sizes = [v["prompt_chars"] for v in results["queries"].values() if v.get("prompt_chars")]
    manifest = {
        "total": len(queries),
        "ok": results["ok"],
        "error": results["error"],
        "elapsed_s": round(total_elapsed, 1),
        "output_dir": str(output_dir),
        "timestamp": ts,
        "engine": ENGINE,
        "dialect": DIALECT,
        "database": DB_PATH,
        "scale_factor": cfg.scale_factor if cfg else 10,
        "mode": "swarm (4 workers per query)",
        "prompt_stats": {
            "min_chars": min(prompt_sizes) if prompt_sizes else 0,
            "max_chars": max(prompt_sizes) if prompt_sizes else 0,
            "avg_chars": round(sum(prompt_sizes) / len(prompt_sizes)) if prompt_sizes else 0,
            "total_chars": sum(prompt_sizes),
        },
        "queries": results["queries"],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    # Verify explains
    explains_dir = BENCHMARK_DIR / "explains" / "sf10"
    if not explains_dir.exists():
        explains_dir = BENCHMARK_DIR / "explains" / "sf5"
    total_explains = len(list(explains_dir.glob("*.json")))

    log.info("")
    log.info("=" * 70)
    log.info("  PREP COMPLETE")
    log.info(f"  Engine:    {ENGINE}")
    log.info(f"  Explains:  {total_explains} cached")
    log.info(f"  Queries:   {results['ok']}/{len(queries)} prompts ready, {results['error']} errors")
    if prompt_sizes:
        log.info(f"  Prompts:   {sum(prompt_sizes):,} total chars ({sum(prompt_sizes)//len(prompt_sizes):,} avg)")
    log.info(f"  Elapsed:   {total_elapsed:.1f}s ({total_elapsed/len(queries):.1f}s/query)")
    log.info(f"  Output:    {output_dir}")
    log.info("")
    log.info(f"  NEXT STEP: Fire analyst calls (Phase 2)")
    log.info(f"  All {results['ok']} fan_out_prompt.txt files are ready to send.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
