#!/usr/bin/env python3
"""Swarm full run — LLM generation + benchmark pipeline.

RESUME-SAFE: Every LLM response and benchmark result is saved to disk
immediately. On restart, existing artifacts are loaded and skipped.
808 API calls are expensive — this pipeline survives disconnects.

Pipeline:
  Iter 0: 101 analyst fan-out -> 404 workers -> benchmark (5x trimmed mean)
  Iter 1: <=101 snipe workers (original SQL + best candidate info) -> benchmark
  Iter 2: <=101 analyst re-analyze -> <=101 workers -> benchmark
  Exit:   >=2.0x confirmed via 5x trimmed mean

LLM:       100 max concurrent API connections
Benchmark: 2 parallel slots (tpcds_sf10_1.duckdb, tpcds_sf10_2.duckdb)
           Single-threaded DuckDB, 5x trimmed mean (drop min+max, avg 3)

Resume artifacts per query:
  fan_out_response.txt       -> Phase 2 done
  assignments.json           -> Phase 2.5 done
  worker_N_response.txt      -> Phase 3 done (per worker)
  worker_N_sql.sql           -> Phase 3.5 done (per worker)
  benchmark_iter0.json       -> Phase 4 done
  snipe_worker_response.txt  -> Phase 5 done
  snipe_worker_sql.sql       -> Phase 5 extract done
  benchmark_iter1.json       -> Phase 5.5 done
  reanalyze_response.txt     -> Phase 6 done
  final_worker_response.txt  -> Phase 6.5 done
  final_worker_sql.sql       -> Phase 7 extract done
  benchmark_iter2.json       -> Phase 7.5 done

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.swarm_run [--batch DIR]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import sys
import threading
import time
import traceback as tb_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# ─── Bootstrap ───────────────────────────────────────────────────────
PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
os.chdir(PROJECT_ROOT)
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from ado.pipeline import Pipeline
from ado.generate import CandidateGenerator
from ado.sql_rewriter import SQLRewriter
from ado.schemas import WorkerResult
from ado.prompts import (
    build_snipe_prompt,
    build_worker_strategy_header,
    WorkerAssignment,
    parse_fan_out_response,
    parse_snipe_response,
)
from ado.schemas import BenchmarkConfig

# ─── Config ──────────────────────────────────────────────────────────
MAX_LLM_CONCURRENT = 100
EXIT_SPEEDUP = 2.0

# Module-level config — set in main() from --benchmark-dir arg
BENCHMARK_DIR = Path("packages/qt-sql/ado/benchmarks/duckdb_tpcds")
DIALECT = "duckdb"
ENGINE = "duckdb"
DB_SLOT_1 = "/mnt/d/TPC-DS/tpcds_sf10_1.duckdb"
DB_SLOT_2 = "/mnt/d/TPC-DS/tpcds_sf10_2.duckdb"
TIMEOUT_SECONDS = 300

# ─── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
    force=True,
)
log = logging.getLogger("swarm_run")


# =====================================================================
# Data Structures
# =====================================================================

@dataclass
class WorkerBench:
    worker_id: int
    times_ms: list[float]
    trimmed_mean_ms: float
    speedup: float
    row_count: int
    rows_match: bool
    status: str  # pass | fail | error
    error: str = ""


@dataclass
class QueryBench:
    query_id: str
    baseline_times_ms: list[float]
    baseline_trimmed_mean_ms: float
    baseline_row_count: int
    workers: list[WorkerBench]
    best_worker_id: int
    best_speedup: float
    exited: bool


@dataclass
class BenchmarkJob:
    query_id: str
    original_sql: str
    workers: list[tuple[int, str]]  # [(worker_id, optimized_sql), ...]
    seq_num: int
    total: int


# =====================================================================
# Utilities
# =====================================================================

def trimmed_mean_5(times: list[float]) -> float:
    """5x trimmed mean: sort, drop min+max, average middle 3."""
    s = sorted(times)
    return sum(s[1:4]) / 3


def save_checkpoint(batch_dir: Path, phase: str, data: dict):
    """Save checkpoint to track pipeline progress."""
    cp_path = batch_dir / "checkpoint.json"
    cp = {}
    if cp_path.exists():
        cp = json.loads(cp_path.read_text())
    cp[phase] = {"completed_at": datetime.now().isoformat(), **data}
    cp_path.write_text(json.dumps(cp, indent=2))


def load_querybench_from_json(path: Path) -> QueryBench:
    """Reload a QueryBench from saved JSON."""
    d = json.loads(path.read_text())
    workers = [WorkerBench(**w) for w in d.pop("workers")]
    return QueryBench(**d, workers=workers)


def load_assignments_from_json(path: Path) -> list[WorkerAssignment]:
    """Reload WorkerAssignment list from saved JSON."""
    data = json.loads(path.read_text())
    return [
        WorkerAssignment(
            worker_id=a["worker_id"], strategy=a["strategy"],
            examples=a["examples"], hint=a["hint"],
        )
        for a in data
    ]


# =====================================================================
# LLM Call Engine — resume-safe, saves immediately
# =====================================================================

def fire_llm_calls(
    generator: CandidateGenerator,
    jobs: list[tuple[str, str, Path]],  # (job_id, prompt, response_path)
    phase_name: str,
) -> dict[str, str]:
    """Fire LLM calls, max 100 concurrent. Resume-safe.

    Skips any job where response_path already exists with >100 bytes.
    Returns {job_id: response_text} for ALL jobs (resumed + fresh).
    """
    n = len(jobs)
    if n == 0:
        return {}

    # Count how many are already done
    already_done = sum(
        1 for _, _, p in jobs if p.exists() and p.stat().st_size > 100
    )

    log.info("=" * 70)
    log.info(f"  {phase_name} ({n} calls, {already_done} resumed, "
             f"max {MAX_LLM_CONCURRENT} concurrent)")
    log.info("=" * 70)

    done_count = [already_done]
    fresh_count = [0]
    lock = threading.Lock()

    def call(job_id: str, prompt: str, resp_path: Path) -> tuple[str, str | None]:
        # Resume: load from disk if already saved
        if resp_path.exists() and resp_path.stat().st_size > 100:
            return job_id, resp_path.read_text()

        try:
            t0 = time.time()
            response = generator._analyze(prompt)
            elapsed = time.time() - t0

            # Save IMMEDIATELY — crash-safe
            resp_path.parent.mkdir(parents=True, exist_ok=True)
            resp_path.write_text(response)

            with lock:
                fresh_count[0] += 1
                done_count[0] += 1
                log.info(f"  [{done_count[0]:3d}/{n}] {job_id}  OK  "
                         f"({elapsed:.1f}s, {len(response):,} chars)")
            return job_id, response

        except Exception as e:
            resp_path.parent.mkdir(parents=True, exist_ok=True)
            err_path = resp_path.parent / f"{resp_path.stem}_error.txt"
            err_path.write_text(f"{type(e).__name__}: {e}\n\n{tb_mod.format_exc()}")
            with lock:
                done_count[0] += 1
                log.error(f"  [{done_count[0]:3d}/{n}] {job_id}  ERROR: {e}")
            return job_id, None

    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_LLM_CONCURRENT) as pool:
        futures = {
            pool.submit(call, jid, prompt, path): jid
            for jid, prompt, path in jobs
        }
        for future in as_completed(futures):
            try:
                jid, resp = future.result()
                if resp:
                    results[jid] = resp
            except Exception as e:
                log.error(f"  Future exception: {e}")

    ok = len(results)
    log.info(f"  {phase_name} done: {ok}/{n} OK "
             f"({fresh_count[0]} fresh, {already_done} resumed, "
             f"{n - ok} errors)")
    return results


# =====================================================================
# Benchmark Engine — 2 parallel slots, 5x trimmed mean, saves per-query
# =====================================================================

class BenchmarkSlot(threading.Thread):
    """Benchmark worker with its own database connection (DuckDB or PostgreSQL)."""

    def __init__(self, slot_id: int, dsn: str,
                 work_queue: queue.Queue,
                 results: dict, results_lock: threading.Lock,
                 save_dir: Path, iter_name: str,
                 engine: str = "duckdb", timeout_seconds: int = 300):
        super().__init__(daemon=True, name=f"bench-slot-{slot_id}")
        self.slot_id = slot_id
        self.dsn = dsn
        self.engine = engine
        self.timeout_seconds = timeout_seconds
        self.work_queue = work_queue
        self.results = results
        self.results_lock = results_lock
        self.save_dir = save_dir
        self.iter_name = iter_name
        self.conn = None
        self._cursor = None  # For PostgreSQL

    def _connect(self):
        if self.engine == "duckdb":
            import duckdb
            self.conn = duckdb.connect(self.dsn, read_only=True)
            self.conn.execute("SET threads TO 1")
            self.conn.execute("SET enable_progress_bar = false")
        elif self.engine in ("postgresql", "postgres"):
            import psycopg2
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = True
            self._cursor = self.conn.cursor()
            self._cursor.execute(
                f"SET statement_timeout = '{self.timeout_seconds * 1000}'"
            )
        else:
            raise ValueError(f"Unsupported engine: {self.engine}")

    def _execute(self, sql: str) -> list:
        """Execute SQL and return rows, engine-aware."""
        if self.engine == "duckdb":
            return self.conn.execute(sql).fetchall()
        else:
            self._cursor.execute(sql)
            return self._cursor.fetchall()

    def run(self):
        self._connect()
        try:
            while True:
                job = self.work_queue.get()
                if job is None:
                    break
                try:
                    result = self._benchmark_query(job)
                    # Save IMMEDIATELY to disk — crash-safe
                    save_path = self.save_dir / job.query_id / f"benchmark_{self.iter_name}.json"
                    save_path.write_text(json.dumps(asdict(result), indent=2))
                    with self.results_lock:
                        self.results[job.query_id] = result
                except Exception as e:
                    log.error(f"[SLOT {self.slot_id}] {job.query_id} ERROR: {e}")
                finally:
                    self.work_queue.task_done()
        finally:
            if self._cursor:
                self._cursor.close()
            if self.conn:
                self.conn.close()

    def _benchmark_query(self, job: BenchmarkJob) -> QueryBench:
        qid, n, total = job.query_id, job.seq_num, job.total

        # ── Baseline: 5 runs ──
        baseline_times = []
        baseline_row_count = 0
        for r in range(1, 6):
            try:
                t0 = time.perf_counter()
                rows = self._execute(job.original_sql)
                ms = (time.perf_counter() - t0) * 1000
                baseline_times.append(ms)
                if r == 1:
                    baseline_row_count = len(rows)
                log.info(f"[BENCH {n:3d}/{total}] {qid:12s}  BASELINE  "
                         f"run {r}/5  {ms:8.1f}ms")
            except Exception as e:
                log.error(f"[BENCH {n:3d}/{total}] {qid:12s}  BASELINE  "
                          f"run {r}/5  ERROR: {e}")
                return QueryBench(
                    query_id=qid, baseline_times_ms=baseline_times,
                    baseline_trimmed_mean_ms=0, baseline_row_count=0,
                    workers=[], best_worker_id=0, best_speedup=0,
                    exited=False,
                )

        btm = trimmed_mean_5(baseline_times)
        log.info(f"[BENCH {n:3d}/{total}] {qid:12s}  BASELINE  "
                 f"trimmed_mean={btm:.1f}ms  ({baseline_row_count} rows)")

        # ── Each worker: 5 runs ──
        worker_results: list[WorkerBench] = []
        for wid, opt_sql in job.workers:
            wb = self._bench_worker(qid, n, total, wid, opt_sql,
                                    btm, baseline_row_count)
            worker_results.append(wb)

        # Best passing worker
        passing = [w for w in worker_results if w.status == "pass"]
        if passing:
            best = max(passing, key=lambda w: w.speedup)
            best_wid, best_spd = best.worker_id, best.speedup
        else:
            best_wid, best_spd = 0, 0.0

        exited = best_spd >= EXIT_SPEEDUP

        # Summary line
        parts = []
        for w in worker_results:
            if w.status == "pass":
                parts.append(f"W{w.worker_id} {w.speedup:.2f}x")
            elif w.status == "fail":
                parts.append(f"W{w.worker_id} FAIL")
            else:
                parts.append(f"W{w.worker_id} ERR")
        tag = " ★ EXIT" if exited else ""
        log.info(f"[BENCH {n:3d}/{total}] {qid:12s}  "
                 f"{'  '.join(parts)}{tag}")

        return QueryBench(
            query_id=qid, baseline_times_ms=baseline_times,
            baseline_trimmed_mean_ms=btm, baseline_row_count=baseline_row_count,
            workers=worker_results, best_worker_id=best_wid,
            best_speedup=best_spd, exited=exited,
        )

    def _bench_worker(self, qid: str, n: int, total: int,
                      wid: int, opt_sql: str,
                      btm: float, baseline_rc: int) -> WorkerBench:
        times = []
        wrc = 0
        for r in range(1, 6):
            try:
                t0 = time.perf_counter()
                rows = self._execute(opt_sql)
                ms = (time.perf_counter() - t0) * 1000
                times.append(ms)
                if r == 1:
                    wrc = len(rows)
                log.info(f"[BENCH {n:3d}/{total}] {qid:12s}  W{wid}        "
                         f"run {r}/5  {ms:8.1f}ms")
            except Exception as e:
                log.error(f"[BENCH {n:3d}/{total}] {qid:12s}  W{wid}        "
                          f"run {r}/5  ERROR: {e}")
                return WorkerBench(
                    worker_id=wid, times_ms=times, trimmed_mean_ms=0,
                    speedup=0, row_count=0, rows_match=False,
                    status="error", error=str(e),
                )

        wtm = trimmed_mean_5(times)
        speedup = btm / wtm if wtm > 0 else 0
        rows_match = wrc == baseline_rc
        status = "pass" if rows_match else "fail"

        log.info(f"[BENCH {n:3d}/{total}] {qid:12s}  W{wid}        "
                 f"trimmed_mean={wtm:.1f}ms  {speedup:.2f}x  "
                 f"{'PASS' if status == 'pass' else 'FAIL (rows)'}")

        return WorkerBench(
            worker_id=wid, times_ms=times, trimmed_mean_ms=wtm,
            speedup=speedup, row_count=wrc, rows_match=rows_match,
            status=status,
        )


def run_benchmarks(
    jobs: list[BenchmarkJob], phase_name: str,
    batch_dir: Path, iter_name: str,
) -> dict[str, QueryBench]:
    """Run benchmark jobs across 2 parallel slots.

    RESUME-SAFE: Skips queries with existing benchmark_{iter_name}.json.
    """
    if not jobs:
        return {}

    # Check for existing results — resume
    results: dict[str, QueryBench] = {}
    remaining_jobs = []
    for job in jobs:
        saved = batch_dir / job.query_id / f"benchmark_{iter_name}.json"
        if saved.exists():
            try:
                qb = load_querybench_from_json(saved)
                results[job.query_id] = qb
                log.info(f"  [RESUMED] {job.query_id}  "
                         f"best={qb.best_speedup:.2f}x")
                continue
            except Exception:
                pass  # Re-benchmark if JSON is corrupt
        remaining_jobs.append(job)

    # Renumber remaining jobs for display
    for i, job in enumerate(remaining_jobs, 1):
        job.seq_num = i
        job.total = len(remaining_jobs)

    log.info("=" * 70)
    log.info(f"  {phase_name} ({len(jobs)} queries: "
             f"{len(results)} resumed, {len(remaining_jobs)} to run, "
             f"2 slots, 5x trimmed mean)")
    log.info("=" * 70)

    if not remaining_jobs:
        return results

    work_queue: queue.Queue = queue.Queue()
    results_lock = threading.Lock()

    slot1 = BenchmarkSlot(1, DB_SLOT_1, work_queue, results, results_lock,
                          batch_dir, iter_name, engine=ENGINE,
                          timeout_seconds=TIMEOUT_SECONDS)
    slot2 = BenchmarkSlot(2, DB_SLOT_2, work_queue, results, results_lock,
                          batch_dir, iter_name, engine=ENGINE,
                          timeout_seconds=TIMEOUT_SECONDS)
    slot1.start()
    slot2.start()

    for job in remaining_jobs:
        work_queue.put(job)

    work_queue.join()
    work_queue.put(None)
    work_queue.put(None)
    slot1.join()
    slot2.join()

    log.info(f"  {phase_name} done: {len(results)}/{len(jobs)} benchmarked")
    return results


# =====================================================================
# Extract optimized SQL from LLM worker responses
# =====================================================================

def extract_sql_from_responses(
    queries: dict[str, dict],
    responses: dict[str, str],
    batch_dir: Path,
    suffix: str = "",
) -> dict[str, dict[int, str]]:
    """Parse worker responses -> optimized SQL. Saves each to disk.

    Returns {qid: {worker_id: sql}}.
    """
    result: dict[str, dict[int, str]] = {}
    for job_id, response in responses.items():
        parts = job_id.split("/")
        qid = parts[0]
        tag = parts[1]

        if tag.startswith("W"):
            wid = int(tag[1:])
        elif tag == "snipe":
            wid = 5
        elif tag == "final":
            wid = 6
        else:
            wid = 0

        original_sql = queries[qid]["sql"]
        rewriter = SQLRewriter(original_sql, dialect=DIALECT)
        rr = rewriter.apply_response(response)

        result.setdefault(qid, {})[wid] = rr.optimized_sql

        if suffix:
            fname = f"{suffix}_sql.sql"
        else:
            fname = f"worker_{wid}_sql.sql"
        (batch_dir / qid / fname).write_text(rr.optimized_sql)

    return result


def reload_worker_sqls(batch_dir: Path, queries: dict) -> dict[str, dict[int, str]]:
    """Reload previously extracted worker SQL from disk for resume."""
    result: dict[str, dict[int, str]] = {}
    for qid in queries:
        qdir = batch_dir / qid
        for wid in range(1, 5):
            sql_path = qdir / f"worker_{wid}_sql.sql"
            if sql_path.exists():
                result.setdefault(qid, {})[wid] = sql_path.read_text()
    return result


def reload_snipe_sqls(batch_dir: Path, queries: dict) -> dict[str, str]:
    """Reload snipe worker SQL from disk."""
    result: dict[str, str] = {}
    for qid in queries:
        p = batch_dir / qid / "snipe_worker_sql.sql"
        if p.exists():
            result[qid] = p.read_text()
    return result


# =====================================================================
# Main
# =====================================================================

def _load_config(benchmark_dir: Path) -> BenchmarkConfig:
    """Load BenchmarkConfig from a benchmark directory's config.json."""
    cfg_path = benchmark_dir / "config.json"
    if not cfg_path.exists():
        log.error(f"config.json not found in {benchmark_dir}")
        sys.exit(1)
    return BenchmarkConfig.from_file(cfg_path)


def main():
    global BENCHMARK_DIR, DIALECT, ENGINE, DB_SLOT_1, DB_SLOT_2, TIMEOUT_SECONDS

    parser = argparse.ArgumentParser(description="Swarm full run")
    parser.add_argument("--batch", type=str, help="Prep batch directory")
    parser.add_argument("--query", type=str, help="Run single query (e.g. query_42)")
    parser.add_argument("--benchmark-dir", type=str,
                        help="Benchmark directory (default: duckdb_tpcds)")
    args = parser.parse_args()

    # ── Load config ──
    if args.benchmark_dir:
        BENCHMARK_DIR = Path(args.benchmark_dir)
    cfg = _load_config(BENCHMARK_DIR)
    ENGINE = cfg.engine
    DIALECT = cfg.engine if cfg.engine != "postgresql" else "postgres"
    TIMEOUT_SECONDS = cfg.timeout_seconds

    # ── DB slots ──
    if ENGINE == "duckdb":
        base = cfg.benchmark_dsn or cfg.db_path_or_dsn
        stem = Path(base).stem
        parent = Path(base).parent
        DB_SLOT_1 = str(parent / f"{stem}_1.duckdb")
        DB_SLOT_2 = str(parent / f"{stem}_2.duckdb")
    else:
        # PostgreSQL: both slots use the same benchmark DSN
        DB_SLOT_1 = cfg.benchmark_dsn or cfg.db_path_or_dsn
        DB_SLOT_2 = DB_SLOT_1

    # ── Find batch ──
    if args.batch:
        batch_dir = Path(args.batch)
    else:
        batches = sorted(BENCHMARK_DIR.glob("swarm_batch_*"), reverse=True)
        batch_dir = next(
            (b for b in batches if (b / "manifest.json").exists()), None
        )
        if not batch_dir:
            log.error("No prep batch found. Run swarm_prep.py first.")
            sys.exit(1)

    # ── Preflight ──
    if ENGINE == "duckdb":
        for db in [DB_SLOT_1, DB_SLOT_2]:
            if not Path(db).exists():
                log.error(f"Database not found: {db}")
                log.error("Create copies: cp tpcds_sf10.duckdb tpcds_sf10_1.duckdb")
                sys.exit(1)
    else:
        # PostgreSQL: test connectivity
        try:
            import psycopg2
            conn = psycopg2.connect(DB_SLOT_1)
            conn.close()
            log.info(f"  PostgreSQL preflight OK: {DB_SLOT_1}")
        except Exception as e:
            log.error(f"PostgreSQL connection failed: {e}")
            sys.exit(1)

    log_fh = logging.FileHandler(batch_dir / "run.log", mode="a")
    log_fh.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%H:%M:%S"))
    log.addHandler(log_fh)

    log.info("")
    log.info("=" * 70)
    log.info("  SWARM RUN (resume-safe)")
    log.info(f"  Batch:  {batch_dir}")
    log.info(f"  Slot 1: {DB_SLOT_1}")
    log.info(f"  Slot 2: {DB_SLOT_2}")
    log.info(f"  Exit:   >={EXIT_SPEEDUP}x")
    log.info(f"  Time:   {datetime.now().isoformat()}")
    log.info("=" * 70)

    # ── Load prep data ──
    queries: dict[str, dict] = {}
    for qdir in sorted(batch_dir.iterdir()):
        if not qdir.is_dir() or not qdir.name.startswith("query"):
            continue
        sql_p = qdir / "original.sql"
        prompt_p = qdir / "fan_out_prompt.txt"
        if sql_p.exists() and prompt_p.exists():
            queries[qdir.name] = {
                "sql": sql_p.read_text(),
                "fan_out_prompt": prompt_p.read_text(),
            }
    # Filter to single query if --query specified
    if args.query:
        if args.query not in queries:
            log.error(f"Query {args.query} not found in batch")
            sys.exit(1)
        queries = {args.query: queries[args.query]}

    log.info(f"  Loaded {len(queries)} queries")

    # ── Pipeline + Generator ──
    pipeline = Pipeline(str(BENCHMARK_DIR), provider="deepseek", model="deepseek-reasoner")
    generator = CandidateGenerator(provider="deepseek", model="deepseek-reasoner")
    all_available = pipeline._list_gold_examples(ENGINE)
    global_learnings = pipeline.learner.build_learning_summary() or None

    t_start = time.time()
    exited: set[str] = set()

    # =================================================================
    # ITERATION 0 — Fan-out: 101 analyst + 404 workers
    # =================================================================

    # Phase 2: Analyst fan-out (resume: skips existing fan_out_response.txt)
    analyst_jobs = [
        (qid, data["fan_out_prompt"], batch_dir / qid / "fan_out_response.txt")
        for qid, data in queries.items()
    ]
    analyst_responses = fire_llm_calls(
        generator, analyst_jobs, "PHASE 2: Analyst Fan-Out"
    )
    save_checkpoint(batch_dir, "phase2", {"ok": len(analyst_responses)})

    # Phase 2.5: Parse assignments -> build worker prompts
    # Resume: reload assignments.json if they exist
    log.info("=" * 70)
    log.info("  PHASE 2.5: Parse Assignments + Build Worker Prompts")
    log.info("=" * 70)

    all_assignments: dict[str, list] = {}
    worker_jobs: list[tuple[str, str, Path]] = []

    for qid in sorted(analyst_responses):
        qdir = batch_dir / qid
        data = queries[qid]
        asn_path = qdir / "assignments.json"

        try:
            # Resume: reload from disk if already parsed
            if asn_path.exists():
                assignments = load_assignments_from_json(asn_path)
                log.info(f"  {qid}  RESUMED {len(assignments)} assignments")
            else:
                assignments = parse_fan_out_response(analyst_responses[qid])
                asn_path.write_text(json.dumps(
                    [{"worker_id": a.worker_id, "strategy": a.strategy,
                      "examples": a.examples, "hint": a.hint}
                     for a in assignments], indent=2,
                ))
                log.info(f"  {qid}  {len(assignments)} worker prompts")

            all_assignments[qid] = assignments

            dag, costs, _ = pipeline._parse_dag(
                data["sql"], dialect=DIALECT, query_id=qid,
            )
            rw = pipeline._find_regression_warnings(data["sql"], engine=ENGINE, k=2)

            for a in assignments:
                resp_path = qdir / f"worker_{a.worker_id}_response.txt"
                prompt_path = qdir / f"worker_{a.worker_id}_prompt.txt"

                # Build prompt (needed even for resume to populate worker_jobs)
                if not prompt_path.exists():
                    examples = pipeline._load_examples_by_id(a.examples, ENGINE)
                    base_prompt = pipeline.prompter.build_prompt(
                        query_id=f"{qid}_w{a.worker_id}",
                        full_sql=data["sql"], dag=dag, costs=costs,
                        history=None, examples=examples,
                        expert_analysis=None,
                        global_learnings=global_learnings,
                        regression_warnings=rw, dialect=DIALECT,
                        semantic_intents=pipeline.get_semantic_intents(qid),
                        engine_version=pipeline._engine_version,
                    )
                    wp = build_worker_strategy_header(a.strategy, a.hint) + base_prompt
                    prompt_path.write_text(wp)
                else:
                    wp = prompt_path.read_text()

                worker_jobs.append((
                    f"{qid}/W{a.worker_id}", wp, resp_path,
                ))

        except Exception as e:
            log.error(f"  {qid}  PARSE ERROR: {e}")
            (qdir / "parse_error.txt").write_text(tb_mod.format_exc())

    log.info(f"  Phase 2.5 done: {len(worker_jobs)} worker prompts")
    save_checkpoint(batch_dir, "phase2_5", {"worker_jobs": len(worker_jobs)})

    # Phase 3: Worker generation (resume: skips existing worker_N_response.txt)
    worker_responses = fire_llm_calls(
        generator, worker_jobs, "PHASE 3: Worker Generation"
    )
    save_checkpoint(batch_dir, "phase3", {"ok": len(worker_responses)})

    # Phase 3.5: Extract SQL (resume: reload from disk if .sql files exist)
    worker_sqls = extract_sql_from_responses(queries, worker_responses, batch_dir)
    # Also reload any SQL that was extracted in a prior run but whose response
    # might not be in worker_responses (e.g., response loaded from disk)
    disk_sqls = reload_worker_sqls(batch_dir, queries)
    for qid, wmap in disk_sqls.items():
        for wid, sql in wmap.items():
            worker_sqls.setdefault(qid, {})[wid] = sql
    log.info(f"  Phase 3.5: SQL for {len(worker_sqls)} queries")

    # Phase 4: Benchmark iter 0 (resume: skips existing benchmark_iter0.json)
    bench_jobs = []
    seq = 0
    for qid in sorted(queries):
        if qid not in worker_sqls:
            continue
        seq += 1
        bench_jobs.append(BenchmarkJob(
            query_id=qid,
            original_sql=queries[qid]["sql"],
            workers=sorted(worker_sqls[qid].items()),
            seq_num=seq, total=len(worker_sqls),
        ))

    bench0 = run_benchmarks(
        bench_jobs, "BENCHMARK ITER 0", batch_dir, "iter0"
    )
    save_checkpoint(batch_dir, "phase4", {
        "benchmarked": len(bench0),
        "exited": sum(1 for qb in bench0.values() if qb.exited),
    })

    exited = {qid for qid, qb in bench0.items() if qb.exited}
    need_snipe = {qid for qid in bench0 if qid not in exited}
    log.info(f"  Iter 0 summary: {len(exited)} exited (>={EXIT_SPEEDUP}x), "
             f"{len(need_snipe)} need snipe")

    # =================================================================
    # ITERATION 1 — Snipe: workers (original SQL + best info)
    # =================================================================

    snipe_sqls: dict[str, str] = reload_snipe_sqls(batch_dir, queries)
    bench1: dict[str, QueryBench] = {}

    if need_snipe:
        log.info("=" * 70)
        log.info(f"  PHASE 5: Snipe Workers ({len(need_snipe)} queries)")
        log.info("=" * 70)

        snipe_jobs: list[tuple[str, str, Path]] = []
        for qid in sorted(need_snipe):
            data = queries[qid]
            qdir = batch_dir / qid
            br = bench0[qid]

            passing = [w for w in br.workers if w.status == "pass"]
            if passing:
                best_w = max(passing, key=lambda w: w.speedup)
                asn_list = all_assignments.get(qid, [])
                best_strat = next(
                    (a.strategy for a in asn_list
                     if a.worker_id == best_w.worker_id),
                    "unknown",
                )
                ctx = (f"Best result: W{best_w.worker_id} achieved "
                       f"{best_w.speedup:.2f}x via {best_strat}")
            else:
                ctx = "All 4 workers failed to produce valid results"

            snipe_hint = (
                f"CONTEXT: {ctx}. "
                f"Your goal: optimize the ORIGINAL SQL to >={EXIT_SPEEDUP}x. "
                f"Try a DIFFERENT approach."
            )

            try:
                prompt_path = qdir / "snipe_worker_prompt.txt"
                if not prompt_path.exists():
                    dag, costs, _ = pipeline._parse_dag(
                        data["sql"], dialect=DIALECT, query_id=qid,
                    )
                    faiss_ex = pipeline._find_examples(
                        data["sql"], engine=ENGINE, k=5,
                    )
                    rw = pipeline._find_regression_warnings(
                        data["sql"], engine=ENGINE, k=2,
                    )
                    base_prompt = pipeline.prompter.build_prompt(
                        query_id=f"{qid}_snipe", full_sql=data["sql"],
                        dag=dag, costs=costs, history=None,
                        examples=faiss_ex, expert_analysis=None,
                        global_learnings=global_learnings,
                        regression_warnings=rw, dialect=DIALECT,
                        semantic_intents=pipeline.get_semantic_intents(qid),
                        engine_version=pipeline._engine_version,
                    )
                    wp = build_worker_strategy_header(
                        "refined_snipe", snipe_hint,
                    ) + base_prompt
                    prompt_path.write_text(wp)
                else:
                    wp = prompt_path.read_text()

                snipe_jobs.append((
                    f"{qid}/snipe", wp,
                    qdir / "snipe_worker_response.txt",
                ))
            except Exception as e:
                log.error(f"  {qid}  snipe prompt ERROR: {e}")

        snipe_responses = fire_llm_calls(
            generator, snipe_jobs, "PHASE 5: Snipe Workers"
        )
        save_checkpoint(batch_dir, "phase5", {"ok": len(snipe_responses)})

        # Extract snipe SQL
        snipe_sql_map = extract_sql_from_responses(
            queries, snipe_responses, batch_dir, suffix="snipe_worker",
        )
        for qid, wmap in snipe_sql_map.items():
            snipe_sqls[qid] = wmap.get(5, queries[qid]["sql"])

        # Benchmark snipe (resume: skips existing benchmark_iter1.json)
        snipe_bench_jobs = []
        seq = 0
        for qid in sorted(snipe_sqls):
            if qid in exited:
                continue
            seq += 1
            snipe_bench_jobs.append(BenchmarkJob(
                query_id=qid,
                original_sql=queries[qid]["sql"],
                workers=[(5, snipe_sqls[qid])],
                seq_num=seq, total=len(snipe_sqls),
            ))

        bench1 = run_benchmarks(
            snipe_bench_jobs, "BENCHMARK ITER 1 (Snipe)", batch_dir, "iter1",
        )
        save_checkpoint(batch_dir, "phase5_bench", {
            "benchmarked": len(bench1),
            "exited": sum(1 for qb in bench1.values() if qb.exited),
        })

        newly_exited = {qid for qid, qb in bench1.items() if qb.exited}
        exited.update(newly_exited)
        log.info(f"  Iter 1 summary: {len(newly_exited)} newly exited, "
                 f"{len(need_snipe) - len(newly_exited)} need re-analyze")

    # =================================================================
    # ITERATION 2 — Re-analyze: analyst + workers
    # =================================================================

    need_reanalyze = {qid for qid in need_snipe if qid not in exited}
    bench2: dict[str, QueryBench] = {}

    if need_reanalyze:
        log.info("=" * 70)
        log.info(f"  PHASE 6: Re-Analyze ({len(need_reanalyze)} queries)")
        log.info("=" * 70)

        # Build re-analyze analyst prompts
        reanalyze_jobs: list[tuple[str, str, Path]] = []
        for qid in sorted(need_reanalyze):
            data = queries[qid]
            qdir = batch_dir / qid

            # Collect WorkerResult objects from iter 0 + iter 1
            all_wr: list[WorkerResult] = []

            br0 = bench0.get(qid)
            asn_list = all_assignments.get(qid, [])
            if br0:
                for wb in br0.workers:
                    a = next(
                        (a for a in asn_list if a.worker_id == wb.worker_id),
                        None,
                    )
                    all_wr.append(WorkerResult(
                        worker_id=wb.worker_id,
                        strategy=a.strategy if a else f"worker_{wb.worker_id}",
                        examples_used=a.examples if a else [],
                        optimized_sql=worker_sqls.get(qid, {}).get(
                            wb.worker_id, data["sql"]
                        ),
                        speedup=wb.speedup, status=wb.status,
                        transforms=[], hint=a.hint if a else "",
                        error_message=wb.error or None,
                    ))

            br1 = bench1.get(qid)
            if br1 and br1.workers:
                wb = br1.workers[0]
                all_wr.append(WorkerResult(
                    worker_id=5, strategy="refined_snipe",
                    examples_used=[],
                    optimized_sql=snipe_sqls.get(qid, data["sql"]),
                    speedup=wb.speedup, status=wb.status,
                    transforms=[], hint="Snipe from iter 1",
                    error_message=wb.error or None,
                ))

            try:
                prompt_path = qdir / "reanalyze_prompt.txt"
                if not prompt_path.exists():
                    dag, costs, _ = pipeline._parse_dag(
                        data["sql"], dialect=DIALECT, query_id=qid,
                    )
                    sp = build_snipe_prompt(
                        query_id=qid, original_sql=data["sql"],
                        worker_results=all_wr, target_speedup=EXIT_SPEEDUP,
                        dag=dag, costs=costs,
                        all_available_examples=all_available, dialect=DIALECT,
                    )
                    prompt_path.write_text(sp)
                else:
                    sp = prompt_path.read_text()

                reanalyze_jobs.append((
                    f"{qid}/reanalyze", sp,
                    qdir / "reanalyze_response.txt",
                ))
            except Exception as e:
                log.error(f"  {qid}  reanalyze prompt ERROR: {e}")

        reanalyze_responses = fire_llm_calls(
            generator, reanalyze_jobs, "PHASE 6: Re-Analyze (Analyst)"
        )
        save_checkpoint(batch_dir, "phase6", {"ok": len(reanalyze_responses)})

        # Parse + build final worker prompts
        log.info("=" * 70)
        log.info("  PHASE 6.5: Build Final Worker Prompts")
        log.info("=" * 70)

        final_jobs: list[tuple[str, str, Path]] = []
        for job_id, response in reanalyze_responses.items():
            qid = job_id.split("/")[0]
            data = queries[qid]
            qdir = batch_dir / qid

            try:
                analysis = parse_snipe_response(response)
                (qdir / "reanalyze_parsed.json").write_text(json.dumps({
                    "failure_analysis": analysis.failure_analysis,
                    "unexplored": analysis.unexplored,
                    "refined_strategy": analysis.refined_strategy,
                    "examples": analysis.examples,
                    "hint": analysis.hint,
                }, indent=2))

                prompt_path = qdir / "final_worker_prompt.txt"
                if not prompt_path.exists():
                    dag, costs, _ = pipeline._parse_dag(
                        data["sql"], dialect=DIALECT, query_id=qid,
                    )
                    examples = pipeline._load_examples_by_id(
                        analysis.examples, ENGINE,
                    )
                    if not examples:
                        examples = pipeline._find_examples(
                            data["sql"], engine=ENGINE, k=5,
                        )
                    rw = pipeline._find_regression_warnings(
                        data["sql"], engine=ENGINE, k=2,
                    )
                    base_prompt = pipeline.prompter.build_prompt(
                        query_id=f"{qid}_final", full_sql=data["sql"],
                        dag=dag, costs=costs, history=None,
                        examples=examples, expert_analysis=None,
                        global_learnings=global_learnings,
                        regression_warnings=rw, dialect=DIALECT,
                        semantic_intents=pipeline.get_semantic_intents(qid),
                        engine_version=pipeline._engine_version,
                    )
                    hint = analysis.hint or "Apply the refined strategy."
                    strat = analysis.refined_strategy or "final_refined"
                    wp = build_worker_strategy_header(strat, hint) + base_prompt
                    prompt_path.write_text(wp)
                else:
                    wp = prompt_path.read_text()

                final_jobs.append((
                    f"{qid}/final", wp,
                    qdir / "final_worker_response.txt",
                ))
            except Exception as e:
                log.error(f"  {qid}  final prompt ERROR: {e}")

        final_responses = fire_llm_calls(
            generator, final_jobs, "PHASE 7: Final Workers"
        )
        save_checkpoint(batch_dir, "phase7", {"ok": len(final_responses)})

        # Extract final SQL
        final_sql_map = extract_sql_from_responses(
            queries, final_responses, batch_dir, suffix="final_worker",
        )
        final_sqls: dict[str, str] = {}
        for qid, wmap in final_sql_map.items():
            final_sqls[qid] = wmap.get(6, queries[qid]["sql"])

        # Benchmark final (resume: skips existing benchmark_iter2.json)
        final_bench_jobs = []
        seq = 0
        for qid in sorted(final_sqls):
            seq += 1
            final_bench_jobs.append(BenchmarkJob(
                query_id=qid,
                original_sql=queries[qid]["sql"],
                workers=[(6, final_sqls[qid])],
                seq_num=seq, total=len(final_sqls),
            ))

        bench2 = run_benchmarks(
            final_bench_jobs, "BENCHMARK ITER 2 (Final)", batch_dir, "iter2",
        )
        save_checkpoint(batch_dir, "phase7_bench", {
            "benchmarked": len(bench2),
            "exited": sum(1 for qb in bench2.values() if qb.exited),
        })

    # =================================================================
    # LEADERBOARD
    # =================================================================

    elapsed = time.time() - t_start

    leaderboard = []
    for qid in sorted(queries):
        best_spd, best_wid, best_iter = 0.0, 0, -1
        for iter_num, bench in enumerate([bench0, bench1, bench2]):
            qb = bench.get(qid)
            if not qb:
                continue
            for w in qb.workers:
                if w.status == "pass" and w.speedup > best_spd:
                    best_spd = w.speedup
                    best_wid = w.worker_id
                    best_iter = iter_num

        if best_spd >= EXIT_SPEEDUP:
            status = "WIN"
        elif best_spd >= 1.1:
            status = "IMPROVED"
        elif best_spd >= 0.95:
            status = "NEUTRAL"
        elif best_spd > 0:
            status = "REGRESSION"
        else:
            status = "ERROR"

        leaderboard.append({
            "query_id": qid, "best_speedup": round(best_spd, 2),
            "best_worker": best_wid, "best_iteration": best_iter,
            "status": status,
        })

        (batch_dir / qid / "result.json").write_text(json.dumps({
            "query_id": qid, "status": status,
            "best_speedup": round(best_spd, 2),
            "best_worker": best_wid, "best_iteration": best_iter,
        }, indent=2))

    leaderboard.sort(key=lambda x: x["best_speedup"], reverse=True)

    wins = sum(1 for e in leaderboard if e["status"] == "WIN")
    improved = sum(1 for e in leaderboard if e["status"] == "IMPROVED")
    neutral = sum(1 for e in leaderboard if e["status"] == "NEUTRAL")
    regression = sum(1 for e in leaderboard if e["status"] == "REGRESSION")
    errors = sum(1 for e in leaderboard if e["status"] == "ERROR")

    log.info("")
    log.info("=" * 70)
    log.info("  SWARM RUN COMPLETE")
    log.info("=" * 70)
    log.info(f"  WIN (>={EXIT_SPEEDUP}x):  {wins}")
    log.info(f"  IMPROVED (>=1.1x): {improved}")
    log.info(f"  NEUTRAL:           {neutral}")
    log.info(f"  REGRESSION:        {regression}")
    log.info(f"  ERROR:             {errors}")
    log.info(f"  Elapsed:           {elapsed / 60:.1f} min")
    log.info("")
    log.info("  Top 10:")
    for e in leaderboard[:10]:
        log.info(f"    {e['query_id']:12s}  {e['best_speedup']:.2f}x  "
                 f"W{e['best_worker']}  iter{e['best_iteration']}  "
                 f"{e['status']}")

    (batch_dir / "leaderboard.json").write_text(
        json.dumps(leaderboard, indent=2)
    )
    (batch_dir / "run_manifest.json").write_text(json.dumps({
        "total_queries": len(queries),
        "wins": wins, "improved": improved, "neutral": neutral,
        "regression": regression, "errors": errors,
        "elapsed_minutes": round(elapsed / 60, 1),
        "exit_speedup": EXIT_SPEEDUP,
        "max_llm_concurrent": MAX_LLM_CONCURRENT,
        "engine": ENGINE, "dialect": DIALECT,
        "db_slot_1": DB_SLOT_1, "db_slot_2": DB_SLOT_2,
        "timestamp": datetime.now().isoformat(),
    }, indent=2))

    save_checkpoint(batch_dir, "complete", {
        "wins": wins, "improved": improved,
        "neutral": neutral, "regression": regression,
    })
    log.info(f"  Leaderboard: {batch_dir / 'leaderboard.json'}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
