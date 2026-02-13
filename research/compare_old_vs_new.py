#!/usr/bin/env python3
"""Head-to-head comparison: OLD per-worker prompts vs NEW shared-prefix + coach.

Runs Q35 TPC-DS DuckDB SF10 through BOTH architectures:
  - OLD: 4 independent worker prompts → snipe retry (1 worker)
  - NEW: shared-prefix + assignment → coach → 4 refined workers

Logs every LLM call with full token usage, cache metrics, timings,
strategies, EXPLAIN ANALYZE plans, and speedup results.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 research/compare_old_vs_new.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

def P(msg: str = ""):
    """Print with flush."""
    print(msg, flush=True)

# ── Setup paths ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)

# ── Imports ─────────────────────────────────────────────────────────────
from qt_shared.llm.factory import create_llm_client
from qt_sql.prompts.worker import build_worker_prompt
from qt_sql.prompts.worker_shared_prefix import (
    build_shared_worker_prefix,
    build_worker_assignment,
)
from qt_sql.prompts.coach import build_coach_prompt, build_coach_refinement_prefix
from qt_sql.prompts.swarm_snipe import build_retry_worker_prompt
from qt_sql.generate import CandidateGenerator, Candidate
from qt_sql.sql_rewriter import SQLRewriter, extract_transforms_from_response
from qt_sql.explain_signals import extract_vital_signs
from qt_sql.schemas import WorkerResult

# ── Config ──────────────────────────────────────────────────────────────
BENCHMARK_DIR = PROJECT_ROOT / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "duckdb_tpcds"
DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10_1.duckdb"
QUERY_ID = "query_35"
DIALECT = "duckdb"
OUTPUT_DIR = PROJECT_ROOT / "research" / "comparison_results"


@dataclass
class LLMCallLog:
    """Log entry for a single LLM call."""
    label: str
    prompt_chars: int = 0
    response_chars: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cache_hit_tokens: int = 0
    cache_miss_tokens: int = 0
    duration_s: float = 0.0
    cost_usd: float = 0.0


@dataclass
class WorkerLog:
    """Log entry for a single worker's results."""
    worker_id: int
    strategy: str
    transforms: List[str]
    optimized_sql: str = ""
    sql_chars: int = 0
    speedup: float = 0.0
    status: str = ""
    time_ms: float = 0.0
    explain_text: str = ""
    vital_signs: str = ""
    error: str = ""


def estimate_cost(log: LLMCallLog) -> float:
    """Estimate USD cost for DeepSeek R1 via OpenRouter.
    Input: $0.55/M (miss) or $0.14/M (hit). Output: $2.19/M.
    """
    miss_tokens = log.cache_miss_tokens or (log.prompt_tokens - log.cache_hit_tokens)
    input_cost = miss_tokens * 0.55 / 1e6 + log.cache_hit_tokens * 0.14 / 1e6
    output_cost = log.completion_tokens * 2.19 / 1e6
    return input_cost + output_cost


def make_llm_call(client, prompt: str, label: str) -> Tuple[str, LLMCallLog]:
    """Make an LLM call and capture full metrics."""
    log = LLMCallLog(label=label, prompt_chars=len(prompt))
    t0 = time.time()
    response = client.analyze(prompt)
    log.duration_s = time.time() - t0
    log.response_chars = len(response)

    u = getattr(client, 'last_usage', {})
    log.prompt_tokens = u.get('prompt_tokens', 0)
    log.completion_tokens = u.get('completion_tokens', 0)
    log.total_tokens = u.get('total_tokens', 0)
    log.cache_hit_tokens = u.get('prompt_cache_hit_tokens', 0) or u.get('cached_tokens', 0)
    log.cache_miss_tokens = u.get('prompt_cache_miss_tokens', 0)
    log.cost_usd = estimate_cost(log)
    return response, log


def run_explain_analyze(db_path: str, sql: str) -> Dict[str, Any]:
    """Run EXPLAIN ANALYZE on DuckDB, return plan text + JSON."""
    import duckdb
    result = {"plan_text": "", "plan_json": None, "execution_time_ms": 0}
    try:
        conn = duckdb.connect(db_path, read_only=True)
        # Text plan with ANALYZE (actual timings)
        text_rows = conn.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
        result["plan_text"] = "\n".join(
            row[1] if len(row) > 1 else row[0] for row in text_rows
        )
        # JSON plan
        try:
            json_rows = conn.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}").fetchall()
            for plan_type, plan_json in json_rows:
                parsed = json.loads(plan_json)
                if plan_type == "analyzed_plan" and isinstance(parsed, dict):
                    result["plan_json"] = parsed
                    result["execution_time_ms"] = parsed.get("latency", 0) * 1000
                    break
        except Exception:
            pass
        conn.close()
    except Exception as e:
        result["plan_text"] = f"EXPLAIN failed: {e}"
    return result


def time_query(db_path: str, sql: str, runs: int = 3) -> Tuple[float, int]:
    """Time a query — 3 runs, discard warmup, avg last 2."""
    import duckdb
    times = []
    row_count = 0
    for i in range(runs):
        conn = duckdb.connect(db_path, read_only=True)
        t0 = time.perf_counter()
        rows = conn.execute(sql).fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        row_count = len(rows)
        conn.close()
        times.append(elapsed)
    # Discard warmup, avg rest
    avg_ms = sum(times[1:]) / len(times[1:]) if len(times) >= 2 else times[0]
    return avg_ms, row_count


def validate_and_explain(
    db_path: str, candidate_sql: str, orig_time_ms: float, orig_rows: int,
) -> WorkerLog:
    """Validate + EXPLAIN ANALYZE a candidate."""
    wlog = WorkerLog(worker_id=0, strategy="", transforms=[])
    try:
        cand_time, cand_rows = time_query(db_path, candidate_sql, runs=3)
    except Exception as e:
        wlog.status = "ERROR"
        wlog.error = str(e)
        return wlog

    wlog.time_ms = cand_time
    if cand_rows != orig_rows:
        wlog.status = "FAIL"
        wlog.error = f"Row mismatch: {orig_rows} vs {cand_rows}"
        return wlog

    speedup = orig_time_ms / cand_time if cand_time > 0 else 0.0
    wlog.speedup = speedup
    wlog.status = (
        "WIN" if speedup >= 1.10 else
        "IMPROVED" if speedup >= 1.05 else
        "NEUTRAL" if speedup >= 0.95 else
        "REGRESSION"
    )

    # EXPLAIN ANALYZE
    explain = run_explain_analyze(db_path, candidate_sql)
    wlog.explain_text = explain.get("plan_text", "")
    wlog.vital_signs = extract_vital_signs(
        wlog.explain_text, explain.get("plan_json"), dialect=DIALECT
    )
    return wlog


def parse_rewrite(original_sql: str, response: str) -> Tuple[str, List[str]]:
    """Parse LLM response → (optimized_sql, transforms)."""
    rewriter = SQLRewriter(original_sql, dialect=DIALECT)
    result = rewriter.apply_response(response)
    sql = result.optimized_sql
    if result.rewrite_set and result.rewrite_set.transform:
        transforms = [result.rewrite_set.transform]
    elif result.transform and result.transform != "semantic_rewrite":
        transforms = [result.transform]
    else:
        transforms = extract_transforms_from_response(response, original_sql, sql)
    return sql, transforms


def print_log(log: LLMCallLog):
    P(f"    {log.prompt_tokens:,} prompt | cache_hit={log.cache_hit_tokens:,} miss={log.cache_miss_tokens:,} "
      f"| comp={log.completion_tokens:,} | {log.duration_s:.1f}s | ${log.cost_usd:.4f}")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_logs: List[LLMCallLog] = []

    P("=" * 80)
    P("OLD vs NEW PROMPT ARCHITECTURE — FULL HEAD-TO-HEAD COMPARISON")
    P("=" * 80)
    P(f"Query: {QUERY_ID}  |  DB: DuckDB TPC-DS SF10  |  Dialect: {DIALECT}")
    P()

    # ── Load query ──────────────────────────────────────────────────────
    query_path = BENCHMARK_DIR / "queries" / f"{QUERY_ID}.sql"
    original_sql = query_path.read_text().strip()
    P(f"Original SQL: {len(original_sql)} chars")

    # ── Baseline timing ─────────────────────────────────────────────────
    P("\n--- BASELINE (3 runs, discard warmup) ---")
    orig_time, orig_rows = time_query(DB_PATH, original_sql, runs=3)
    P(f"Original: {orig_time:.0f}ms  |  {orig_rows} rows")

    orig_explain = run_explain_analyze(DB_PATH, original_sql)
    orig_vital = extract_vital_signs(
        orig_explain.get("plan_text", ""), orig_explain.get("plan_json"), dialect=DIALECT,
    )
    P(f"EXPLAIN vital signs: {orig_vital}")

    # ── Pipeline setup ──────────────────────────────────────────────────
    P("\n--- SETUP ---")
    from qt_sql.pipeline import Pipeline
    pipeline = Pipeline(BENCHMARK_DIR)
    client = create_llm_client()
    P(f"Client: {type(client).__name__}")

    # ── Analyst (shared between OLD and NEW) ────────────────────────────
    P("\n" + "=" * 80)
    P("STEP 1: ANALYST (shared)")
    P("=" * 80)

    ctx = pipeline.gather_analyst_context(
        query_id=QUERY_ID, sql=original_sql, dialect=DIALECT, engine="duckdb",
    )

    from qt_sql.prompts import build_analyst_briefing_prompt, parse_briefing_response
    from qt_sql.logic_tree import build_logic_tree
    from qt_sql.prompter import Prompter, _build_node_intent_map

    dag, costs, _ = pipeline._parse_logical_tree(original_sql, dialect=DIALECT, query_id=QUERY_ID)
    output_columns = Prompter._extract_output_columns(dag)

    node_intents = _build_node_intent_map(ctx["semantic_intents"])
    if ctx["semantic_intents"]:
        qi = ctx["semantic_intents"].get("query_intent", "")
        if qi and "main_query" not in node_intents:
            node_intents["main_query"] = qi
    original_logic_tree = build_logic_tree(original_sql, dag, costs, DIALECT, node_intents)

    analyst_prompt = build_analyst_briefing_prompt(
        query_id=QUERY_ID, sql=original_sql,
        explain_plan_text=ctx["explain_plan_text"], dag=dag, costs=costs,
        semantic_intents=ctx["semantic_intents"],
        global_knowledge=ctx["global_knowledge"],
        constraints=ctx["constraints"], dialect=DIALECT,
        strategy_leaderboard=ctx["strategy_leaderboard"],
        query_archetype=ctx["query_archetype"],
        engine_profile=ctx["engine_profile"],
        resource_envelope=ctx["resource_envelope"],
        exploit_algorithm_text=ctx["exploit_algorithm_text"],
        plan_scanner_text=ctx["plan_scanner_text"],
        detected_transforms=ctx.get("detected_transforms", []),
        qerror_analysis=ctx.get("qerror_analysis"),
    )
    P(f"Analyst prompt: {len(analyst_prompt)} chars")
    P("Calling analyst LLM...")

    analyst_response, analyst_log = make_llm_call(client, analyst_prompt, "ANALYST")
    all_logs.append(analyst_log)
    print_log(analyst_log)

    briefing = parse_briefing_response(analyst_response)
    for w in briefing.workers:
        P(f"  W{w.worker_id}: {w.strategy}")

    # Load examples for all workers
    all_worker_examples: Dict[int, list] = {}
    for wb in briefing.workers:
        loaded = pipeline._load_examples_by_id(wb.examples, "duckdb")
        all_worker_examples[wb.worker_id] = loaded

    # ═══════════════════════════════════════════════════════════════════
    # PATH A: OLD ARCHITECTURE — 4 independent prompts → snipe retry
    # ═══════════════════════════════════════════════════════════════════
    P("\n" + "=" * 80)
    P("PATH A: OLD (per-worker prompts, no cache reuse)")
    P("=" * 80)

    old_logs: List[LLMCallLog] = []
    old_workers: List[WorkerLog] = []

    for wb in sorted(briefing.workers, key=lambda w: w.worker_id):
        wid = wb.worker_id
        examples = all_worker_examples.get(wid, [])

        old_prompt = build_worker_prompt(
            worker_briefing=wb, shared_briefing=briefing.shared,
            examples=examples, original_sql=original_sql,
            output_columns=output_columns, dialect=DIALECT,
            engine_version=pipeline._engine_version,
            original_logic_tree=original_logic_tree,
        )

        P(f"\n  OLD-W{wid} [{wb.strategy}]: {len(old_prompt)} chars")
        response, log = make_llm_call(client, old_prompt, f"OLD-W{wid}")
        old_logs.append(log)
        all_logs.append(log)
        print_log(log)

        try:
            opt_sql, transforms = parse_rewrite(original_sql, response)
        except Exception as e:
            opt_sql, transforms = original_sql, []
            P(f"    PARSE ERROR: {e}")

        wlog = WorkerLog(worker_id=wid, strategy=wb.strategy, transforms=transforms,
                         optimized_sql=opt_sql, sql_chars=len(opt_sql))

        if opt_sql.strip() != original_sql.strip():
            vlog = validate_and_explain(DB_PATH, opt_sql, orig_time, orig_rows)
            wlog.status, wlog.speedup, wlog.time_ms = vlog.status, vlog.speedup, vlog.time_ms
            wlog.vital_signs, wlog.explain_text = vlog.vital_signs, vlog.explain_text
            wlog.error = vlog.error
        else:
            wlog.status, wlog.speedup, wlog.time_ms = "NO_CHANGE", 1.0, orig_time

        old_workers.append(wlog)
        P(f"    → {wlog.status} {wlog.speedup:.2f}x | {wlog.time_ms:.0f}ms | {wlog.transforms}")

    # ── OLD Snipe Retry ────────────────────────────────────────────────
    P("\n  --- OLD SNIPE RETRY (1 worker) ---")

    old_worker_results = [
        WorkerResult(
            worker_id=w.worker_id, strategy=w.strategy,
            examples_used=[], optimized_sql=w.optimized_sql,
            speedup=w.speedup, status=w.status, transforms=w.transforms,
            error_message=w.error or None,
        )
        for w in old_workers
    ]
    best_old = max(old_workers, key=lambda w: w.speedup)
    best_sql = best_old.optimized_sql if best_old.speedup > 1.0 else None

    snipe_prompt = build_retry_worker_prompt(
        original_sql=original_sql,
        worker_results=old_worker_results,
        best_worker_sql=best_sql,
        examples=sum(all_worker_examples.values(), []),
        output_columns=output_columns,
        dag=dag, costs=costs,
        explain_plan_text=ctx["explain_plan_text"],
        candidate_explains={},
        race_timings=None,
        engine_profile=ctx["engine_profile"],
        constraints=ctx["constraints"],
        semantic_intents=ctx["semantic_intents"],
        regression_warnings=ctx["regression_warnings"],
        shared_briefing=briefing.shared,
        dialect=DIALECT,
        engine_version=pipeline._engine_version,
        target_speedup=2.0,
    )

    P(f"  SNIPE: {len(snipe_prompt)} chars")
    snipe_response, snipe_log = make_llm_call(client, snipe_prompt, "OLD-SNIPE")
    old_logs.append(snipe_log)
    all_logs.append(snipe_log)
    print_log(snipe_log)

    try:
        snipe_sql, snipe_transforms = parse_rewrite(original_sql, snipe_response)
    except Exception:
        snipe_sql, snipe_transforms = original_sql, []

    snipe_wlog = WorkerLog(worker_id=5, strategy="snipe_retry", transforms=snipe_transforms,
                           optimized_sql=snipe_sql, sql_chars=len(snipe_sql))
    if snipe_sql.strip() != original_sql.strip():
        vlog = validate_and_explain(DB_PATH, snipe_sql, orig_time, orig_rows)
        snipe_wlog.status, snipe_wlog.speedup, snipe_wlog.time_ms = vlog.status, vlog.speedup, vlog.time_ms
        snipe_wlog.vital_signs = vlog.vital_signs
    else:
        snipe_wlog.status, snipe_wlog.speedup, snipe_wlog.time_ms = "NO_CHANGE", 1.0, orig_time
    old_workers.append(snipe_wlog)
    P(f"    → {snipe_wlog.status} {snipe_wlog.speedup:.2f}x | {snipe_wlog.time_ms:.0f}ms")

    # ═══════════════════════════════════════════════════════════════════
    # PATH B: NEW ARCHITECTURE — shared prefix → coach → 4 refined
    # ═══════════════════════════════════════════════════════════════════
    P("\n" + "=" * 80)
    P("PATH B: NEW (shared-prefix + coach refinement)")
    P("=" * 80)

    new_logs: List[LLMCallLog] = []
    new_workers: List[WorkerLog] = []

    # Build shared prefix
    shared_prefix = build_shared_worker_prefix(
        analyst_response=analyst_response,
        shared_briefing=briefing.shared,
        all_worker_briefings=list(briefing.workers),
        all_examples=all_worker_examples,
        original_sql=original_sql,
        output_columns=output_columns,
        dialect=DIALECT,
        engine_version=pipeline._engine_version,
        original_logic_tree=original_logic_tree,
    )
    P(f"Shared prefix: {len(shared_prefix)} chars")

    # ── Round 1: Fan-out with shared prefix ─────────────────────────────
    P("\n  --- ROUND 1: FAN-OUT (shared prefix + assignment) ---")

    for wb in sorted(briefing.workers, key=lambda w: w.worker_id):
        wid = wb.worker_id
        suffix = build_worker_assignment(wid)
        new_prompt = shared_prefix + "\n\n" + suffix

        P(f"\n  NEW-W{wid} [{wb.strategy}]: {len(new_prompt)} chars (prefix={len(shared_prefix)}, suffix={len(suffix)})")
        response, log = make_llm_call(client, new_prompt, f"NEW-W{wid}")
        new_logs.append(log)
        all_logs.append(log)
        print_log(log)

        try:
            opt_sql, transforms = parse_rewrite(original_sql, response)
        except Exception as e:
            opt_sql, transforms = original_sql, []
            P(f"    PARSE ERROR: {e}")

        wlog = WorkerLog(worker_id=wid, strategy=wb.strategy, transforms=transforms,
                         optimized_sql=opt_sql, sql_chars=len(opt_sql))

        if opt_sql.strip() != original_sql.strip():
            vlog = validate_and_explain(DB_PATH, opt_sql, orig_time, orig_rows)
            wlog.status, wlog.speedup, wlog.time_ms = vlog.status, vlog.speedup, vlog.time_ms
            wlog.vital_signs, wlog.explain_text = vlog.vital_signs, vlog.explain_text
            wlog.error = vlog.error
        else:
            wlog.status, wlog.speedup, wlog.time_ms = "NO_CHANGE", 1.0, orig_time

        new_workers.append(wlog)
        P(f"    → {wlog.status} {wlog.speedup:.2f}x | {wlog.time_ms:.0f}ms | {wlog.transforms}")

    # ── Coach Analysis ──────────────────────────────────────────────────
    P("\n  --- COACH ANALYSIS ---")

    new_worker_results = [
        WorkerResult(
            worker_id=w.worker_id, strategy=w.strategy,
            examples_used=[], optimized_sql=w.optimized_sql,
            speedup=w.speedup, status=w.status, transforms=w.transforms,
            error_message=w.error or None,
        )
        for w in new_workers
    ]
    vital_signs = {
        w.worker_id: w.vital_signs
        for w in new_workers if w.vital_signs
    }

    coach_prompt = build_coach_prompt(
        original_sql=original_sql,
        worker_results=new_worker_results,
        vital_signs=vital_signs,
        race_timings=None,
        engine_profile=ctx["engine_profile"],
        dialect=DIALECT,
        target_speedup=2.0,
    )

    P(f"  Coach prompt: {len(coach_prompt)} chars")
    coach_response, coach_log = make_llm_call(client, coach_prompt, "COACH")
    new_logs.append(coach_log)
    all_logs.append(coach_log)
    print_log(coach_log)

    # ── Round 2: Refined workers via coach ──────────────────────────────
    P("\n  --- ROUND 2: COACH-REFINED WORKERS ---")

    race_summary = "\n".join(
        f"W{w.worker_id} ({w.strategy}): {w.speedup:.2f}x {w.status}"
        + (f" — {w.vital_signs.split(chr(10))[0]}" if w.vital_signs else "")
        for w in sorted(new_workers, key=lambda w: w.speedup, reverse=True)
    )

    refinement_prefix = build_coach_refinement_prefix(
        round1_prefix=shared_prefix,
        coach_directives=coach_response,
        round_results_summary=race_summary,
    )
    P(f"  Refinement prefix: {len(refinement_prefix)} chars (round1={len(shared_prefix)} + coach={len(refinement_prefix)-len(shared_prefix)})")

    coach_workers: List[WorkerLog] = []
    for wid in [1, 2, 3, 4]:
        suffix = build_worker_assignment(wid)
        prompt = refinement_prefix + "\n\n" + suffix
        strategy_name = next(
            (wb.strategy for wb in briefing.workers if wb.worker_id == wid), f"w{wid}"
        )

        P(f"\n  NEW-R2-W{wid} [{strategy_name}]: {len(prompt)} chars")
        response, log = make_llm_call(client, prompt, f"NEW-R2-W{wid}")
        new_logs.append(log)
        all_logs.append(log)
        print_log(log)

        try:
            opt_sql, transforms = parse_rewrite(original_sql, response)
        except Exception as e:
            opt_sql, transforms = original_sql, []
            P(f"    PARSE ERROR: {e}")

        wlog = WorkerLog(worker_id=wid, strategy=f"coach_{strategy_name}",
                         transforms=transforms, optimized_sql=opt_sql, sql_chars=len(opt_sql))

        if opt_sql.strip() != original_sql.strip():
            vlog = validate_and_explain(DB_PATH, opt_sql, orig_time, orig_rows)
            wlog.status, wlog.speedup, wlog.time_ms = vlog.status, vlog.speedup, vlog.time_ms
            wlog.vital_signs, wlog.explain_text = vlog.vital_signs, vlog.explain_text
            wlog.error = vlog.error
        else:
            wlog.status, wlog.speedup, wlog.time_ms = "NO_CHANGE", 1.0, orig_time

        coach_workers.append(wlog)
        P(f"    → {wlog.status} {wlog.speedup:.2f}x | {wlog.time_ms:.0f}ms | {wlog.transforms}")

    # ═══════════════════════════════════════════════════════════════════
    # COMPARISON REPORT
    # ═══════════════════════════════════════════════════════════════════
    P("\n" + "=" * 80)
    P("COMPARISON REPORT")
    P("=" * 80)

    # ── LLM Call Log ────────────────────────────────────────────────────
    P("\n--- ALL LLM CALLS ---")
    P(f"{'Call':<12} {'Prompt':>8} {'CacheHit':>9} {'CacheMiss':>10} {'Comp':>8} {'Time':>7} {'Cost':>10}")
    P("-" * 70)
    for log in all_logs:
        P(f"{log.label:<12} {log.prompt_tokens:>8,} {log.cache_hit_tokens:>9,} {log.cache_miss_tokens:>10,} "
          f"{log.completion_tokens:>8,} {log.duration_s:>6.1f}s ${log.cost_usd:>8.4f}")

    # ── Cost Summary ────────────────────────────────────────────────────
    old_fanout_cost = sum(l.cost_usd for l in old_logs[:4])
    old_snipe_cost = old_logs[4].cost_usd if len(old_logs) > 4 else 0
    old_total = analyst_log.cost_usd + sum(l.cost_usd for l in old_logs)

    new_r1_cost = sum(l.cost_usd for l in new_logs[:4])
    new_coach_cost = new_logs[4].cost_usd if len(new_logs) > 4 else 0
    new_r2_cost = sum(l.cost_usd for l in new_logs[5:9])
    new_total = analyst_log.cost_usd + sum(l.cost_usd for l in new_logs)

    P("\n--- COST BREAKDOWN ---")
    P(f"{'Phase':<28} {'OLD ($)':>10} {'NEW ($)':>10}")
    P("-" * 50)
    P(f"{'Analyst (shared)':<28} ${analyst_log.cost_usd:>8.4f}  ${analyst_log.cost_usd:>8.4f}")
    P(f"{'Fan-out (4 workers)':<28} ${old_fanout_cost:>8.4f}  ${new_r1_cost:>8.4f}")
    P(f"{'Retry: snipe/coach call':<28} ${old_snipe_cost:>8.4f}  ${new_coach_cost:>8.4f}")
    P(f"{'Retry: 4 refined workers':<28} ${'—':>8s}  ${new_r2_cost:>8.4f}")
    P(f"{'-'*50}")
    P(f"{'TOTAL':<28} ${old_total:>8.4f}  ${new_total:>8.4f}")
    if old_total > 0:
        diff = new_total - old_total
        sign = "+" if diff > 0 else ""
        P(f"{'Difference':<28} {'':>10s}  {sign}${diff:.4f} ({sign}{diff/old_total*100:.1f}%)")

    # ── Cache Efficiency ────────────────────────────────────────────────
    P("\n--- CACHE EFFICIENCY ---")
    old_total_cache_hit = sum(l.cache_hit_tokens for l in old_logs)
    new_r1_hits = [l.cache_hit_tokens for l in new_logs[:4]]
    new_r2_hits = [l.cache_hit_tokens for l in new_logs[5:9]]

    P(f"OLD path total cache hits: {old_total_cache_hit:,}")
    P(f"NEW R1 cache hits: W1={new_r1_hits[0]:,}, W2={new_r1_hits[1] if len(new_r1_hits)>1 else 0:,}, "
      f"W3={new_r1_hits[2] if len(new_r1_hits)>2 else 0:,}, W4={new_r1_hits[3] if len(new_r1_hits)>3 else 0:,}")
    if new_r2_hits:
        P(f"NEW R2 cache hits: W1={new_r2_hits[0]:,}, W2={new_r2_hits[1] if len(new_r2_hits)>1 else 0:,}, "
          f"W3={new_r2_hits[2] if len(new_r2_hits)>2 else 0:,}, W4={new_r2_hits[3] if len(new_r2_hits)>3 else 0:,}")

    avg_new_r1_hit = sum(new_r1_hits[1:]) / max(len(new_r1_hits[1:]), 1)
    P(f"\nR1 W2-4 avg cache hits: {avg_new_r1_hit:.0f} tokens {'(CACHING WORKING)' if avg_new_r1_hit > 0 else '(NO CACHE)'}")

    # ── Optimization Results ────────────────────────────────────────────
    P("\n--- OPTIMIZATION RESULTS ---")
    P(f"Original: {orig_time:.0f}ms | {orig_rows} rows")
    P()

    P(f"{'Worker':<20} {'Strategy':<26} {'Speedup':>8} {'Status':<10} {'Time(ms)':>10} {'Transforms'}")
    P("-" * 100)
    P("OLD Fan-out:")
    for w in old_workers[:4]:
        tfm = ", ".join(w.transforms[:2]) if w.transforms else "—"
        P(f"  W{w.worker_id:<17d} {w.strategy:<26} {w.speedup:>6.2f}x  {w.status:<10} {w.time_ms:>8.0f}   {tfm}")
    P("OLD Snipe:")
    w = old_workers[4] if len(old_workers) > 4 else None
    if w:
        tfm = ", ".join(w.transforms[:2]) if w.transforms else "—"
        P(f"  Snipe{'':<14} {w.strategy:<26} {w.speedup:>6.2f}x  {w.status:<10} {w.time_ms:>8.0f}   {tfm}")
    P()

    P("NEW Fan-out (Round 1):")
    for w in new_workers:
        tfm = ", ".join(w.transforms[:2]) if w.transforms else "—"
        P(f"  W{w.worker_id:<17d} {w.strategy:<26} {w.speedup:>6.2f}x  {w.status:<10} {w.time_ms:>8.0f}   {tfm}")
    P("NEW Coach-Refined (Round 2):")
    for w in coach_workers:
        tfm = ", ".join(w.transforms[:2]) if w.transforms else "—"
        P(f"  W{w.worker_id:<17d} {w.strategy:<26} {w.speedup:>6.2f}x  {w.status:<10} {w.time_ms:>8.0f}   {tfm}")

    # ── Best Results ────────────────────────────────────────────────────
    all_old = old_workers
    all_new = new_workers + coach_workers
    best_old = max(all_old, key=lambda w: w.speedup)
    best_new = max(all_new, key=lambda w: w.speedup)

    P(f"\n{'='*50}")
    P(f"BEST OLD: W{best_old.worker_id} ({best_old.strategy}) → {best_old.speedup:.2f}x {best_old.status}")
    P(f"BEST NEW: W{best_new.worker_id} ({best_new.strategy}) → {best_new.speedup:.2f}x {best_new.status}")
    P(f"{'='*50}")

    # ── EXPLAIN Vital Signs (detailed) ──────────────────────────────────
    P("\n--- EXPLAIN VITAL SIGNS ---")
    P(f"Original: {orig_vital}")
    for w in (new_workers + coach_workers):
        if w.vital_signs and w.status not in ("NO_CHANGE",):
            P(f"W{w.worker_id} ({w.strategy}) [{w.status} {w.speedup:.2f}x]: {w.vital_signs.split(chr(10))[0]}")

    # ── LLM Call Count ──────────────────────────────────────────────────
    old_calls = 1 + 4 + 1  # analyst + 4 workers + snipe
    new_calls = 1 + 4 + 1 + 4  # analyst + 4 workers + coach + 4 refined
    P(f"\n--- LLM CALL COUNT ---")
    P(f"OLD path: {old_calls} calls (1 analyst + 4 workers + 1 snipe)")
    P(f"NEW path: {new_calls} calls (1 analyst + 4 workers + 1 coach + 4 refined)")

    # ── Save JSON ───────────────────────────────────────────────────────
    results = {
        "query_id": QUERY_ID,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "original": {"time_ms": orig_time, "rows": orig_rows, "vital_signs": orig_vital},
        "analyst": {"prompt_tokens": analyst_log.prompt_tokens, "completion_tokens": analyst_log.completion_tokens,
                     "cache_hit": analyst_log.cache_hit_tokens, "cost_usd": analyst_log.cost_usd, "duration_s": analyst_log.duration_s},
        "old_path": {
            "total_cost": old_total,
            "calls": [{"label": l.label, "prompt_tok": l.prompt_tokens, "comp_tok": l.completion_tokens,
                        "cache_hit": l.cache_hit_tokens, "cache_miss": l.cache_miss_tokens,
                        "cost": l.cost_usd, "duration": l.duration_s} for l in old_logs],
            "workers": [{"wid": w.worker_id, "strategy": w.strategy, "speedup": w.speedup,
                         "status": w.status, "time_ms": w.time_ms, "transforms": w.transforms,
                         "vital_signs": w.vital_signs} for w in old_workers],
            "best_speedup": best_old.speedup,
        },
        "new_path": {
            "total_cost": new_total,
            "shared_prefix_chars": len(shared_prefix),
            "calls": [{"label": l.label, "prompt_tok": l.prompt_tokens, "comp_tok": l.completion_tokens,
                        "cache_hit": l.cache_hit_tokens, "cache_miss": l.cache_miss_tokens,
                        "cost": l.cost_usd, "duration": l.duration_s} for l in new_logs],
            "r1_workers": [{"wid": w.worker_id, "strategy": w.strategy, "speedup": w.speedup,
                            "status": w.status, "time_ms": w.time_ms, "transforms": w.transforms,
                            "vital_signs": w.vital_signs} for w in new_workers],
            "r2_workers": [{"wid": w.worker_id, "strategy": w.strategy, "speedup": w.speedup,
                            "status": w.status, "time_ms": w.time_ms, "transforms": w.transforms,
                            "vital_signs": w.vital_signs} for w in coach_workers],
            "best_speedup": best_new.speedup,
        },
    }

    out_path = OUTPUT_DIR / f"comparison_{QUERY_ID}_{int(time.time())}.json"
    out_path.write_text(json.dumps(results, indent=2, default=str))
    P(f"\nFull results saved: {out_path}")
    P("DONE")


if __name__ == "__main__":
    main()
