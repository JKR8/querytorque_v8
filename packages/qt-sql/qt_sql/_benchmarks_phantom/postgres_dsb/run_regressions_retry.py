#!/usr/bin/env python3
"""Retry 4 PG DSB regressions with EXPLAIN cost gate + 5x trimmed mean.

Flow per query (all 4 queries run in PARALLEL):
  1. LLM generation only: analyst + 4 workers (no execution)
  2. EXPLAIN cost gate on SF10: reject cost-increasing candidates
  3. 5x trimmed mean on SF10: only on the best cost-gated candidate

Targets:
  query085_agg     0.88x regression
  query038_multi   0.77x regression
  query031_multi   0.74x regression
  query085_spj_spj 0.49x regression

Usage (from project root):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 \
        packages/qt-sql/ado/benchmarks/postgres_dsb/run_regressions_retry.py
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(".env")

import psycopg2

BENCHMARK_DIR = Path("packages/qt-sql/ado/benchmarks/postgres_dsb")
QUERIES_DIR = BENCHMARK_DIR / "queries"
SESSION_DIR = BENCHMARK_DIR / "swarm_sessions"
DSN_SF10 = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"

QUERIES = [
    ("query085_agg", 0.88),
    ("query038_multi", 0.77),
    ("query031_multi", 0.74),
    ("query085_spj_spj", 0.49),
]


# ── Helpers ──────────────────────────────────────────────────────────

def fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"



def run_5x(dsn: str, sql: str, label: str, timeout_ms: int = 300_000) -> list[float]:
    """Run a query 5 times, return all timings."""
    con = psycopg2.connect(dsn)
    con.autocommit = True
    cur = con.cursor()
    times = []
    for i in range(5):
        try:
            cur.execute(f"SET statement_timeout = {timeout_ms}")
            t0 = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            elapsed = (time.perf_counter() - t0) * 1000
            times.append(elapsed)
            print(f"    [{label}] Run {i+1}: {elapsed:.1f}ms", flush=True)
            cur.execute("SET statement_timeout = 0")
        except Exception as e:
            try:
                cur.execute("ROLLBACK")
                cur.execute("SET statement_timeout = 0")
            except Exception:
                con = psycopg2.connect(dsn)
                con.autocommit = True
                cur = con.cursor()
            times.append(float("inf"))
            print(f"    [{label}] Run {i+1}: TIMEOUT/ERROR ({e})", flush=True)
    con.close()
    return times


def trimmed_mean(times: list[float]) -> float:
    """5x trimmed mean: remove min/max, average remaining 3."""
    finite = [t for t in times if t != float("inf")]
    if len(finite) < 3:
        return float("inf")
    finite.sort()
    return sum(finite[1:-1]) / (len(finite) - 2)


# ── Phase 1: LLM Generation (no execution) ─────────────────────────

def generate_candidates(query_id: str, sql: str) -> list[dict]:
    """Run analyst + 4 workers, return SQL candidates without executing.

    Uses the Pipeline's generation components directly. Saves prompts
    and results to swarm_sessions/ for debugging.
    """
    from ado.pipeline import Pipeline
    from ado.generate import CandidateGenerator
    from ado.prompts import build_fan_out_prompt, parse_fan_out_response

    pipeline = Pipeline(str(BENCHMARK_DIR))
    dialect = "postgres"
    engine = "postgresql"

    print(f"  [{query_id}] PARSE: Building logical tree...", flush=True)
    dag, costs, _explain = pipeline._parse_logical_tree(sql, dialect=dialect, query_id=query_id)
    print(f"  [{query_id}] PARSE: done", flush=True)

    # Gather data
    matched_examples = pipeline._find_examples(sql, engine=engine, k=20)
    all_available = pipeline._list_gold_examples(engine)
    regression_warnings = pipeline._find_regression_warnings(sql, engine=engine, k=3)
    print(f"  [{query_id}] EXAMPLES: {len(matched_examples)} matched", flush=True)

    # Build analyst prompt
    fan_out_prompt = build_fan_out_prompt(
        query_id=query_id,
        sql=sql,
        dag=dag,
        costs=costs,
        matched_examples=matched_examples,
        all_available_examples=all_available,
        regression_warnings=regression_warnings,
        dialect=dialect,
    )

    # Analyst call
    print(f"  [{query_id}] ANALYST: Distributing strategies...", flush=True)
    t0 = time.time()
    generator = CandidateGenerator(
        provider=pipeline.provider,
        model=pipeline.model,
        analyze_fn=pipeline.analyze_fn,
    )

    analyst_response = generator._analyze(fan_out_prompt)
    assignments = parse_fan_out_response(analyst_response)
    print(f"  [{query_id}] ANALYST: {len(assignments)} workers ({fmt(time.time() - t0)})", flush=True)
    for a in assignments:
        print(f"    [{query_id}] W{a.worker_id}: {a.strategy}", flush=True)

    # Generate 4 workers in parallel
    print(f"  [{query_id}] GENERATE: 4 workers in parallel...", flush=True)
    t_gen = time.time()
    global_learnings = pipeline.learner.build_learning_summary() or None

    candidates = []

    def gen_worker(assignment):
        examples = pipeline._load_examples_by_id(assignment.examples, engine)
        from ado.prompts import build_worker_strategy_header

        base_prompt = pipeline.prompter.build_prompt(
            query_id=f"{query_id}_w{assignment.worker_id}",
            full_sql=sql,
            dag=dag,
            costs=costs,
            history=None,
            examples=examples,
            expert_analysis=None,
            global_learnings=global_learnings,
            regression_warnings=regression_warnings,
            dialect=dialect,
            semantic_intents=pipeline.get_semantic_intents(query_id),
            engine_version=pipeline._engine_version,
        )
        prompt = build_worker_strategy_header(assignment.strategy, assignment.hint) + base_prompt

        example_ids = [e.get("id", "?") for e in examples]
        candidate = generator.generate_one(
            sql=sql,
            prompt=prompt,
            examples_used=example_ids,
            worker_id=assignment.worker_id,
            dialect=dialect,
        )

        # Syntax check
        optimized_sql = candidate.optimized_sql
        try:
            import sqlglot
            sqlglot.parse_one(optimized_sql, dialect="postgres")
        except Exception:
            optimized_sql = None  # Mark as broken

        return {
            "worker_id": assignment.worker_id,
            "strategy": assignment.strategy,
            "transforms": candidate.transforms,
            "sql": optimized_sql,
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(gen_worker, a): a for a in assignments}
        for future in as_completed(futures):
            a = futures[future]
            try:
                result = future.result()
                candidates.append(result)
                marker = "OK" if result["sql"] else "SYNTAX_ERROR"
                print(f"    [{query_id}] W{result['worker_id']} ({result['strategy']}): "
                      f"{marker} ({fmt(time.time() - t_gen)})", flush=True)
            except Exception as e:
                print(f"    [{query_id}] W{a.worker_id} FAILED: {e}", flush=True)
                candidates.append({
                    "worker_id": a.worker_id,
                    "strategy": a.strategy,
                    "transforms": [],
                    "sql": None,
                })

    # Save to disk
    out_dir = SESSION_DIR / query_id / "regression_retry"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "analyst_prompt.txt").write_text(fan_out_prompt)
    (out_dir / "analyst_response.txt").write_text(analyst_response)
    for c in candidates:
        wdir = out_dir / f"worker_{c['worker_id']:02d}"
        wdir.mkdir(parents=True, exist_ok=True)
        if c["sql"]:
            (wdir / "optimized.sql").write_text(c["sql"])
        (wdir / "result.json").write_text(json.dumps({
            "worker_id": c["worker_id"],
            "strategy": c["strategy"],
            "transforms": c["transforms"],
        }, indent=2))

    print(f"  [{query_id}] GENERATE: done ({fmt(time.time() - t_gen)})", flush=True)
    return candidates


# ── Phase 2: Quick 2x runtime screen ─────────────────────────────────
# EXPLAIN cost gates are unreliable (CTE short-circuit, OR-join fan-out).
# Instead, run each candidate twice (discard warmup, keep 2nd) and pick best.

def runtime_screen_2x(query_id: str, original_sql: str, candidates: list[dict]) -> dict | None:
    """Quick 2x runtime screen: run twice (discard warmup), pick fastest."""
    print(f"  [{query_id}] 2x RUNTIME SCREEN (SF10)...", flush=True)

    def run_2x(dsn, sql, label):
        """Run twice, return 2nd run time (1st is warmup)."""
        try:
            con = psycopg2.connect(dsn)
            con.autocommit = True
            cur = con.cursor()
            cur.execute(f"SET statement_timeout = 120000")  # 2 min timeout
            # warmup
            t0 = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            warmup = (time.perf_counter() - t0) * 1000
            # real run
            t0 = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            real = (time.perf_counter() - t0) * 1000
            cur.execute("SET statement_timeout = 0")
            con.close()
            print(f"    [{label}] warmup={warmup:.0f}ms run={real:.0f}ms", flush=True)
            return real
        except Exception as e:
            print(f"    [{label}] ERROR: {e}", flush=True)
            return float("inf")

    # Baseline
    orig_ms = run_2x(DSN_SF10, original_sql, f"{query_id}/orig")
    print(f"    [{query_id}] Original: {orig_ms:.0f}ms", flush=True)

    best = None
    best_speedup = 0.0

    for c in candidates:
        if not c["sql"]:
            print(f"    [{query_id}] W{c['worker_id']}: SKIP (no SQL)", flush=True)
            continue

        opt_ms = run_2x(DSN_SF10, c["sql"], f"{query_id}/W{c['worker_id']}")
        speedup = orig_ms / opt_ms if opt_ms > 0 and opt_ms != float("inf") else 0.0
        c["screen_ms"] = opt_ms
        c["screen_speedup"] = speedup
        marker = "*" if speedup > 1.0 else " "
        print(f"   {marker}[{query_id}] W{c['worker_id']} ({c['strategy']}): "
              f"{opt_ms:.0f}ms ({speedup:.2f}x)", flush=True)

        if speedup > best_speedup:
            best_speedup = speedup
            best = c

    if best and best_speedup >= 0.9:  # not an obvious regression
        print(f"    [{query_id}] WINNER: W{best['worker_id']} ({best_speedup:.2f}x screen)", flush=True)
        return best
    elif best:
        print(f"    [{query_id}] Best W{best['worker_id']} screened at {best_speedup:.2f}x "
              f"(regression) — validating anyway", flush=True)
        return best
    else:
        print(f"    [{query_id}] No valid candidates!", flush=True)
        return None


# ── Phase 3: 5x trimmed mean ───────────────────────────────────────

def validate_5x(query_id: str, original_sql: str, winner: dict) -> dict:
    """5x trimmed mean validation on SF10 for the winner only."""
    print(f"  [{query_id}] 5x VALIDATION (SF10)...", flush=True)

    # Original baseline
    print(f"    [{query_id}] Baseline 5x:", flush=True)
    orig_times = run_5x(DSN_SF10, original_sql, f"{query_id}/orig")
    orig_tm = trimmed_mean(orig_times)
    print(f"    [{query_id}] Baseline trimmed mean: {orig_tm:.1f}ms", flush=True)

    # Winner
    print(f"    [{query_id}] W{winner['worker_id']} 5x:", flush=True)
    opt_times = run_5x(DSN_SF10, winner["sql"], f"{query_id}/W{winner['worker_id']}")
    opt_tm = trimmed_mean(opt_times)
    print(f"    [{query_id}] Optimized trimmed mean: {opt_tm:.1f}ms", flush=True)

    speedup = orig_tm / opt_tm if opt_tm > 0 and opt_tm != float("inf") else 0.0
    if speedup >= 1.1:
        status = "WIN"
    elif speedup >= 0.95:
        status = "PASS"
    else:
        status = "REGRESSION"

    print(f"  [{query_id}] RESULT: {status} {speedup:.2f}x "
          f"({orig_tm:.0f}ms -> {opt_tm:.0f}ms)", flush=True)

    return {
        "query_id": query_id,
        "worker_id": winner["worker_id"],
        "strategy": winner["strategy"],
        "transforms": winner["transforms"],
        "status": status,
        "speedup": round(speedup, 3),
        "original_trimmed_mean_ms": round(orig_tm, 1),
        "optimized_trimmed_mean_ms": round(opt_tm, 1),
        "original_times_ms": [round(t, 1) if t != float("inf") else "TIMEOUT" for t in orig_times],
        "optimized_times_ms": [round(t, 1) if t != float("inf") else "TIMEOUT" for t in opt_times],
        "explain_cost": winner.get("explain_cost"),
        "cost_ratio": winner.get("cost_ratio"),
    }


# ── Per-query pipeline ──────────────────────────────────────────────

def process_query(query_id: str, prev_speedup: float) -> dict:
    """Full pipeline for one query: generate -> 2x runtime screen -> 5x validate."""
    t0 = time.time()
    sql_path = QUERIES_DIR / f"{query_id}.sql"
    if not sql_path.exists():
        return {"query_id": query_id, "status": "SKIP", "error": "file not found"}

    original_sql = sql_path.read_text()

    print(f"\n{'='*60}", flush=True)
    print(f"  {query_id} (was {prev_speedup:.2f}x)", flush=True)
    print(f"{'='*60}", flush=True)

    try:
        # Phase 1: LLM generation
        candidates = generate_candidates(query_id, original_sql)

        # Phase 2: Quick 2x runtime screen
        winner = runtime_screen_2x(query_id, original_sql, candidates)
        if not winner:
            return {
                "query_id": query_id,
                "status": "NO_CANDIDATES",
                "elapsed_s": round(time.time() - t0, 1),
            }

        # Save screen checkpoint
        out_dir = SESSION_DIR / query_id / "regression_retry"
        (out_dir / "checkpoint_screen.json").write_text(json.dumps({
            "winner_worker_id": winner["worker_id"],
            "candidates": [{
                "worker_id": c["worker_id"],
                "strategy": c["strategy"],
                "screen_ms": c.get("screen_ms"),
                "screen_speedup": c.get("screen_speedup"),
            } for c in candidates],
        }, indent=2, default=str))

        # Phase 3: 5x trimmed mean on winner only
        result = validate_5x(query_id, original_sql, winner)
        result["previous_speedup"] = prev_speedup
        result["elapsed_s"] = round(time.time() - t0, 1)

        # Save final result
        (out_dir / "validation_result.json").write_text(json.dumps(result, indent=2, default=str))

        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "query_id": query_id,
            "status": "CRASH",
            "error": str(e),
            "elapsed_s": round(time.time() - t0, 1),
        }


# ── Main ────────────────────────────────────────────────────────────

def main():
    t_batch = time.time()

    print(f"\n{'#'*60}", flush=True)
    print(f"  REGRESSION RETRY: {len(QUERIES)} PG DSB queries", flush=True)
    print(f"  Method: LLM gen -> 2x runtime screen -> 5x trimmed mean (winner only)", flush=True)
    print(f"  All {len(QUERIES)} queries in PARALLEL", flush=True)
    print(f"{'#'*60}\n", flush=True)

    results = []

    with ThreadPoolExecutor(max_workers=len(QUERIES)) as pool:
        futures = {
            pool.submit(process_query, qid, prev): qid
            for qid, prev in QUERIES
        }
        for future in as_completed(futures):
            qid = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append({"query_id": qid, "status": "CRASH", "error": str(e)})

    # Sort by query name for readable output
    results.sort(key=lambda r: r.get("query_id", ""))

    # Summary table
    total_elapsed = time.time() - t_batch
    print(f"\n\n{'#'*60}", flush=True)
    print(f"  REGRESSION RETRY COMPLETE — {fmt(total_elapsed)}", flush=True)
    print(f"{'#'*60}\n", flush=True)

    print(f"  {'Query':<25} {'Prev':>6} {'New':>8} {'Status':<12} {'Time':>8}", flush=True)
    print(f"  {'─'*25} {'─'*6} {'─'*8} {'─'*12} {'─'*8}", flush=True)

    improved = 0
    for r in results:
        qid = r.get("query_id", "?")
        prev = r.get("previous_speedup", 0)
        speedup = r.get("speedup", 0)
        status = r.get("status", "?")
        elapsed = r.get("elapsed_s", 0)

        marker = "*" if speedup and speedup > prev else " "
        prev_str = f"{prev:.2f}x" if prev else "—"
        new_str = f"{speedup:.2f}x" if speedup else "—"
        print(f" {marker}{qid:<25} {prev_str:>6} {new_str:>8} {status:<12} {fmt(elapsed):>8}", flush=True)
        if speedup and speedup > prev:
            improved += 1

    print(f"\n  Improved over previous: {improved}/{len(results)}", flush=True)
    print(f"  Total time: {fmt(total_elapsed)}", flush=True)

    # Save results
    out_path = BENCHMARK_DIR / "regression_retry_results.json"
    out_path.write_text(json.dumps({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "method": "2x_runtime_screen_5x_trimmed_mean_sf10",
        "total_elapsed_s": round(total_elapsed, 1),
        "n_queries": len(results),
        "n_improved": improved,
        "results": results,
    }, indent=2, default=str))
    print(f"\n  Results saved: {out_path}", flush=True)


if __name__ == "__main__":
    main()
