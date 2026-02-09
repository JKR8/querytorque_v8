#!/usr/bin/env python3
"""Test LLM-driven per-query PG tuning on real DSB queries.

Calls the LLM with our tuner prompt, validates the response,
then benchmarks the recommended config with 4x triage.

Tests 4 diverse queries:
  - Q059 (40s): Sort spills 39MB to disk, 96 JIT functions
  - Q080 (config winner 2.39x from blanket): large scan + parallel
  - Q069 (blanket REGRESSION 0.52x): needs targeted, not blanket
  - Q085 (I/O bound): bitmap scans, minimal spills

Run:
  cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.tests.test_pg_tuner_llm
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

from qt_sql.pg_tuning import validate_tuning_config, build_set_local_sql
from qt_sql.prompts.pg_tuner import build_pg_tuner_prompt
from qt_sql.execution.postgres_executor import PostgresExecutor

PG_CONN = {
    "host": "127.0.0.1",
    "port": 5433,
    "database": "dsb_sf10",
    "user": "jakc9",
    "password": "jakc9",
}

QUERY_DIR = PROJECT_ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/queries"
EXPLAIN_DIR = PROJECT_ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/explains/sf10"
ENGINE_PROFILE_PATH = PROJECT_ROOT / "packages/qt-sql/ado/constraints/engine_profile_postgresql.json"

# Queries to test — diverse bottleneck profiles
TEST_QUERIES = [
    "query059_multi",   # Sort spills, JIT overhead
    "query080_multi",   # Blanket config winner (2.39x)
    "query069_multi",   # Blanket config REGRESSION (0.52x)
    "query085_agg",     # I/O bound, bitmap scans
]

TIMEOUT_MS = 120_000


def load_query(qid: str) -> str:
    return (QUERY_DIR / f"{qid}.sql").read_text().strip()


def load_plan_json(qid: str) -> dict:
    path = EXPLAIN_DIR / f"{qid}.json"
    if path.exists():
        data = json.loads(path.read_text())
        return data.get("plan_json", {})
    return {}


def triage_measure(
    executor: PostgresExecutor,
    sql: str,
    config_cmds: list[str] | None = None,
) -> tuple[float, int]:
    """4x triage: warmup orig, warmup config, measure orig, measure config.

    If config_cmds is None, just does 3-run on plain SQL.
    Returns (avg_ms, row_count).
    """
    if config_cmds:
        # Warmup plain
        executor.execute(sql, timeout_ms=TIMEOUT_MS)
        # Warmup config
        executor.execute_with_config(sql, config_cmds, timeout_ms=TIMEOUT_MS)
        # Measure plain (for interleaving)
        t0 = time.perf_counter()
        executor.execute(sql, timeout_ms=TIMEOUT_MS)
        t_plain = (time.perf_counter() - t0) * 1000
        # Measure config
        t0 = time.perf_counter()
        rows = executor.execute_with_config(sql, config_cmds, timeout_ms=TIMEOUT_MS)
        t_config = (time.perf_counter() - t0) * 1000
        return t_config, len(rows)
    else:
        # 3-run: warmup + 2 measures
        executor.execute(sql, timeout_ms=TIMEOUT_MS)
        t0 = time.perf_counter()
        rows = executor.execute(sql, timeout_ms=TIMEOUT_MS)
        t1 = (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        executor.execute(sql, timeout_ms=TIMEOUT_MS)
        t2 = (time.perf_counter() - t0) * 1000
        return (t1 + t2) / 2, len(rows)


def parse_llm_response(response: str) -> dict:
    """Extract JSON from LLM response (may have markdown fences)."""
    text = response.strip()
    # Strip markdown fences
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()
    # Try to find JSON object
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        text = text[start:end]
    return json.loads(text)


def main():
    print()
    print("=" * 80)
    print("  LLM-DRIVEN PER-QUERY PG TUNING TEST")
    print("=" * 80)

    # Load engine profile
    engine_profile = {}
    if ENGINE_PROFILE_PATH.exists():
        engine_profile = json.loads(ENGINE_PROFILE_PATH.read_text())

    # Create LLM client
    from qt_shared.llm import create_llm_client
    from qt_shared.config import get_settings
    settings = get_settings()
    print(f"\n  LLM: {settings.llm_provider} / {settings.llm_model}")

    client = create_llm_client(
        provider=settings.llm_provider,
        model=settings.llm_model,
    )

    # Connect to PG
    executor = PostgresExecutor(**PG_CONN)
    executor.connect()
    pg_settings = executor.get_settings()
    print(f"  PG settings: {len(pg_settings)} params loaded")
    print()

    results = {}

    for qid in TEST_QUERIES:
        print("-" * 80)
        print(f"  QUERY: {qid}")
        print("-" * 80)

        sql = load_query(qid)
        plan_json = load_plan_json(qid)

        if not plan_json:
            print(f"  SKIP: No EXPLAIN plan for {qid}")
            continue

        # Get baseline from the explain file
        explain_data = json.loads((EXPLAIN_DIR / f"{qid}.json").read_text())
        baseline_ms = explain_data.get("execution_time_ms", 0)

        # Build prompt
        prompt = build_pg_tuner_prompt(
            query_sql=sql,
            plan_json=plan_json,
            current_settings=pg_settings,
            engine_profile=engine_profile,
            baseline_ms=baseline_ms,
        )
        print(f"  Prompt: {len(prompt)} chars ({len(prompt.split())} words)")

        # Call LLM
        print(f"  Calling {settings.llm_provider}/{settings.llm_model}...")
        t0 = time.perf_counter()
        try:
            response = client.analyze(prompt)
        except Exception as e:
            print(f"  LLM ERROR: {e}")
            continue
        llm_time = time.perf_counter() - t0
        print(f"  LLM response: {len(response)} chars in {llm_time:.1f}s")
        print()

        # Parse response
        try:
            parsed = parse_llm_response(response)
            params = parsed.get("params", {})
            reasoning = parsed.get("reasoning", "")
        except Exception as e:
            print(f"  PARSE ERROR: {e}")
            print(f"  Raw response: {response[:500]}")
            continue

        # Validate against whitelist
        cleaned = validate_tuning_config(params)
        config_cmds = build_set_local_sql(cleaned)

        print(f"  LLM recommended {len(params)} params, {len(cleaned)} after validation:")
        for k, v in sorted(cleaned.items()):
            print(f"    {k} = {v}")
        print(f"  Reasoning: {reasoning[:200]}")
        print()

        if not config_cmds:
            print(f"  LLM says no config changes needed — skipping benchmark")
            results[qid] = {
                "params": {},
                "reasoning": reasoning,
                "config_speedup": 1.0,
                "verdict": "NO_CHANGE",
            }
            continue

        # Benchmark: 4x triage
        print(f"  Benchmarking (4x triage)...")
        try:
            t_baseline, rc_base = triage_measure(executor, sql)
            t_config, rc_config = triage_measure(executor, sql, config_cmds)
            rows_match = rc_base == rc_config
            speedup = t_baseline / t_config if t_config > 0 else 1.0

            tag = ""
            if speedup >= 1.10:
                tag = "WIN"
            elif speedup < 0.95:
                tag = "REGRESSION"
            else:
                tag = "NEUTRAL"

            print(f"  Baseline:  {t_baseline:.1f}ms ({rc_base} rows)")
            print(f"  Config:    {t_config:.1f}ms ({rc_config} rows)")
            print(f"  Speedup:   {speedup:.2f}x  [{tag}]")
            print(f"  Rows match: {'OK' if rows_match else 'FAIL'}")

            results[qid] = {
                "params": cleaned,
                "reasoning": reasoning,
                "config_cmds": config_cmds,
                "baseline_ms": round(t_baseline, 1),
                "config_ms": round(t_config, 1),
                "config_speedup": round(speedup, 3),
                "rows_match": rows_match,
                "verdict": tag,
            }
        except Exception as e:
            print(f"  BENCHMARK ERROR: {e}")
            try:
                executor.rollback()
            except Exception:
                executor = PostgresExecutor(**PG_CONN)
                executor.connect()

        print()

    # Summary
    print("=" * 80)
    print("  SUMMARY")
    print("=" * 80)
    print(f"  {'Query':25s}  {'Speedup':>8s}  {'Verdict':>10s}  Params")
    print(f"  {'-'*25}  {'-'*8}  {'-'*10}  {'-'*30}")
    for qid, r in sorted(results.items()):
        params_str = ", ".join(f"{k}={v}" for k, v in sorted(r.get("params", {}).items()))
        print(f"  {qid:25s}  {r['config_speedup']:7.2f}x  {r['verdict']:>10s}  {params_str}")
    print("=" * 80)

    # Save
    out_path = PROJECT_ROOT / "research/pg_tuner_llm_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\n  Results saved to: {out_path}")

    executor.close()


if __name__ == "__main__":
    main()
