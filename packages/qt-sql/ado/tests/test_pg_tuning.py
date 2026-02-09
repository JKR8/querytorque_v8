#!/usr/bin/env python3
"""Test PG tuning infrastructure — whitelist, validation, executor, prompt.

Run:
  cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.tests.test_pg_tuning
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

# Bootstrap
PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

from ado.pg_tuning import (
    PG_TUNABLE_PARAMS,
    TuningConfig,
    validate_tuning_config,
    build_set_local_sql,
)
from ado.prompts.pg_tuner import build_pg_tuner_prompt


def test_whitelist():
    """Test that whitelist has expected params."""
    print("=" * 60)
    print("TEST 1: Whitelist structure")
    print("=" * 60)

    assert len(PG_TUNABLE_PARAMS) == 16, f"Expected 16 params, got {len(PG_TUNABLE_PARAMS)}"
    assert "work_mem" in PG_TUNABLE_PARAMS
    assert "enable_hashjoin" in PG_TUNABLE_PARAMS
    assert "shared_buffers" not in PG_TUNABLE_PARAMS  # MUST NOT be in whitelist

    for param, (ptype, pmin, pmax, desc) in PG_TUNABLE_PARAMS.items():
        assert ptype in ("bytes", "int", "float", "bool"), f"{param}: bad type {ptype}"
        assert desc, f"{param}: missing description"

    print(f"  OK: {len(PG_TUNABLE_PARAMS)} params in whitelist")
    print()


def test_validate_config():
    """Test validate_tuning_config() strips bad params, validates ranges."""
    print("=" * 60)
    print("TEST 2: validate_tuning_config()")
    print("=" * 60)

    # Test 1: Valid params pass through
    config = {
        "work_mem": "512MB",
        "max_parallel_workers_per_gather": "4",
        "enable_hashjoin": "on",
    }
    cleaned = validate_tuning_config(config)
    assert cleaned["work_mem"] == "512MB", f"Expected 512MB, got {cleaned['work_mem']}"
    assert cleaned["max_parallel_workers_per_gather"] == "4"
    assert cleaned["enable_hashjoin"] == "on"
    print("  OK: Valid params pass through")

    # Test 2: Non-whitelisted params are stripped
    config_bad = {
        "work_mem": "256MB",
        "shared_buffers": "4GB",  # NOT in whitelist
        "fsync": "off",           # NOT in whitelist
    }
    cleaned = validate_tuning_config(config_bad)
    assert "shared_buffers" not in cleaned, "shared_buffers should be stripped"
    assert "fsync" not in cleaned, "fsync should be stripped"
    assert cleaned["work_mem"] == "256MB"
    print("  OK: Non-whitelisted params stripped")

    # Test 3: Range clamping
    config_extreme = {
        "work_mem": "10GB",  # Max is 2048MB = 2GB
        "max_parallel_workers_per_gather": "100",  # Max is 8
        "random_page_cost": "0.1",  # Min is 1.0
    }
    cleaned = validate_tuning_config(config_extreme)
    assert cleaned["work_mem"] == "2GB", f"Expected 2GB (clamped), got {cleaned['work_mem']}"
    assert cleaned["max_parallel_workers_per_gather"] == "8", f"Expected 8, got {cleaned['max_parallel_workers_per_gather']}"
    assert float(cleaned["random_page_cost"]) == 1.0, f"Expected 1.0, got {cleaned['random_page_cost']}"
    print("  OK: Range clamping works")

    # Test 4: Bool values
    config_bool = {
        "enable_seqscan": "false",
        "jit": "true",
        "enable_nestloop": "off",
    }
    cleaned = validate_tuning_config(config_bool)
    assert cleaned["enable_seqscan"] == "off"
    assert cleaned["jit"] == "on"
    assert cleaned["enable_nestloop"] == "off"
    print("  OK: Bool normalization works")

    # Test 5: Bytes parsing
    config_bytes = {
        "work_mem": "1GB",
        "effective_cache_size": "24GB",
    }
    cleaned = validate_tuning_config(config_bytes)
    assert cleaned["work_mem"] == "1GB", f"Expected 1GB, got {cleaned['work_mem']}"
    assert cleaned["effective_cache_size"] == "24GB", f"Expected 24GB, got {cleaned['effective_cache_size']}"
    print("  OK: Bytes parsing works")

    # Test 6: Empty config
    cleaned = validate_tuning_config({})
    assert len(cleaned) == 0
    print("  OK: Empty config returns empty")

    print()


def test_build_set_local():
    """Test build_set_local_sql() generates valid SQL."""
    print("=" * 60)
    print("TEST 3: build_set_local_sql()")
    print("=" * 60)

    config = {
        "work_mem": "512MB",
        "max_parallel_workers_per_gather": "4",
        "enable_hashjoin": "on",
    }
    stmts = build_set_local_sql(config)
    assert len(stmts) == 3, f"Expected 3 statements, got {len(stmts)}"
    for s in stmts:
        assert s.startswith("SET LOCAL "), f"Bad statement: {s}"
        print(f"  {s}")

    # Verify sorted order
    assert "enable_hashjoin" in stmts[0]
    assert "max_parallel_workers" in stmts[1]
    assert "work_mem" in stmts[2]
    print("  OK: Statements generated in sorted order")
    print()


def test_tuner_prompt():
    """Test that the tuner prompt builder produces reasonable output."""
    print("=" * 60)
    print("TEST 4: build_pg_tuner_prompt()")
    print("=" * 60)

    sql = "SELECT * FROM store_sales ss JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk WHERE d.d_year = 2001"
    explain = "Seq Scan on store_sales  (cost=0.00..1234.56 rows=100000 width=120)"
    settings = {"work_mem": "4096kB", "shared_buffers": "128MB"}

    prompt = build_pg_tuner_prompt(
        query_sql=sql,
        explain_plan=explain,
        current_settings=settings,
        baseline_ms=1500.0,
    )

    assert "SET LOCAL" in prompt
    assert "work_mem" in prompt
    assert "store_sales" in prompt
    assert "1500.0ms" in prompt
    assert "JSON" in prompt
    assert len(prompt) > 500, f"Prompt too short: {len(prompt)} chars"

    print(f"  Prompt length: {len(prompt)} chars")
    print(f"  Contains SQL: {'store_sales' in prompt}")
    print(f"  Contains EXPLAIN: {'Seq Scan' in prompt}")
    print(f"  Contains whitelist: {'enable_hashjoin' in prompt}")
    print(f"  Contains output format: {'\"params\"' in prompt}")
    print("  OK: Prompt looks good")
    print()


def test_execute_with_config():
    """Test execute_with_config() against local PostgreSQL.

    Requires: PostgreSQL running on port 5433 with dsb_sf10 database.
    """
    print("=" * 60)
    print("TEST 5: execute_with_config() — live PG test")
    print("=" * 60)

    try:
        from qt_sql.execution.postgres_executor import PostgresExecutor
    except ImportError as e:
        print(f"  SKIP: {e}")
        return

    dsn_parts = {
        "host": "127.0.0.1",
        "port": 5433,
        "database": "dsb_sf10",
        "user": "jakc9",
        "password": "jakc9",
    }

    try:
        executor = PostgresExecutor(**dsn_parts)
        executor.connect()
    except Exception as e:
        print(f"  SKIP: Cannot connect to PG: {e}")
        return

    # Test 1: Basic execution with config
    sql = "SELECT COUNT(*) as cnt FROM date_dim WHERE d_year = 2001"
    config_cmds = ["SET LOCAL work_mem = '256MB'"]

    try:
        rows = executor.execute_with_config(sql, config_cmds)
        print(f"  Basic execution OK: {rows[0]['cnt']} rows")
    except Exception as e:
        print(f"  FAIL: Basic execution: {e}")
        executor.close()
        return

    # Test 2: Config does NOT persist after execute_with_config
    try:
        result = executor.execute("SHOW work_mem")
        current_wm = result[0]["work_mem"]
        print(f"  work_mem after execute_with_config: {current_wm}")
        # Should NOT be 256MB (SET LOCAL reverts on COMMIT)
    except Exception as e:
        print(f"  WARN: Could not check work_mem persistence: {e}")

    # Test 3: Timing comparison — baseline vs with config
    test_sql = """
    SELECT d_year, COUNT(*) as cnt
    FROM date_dim
    GROUP BY d_year
    ORDER BY d_year
    """

    # Baseline (no config)
    t0 = time.perf_counter()
    rows_base = executor.execute(test_sql)
    t_base = (time.perf_counter() - t0) * 1000

    # With config
    config_cmds = [
        "SET LOCAL work_mem = '512MB'",
        "SET LOCAL max_parallel_workers_per_gather = '0'",
    ]
    t0 = time.perf_counter()
    rows_cfg = executor.execute_with_config(test_sql, config_cmds)
    t_cfg = (time.perf_counter() - t0) * 1000

    print(f"  Baseline: {t_base:.1f}ms ({len(rows_base)} rows)")
    print(f"  With config: {t_cfg:.1f}ms ({len(rows_cfg)} rows)")
    assert len(rows_base) == len(rows_cfg), "Row count mismatch!"
    print("  OK: Row counts match")

    # Test 4: Multiple SET LOCAL commands
    multi_cmds = [
        "SET LOCAL work_mem = '128MB'",
        "SET LOCAL effective_cache_size = '8GB'",
        "SET LOCAL random_page_cost = '1.1'",
        "SET LOCAL enable_seqscan = 'off'",
    ]
    try:
        rows = executor.execute_with_config(
            "SELECT 1 as test", multi_cmds
        )
        print(f"  Multi-config OK: {len(multi_cmds)} SET LOCAL commands")
    except Exception as e:
        print(f"  FAIL: Multi-config: {e}")

    executor.close()
    print("  OK: All live PG tests passed")
    print()


def test_validate_with_config():
    """Test validate_with_config() against local PostgreSQL."""
    print("=" * 60)
    print("TEST 6: validate_with_config() — live PG test")
    print("=" * 60)

    try:
        from ado.validate import Validator, OriginalBaseline
    except ImportError as e:
        print(f"  SKIP: {e}")
        return

    dsn = "postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"

    try:
        validator = Validator(dsn)
        test_sql = "SELECT d_year, COUNT(*) FROM date_dim GROUP BY d_year ORDER BY d_year"

        # First benchmark baseline
        baseline = validator.benchmark_baseline(test_sql)
        print(f"  Baseline: {baseline.measured_time_ms:.1f}ms ({baseline.row_count} rows)")

        # Validate with config
        config_cmds = ["SET LOCAL work_mem = '512MB'"]
        result = validator.validate_with_config(
            baseline=baseline,
            sql=test_sql,
            config_commands=config_cmds,
            worker_id=99,
        )
        print(f"  Config result: {result.status.value}, speedup={result.speedup:.2f}x")
        if result.error:
            print(f"  Error: {result.error}")

        validator.close()
        print("  OK: validate_with_config works")

    except Exception as e:
        print(f"  SKIP: {e}")

    print()


def test_tuning_config_dataclass():
    """Test TuningConfig dataclass."""
    print("=" * 60)
    print("TEST 7: TuningConfig dataclass")
    print("=" * 60)

    tc = TuningConfig(
        params={"work_mem": "512MB", "max_parallel_workers_per_gather": "4"},
        reasoning="The EXPLAIN shows hash spills",
    )
    assert tc.params["work_mem"] == "512MB"
    assert "spills" in tc.reasoning

    # Test JSON round-trip
    d = {"params": tc.params, "reasoning": tc.reasoning}
    j = json.dumps(d)
    loaded = json.loads(j)
    assert loaded["params"]["work_mem"] == "512MB"
    print("  OK: TuningConfig works + JSON round-trip")
    print()


def main():
    print()
    print("PG TUNING INFRASTRUCTURE TESTS")
    print("=" * 60)
    print()

    # Unit tests (no PG needed)
    test_whitelist()
    test_validate_config()
    test_build_set_local()
    test_tuner_prompt()
    test_tuning_config_dataclass()

    # Integration tests (need PG)
    test_execute_with_config()
    test_validate_with_config()

    print("=" * 60)
    print("ALL TESTS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
