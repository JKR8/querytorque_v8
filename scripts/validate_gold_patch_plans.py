#!/usr/bin/env python3
"""Validate gold example patch plans via IR engine + database execution.

Per-example pipeline:
  1. Load JSON, extract original_sql and patch_plan
  2. Parse original_sql → ScriptIR
  3. Convert patch_plan dict → PatchPlan
  4. Apply patch plan → PatchResult
  5. Column check: compare output columns (original vs patched)
  6. DB execution (DuckDB only): run both, compare results

Usage:
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 scripts/validate_gold_patch_plans.py
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 scripts/validate_gold_patch_plans.py --fix
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

# ── IR imports ──
from qt_sql.ir import build_script_ir, dict_to_plan, apply_patch_plan
from qt_sql.ir.schema import Dialect

# ── Equivalence checking ──
from qt_sql.validation.equivalence_checker import EquivalenceChecker

# ── sqlglot for column extraction ──
import sqlglot

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DUCKDB_PATH = "/mnt/d/TPC-DS/tpcds_sf10_1.duckdb"
QUERY_TIMEOUT_S = 120

EXAMPLES_ROOT = Path("packages/qt-sql/qt_sql/examples")
DUCKDB_DIR = EXAMPLES_ROOT / "duckdb"
POSTGRES_DIR = EXAMPLES_ROOT / "postgres"
SNOWFLAKE_DIR = EXAMPLES_ROOT / "snowflake"


class Verdict(Enum):
    PASS = "PASS"
    PATCH_FAIL = "PATCH_FAIL"
    COLUMN_MISMATCH = "COLUMN_MISMATCH"
    RESULT_MISMATCH = "RESULT_MISMATCH"
    EXEC_ERROR = "EXEC_ERROR"
    STRUCTURAL_OK = "STRUCTURAL_OK"  # PG/Snowflake: patch + columns OK, no DB


@dataclass
class ValidationResult:
    name: str
    dialect: str
    verdict: Verdict
    steps_applied: int = 0
    steps_total: int = 0
    errors: list[str] = field(default_factory=list)
    original_cols: list[str] = field(default_factory=list)
    patched_cols: list[str] = field(default_factory=list)
    original_rows: int = -1
    patched_rows: int = -1
    elapsed_s: float = 0.0


# ---------------------------------------------------------------------------
# Column extraction via sqlglot
# ---------------------------------------------------------------------------

def _extract_output_columns(sql: str, dialect: str) -> list[str]:
    """Extract top-level SELECT column aliases/names from SQL.

    Returns lowercase column names. For expressions without an alias,
    uses the sqlglot-generated alias or the expression text.
    """
    glot_dialect = {"duckdb": "duckdb", "postgres": "postgres", "snowflake": "snowflake"}.get(dialect, dialect)
    try:
        parsed = sqlglot.parse(sql, read=glot_dialect)
    except Exception:
        return []

    # Find the outermost SELECT — last statement, outermost query
    for stmt in reversed(parsed):
        if stmt is None:
            continue
        # Find the top-level SELECT (not subqueries)
        select = stmt.find(sqlglot.exp.Select)
        if select is None:
            continue
        cols = []
        for expr in select.expressions:
            if isinstance(expr, sqlglot.exp.Alias):
                cols.append(expr.alias.lower())
            elif isinstance(expr, sqlglot.exp.Column):
                cols.append(expr.name.lower())
            elif isinstance(expr, sqlglot.exp.Star):
                cols.append("*")
            else:
                # Use the alias if generated, else the SQL text
                alias = expr.alias
                if alias:
                    cols.append(alias.lower())
                else:
                    cols.append(expr.sql(dialect=glot_dialect).lower()[:60])
        return cols
    return []


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------

def validate_example(
    path: Path,
    dialect_str: str,
    executor=None,
) -> ValidationResult:
    """Validate a single gold example."""
    t0 = time.time()
    name = path.stem

    with open(path) as f:
        data = json.load(f)

    original_sql = data.get("original_sql", "")
    patch_plan_dict = data.get("patch_plan")

    if not patch_plan_dict:
        return ValidationResult(
            name=name, dialect=dialect_str,
            verdict=Verdict.PATCH_FAIL,
            errors=["No patch_plan field in JSON"],
            elapsed_s=time.time() - t0,
        )

    # ── Step 1: Parse original SQL → IR ──
    dialect_enum = {"duckdb": Dialect.DUCKDB, "postgres": Dialect.POSTGRES, "snowflake": Dialect.SNOWFLAKE}[dialect_str]
    try:
        script_ir = build_script_ir(original_sql, dialect_enum)
    except Exception as e:
        return ValidationResult(
            name=name, dialect=dialect_str,
            verdict=Verdict.PATCH_FAIL,
            errors=[f"IR parse failed: {e}"],
            elapsed_s=time.time() - t0,
        )

    # ── Step 2: Convert dict → PatchPlan ──
    try:
        plan = dict_to_plan(patch_plan_dict)
    except Exception as e:
        return ValidationResult(
            name=name, dialect=dialect_str,
            verdict=Verdict.PATCH_FAIL,
            errors=[f"dict_to_plan failed: {e}"],
            elapsed_s=time.time() - t0,
        )

    # ── Step 3: Apply patch plan ──
    try:
        result = apply_patch_plan(script_ir, plan)
    except Exception as e:
        return ValidationResult(
            name=name, dialect=dialect_str,
            verdict=Verdict.PATCH_FAIL,
            errors=[f"apply_patch_plan exception: {e}"],
            steps_total=len(plan.steps),
            elapsed_s=time.time() - t0,
        )

    if not result.success:
        return ValidationResult(
            name=name, dialect=dialect_str,
            verdict=Verdict.PATCH_FAIL,
            steps_applied=result.steps_applied,
            steps_total=result.steps_total,
            errors=result.errors,
            elapsed_s=time.time() - t0,
        )

    patched_sql = result.output_sql

    # ── Step 4: Column check ──
    orig_cols = _extract_output_columns(original_sql, dialect_str)
    patch_cols = _extract_output_columns(patched_sql, dialect_str)

    # Normalize: if either has * we skip column comparison
    cols_match = True
    if "*" not in orig_cols and "*" not in patch_cols:
        if orig_cols != patch_cols:
            cols_match = False

    if not cols_match:
        return ValidationResult(
            name=name, dialect=dialect_str,
            verdict=Verdict.COLUMN_MISMATCH,
            steps_applied=result.steps_applied,
            steps_total=result.steps_total,
            original_cols=orig_cols,
            patched_cols=patch_cols,
            errors=[f"Column mismatch: {orig_cols} vs {patch_cols}"],
            elapsed_s=time.time() - t0,
        )

    # ── Step 5: DB execution (DuckDB only) ──
    if executor is not None and dialect_str == "duckdb":
        try:
            orig_rows = executor.execute(original_sql)
        except Exception as e:
            return ValidationResult(
                name=name, dialect=dialect_str,
                verdict=Verdict.EXEC_ERROR,
                steps_applied=result.steps_applied,
                steps_total=result.steps_total,
                errors=[f"Original SQL exec error: {e}"],
                elapsed_s=time.time() - t0,
            )

        try:
            patch_rows = executor.execute(patched_sql)
        except Exception as e:
            return ValidationResult(
                name=name, dialect=dialect_str,
                verdict=Verdict.EXEC_ERROR,
                steps_applied=result.steps_applied,
                steps_total=result.steps_total,
                errors=[f"Patched SQL exec error: {e}"],
                original_rows=len(orig_rows),
                elapsed_s=time.time() - t0,
            )

        # Compare results
        checker = EquivalenceChecker()
        is_eq, detail = checker.check_equivalence(orig_rows, patch_rows, detailed=True)

        if not is_eq:
            diff_info = []
            if len(orig_rows) != len(patch_rows):
                diff_info.append(f"Row count: {len(orig_rows)} vs {len(patch_rows)}")
            if detail and detail.differences:
                for d in detail.differences[:5]:
                    diff_info.append(f"  col={d.column} row={d.row_index}: {d.original_value!r} vs {d.optimized_value!r}")
            return ValidationResult(
                name=name, dialect=dialect_str,
                verdict=Verdict.RESULT_MISMATCH,
                steps_applied=result.steps_applied,
                steps_total=result.steps_total,
                original_rows=len(orig_rows),
                patched_rows=len(patch_rows),
                errors=diff_info or ["Results differ (checksum mismatch)"],
                elapsed_s=time.time() - t0,
            )

        return ValidationResult(
            name=name, dialect=dialect_str,
            verdict=Verdict.PASS,
            steps_applied=result.steps_applied,
            steps_total=result.steps_total,
            original_rows=len(orig_rows),
            patched_rows=len(patch_rows),
            elapsed_s=time.time() - t0,
        )

    # No DB available — structural-only pass
    return ValidationResult(
        name=name, dialect=dialect_str,
        verdict=Verdict.STRUCTURAL_OK,
        steps_applied=result.steps_applied,
        steps_total=result.steps_total,
        original_cols=orig_cols,
        patched_cols=patch_cols,
        elapsed_s=time.time() - t0,
    )


# ---------------------------------------------------------------------------
# Summary display
# ---------------------------------------------------------------------------

def print_summary(results: list[ValidationResult]) -> int:
    """Print results table. Returns number of failures."""
    # Group by dialect
    by_dialect: dict[str, list[ValidationResult]] = {}
    for r in results:
        by_dialect.setdefault(r.dialect, []).append(r)

    total_fail = 0

    for dialect, group in sorted(by_dialect.items()):
        print(f"\n{'='*70}")
        print(f"  {dialect.upper()} ({len(group)} examples)")
        print(f"{'='*70}")
        print(f"{'Name':<42} {'Verdict':<18} {'Steps':>5} {'Rows':>12} {'Time':>6}")
        print("-" * 70)

        for r in sorted(group, key=lambda x: x.name):
            steps_str = f"{r.steps_applied}/{r.steps_total}" if r.steps_total > 0 else "-"
            rows_str = f"{r.original_rows}/{r.patched_rows}" if r.original_rows >= 0 else "-"
            time_str = f"{r.elapsed_s:.1f}s"
            verdict_str = r.verdict.value
            # Color coding
            if r.verdict == Verdict.PASS:
                verdict_str = f"\033[92m{verdict_str}\033[0m"
            elif r.verdict == Verdict.STRUCTURAL_OK:
                verdict_str = f"\033[96m{verdict_str}\033[0m"
            else:
                verdict_str = f"\033[91m{verdict_str}\033[0m"
                total_fail += 1

            print(f"{r.name:<42} {verdict_str:<27} {steps_str:>5} {rows_str:>12} {time_str:>6}")

            if r.errors:
                for err in r.errors[:3]:
                    print(f"  \033[93m>> {err}\033[0m")

    # Summary
    verdicts = [r.verdict for r in results]
    print(f"\n{'='*70}")
    print(f"  SUMMARY: {len(results)} examples")
    print(f"    PASS:            {verdicts.count(Verdict.PASS)}")
    print(f"    STRUCTURAL_OK:   {verdicts.count(Verdict.STRUCTURAL_OK)}")
    print(f"    PATCH_FAIL:      {verdicts.count(Verdict.PATCH_FAIL)}")
    print(f"    COLUMN_MISMATCH: {verdicts.count(Verdict.COLUMN_MISMATCH)}")
    print(f"    RESULT_MISMATCH: {verdicts.count(Verdict.RESULT_MISMATCH)}")
    print(f"    EXEC_ERROR:      {verdicts.count(Verdict.EXEC_ERROR)}")
    print(f"{'='*70}")

    return total_fail


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate gold example patch plans")
    parser.add_argument("--fix", action="store_true", help="Attempt to fix broken patch plans")
    parser.add_argument("--only", help="Run only a specific example (stem name)")
    parser.add_argument("--dialect", help="Run only a specific dialect (duckdb/postgres/snowflake)")
    parser.add_argument("--no-db", action="store_true", help="Skip database execution")
    args = parser.parse_args()

    # ── Collect example files ──
    examples: list[tuple[Path, str]] = []

    if not args.dialect or args.dialect == "duckdb":
        for p in sorted(DUCKDB_DIR.glob("*.json")):
            if args.only and p.stem != args.only:
                continue
            examples.append((p, "duckdb"))

    if not args.dialect or args.dialect == "postgres":
        for p in sorted(POSTGRES_DIR.glob("*.json")):
            if args.only and p.stem != args.only:
                continue
            examples.append((p, "postgres"))

    if not args.dialect or args.dialect == "snowflake":
        if SNOWFLAKE_DIR.exists():
            for p in sorted(SNOWFLAKE_DIR.glob("*.json")):
                if args.only and p.stem != args.only:
                    continue
                examples.append((p, "snowflake"))

    if not examples:
        print("No examples found.")
        return 1

    print(f"Found {len(examples)} gold examples to validate")

    # ── Setup DuckDB executor ──
    executor = None
    if not args.no_db:
        db_path = Path(DUCKDB_PATH)
        if db_path.exists():
            from qt_sql.execution.duckdb_executor import DuckDBExecutor
            executor = DuckDBExecutor(database=str(db_path), read_only=True)
            executor.connect()
            print(f"DuckDB connected: {db_path}")
        else:
            print(f"DuckDB not found at {db_path}, skipping DB checks")

    # ── Validate each ──
    results: list[ValidationResult] = []
    for path, dialect in examples:
        r = validate_example(path, dialect, executor=executor if dialect == "duckdb" else None)
        results.append(r)
        # Progress indicator
        icon = {
            Verdict.PASS: ".",
            Verdict.STRUCTURAL_OK: "s",
            Verdict.PATCH_FAIL: "F",
            Verdict.COLUMN_MISMATCH: "C",
            Verdict.RESULT_MISMATCH: "R",
            Verdict.EXEC_ERROR: "E",
        }[r.verdict]
        print(icon, end="", flush=True)

    print()  # Newline after progress dots

    # ── Cleanup ──
    if executor:
        executor.close()

    # ── Print summary ──
    failures = print_summary(results)

    return 1 if failures > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
