"""DuckDB harness for local query validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Optional

from query_torque.execution.duckdb_executor import DuckDBExecutor
from query_torque.execution.fixture_loader import FixtureLoader
from query_torque.validation.equivalence_validator import (
    ResultEquivalenceResult,
    ResultEquivalenceValidator,
)
from query_torque.validation.pipeline import ValidationPipeline, ValidationResult


@dataclass
class HarnessResult:
    """Result of executing a query in DuckDB."""
    rows: list[dict]
    execution_time_ms: float
    explain_plan: Optional[dict] = None


@dataclass
class EquivalenceRun:
    """Result of running equivalence check in DuckDB."""
    equivalence: ResultEquivalenceResult
    original: HarnessResult
    optimized: HarnessResult


class DuckDBHarness:
    """Reusable DuckDB harness with fixture loading."""

    def __init__(self, database: str = ":memory:") -> None:
        self.database = database

    def run_query(
        self,
        sql: str,
        fixtures_path: Path,
        explain: bool = False,
    ) -> HarnessResult:
        """Execute a SQL query against fixtures."""
        with DuckDBExecutor(self.database) as executor:
            FixtureLoader(executor).load_fixtures(fixtures_path)
            start = perf_counter()
            rows = executor.execute(sql)
            end = perf_counter()
            plan = executor.explain(sql) if explain else None
            return HarnessResult(
                rows=rows,
                execution_time_ms=(end - start) * 1000,
                explain_plan=plan,
            )

    def run_equivalence(
        self,
        original_sql: str,
        optimized_sql: str,
        fixtures_path: Path,
        validator: Optional[ResultEquivalenceValidator] = None,
    ) -> EquivalenceRun:
        """Run equivalence validation between two SQL queries."""
        validator = validator or ResultEquivalenceValidator()
        original = self.run_query(original_sql, fixtures_path)
        optimized = self.run_query(optimized_sql, fixtures_path)
        equivalence_result = validator.validate(
            original.rows,
            optimized.rows,
            original_time_ms=original.execution_time_ms,
            optimized_time_ms=optimized.execution_time_ms,
        )
        return EquivalenceRun(
            equivalence=equivalence_result,
            original=original,
            optimized=optimized,
        )

    def validate_with_pipeline(
        self,
        original_sql: str,
        optimized_sql: str,
        fixtures_path: Path,
        schema_context: Optional[dict] = None,
        original_issues: Optional[list[dict]] = None,
    ) -> ValidationResult:
        """Run validation pipeline with optional equivalence check."""
        pipeline = ValidationPipeline(
            syntax_check=True,
            schema_check=bool(schema_context),
            regression_check=original_issues is not None,
            equivalence_check=True,
        )
        result = pipeline.validate(
            original_code=original_sql,
            optimized_code=optimized_sql,
            query_type="sql",
            schema_context=schema_context,
            original_issues=original_issues,
            sql_dialect="duckdb",
        )
        if pipeline.equivalence_check:
            equivalence_run = self.run_equivalence(
                original_sql,
                optimized_sql,
                fixtures_path,
            )
            result.equivalence = equivalence_run.equivalence
            if not result.equivalence.equivalent:
                result.all_passed = False
                result.errors.extend(result.equivalence.errors)
        return result
