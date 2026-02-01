"""Main SQL validator orchestrating the validation pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

try:
    import sqlglot
except ImportError:
    sqlglot = None

from ..execution import DuckDBExecutor
from .benchmarker import QueryBenchmarker
from .equivalence_checker import EquivalenceChecker
from .query_normalizer import QueryNormalizer
from .schemas import (
    LimitStrategy,
    ValidationMode,
    ValidationResult,
    ValidationStatus,
)


class SQLValidator:
    """Validates SQL optimization equivalence.

    Orchestrates:
    1. Syntax validation (sqlglot)
    2. LIMIT/ORDER BY normalization
    3. Benchmarking (1-1-2-2 pattern)
    4. Equivalence checking (row counts, checksums, values)

    Two modes:
    - SAMPLE: Uses sample database for signal (fast but approximate)
    - FULL: Uses full database for confidence (slower but accurate)
    """

    def __init__(
        self,
        database: str = ":memory:",
        mode: ValidationMode = ValidationMode.SAMPLE,
        sample_pct: float = 1.0,
        limit_strategy: LimitStrategy = LimitStrategy.ADD_ORDER,
        float_tolerance: float = 1e-9,
    ):
        """Initialize validator.

        Args:
            database: Path to DuckDB database or ":memory:".
            mode: Validation mode (SAMPLE or FULL).
            sample_pct: Sample percentage for SAMPLE mode.
            limit_strategy: How to handle LIMIT without ORDER BY.
            float_tolerance: Tolerance for floating point comparison.
        """
        self.database = database
        self.mode = mode
        self.sample_pct = sample_pct
        self.limit_strategy = limit_strategy
        self.float_tolerance = float_tolerance

        # Components
        self._executor: Optional[DuckDBExecutor] = None
        self._normalizer: Optional[QueryNormalizer] = None
        self._checker: Optional[EquivalenceChecker] = None
        self._benchmarker: Optional[QueryBenchmarker] = None

    def _get_executor(self) -> DuckDBExecutor:
        """Get or create executor."""
        if self._executor is None:
            # In-memory databases cannot be read-only
            read_only = self.database != ":memory:"
            self._executor = DuckDBExecutor(self.database, read_only=read_only)
            self._executor.connect()
        return self._executor

    def _get_normalizer(self) -> QueryNormalizer:
        """Get or create normalizer."""
        if self._normalizer is None:
            self._normalizer = QueryNormalizer(dialect="duckdb")
        return self._normalizer

    def _get_checker(self) -> EquivalenceChecker:
        """Get or create equivalence checker."""
        if self._checker is None:
            self._checker = EquivalenceChecker(float_tolerance=self.float_tolerance)
        return self._checker

    def _get_benchmarker(self) -> QueryBenchmarker:
        """Get or create benchmarker."""
        if self._benchmarker is None:
            self._benchmarker = QueryBenchmarker(self._get_executor())
        return self._benchmarker

    def close(self) -> None:
        """Close database connection."""
        if self._executor is not None:
            self._executor.close()
            self._executor = None

    def __enter__(self) -> "SQLValidator":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def validate_syntax(self, sql: str) -> tuple[bool, list[str]]:
        """Validate SQL syntax using sqlglot.

        Args:
            sql: SQL query to validate.

        Returns:
            Tuple of (is_valid, list of errors).
        """
        if sqlglot is None:
            return True, []  # Can't validate without sqlglot

        try:
            sqlglot.parse_one(sql, dialect="duckdb")
            return True, []
        except sqlglot.errors.ParseError as e:
            return False, [str(e)]
        except Exception as e:
            return False, [f"Parse error: {e}"]

    def load_schema(self, schema_sql: str) -> None:
        """Load schema into the database.

        Only works for in-memory databases.

        Args:
            schema_sql: SQL script with CREATE TABLE statements.
        """
        executor = self._get_executor()
        executor.execute_script(schema_sql)
        # Reset benchmarker to use the executor after schema is loaded
        self._benchmarker = None

    def validate(
        self,
        original_sql: str,
        optimized_sql: str,
        schema_sql: Optional[str] = None,
    ) -> ValidationResult:
        """Validate that optimized SQL is equivalent to original.

        Flow:
        1. Syntax validation (sqlglot)
        2. LIMIT/ORDER BY normalization (both modes)
        3. Execute 1-1-2-2 benchmark
        4. Compare results:
           - Row counts (must match exactly)
           - Checksum comparison
           - If checksum differs: value-by-value comparison
        5. Calculate metrics: speedup, cost_reduction_pct
        6. Return ValidationResult

        Args:
            original_sql: Original SQL query.
            optimized_sql: Optimized SQL query.
            schema_sql: Optional schema to load (for in-memory DB).

        Returns:
            ValidationResult with all metrics and status.
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Load schema if provided
        if schema_sql:
            try:
                self.load_schema(schema_sql)
            except Exception as e:
                return self._error_result(f"Failed to load schema: {e}")

        # Step 1: Syntax validation
        orig_valid, orig_errors = self.validate_syntax(original_sql)
        if not orig_valid:
            return self._error_result(f"Original SQL syntax error: {orig_errors}")

        opt_valid, opt_errors = self.validate_syntax(optimized_sql)
        if not opt_valid:
            return self._error_result(f"Optimized SQL syntax error: {opt_errors}")

        # Step 2: LIMIT/ORDER BY normalization
        normalizer = self._get_normalizer()
        orig_norm, opt_norm = normalizer.normalize_pair(
            original_sql, optimized_sql, self.limit_strategy
        )

        limit_detected = orig_norm.had_limit_without_order or opt_norm.had_limit_without_order
        strategy_applied = None

        if limit_detected:
            warnings.append("LIMIT without ORDER BY detected - results normalized for comparison")
            strategy_applied = self.limit_strategy
            if orig_norm.error:
                warnings.append(f"Original normalization warning: {orig_norm.error}")
            if opt_norm.error:
                warnings.append(f"Optimized normalization warning: {opt_norm.error}")

        # Use normalized SQL for execution
        exec_original_sql = orig_norm.sql
        exec_optimized_sql = opt_norm.sql

        # Step 3: Execute 1-1-2-2 benchmark
        try:
            benchmarker = self._get_benchmarker()
            benchmark_result = benchmarker.benchmark_pair_with_checksum(
                exec_original_sql, exec_optimized_sql
            )
        except Exception as e:
            return self._error_result(f"Benchmark failed: {e}")

        # Check for execution errors
        if benchmark_result.original.error:
            return self._error_result(f"Original query execution failed: {benchmark_result.original.error}")
        if benchmark_result.optimized.error:
            return self._error_result(f"Optimized query execution failed: {benchmark_result.optimized.error}")

        # Step 4: Compare results
        checker = self._get_checker()

        # 4a: Row counts
        row_counts_match = benchmark_result.original.row_count == benchmark_result.optimized.row_count

        # 4b: Checksum comparison
        checksum_match = benchmark_result.original.checksum == benchmark_result.optimized.checksum

        # 4c: Value comparison if checksums differ
        values_match = checksum_match
        value_differences = []

        if not checksum_match and benchmark_result.original.rows and benchmark_result.optimized.rows:
            value_result = checker.compare_values(
                benchmark_result.original.rows,
                benchmark_result.optimized.rows,
            )
            values_match = value_result.match
            value_differences = value_result.differences

        # Step 5: Calculate metrics
        original_timing = benchmark_result.original.timing.measured_time_ms
        optimized_timing = benchmark_result.optimized.timing.measured_time_ms

        if optimized_timing > 0:
            speedup = original_timing / optimized_timing
        else:
            speedup = float("inf") if original_timing > 0 else 1.0

        original_cost = benchmark_result.original.cost.estimated_cost
        optimized_cost = benchmark_result.optimized.cost.estimated_cost

        if original_cost > 0 and original_cost != float("inf"):
            cost_reduction_pct = ((original_cost - optimized_cost) / original_cost) * 100
        else:
            cost_reduction_pct = 0.0

        # Step 6: Determine status
        if not row_counts_match:
            status = ValidationStatus.FAIL
            errors.append(
                f"Row count mismatch: original={benchmark_result.original.row_count}, "
                f"optimized={benchmark_result.optimized.row_count}"
            )
        elif not values_match:
            status = ValidationStatus.FAIL
            errors.append("Value mismatch detected between original and optimized results")
        else:
            status = ValidationStatus.PASS

        # Add note about sample mode
        if self.mode == ValidationMode.SAMPLE and status == ValidationStatus.PASS:
            warnings.append(
                "Sample mode: Values may be 0/small on sample data. "
                "Use --mode full for confidence."
            )

        return ValidationResult(
            status=status,
            mode=self.mode,
            original_row_count=benchmark_result.original.row_count,
            optimized_row_count=benchmark_result.optimized.row_count,
            row_counts_match=row_counts_match,
            original_timing_ms=original_timing,
            optimized_timing_ms=optimized_timing,
            speedup=speedup,
            original_cost=original_cost,
            optimized_cost=optimized_cost,
            cost_reduction_pct=cost_reduction_pct,
            values_match=values_match,
            checksum_match=checksum_match,
            value_differences=value_differences,
            limit_detected=limit_detected,
            limit_strategy_applied=strategy_applied,
            normalized_original_sql=exec_original_sql if limit_detected else None,
            normalized_optimized_sql=exec_optimized_sql if limit_detected else None,
            errors=errors,
            warnings=warnings,
            original_result=benchmark_result.original,
            optimized_result=benchmark_result.optimized,
        )

    def _error_result(self, error: str) -> ValidationResult:
        """Create an error ValidationResult."""
        return ValidationResult(
            status=ValidationStatus.ERROR,
            mode=self.mode,
            original_row_count=0,
            optimized_row_count=0,
            row_counts_match=False,
            original_timing_ms=0,
            optimized_timing_ms=0,
            speedup=0,
            original_cost=0,
            optimized_cost=0,
            cost_reduction_pct=0,
            values_match=False,
            errors=[error],
        )


def validate_sql_files(
    original_path: str,
    optimized_path: str,
    database: str = ":memory:",
    schema_path: Optional[str] = None,
    mode: ValidationMode = ValidationMode.SAMPLE,
    limit_strategy: LimitStrategy = LimitStrategy.ADD_ORDER,
) -> ValidationResult:
    """Convenience function to validate SQL files.

    Args:
        original_path: Path to original SQL file.
        optimized_path: Path to optimized SQL file.
        database: Path to database or ":memory:".
        schema_path: Optional path to schema SQL file.
        mode: Validation mode.
        limit_strategy: Strategy for LIMIT without ORDER BY.

    Returns:
        ValidationResult.
    """
    original_sql = Path(original_path).read_text(encoding="utf-8")
    optimized_sql = Path(optimized_path).read_text(encoding="utf-8")
    schema_sql = Path(schema_path).read_text(encoding="utf-8") if schema_path else None

    with SQLValidator(
        database=database,
        mode=mode,
        limit_strategy=limit_strategy,
    ) as validator:
        return validator.validate(original_sql, optimized_sql, schema_sql)
