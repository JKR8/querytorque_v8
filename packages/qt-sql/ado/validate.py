"""Validation + scoring for ADO.

This module validates optimized SQL candidates using the qt_sql validation
infrastructure. It benchmarks performance and checks semantic equivalence.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, List, Optional

from .schemas import ValidationStatus, ValidationResult

logger = logging.getLogger(__name__)


@dataclass
class OriginalBaseline:
    """Cached baseline from benchmarking the original query once.

    Used by SwarmSession to avoid re-timing the original for every worker.
    """
    measured_time_ms: float
    row_count: int
    rows: Optional[List[Any]] = None
    checksum: Optional[str] = None


def categorize_error(error_msg: str) -> str:
    """Categorize error message for learning.

    Returns: "syntax" | "semantic" | "timeout" | "execution" | "unknown"
    """
    error_lower = error_msg.lower()

    # Syntax errors
    if any(x in error_lower for x in ["syntax", "parse error", "invalid sql", "unexpected", "sql error"]):
        return "syntax"

    # Semantic errors (wrong results)
    if any(x in error_lower for x in ["mismatch", "count differ", "value mismatch", "not equal", "semantic"]):
        return "semantic"

    # Timeout
    if any(x in error_lower for x in ["timeout", "timed out", "cancelled"]):
        return "timeout"

    # Execution errors
    if any(x in error_lower for x in ["execution", "failed", "error", "exception"]):
        return "execution"

    return "unknown"


def cost_rank_candidates(
    db_path: str,
    original_sql: str,
    candidate_sqls: List[str],
    top_k: int = 2,
) -> List[int]:
    """Rank candidates by EXPLAIN cost estimate (DuckDB only, zero executions).

    Uses DuckDB's EXPLAIN (FORMAT JSON) to get estimated cardinality
    as a cost proxy. Returns indices of the top_k candidates most likely
    to improve performance (lowest estimated cost).

    Falls back to returning all indices if cost estimation fails.

    Args:
        db_path: DuckDB database path
        original_sql: Original SQL for baseline cost
        candidate_sqls: List of optimized SQL candidates
        top_k: Number of top candidates to return

    Returns:
        List of indices (0-based) into candidate_sqls, sorted by cost (best first)
    """
    if not candidate_sqls:
        return []

    try:
        from qt_sql.execution.duckdb_executor import DuckDBExecutor

        executor = DuckDBExecutor(db_path)
        original_cost = executor.get_cost_estimate(original_sql)

        costs = []
        for i, sql in enumerate(candidate_sqls):
            try:
                cost = executor.get_cost_estimate(sql)
                costs.append((i, cost))
            except Exception:
                costs.append((i, float("inf")))

        executor.close()

        # Sort by cost ascending (lower is better)
        costs.sort(key=lambda x: x[1])

        # Return top_k indices
        return [idx for idx, _ in costs[:top_k]]

    except Exception as e:
        logger.warning(f"Cost ranking failed, returning all: {e}")
        return list(range(len(candidate_sqls)))


class Validator:
    """Validate optimization candidates on sample/full database.

    Uses qt_sql.validation.SQLValidator for:
    - Syntax validation
    - Equivalence checking (row counts, checksums)
    - Performance benchmarking (1-1-2-2 pattern)
    """

    def __init__(self, sample_db: str):
        """Initialize validator.

        Args:
            sample_db: Database connection string for validation
                       (DuckDB path or PostgreSQL DSN)
        """
        self.sample_db = sample_db
        self._validator = None

    def _get_validator(self):
        """Get or create the SQLValidator instance."""
        if self._validator is None:
            try:
                # Detect database type from connection string
                if self.sample_db.startswith("postgres://") or self.sample_db.startswith("postgresql://"):
                    # PostgreSQL - use executor-based validation
                    self._validator = self._create_pg_validator()
                else:
                    # DuckDB - use SQLValidator directly
                    from qt_sql.validation.sql_validator import SQLValidator
                    self._validator = SQLValidator(database=self.sample_db)

            except ImportError as e:
                logger.warning(f"SQLValidator not available: {e}")
                self._validator = None

        return self._validator

    def _create_pg_validator(self):
        """Create a PostgreSQL-compatible validator wrapper."""
        # For PostgreSQL, we create a simple wrapper that uses the executor
        return PostgresValidatorWrapper(self.sample_db)

    def validate(
        self,
        original_sql: str,
        candidate_sql: str,
        worker_id: int,
    ) -> ValidationResult:
        """Validate an optimization candidate.

        Args:
            original_sql: The original SQL query
            candidate_sql: The optimized SQL candidate
            worker_id: Worker ID for tracking

        Returns:
            ValidationResult with status, speedup, and errors
        """
        validator = self._get_validator()

        if validator is None:
            error_msg = "Validator not available (missing qt_sql.validation)"
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_msg,
                optimized_sql=candidate_sql,
                errors=[error_msg],
                error_category="execution",
            )

        try:
            result = validator.validate(original_sql, candidate_sql)

            # Map qt_sql ValidationStatus to ADO ValidationStatus
            from qt_sql.validation.schemas import ValidationStatus as QtStatus

            status_map = {
                QtStatus.PASS: ValidationStatus.PASS,
                QtStatus.FAIL: ValidationStatus.FAIL,
                QtStatus.WARN: ValidationStatus.FAIL,  # Treat warnings as failures for ADO
                QtStatus.ERROR: ValidationStatus.ERROR,
            }

            ado_status = status_map.get(result.status, ValidationStatus.ERROR)

            # Extract error messages
            error_msg = None
            error_category = None
            all_errors = []

            if result.errors:
                all_errors = result.errors
                error_msg = " | ".join(result.errors)  # Combine all errors
                error_category = categorize_error(all_errors[0])

            return ValidationResult(
                worker_id=worker_id,
                status=ado_status,
                speedup=result.speedup if result.speedup else 0.0,
                error=error_msg,
                optimized_sql=candidate_sql,
                errors=all_errors,
                error_category=error_category,
            )

        except Exception as e:
            error_str = str(e)
            logger.warning(f"Validation failed for worker {worker_id}: {error_str}")
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_str,
                optimized_sql=candidate_sql,
                errors=[error_str],
                error_category=categorize_error(error_str),
            )

    def benchmark_baseline(self, original_sql: str) -> OriginalBaseline:
        """Benchmark original SQL once and return cached baseline.

        Uses 3-run pattern (warmup + 2 measures, average) for DuckDB,
        or simple 3-run for PostgreSQL. The baseline can be reused for
        multiple validate_against_baseline() calls.

        Args:
            original_sql: The original SQL query

        Returns:
            OriginalBaseline with timing, rows, and checksum

        Raises:
            RuntimeError: If the original query fails
        """
        validator = self._get_validator()
        if validator is None:
            raise RuntimeError("Validator not available (missing qt_sql.validation)")

        if isinstance(validator, PostgresValidatorWrapper):
            # PostgreSQL: 3-run pattern using executor
            # Use 300s timeout (matches R-Bot's timeout for fair comparison)
            pg_timeout_ms = 300_000
            executor = validator._get_executor()

            try:
                # Warmup
                executor.execute(original_sql, timeout_ms=pg_timeout_ms)

                # Measure 1 (capture rows)
                start = time.perf_counter()
                rows = executor.execute(original_sql, timeout_ms=pg_timeout_ms)
                t1 = (time.perf_counter() - start) * 1000

                # Measure 2
                start = time.perf_counter()
                executor.execute(original_sql, timeout_ms=pg_timeout_ms)
                t2 = (time.perf_counter() - start) * 1000

                avg_ms = (t1 + t2) / 2
                logger.info(f"Baseline (PG): {avg_ms:.1f}ms ({len(rows)} rows)")

                return OriginalBaseline(
                    measured_time_ms=avg_ms,
                    row_count=len(rows),
                    rows=rows,
                )
            except Exception as e:
                # Timeout or error — create timeout baseline
                # This allows the swarm to still optimize timeout queries
                # by recording the timeout ceiling as the baseline time
                error_lower = str(e).lower()
                if "timeout" in error_lower or "cancel" in error_lower:
                    logger.warning(
                        f"Baseline (PG): TIMEOUT at {pg_timeout_ms}ms — "
                        f"using timeout ceiling as baseline"
                    )
                    # Rollback the timed-out transaction
                    try:
                        executor.rollback()
                    except Exception:
                        pass
                    return OriginalBaseline(
                        measured_time_ms=float(pg_timeout_ms),
                        row_count=0,
                        rows=None,
                    )
                raise  # Re-raise non-timeout errors
        else:
            # DuckDB: use benchmarker's benchmark_single (3-run)
            benchmarker = validator._get_benchmarker()
            result = benchmarker.benchmark_single(original_sql, capture_results=True)

            if result.error:
                raise RuntimeError(f"Original query failed: {result.error}")

            # Compute checksum
            checksum = None
            if result.rows:
                checker = validator._get_checker()
                checksum = checker.compute_checksum(result.rows)

            logger.info(
                f"Baseline (DuckDB): {result.timing.measured_time_ms:.1f}ms "
                f"({result.row_count} rows)"
            )

            return OriginalBaseline(
                measured_time_ms=result.timing.measured_time_ms,
                row_count=result.row_count,
                rows=result.rows,
                checksum=checksum,
            )

    def validate_against_baseline(
        self,
        baseline: OriginalBaseline,
        candidate_sql: str,
        worker_id: int,
    ) -> ValidationResult:
        """Validate optimized SQL against a pre-computed baseline.

        Only benchmarks the candidate — does NOT re-run the original.
        Speedup is computed as baseline.measured_time_ms / candidate_time_ms.

        Args:
            baseline: Pre-computed original baseline
            candidate_sql: The optimized SQL to validate
            worker_id: Worker ID for tracking

        Returns:
            ValidationResult with status, speedup, and errors
        """
        validator = self._get_validator()
        if validator is None:
            error_msg = "Validator not available (missing qt_sql.validation)"
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_msg,
                optimized_sql=candidate_sql,
                errors=[error_msg],
                error_category="execution",
            )

        try:
            if isinstance(validator, PostgresValidatorWrapper):
                return self._validate_against_baseline_pg(
                    validator, baseline, candidate_sql, worker_id
                )
            else:
                return self._validate_against_baseline_duckdb(
                    validator, baseline, candidate_sql, worker_id
                )
        except Exception as e:
            error_str = str(e)
            logger.warning(f"Validation failed for worker {worker_id}: {error_str}")
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_str,
                optimized_sql=candidate_sql,
                errors=[error_str],
                error_category=categorize_error(error_str),
            )

    def _validate_against_baseline_duckdb(
        self,
        validator,
        baseline: OriginalBaseline,
        candidate_sql: str,
        worker_id: int,
    ) -> ValidationResult:
        """DuckDB path: benchmark candidate only, compare against baseline."""
        from qt_sql.validation.schemas import ValidationStatus as QtStatus

        errors = []

        # Syntax check
        try:
            import sqlglot
            sqlglot.parse_one(candidate_sql, dialect="duckdb")
        except Exception as e:
            error_msg = f"Optimized SQL syntax error: {e}"
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_msg,
                optimized_sql=candidate_sql,
                errors=[error_msg],
                error_category="syntax",
            )

        # Benchmark candidate only (3-run)
        benchmarker = validator._get_benchmarker()
        opt_result = benchmarker.benchmark_single(candidate_sql, capture_results=True)

        if opt_result.error:
            error_msg = f"Optimized query execution failed: {opt_result.error}"
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_msg,
                optimized_sql=candidate_sql,
                errors=[error_msg],
                error_category=categorize_error(error_msg),
            )

        # Compare row counts
        row_counts_match = opt_result.row_count == baseline.row_count
        if not row_counts_match:
            errors.append(
                f"Row count mismatch: original={baseline.row_count}, "
                f"optimized={opt_result.row_count}"
            )

        # Compare checksums
        opt_checksum = None
        values_match = False
        if opt_result.rows:
            checker = validator._get_checker()
            opt_checksum = checker.compute_checksum(opt_result.rows)
            checksum_match = opt_checksum == baseline.checksum

            if checksum_match:
                values_match = True
            elif baseline.rows and opt_result.rows:
                # Detailed value comparison
                val_result = checker.compare_values(baseline.rows, opt_result.rows)
                values_match = val_result.match
                if not values_match:
                    errors.append("Value mismatch: rows differ between original and optimized")

        # Determine status
        if not row_counts_match:
            ado_status = ValidationStatus.FAIL
        elif not values_match:
            ado_status = ValidationStatus.FAIL
        else:
            ado_status = ValidationStatus.PASS

        # Compute speedup
        if opt_result.timing.measured_time_ms > 0:
            speedup = baseline.measured_time_ms / opt_result.timing.measured_time_ms
        else:
            speedup = 1.0

        error_msg = " | ".join(errors) if errors else None
        error_category = categorize_error(errors[0]) if errors else None

        return ValidationResult(
            worker_id=worker_id,
            status=ado_status,
            speedup=speedup,
            error=error_msg,
            optimized_sql=candidate_sql,
            errors=errors,
            error_category=error_category,
        )

    def _validate_against_baseline_pg(
        self,
        validator: "PostgresValidatorWrapper",
        baseline: OriginalBaseline,
        candidate_sql: str,
        worker_id: int,
    ) -> ValidationResult:
        """PostgreSQL path: execute candidate only, compare against baseline."""
        errors = []
        is_timeout_baseline = baseline.rows is None and baseline.row_count == 0

        executor = validator._get_executor()

        # Time candidate (3-run: warmup + 2 measures) with 300s timeout
        cand_timeout_ms = 300_000
        try:
            executor.execute(candidate_sql, timeout_ms=cand_timeout_ms)  # warmup

            start = time.perf_counter()
            cand_rows = executor.execute(candidate_sql, timeout_ms=cand_timeout_ms)
            t1 = (time.perf_counter() - start) * 1000

            start = time.perf_counter()
            executor.execute(candidate_sql, timeout_ms=cand_timeout_ms)
            t2 = (time.perf_counter() - start) * 1000

            cand_time = (t1 + t2) / 2
        except Exception as e:
            error_msg = f"Execution failed: {e}"
            # Rollback the failed transaction
            try:
                executor.rollback()
            except Exception:
                pass
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_msg,
                optimized_sql=candidate_sql,
                errors=[error_msg],
                error_category=categorize_error(error_msg),
            )

        # Compare row counts
        cand_count = len(cand_rows)

        if is_timeout_baseline:
            # Timeout baseline: can't compare rows, just accept if candidate runs
            ado_status = ValidationStatus.PASS
            logger.info(
                f"Timeout baseline: candidate ran in {cand_time:.1f}ms "
                f"({cand_count} rows) — accepting without row comparison"
            )
        elif cand_count != baseline.row_count:
            errors.append(
                f"Row count mismatch: original={baseline.row_count}, "
                f"optimized={cand_count}"
            )
            ado_status = ValidationStatus.FAIL
        elif baseline.rows and cand_rows != baseline.rows:
            errors.append("Value mismatch: rows differ between original and optimized")
            ado_status = ValidationStatus.FAIL
        else:
            ado_status = ValidationStatus.PASS

        # Compute speedup
        speedup = baseline.measured_time_ms / cand_time if cand_time > 0 else 1.0

        error_msg = " | ".join(errors) if errors else None
        error_category = categorize_error(errors[0]) if errors else None

        return ValidationResult(
            worker_id=worker_id,
            status=ado_status,
            speedup=speedup,
            error=error_msg,
            optimized_sql=candidate_sql,
            errors=errors,
            error_category=error_category,
        )

    def close(self) -> None:
        """Close the validator and release resources."""
        if self._validator is not None:
            if hasattr(self._validator, 'close'):
                self._validator.close()
            self._validator = None


class PostgresValidatorWrapper:
    """Simple PostgreSQL validator wrapper using qt_sql executor.

    This provides a validation interface for PostgreSQL databases
    that don't work with the DuckDB-based SQLValidator.
    """

    def __init__(self, dsn: str):
        """Initialize with PostgreSQL DSN.

        Args:
            dsn: PostgreSQL connection string
        """
        self.dsn = dsn
        self._executor = None

    def _get_executor(self):
        """Get or create PostgreSQL executor."""
        if self._executor is None:
            from qt_sql.execution.factory import create_executor_from_dsn
            self._executor = create_executor_from_dsn(self.dsn)
            self._executor.connect()
        return self._executor

    def validate(self, original_sql: str, candidate_sql: str):
        """Validate candidate against original using PostgreSQL.

        Returns a result object compatible with SQLValidator.
        """
        from qt_sql.validation.schemas import (
            ValidationStatus,
            ValidationResult,
            ValidationMode,
        )

        errors = []
        pg_timeout_ms = 300_000  # 300s timeout (matches R-Bot)

        try:
            executor = self._get_executor()

            # Execute both queries and compare results
            try:
                # Time original
                import time
                start = time.time()
                orig_result = executor.execute(original_sql, timeout_ms=pg_timeout_ms)
                orig_time = (time.time() - start) * 1000  # ms

                # Time candidate
                start = time.time()
                cand_result = executor.execute(candidate_sql, timeout_ms=pg_timeout_ms)
                cand_time = (time.time() - start) * 1000  # ms

            except Exception as e:
                return ValidationResult(
                    status=ValidationStatus.ERROR,
                    mode=ValidationMode.SAMPLE,
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
                    errors=[f"Execution failed: {e}"],
                )

            # Compare row counts
            orig_count = len(orig_result)
            cand_count = len(cand_result)
            row_counts_match = orig_count == cand_count

            # Calculate speedup
            speedup = orig_time / cand_time if cand_time > 0 else 1.0

            # Check for semantic equivalence
            if not row_counts_match:
                status = ValidationStatus.FAIL
                errors.append(
                    f"Row count mismatch: original={orig_count}, optimized={cand_count}"
                )
            else:
                # Simple value comparison (could be enhanced)
                values_match = orig_result == cand_result
                if values_match:
                    status = ValidationStatus.PASS
                else:
                    status = ValidationStatus.FAIL
                    errors.append("Value mismatch: rows differ between original and optimized")

            return ValidationResult(
                status=status,
                mode=ValidationMode.SAMPLE,
                original_row_count=orig_count,
                optimized_row_count=cand_count,
                row_counts_match=row_counts_match,
                original_timing_ms=orig_time,
                optimized_timing_ms=cand_time,
                speedup=speedup,
                original_cost=0,  # Cost not available for PG
                optimized_cost=0,
                cost_reduction_pct=0,
                values_match=row_counts_match,  # Simplified
                errors=errors,
            )

        except Exception as e:
            return ValidationResult(
                status=ValidationStatus.ERROR,
                mode=ValidationMode.SAMPLE,
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
                errors=[str(e)],
            )

    def close(self) -> None:
        """Close the executor."""
        if self._executor is not None:
            self._executor.close()
            self._executor = None
