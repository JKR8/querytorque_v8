"""Validation + scoring for ADO.

This module validates optimized SQL candidates using the qt_sql validation
infrastructure. It benchmarks performance and checks semantic equivalence.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from .schemas import ValidationStatus, ValidationResult

logger = logging.getLogger(__name__)


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

        try:
            executor = self._get_executor()

            # Execute both queries and compare results
            try:
                # Time original
                import time
                start = time.time()
                orig_result = executor.execute(original_sql)
                orig_time = (time.time() - start) * 1000  # ms

                # Time candidate
                start = time.time()
                cand_result = executor.execute(candidate_sql)
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
