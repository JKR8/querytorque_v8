"""Validation + scoring for QueryTorque.

This module validates optimized SQL candidates using the qt_sql validation
infrastructure. It benchmarks performance and checks semantic equivalence.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
    explain_text: Optional[str] = None  # EXPLAIN ANALYZE plan text for original query


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




def _collect_explain_text(db_path: str, sql: str) -> Optional[str]:
    """Collect EXPLAIN ANALYZE plan text for a query. Never raises.

    Returns plan text string or None if collection fails.
    Used after timing runs to capture execution plan without affecting benchmarks.
    """
    try:
        from .execution.database_utils import run_explain_analyze
        result = run_explain_analyze(db_path, sql)
        if not result:
            return None
        plan_text = result.get("plan_text")
        if plan_text:
            return plan_text
        # Fall back to building text from JSON plan
        plan_json = result.get("plan_json")
        if plan_json:
            from .execution.database_utils import _plan_to_text
            return _plan_to_text(plan_json)
        return None
    except Exception as e:
        logger.debug(f"EXPLAIN collection failed (non-fatal): {e}")
        return None


class Validator:
    """Validate optimization candidates on sample/full database.

    Uses qt_sql.validation.SQLValidator for:
    - Syntax validation
    - Equivalence checking (row counts, checksums)
    - Performance benchmarking (1-1-2-2 pattern)
    """

    def __init__(self, sample_db: str = None, *, db_dsn: str = None):
        """Initialize validator.

        Args:
            sample_db: Deprecated alias for db_dsn (backward compatible).
            db_dsn: Database connection string for validation
                    (DuckDB path, PostgreSQL DSN, or Snowflake DSN).
        """
        self.db_dsn = db_dsn or sample_db
        if self.db_dsn is None:
            raise ValueError("Must provide db_dsn (or sample_db)")
        self._validator = None

    def _get_validator(self):
        """Get or create the validator instance."""
        if self._validator is None:
            self._validator = ExecutorValidatorWrapper(self.db_dsn)
        return self._validator

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

            # Map qt_sql ValidationStatus to pipeline ValidationStatus
            from .validation.schemas import ValidationStatus as QtStatus

            status_map = {
                QtStatus.PASS: ValidationStatus.PASS,
                QtStatus.FAIL: ValidationStatus.FAIL,
                QtStatus.WARN: ValidationStatus.FAIL,  # Treat warnings as failures
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

    def benchmark_baseline(self, original_sql: str, runs: int = 3) -> OriginalBaseline:
        """Benchmark original SQL and return cached baseline.

        Args:
            original_sql: The original SQL query
            runs: Number of measurement runs.
                  3 (default): warmup + 2 measures, average last 2.
                  >=5: N measures, drop min/max, average middle (trimmed mean).

        Returns:
            OriginalBaseline with timing, rows, and checksum

        Raises:
            RuntimeError: If the original query fails
        """
        validator = self._get_validator()
        if validator is None:
            raise RuntimeError("Validator not available (missing qt_sql.validation)")

        if isinstance(validator, ExecutorValidatorWrapper):
            # PG/Snowflake path using executor
            pg_timeout_ms = 300_000
            executor = validator._get_executor()

            try:
                avg_ms, rows, all_times = _timed_runs_pg(
                    executor, original_sql, runs=runs,
                    capture_rows=True, timeout_ms=pg_timeout_ms,
                )

                # Compute checksum for correctness verification
                checksum = None
                if rows:
                    from .validation.equivalence_checker import EquivalenceChecker
                    checksum = EquivalenceChecker().compute_checksum(rows)

                logger.info(
                    f"Baseline: {avg_ms:.1f}ms ({len(rows)} rows, "
                    f"checksum={checksum}, runs={runs})"
                )

                # Collect EXPLAIN ANALYZE (non-blocking, after timing)
                explain_text = _collect_explain_text(self.db_dsn, original_sql)

                return OriginalBaseline(
                    measured_time_ms=avg_ms,
                    row_count=len(rows),
                    rows=rows,
                    checksum=checksum,
                    explain_text=explain_text,
                )
            except Exception as e:
                # Timeout or error — create timeout baseline
                error_lower = str(e).lower()
                if "timeout" in error_lower or "cancel" in error_lower:
                    logger.warning(
                        f"Baseline: TIMEOUT at {pg_timeout_ms}ms — "
                        f"using timeout ceiling as baseline"
                    )
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
            # DuckDB path
            benchmarker = validator._get_benchmarker()
            if runs >= 5:
                result = benchmarker.benchmark_single_trimmed_mean(
                    original_sql, runs=runs, capture_results=True,
                )
            else:
                result = benchmarker.benchmark_single(
                    original_sql, capture_results=True,
                )

            if result.error:
                raise RuntimeError(f"Original query failed: {result.error}")

            # Compute checksum
            checksum = None
            if result.rows:
                checker = validator._get_checker()
                checksum = checker.compute_checksum(result.rows)

            logger.info(
                f"Baseline (DuckDB): {result.timing.measured_time_ms:.1f}ms "
                f"({result.row_count} rows, runs={runs})"
            )

            # Collect EXPLAIN ANALYZE (non-blocking, after timing)
            explain_text = _collect_explain_text(self.db_dsn, original_sql)

            return OriginalBaseline(
                measured_time_ms=result.timing.measured_time_ms,
                row_count=result.row_count,
                rows=result.rows,
                checksum=checksum,
                explain_text=explain_text,
            )

    def validate_against_baseline(
        self,
        baseline: OriginalBaseline,
        candidate_sql: str,
        worker_id: int,
        runs: int = 3,
    ) -> ValidationResult:
        """Validate optimized SQL against a pre-computed baseline.

        Only benchmarks the candidate — does NOT re-run the original.
        Speedup is computed as baseline.measured_time_ms / candidate_time_ms.

        Args:
            baseline: Pre-computed original baseline
            candidate_sql: The optimized SQL to validate
            worker_id: Worker ID for tracking
            runs: Number of measurement runs (3 default, >=5 for trimmed mean).

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
            if isinstance(validator, ExecutorValidatorWrapper):
                return self._validate_against_baseline_executor(
                    validator, baseline, candidate_sql, worker_id, runs=runs,
                )
            else:
                return self._validate_against_baseline_duckdb(
                    validator, baseline, candidate_sql, worker_id, runs=runs,
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
        runs: int = 3,
    ) -> ValidationResult:
        """DuckDB path: benchmark candidate only, compare against baseline."""
        from .validation.schemas import ValidationStatus as QtStatus

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

        # Benchmark candidate
        benchmarker = validator._get_benchmarker()
        if runs >= 5:
            opt_result = benchmarker.benchmark_single_trimmed_mean(
                candidate_sql, runs=runs, capture_results=True,
            )
        else:
            opt_result = benchmarker.benchmark_single(
                candidate_sql, capture_results=True,
            )

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

        # Collect EXPLAIN ANALYZE on candidate (non-blocking, after timing)
        candidate_explain = None
        if ado_status == ValidationStatus.PASS:
            candidate_explain = _collect_explain_text(self.db_dsn, candidate_sql)

        return ValidationResult(
            worker_id=worker_id,
            status=ado_status,
            speedup=speedup,
            error=error_msg,
            optimized_sql=candidate_sql,
            errors=errors,
            error_category=error_category,
            explain_plan=candidate_explain,
        )

    def _validate_against_baseline_executor(
        self,
        validator: "ExecutorValidatorWrapper",
        baseline: OriginalBaseline,
        candidate_sql: str,
        worker_id: int,
        runs: int = 3,
    ) -> ValidationResult:
        """PG/Snowflake path: execute candidate only, compare against baseline."""
        errors = []
        is_timeout_baseline = baseline.rows is None and baseline.row_count == 0

        executor = validator._get_executor()

        cand_timeout_ms = 300_000
        try:
            cand_time, cand_rows, all_times = _timed_runs_pg(
                executor, candidate_sql, runs=runs,
                capture_rows=True, timeout_ms=cand_timeout_ms,
            )
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

        # Compare row counts + checksums
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
        elif baseline.checksum and cand_rows:
            # Checksum comparison (MD5 of normalized, sorted rows)
            from .validation.equivalence_checker import EquivalenceChecker
            cand_checksum = EquivalenceChecker().compute_checksum(cand_rows)
            if cand_checksum != baseline.checksum:
                errors.append(
                    f"Checksum mismatch: original={baseline.checksum}, "
                    f"optimized={cand_checksum}"
                )
                ado_status = ValidationStatus.FAIL
            else:
                ado_status = ValidationStatus.PASS
        elif baseline.rows and cand_rows != baseline.rows:
            # Fallback: direct list comparison (if no checksum on baseline)
            errors.append("Value mismatch: rows differ between original and optimized")
            ado_status = ValidationStatus.FAIL
        else:
            ado_status = ValidationStatus.PASS

        # Compute speedup
        speedup = baseline.measured_time_ms / cand_time if cand_time > 0 else 1.0

        error_msg = " | ".join(errors) if errors else None
        error_category = categorize_error(errors[0]) if errors else None

        # Collect EXPLAIN ANALYZE on candidate (non-blocking, after timing)
        candidate_explain = None
        if ado_status == ValidationStatus.PASS:
            candidate_explain = _collect_explain_text(self.db_dsn, candidate_sql)

        return ValidationResult(
            worker_id=worker_id,
            status=ado_status,
            speedup=speedup,
            error=error_msg,
            optimized_sql=candidate_sql,
            errors=errors,
            error_category=error_category,
            explain_plan=candidate_explain,
        )

    def validate_with_config(
        self,
        baseline: OriginalBaseline,
        sql: str,
        config_commands: list[str],
        worker_id: int,
    ) -> ValidationResult:
        """Validate SQL executed with SET LOCAL config against a baseline.

        Uses the 3-run pattern (warmup + 2 measures) but wraps each
        execution in execute_with_config() for SET LOCAL support.

        Only works for PostgreSQL connections.

        Args:
            baseline: Pre-computed original baseline (no config).
            sql: SQL query to execute (original or rewritten).
            config_commands: List of SET LOCAL statements.
            worker_id: Worker ID for tracking.

        Returns:
            ValidationResult with speedup computed against the baseline.
        """
        validator = self._get_validator()
        if validator is None or not isinstance(validator, ExecutorValidatorWrapper):
            error_msg = "Config validation requires PostgreSQL"
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_msg,
                optimized_sql=sql,
                errors=[error_msg],
                error_category="execution",
            )

        try:
            return validator.validate_with_config(
                baseline, sql, config_commands, worker_id
            )
        except Exception as e:
            error_str = str(e)
            logger.warning(f"Config validation failed for worker {worker_id}: {error_str}")
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_str,
                optimized_sql=sql,
                errors=[error_str],
                error_category=categorize_error(error_str),
            )

    def benchmark_three_variants(
        self,
        original_sql: str,
        rewrite_sql: str,
        config_commands: list[str],
    ) -> dict:
        """Interleaved 3-variant benchmark: original, rewrite, rewrite+config.

        Uses 1-2-3-1-2-3-1-2-3 pattern: warmup, measure1, measure2.
        Two measurement rounds averaged for reliability.

        Only works for PostgreSQL.

        Returns dict with keys:
            original_ms, rewrite_ms, config_ms,
            rewrite_speedup, config_speedup, config_additive,
            rewrite_rows, config_rows, rows_match, best_variant
        """
        validator = self._get_validator()
        if validator is None or not isinstance(validator, ExecutorValidatorWrapper):
            return {"error": "Requires PostgreSQL"}
        return validator.benchmark_three_variants(
            original_sql, rewrite_sql, config_commands
        )

    def close(self) -> None:
        """Close the validator and release resources."""
        if self._validator is not None:
            if hasattr(self._validator, 'close'):
                self._validator.close()
            self._validator = None


def _timed_runs_pg(executor, sql: str, runs: int = 3,
                   capture_rows: bool = False,
                   timeout_ms: int = 300_000):
    """Execute sql with proper run pattern for PG/Snowflake.

    Run patterns:
      runs=1: one measured run
      runs=2: two measured runs, average both
      runs=3: warmup + 2 measured runs, average measured runs
      runs=4: warmup + 3 measured runs, average measured runs
      runs>=5: N measured runs, drop min/max, average middle (trimmed mean)

    Args:
        executor: Database executor with .execute(sql, timeout_ms=...) method.
        sql: SQL query to execute.
        runs: Number of measurement runs (3 or >=5).
        capture_rows: If True, capture result rows from first measured run.
        timeout_ms: Per-execution timeout in milliseconds.

    Returns:
        (avg_ms, rows_or_None, all_times) tuple.
    """
    if runs >= 5:
        # Trimmed mean: N runs, drop min/max, average middle
        times = []
        rows = None
        for i in range(runs):
            start = time.perf_counter()
            result = executor.execute(sql, timeout_ms=timeout_ms)
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)
            if i == 0 and capture_rows:
                rows = result
            logger.debug(f"  run {i+1}/{runs}: {elapsed:.1f}ms")
        times_sorted = sorted(times)
        trimmed = times_sorted[1:-1]
        avg_ms = sum(trimmed) / len(trimmed)
        logger.info(
            f"  trimmed mean ({runs} runs, drop min/max): {avg_ms:.1f}ms "
            f"[{', '.join(f'{t:.0f}' for t in times)}]"
        )
        return avg_ms, rows, times

    # runs=1..4 path (supports fast worker checks at 2 runs).
    runs = max(1, int(runs))
    measured_times = []
    captured_rows = None

    # For legacy behavior and cache-stable comparability, keep warmup for runs>=3.
    if runs >= 3:
        executor.execute(sql, timeout_ms=timeout_ms)  # warmup

    measured_count = runs if runs <= 2 else (runs - 1)
    for i in range(measured_count):
        start = time.perf_counter()
        rows = executor.execute(sql, timeout_ms=timeout_ms)
        elapsed = (time.perf_counter() - start) * 1000
        measured_times.append(elapsed)
        if i == 0 and capture_rows:
            captured_rows = rows

    avg_ms = sum(measured_times) / len(measured_times)
    if runs >= 3:
        logger.debug(
            f"  {runs}-run: warmup + {measured_count} measured "
            f"[{', '.join(f'{t:.1f}ms' for t in measured_times)}] "
            f"→ avg {avg_ms:.1f}ms"
        )
    else:
        logger.debug(
            f"  {runs}-run: measured "
            f"[{', '.join(f'{t:.1f}ms' for t in measured_times)}] "
            f"→ avg {avg_ms:.1f}ms"
        )
    return avg_ms, captured_rows, measured_times


class ExecutorValidatorWrapper:
    """Executor-based validator wrapper for PostgreSQL and Snowflake.

    This provides a validation interface for server databases
    that don't work with the DuckDB-based SQLValidator.
    """

    def __init__(self, dsn: str):
        """Initialize with database DSN.

        Args:
            dsn: Database connection string (PostgreSQL or Snowflake DSN)
        """
        self.dsn = dsn
        self._executor = None

    def _get_executor(self):
        """Get or create PostgreSQL executor."""
        if self._executor is None:
            from .execution.factory import create_executor_from_dsn
            self._executor = create_executor_from_dsn(self.dsn)
            self._executor.connect()
        return self._executor

    def validate(self, original_sql: str, candidate_sql: str):
        """Validate candidate against original using PostgreSQL.

        Returns a result object compatible with SQLValidator.
        """
        from .validation.schemas import (
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

    def validate_with_config(
        self,
        baseline: OriginalBaseline,
        sql: str,
        config_commands: list[str],
        worker_id: int,
    ) -> ValidationResult:
        """Benchmark SQL + SET LOCAL config against a pre-computed baseline.

        Uses 3-run pattern: warmup + 2 measures, each wrapped in
        execute_with_config() so SET LOCAL applies per execution.

        Args:
            baseline: Pre-computed original baseline (no config).
            sql: SQL query to execute.
            config_commands: List of SET LOCAL statements.
            worker_id: Worker ID for tracking.

        Returns:
            ValidationResult with speedup computed against the baseline.
        """
        errors = []
        is_timeout_baseline = baseline.rows is None and baseline.row_count == 0
        executor = self._get_executor()
        cand_timeout_ms = 300_000

        try:
            # Warmup with config
            executor.execute_with_config(sql, config_commands, timeout_ms=cand_timeout_ms)

            # Measure 1 (capture rows)
            start = time.perf_counter()
            cand_rows = executor.execute_with_config(
                sql, config_commands, timeout_ms=cand_timeout_ms
            )
            t1 = (time.perf_counter() - start) * 1000

            # Measure 2
            start = time.perf_counter()
            executor.execute_with_config(sql, config_commands, timeout_ms=cand_timeout_ms)
            t2 = (time.perf_counter() - start) * 1000

            cand_time = (t1 + t2) / 2
        except Exception as e:
            error_msg = f"Config execution failed: {e}"
            try:
                executor.rollback()
            except Exception:
                pass
            return ValidationResult(
                worker_id=worker_id,
                status=ValidationStatus.ERROR,
                speedup=0.0,
                error=error_msg,
                optimized_sql=sql,
                errors=[error_msg],
                error_category=categorize_error(error_msg),
            )

        # Compare row counts
        cand_count = len(cand_rows)

        if is_timeout_baseline:
            ado_status = ValidationStatus.PASS
            logger.info(
                f"Timeout baseline + config: candidate ran in {cand_time:.1f}ms "
                f"({cand_count} rows)"
            )
        elif cand_count != baseline.row_count:
            errors.append(
                f"Row count mismatch: original={baseline.row_count}, "
                f"config={cand_count}"
            )
            ado_status = ValidationStatus.FAIL
        else:
            ado_status = ValidationStatus.PASS

        # Compute speedup vs baseline (no config)
        speedup = baseline.measured_time_ms / cand_time if cand_time > 0 else 1.0

        error_msg = " | ".join(errors) if errors else None
        error_category = categorize_error(errors[0]) if errors else None

        return ValidationResult(
            worker_id=worker_id,
            status=ado_status,
            speedup=speedup,
            error=error_msg,
            optimized_sql=sql,
            errors=errors,
            error_category=error_category,
        )

    def benchmark_three_variants(
        self,
        original_sql: str,
        rewrite_sql: str,
        config_commands: list[str],
    ) -> dict:
        """Interleaved 3-variant benchmark with proper validation.

        Pattern: 1-2-3-1-2-3-1-2-3 (3 rounds: warmup, measure1, measure2).
        Each variant measured twice, results averaged.
        Interleaving controls for cache warming and system drift.

        Returns dict with timing, speedups, and best variant.
        """
        executor = self._get_executor()
        timeout_ms = 300_000

        try:
            # Round 1: warmup (all three variants)
            executor.execute(original_sql, timeout_ms=timeout_ms)
            executor.execute(rewrite_sql, timeout_ms=timeout_ms)
            executor.execute_with_config(
                rewrite_sql, config_commands, timeout_ms=timeout_ms
            )

            # Round 2: measure 1
            t0 = time.perf_counter()
            executor.execute(original_sql, timeout_ms=timeout_ms)
            t_orig_1 = (time.perf_counter() - t0) * 1000

            t0 = time.perf_counter()
            rows_rw = executor.execute(rewrite_sql, timeout_ms=timeout_ms)
            t_rewrite_1 = (time.perf_counter() - t0) * 1000

            t0 = time.perf_counter()
            rows_cfg = executor.execute_with_config(
                rewrite_sql, config_commands, timeout_ms=timeout_ms
            )
            t_config_1 = (time.perf_counter() - t0) * 1000

            # Round 3: measure 2
            t0 = time.perf_counter()
            executor.execute(original_sql, timeout_ms=timeout_ms)
            t_orig_2 = (time.perf_counter() - t0) * 1000

            t0 = time.perf_counter()
            executor.execute(rewrite_sql, timeout_ms=timeout_ms)
            t_rewrite_2 = (time.perf_counter() - t0) * 1000

            t0 = time.perf_counter()
            executor.execute_with_config(
                rewrite_sql, config_commands, timeout_ms=timeout_ms
            )
            t_config_2 = (time.perf_counter() - t0) * 1000

        except Exception as e:
            return {"error": str(e)}

        # Average two measurement rounds
        t_orig = (t_orig_1 + t_orig_2) / 2
        t_rewrite = (t_rewrite_1 + t_rewrite_2) / 2
        t_config = (t_config_1 + t_config_2) / 2

        rewrite_speedup = t_orig / t_rewrite if t_rewrite > 0 else 1.0
        config_speedup = t_orig / t_config if t_config > 0 else 1.0
        config_additive = t_rewrite / t_config if t_config > 0 else 1.0

        rows_match = len(rows_rw) == len(rows_cfg)

        # Best variant
        if config_speedup > rewrite_speedup and config_speedup >= 1.05:
            best = "rewrite+config"
        elif rewrite_speedup >= 1.05:
            best = "rewrite"
        else:
            best = "original"

        return {
            "original_ms": round(t_orig, 1),
            "rewrite_ms": round(t_rewrite, 1),
            "config_ms": round(t_config, 1),
            "rewrite_speedup": round(rewrite_speedup, 3),
            "config_speedup": round(config_speedup, 3),
            "config_additive": round(config_additive, 3),
            "rewrite_rows": len(rows_rw),
            "config_rows": len(rows_cfg),
            "rows_match": rows_match,
            "best_variant": best,
        }

    def close(self) -> None:
        """Close the executor."""
        if self._executor is not None:
            self._executor.close()
            self._executor = None


# Backward-compatible alias
PostgresValidatorWrapper = ExecutorValidatorWrapper
