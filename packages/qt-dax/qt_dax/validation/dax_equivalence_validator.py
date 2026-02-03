"""DAX Equivalence validation for optimized DAX expressions.

Compares the actual output of original and optimized DAX queries against
a live Power BI Desktop instance to verify semantic equivalence and measure
performance improvement.

Usage:
    from qt_dax.connections import PBIDesktopConnection
    from qt_dax.validation import DAXEquivalenceValidator

    with PBIDesktopConnection(port) as conn:
        validator = DAXEquivalenceValidator(conn)
        result = validator.validate(original_dax, optimized_dax)

        if result.equivalent:
            print(f"Speedup: {result.speedup_ratio:.2f}x")
        else:
            print(f"Mismatches: {result.sample_mismatches}")
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional, TYPE_CHECKING
import math
import time

if TYPE_CHECKING:
    from qt_dax.connections import PBIDesktopConnection


@dataclass
class DAXEquivalenceResult:
    """Result of comparing two DAX query result sets."""

    equivalent: bool = True
    status: str = "pass"  # pass, fail, skip, error
    row_count_match: bool = True
    original_row_count: int = 0
    optimized_row_count: int = 0

    # Execution timing (minimum of warmup runs)
    original_execution_time_ms: float = 0
    optimized_execution_time_ms: float = 0
    speedup_ratio: float = 1.0

    # Detailed timing from all runs
    original_run_times_ms: list[float] = field(default_factory=list)
    optimized_run_times_ms: list[float] = field(default_factory=list)

    # Sample mismatches for display (limited to avoid huge outputs)
    sample_mismatches: list[dict] = field(default_factory=list)

    # Error handling
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "equivalent": self.equivalent,
            "status": self.status,
            "row_count_match": self.row_count_match,
            "original_row_count": self.original_row_count,
            "optimized_row_count": self.optimized_row_count,
            "original_execution_time_ms": round(self.original_execution_time_ms, 3),
            "optimized_execution_time_ms": round(self.optimized_execution_time_ms, 3),
            "speedup_ratio": round(self.speedup_ratio, 2),
            "original_run_times_ms": [round(t, 3) for t in self.original_run_times_ms],
            "optimized_run_times_ms": [round(t, 3) for t in self.optimized_run_times_ms],
            "sample_mismatches": self.sample_mismatches,
            "errors": self.errors,
            "warnings": self.warnings,
        }


class DAXEquivalenceValidator:
    """Validates that two DAX expressions return equivalent results.

    Executes both original and optimized DAX against a live Power BI Desktop
    instance and compares:
    - Row counts
    - Column names
    - Individual values (with numeric tolerance)
    - Execution timing for speedup calculation
    """

    def __init__(
        self,
        connection: "PBIDesktopConnection",
        tolerance: float = 0.0,
        max_rows_to_compare: int = 10000,
        sample_mismatch_limit: int = 5,
        timeout_ms: int = 30000,
        warmup_runs: int = 2,
        discard_first: bool = True,
        exact_match: bool = True,
    ):
        """Initialize the validator.

        Args:
            connection: PBIDesktopConnection to a running Power BI Desktop instance
            tolerance: Numeric tolerance for floating-point comparisons (0.0 = exact match)
            max_rows_to_compare: Maximum rows to compare (for performance)
            sample_mismatch_limit: Max mismatches to include in result
            timeout_ms: Query execution timeout in milliseconds
            warmup_runs: Number of timed runs per query (after discarding first)
            discard_first: If True, run once to warm cache before timing runs
            exact_match: If True, require exact value match (tolerance=0)
        """
        self.connection = connection
        self.tolerance = 0.0 if exact_match else tolerance
        self.max_rows = max_rows_to_compare
        self.sample_limit = sample_mismatch_limit
        self.timeout_ms = timeout_ms
        self.warmup_runs = warmup_runs
        self.discard_first = discard_first
        self.exact_match = exact_match

    def validate(
        self,
        original_dax: str,
        optimized_dax: str,
    ) -> DAXEquivalenceResult:
        """Compare original and optimized DAX for equivalence.

        Runs each query multiple times (warmup_runs) and uses the minimum time
        for accurate performance comparison. This eliminates cache warming effects.

        Args:
            original_dax: Original DAX expression or query
            optimized_dax: Optimized DAX expression or query

        Returns:
            DAXEquivalenceResult with detailed comparison
        """
        result = DAXEquivalenceResult()

        # Prepare DAX for execution (wrap measures if needed)
        try:
            original_query = self._prepare_dax_for_execution(original_dax)
            optimized_query = self._prepare_dax_for_execution(optimized_dax)
        except Exception as e:
            result.equivalent = False
            result.status = "error"
            result.errors.append(f"Failed to prepare DAX: {e}")
            return result

        # Execute original DAX - first run discarded for cache warmup
        original_results = None
        if self.discard_first:
            try:
                self.connection.execute_dax(original_query)  # Warmup run (discarded)
            except Exception as e:
                result.equivalent = False
                result.status = "error"
                result.errors.append(f"Original DAX warmup failed: {e}")
                return result

        # Timed runs for original
        for run in range(self.warmup_runs):
            try:
                start = time.perf_counter()
                run_results = self.connection.execute_dax(original_query)
                elapsed_ms = (time.perf_counter() - start) * 1000
                result.original_run_times_ms.append(elapsed_ms)
                original_results = run_results
            except Exception as e:
                result.equivalent = False
                result.status = "error"
                result.errors.append(f"Original DAX execution failed (run {run + 1}): {e}")
                return result

        # Use minimum time for original query
        result.original_execution_time_ms = min(result.original_run_times_ms)
        result.original_row_count = len(original_results) if original_results else 0

        # Execute optimized DAX - first run discarded for cache warmup
        optimized_results = None
        if self.discard_first:
            try:
                self.connection.execute_dax(optimized_query)  # Warmup run (discarded)
            except Exception as e:
                result.equivalent = False
                result.status = "error"
                result.errors.append(f"Optimized DAX warmup failed: {e}")
                return result

        # Timed runs for optimized
        for run in range(self.warmup_runs):
            try:
                start = time.perf_counter()
                run_results = self.connection.execute_dax(optimized_query)
                elapsed_ms = (time.perf_counter() - start) * 1000
                result.optimized_run_times_ms.append(elapsed_ms)
                optimized_results = run_results
            except Exception as e:
                result.equivalent = False
                result.status = "error"
                result.errors.append(f"Optimized DAX execution failed (run {run + 1}): {e}")
                return result

        # Use minimum time for optimized query
        result.optimized_execution_time_ms = min(result.optimized_run_times_ms)
        result.optimized_row_count = len(optimized_results) if optimized_results else 0

        # Calculate speedup ratio using minimum times
        if result.original_execution_time_ms > 0 and result.optimized_execution_time_ms > 0:
            result.speedup_ratio = (
                result.original_execution_time_ms / result.optimized_execution_time_ms
            )
        elif result.optimized_execution_time_ms == 0 and result.original_execution_time_ms > 0:
            # Optimized was faster than measurable
            result.speedup_ratio = float("inf")
            result.warnings.append("Optimized query too fast to measure accurately")

        # Calculate % improvement for display
        if result.speedup_ratio > 1:
            pct_improvement = ((result.speedup_ratio - 1) / result.speedup_ratio) * 100
            result.warnings.append(f"Performance improvement: {pct_improvement:.1f}% faster")

        # Compare results
        return self._compare_results(result, original_results, optimized_results)

    def _prepare_dax_for_execution(self, dax: str) -> str:
        """Prepare DAX expression for execution.

        If the DAX is a measure expression (not a query), wraps it in
        EVALUATE ROW() for execution.

        Args:
            dax: DAX expression or query

        Returns:
            DAX query ready for execution
        """
        dax_upper = dax.strip().upper()

        # Already a query - pass through
        if dax_upper.startswith("EVALUATE") or dax_upper.startswith("DEFINE"):
            return dax

        # Measure expression - wrap for execution
        return f'EVALUATE ROW("Result", {dax})'

    def _compare_results(
        self,
        result: DAXEquivalenceResult,
        original_results: list[dict],
        optimized_results: list[dict],
    ) -> DAXEquivalenceResult:
        """Compare two result sets.

        Args:
            result: DAXEquivalenceResult to populate
            original_results: Results from original DAX
            optimized_results: Results from optimized DAX

        Returns:
            Updated DAXEquivalenceResult
        """
        # Check row counts
        result.row_count_match = len(original_results) == len(optimized_results)

        if not result.row_count_match:
            result.equivalent = False
            result.status = "fail"
            result.errors.append(
                f"Row count mismatch: original={len(original_results)}, "
                f"optimized={len(optimized_results)}"
            )
            return result

        # Handle empty results
        if not original_results and not optimized_results:
            result.status = "pass"
            return result

        # Get column names
        original_columns = set(original_results[0].keys()) if original_results else set()
        optimized_columns = set(optimized_results[0].keys()) if optimized_results else set()

        # Check for column mismatches
        missing_columns = original_columns - optimized_columns
        extra_columns = optimized_columns - original_columns

        if missing_columns or extra_columns:
            result.equivalent = False
            result.status = "fail"
            if missing_columns:
                result.errors.append(f"Missing columns: {', '.join(missing_columns)}")
            if extra_columns:
                result.warnings.append(f"Extra columns in optimized: {', '.join(extra_columns)}")

        # Compare values row by row
        common_columns = sorted(original_columns & optimized_columns)
        if common_columns:
            original_results = self._sort_results(original_results, common_columns)
            optimized_results = self._sort_results(optimized_results, common_columns)
        rows_to_check = min(len(original_results), self.max_rows)

        if rows_to_check < len(original_results):
            result.warnings.append(
                f"Only compared first {rows_to_check} of {len(original_results)} rows"
            )

        for i in range(rows_to_check):
            orig_row = original_results[i]
            opt_row = optimized_results[i]

            for col in common_columns:
                orig_val = orig_row.get(col)
                opt_val = opt_row.get(col)

                if not self._values_equal(orig_val, opt_val):
                    result.equivalent = False
                    result.status = "fail"

                    if len(result.sample_mismatches) < self.sample_limit:
                        result.sample_mismatches.append({
                            "row": i,
                            "column": col,
                            "original": self._format_value(orig_val),
                            "optimized": self._format_value(opt_val),
                        })

        if result.equivalent:
            result.status = "pass"

        return result

    def _sort_results(self, rows: list[dict], columns: list[str]) -> list[dict]:
        """Sort result rows by a deterministic column order for comparison."""
        def sort_key(row: dict) -> tuple:
            return tuple(self._sortable_value(row.get(col)) for col in columns)

        try:
            return sorted(rows, key=sort_key)
        except TypeError:
            # Mixed types that cannot be sorted reliably; keep original order.
            return rows

    def _sortable_value(self, val: Any) -> tuple[int, object]:
        """Normalize values into sortable tuples without user-provided ordering."""
        if val is None:
            return (0, "")
        if isinstance(val, bool):
            return (1, int(val))
        if isinstance(val, (int, Decimal)):
            return (2, float(val))
        if isinstance(val, float):
            if math.isnan(val):
                return (3, "nan")
            if math.isinf(val):
                return (3, "inf" if val > 0 else "-inf")
            return (2, val)
        if isinstance(val, str):
            return (4, val)
        return (5, str(val))

    def _values_equal(self, a: Any, b: Any) -> bool:
        """Compare two values with type-aware logic.

        Handles:
        - NULL/None comparison
        - Exact match mode (tolerance=0) or tolerance-based comparison
        - Numeric type coercion
        - NaN and infinity handling
        """
        # Both None
        if a is None and b is None:
            return True

        # One None
        if a is None or b is None:
            return False

        # Exact match mode
        if self.exact_match or self.tolerance == 0.0:
            # For floats, still need to handle NaN specially
            if isinstance(a, float) and isinstance(b, float):
                if math.isnan(a) and math.isnan(b):
                    return True
                if math.isnan(a) or math.isnan(b):
                    return False
                return a == b  # Exact float comparison
            # Direct comparison for same types
            if type(a) == type(b):
                return a == b
            # Numeric coercion for exact comparison
            if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
                try:
                    return float(a) == float(b)
                except (TypeError, ValueError):
                    pass
            return str(a) == str(b)

        # Tolerance-based comparison (legacy mode)
        if type(a) == type(b):
            if isinstance(a, float):
                return self._float_equal(a, b)
            return a == b

        # Try numeric coercion
        if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
            try:
                fa = float(a)
                fb = float(b)
                return self._float_equal(fa, fb)
            except (TypeError, ValueError):
                pass

        # String coercion as last resort
        return str(a) == str(b)

    def _float_equal(self, a: float, b: float) -> bool:
        """Compare floats with tolerance.

        Uses hybrid tolerance: |a - b| <= tol + tol * max(|a|, |b|)
        """
        # Handle NaN
        if math.isnan(a) and math.isnan(b):
            return True
        if math.isnan(a) or math.isnan(b):
            return False

        # Handle infinity
        if math.isinf(a) and math.isinf(b):
            return (a > 0) == (b > 0)  # Same sign infinity
        if math.isinf(a) or math.isinf(b):
            return False

        # Hybrid tolerance
        return abs(a - b) <= self.tolerance + self.tolerance * max(abs(a), abs(b))

    def _format_value(self, val: Any) -> str:
        """Format a value for display in mismatch report."""
        if val is None:
            return "BLANK"
        if isinstance(val, float):
            if math.isnan(val):
                return "NaN"
            return f"{val:.6f}"
        if isinstance(val, str) and len(val) > 50:
            return val[:47] + "..."
        return str(val)


def create_dax_equivalence_validator(
    connection: "PBIDesktopConnection",
    tolerance: float = 1e-9,
    max_rows: int = 10000,
    sample_limit: int = 5,
    warmup_runs: int = 1,
) -> DAXEquivalenceValidator:
    """Factory function to create a DAX equivalence validator.

    Args:
        connection: PBIDesktopConnection instance
        tolerance: Numeric tolerance for float comparison
        max_rows: Maximum rows to compare
        sample_limit: Maximum sample mismatches to include
        warmup_runs: Number of times to run each query (min time used)

    Returns:
        Configured DAXEquivalenceValidator
    """
    return DAXEquivalenceValidator(
        connection=connection,
        tolerance=tolerance,
        max_rows_to_compare=max_rows,
        sample_mismatch_limit=sample_limit,
        warmup_runs=warmup_runs,
    )
