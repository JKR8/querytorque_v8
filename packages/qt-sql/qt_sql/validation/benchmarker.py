"""Query benchmarker implementing 3-run timing (discard first, average last 2)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional

from ..execution import DuckDBExecutor
from .schemas import CostResult, QueryExecutionResult, TimingResult


@dataclass
class BenchmarkResult:
    """Result from benchmarking a pair of queries."""

    original: QueryExecutionResult
    optimized: QueryExecutionResult
    speedup: float  # original_time / optimized_time (>1 means improvement)


class QueryBenchmarker:
    """Benchmarks query pairs using 3-run timing pattern.

    The 3-run pattern (per query):
    1. Run query (warm-up, discard timing)
    2. Run query (measure 1)
    3. Run query (measure 2)
    4. Average measures 1 and 2

    This pattern eliminates cache/JIT warmup effects and provides
    stable, reproducible timing via averaging.
    """

    def __init__(self, executor: DuckDBExecutor):
        """Initialize benchmarker.

        Args:
            executor: DuckDB executor to use for query execution.
        """
        self.executor = executor

    def _execute_timed(
        self, sql: str, capture_results: bool = False
    ) -> tuple[float, list[dict[str, Any]]]:
        """Execute a query and return timing and results.

        Args:
            sql: SQL query to execute.
            capture_results: If True, return full result rows.

        Returns:
            Tuple of (execution_time_ms, rows).
        """
        start = time.perf_counter()
        rows = self.executor.execute(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000

        if not capture_results:
            # Return just the count, not full rows
            return elapsed_ms, [{"__count__": len(rows)}]

        return elapsed_ms, rows

    def _get_cost(self, sql: str) -> CostResult:
        """Get cost estimate for a query.

        Args:
            sql: SQL query.

        Returns:
            CostResult with estimated cost.
        """
        try:
            cost = self.executor.get_cost_estimate(sql)
            return CostResult(estimated_cost=cost)
        except Exception:
            return CostResult(estimated_cost=float("inf"))

    def benchmark_single(
        self, sql: str, capture_results: bool = False
    ) -> QueryExecutionResult:
        """Benchmark a single query: 3 runs, discard first, average last 2.

        Args:
            sql: SQL query to benchmark.
            capture_results: If True, capture full result rows.

        Returns:
            QueryExecutionResult with timing and results.
        """
        try:
            # Run 1: Warm-up (discard)
            warmup_ms, _ = self._execute_timed(sql, capture_results=False)

            # Run 2: Measure 1
            measure1_ms, rows = self._execute_timed(sql, capture_results=capture_results)

            # Run 3: Measure 2
            measure2_ms, _ = self._execute_timed(sql, capture_results=False)

            # Average of runs 2 and 3
            measured_ms = (measure1_ms + measure2_ms) / 2

            # Get cost
            cost = self._get_cost(sql)

            # Determine row count
            if capture_results:
                row_count = len(rows)
            else:
                row_count = rows[0].get("__count__", 0) if rows else 0

            return QueryExecutionResult(
                timing=TimingResult(warmup_time_ms=warmup_ms, measured_time_ms=measured_ms),
                cost=cost,
                row_count=row_count,
                rows=rows if capture_results else None,
            )

        except Exception as e:
            return QueryExecutionResult(
                timing=TimingResult(warmup_time_ms=0, measured_time_ms=0),
                cost=CostResult(estimated_cost=float("inf")),
                row_count=0,
                error=str(e),
            )

    def benchmark_pair(
        self,
        original_sql: str,
        optimized_sql: str,
        capture_results: bool = True,
    ) -> BenchmarkResult:
        """Benchmark a pair of queries: 3 runs each, discard first, average last 2.

        Pattern per query:
        1. Warm-up (discard)
        2. Measure 1
        3. Measure 2
        4. Return average of measures 1 and 2

        Args:
            original_sql: Original SQL query.
            optimized_sql: Optimized SQL query.
            capture_results: If True, capture full result rows for comparison.

        Returns:
            BenchmarkResult with timing for both queries.
        """
        try:
            # Original: 3 runs
            original_warmup_ms, _ = self._execute_timed(original_sql, capture_results=False)
            original_m1_ms, original_rows = self._execute_timed(original_sql, capture_results=capture_results)
            original_m2_ms, _ = self._execute_timed(original_sql, capture_results=False)
            original_measured_ms = (original_m1_ms + original_m2_ms) / 2

            # Optimized: 3 runs
            optimized_warmup_ms, _ = self._execute_timed(optimized_sql, capture_results=False)
            optimized_m1_ms, optimized_rows = self._execute_timed(optimized_sql, capture_results=capture_results)
            optimized_m2_ms, _ = self._execute_timed(optimized_sql, capture_results=False)
            optimized_measured_ms = (optimized_m1_ms + optimized_m2_ms) / 2

            # Get costs
            original_cost = self._get_cost(original_sql)
            optimized_cost = self._get_cost(optimized_sql)

            # Build results
            original_result = QueryExecutionResult(
                timing=TimingResult(
                    warmup_time_ms=original_warmup_ms,
                    measured_time_ms=original_measured_ms,
                ),
                cost=original_cost,
                row_count=len(original_rows) if capture_results else original_rows[0].get("__count__", 0),
                rows=original_rows if capture_results else None,
            )

            optimized_result = QueryExecutionResult(
                timing=TimingResult(
                    warmup_time_ms=optimized_warmup_ms,
                    measured_time_ms=optimized_measured_ms,
                ),
                cost=optimized_cost,
                row_count=len(optimized_rows) if capture_results else optimized_rows[0].get("__count__", 0),
                rows=optimized_rows if capture_results else None,
            )

            # Calculate speedup
            if optimized_measured_ms > 0:
                speedup = original_measured_ms / optimized_measured_ms
            else:
                speedup = float("inf") if original_measured_ms > 0 else 1.0

            return BenchmarkResult(
                original=original_result,
                optimized=optimized_result,
                speedup=speedup,
            )

        except Exception as e:
            # Return error result
            error_result = QueryExecutionResult(
                timing=TimingResult(warmup_time_ms=0, measured_time_ms=0),
                cost=CostResult(estimated_cost=float("inf")),
                row_count=0,
                error=str(e),
            )
            return BenchmarkResult(
                original=error_result,
                optimized=error_result,
                speedup=1.0,
            )

    def benchmark_pair_with_checksum(
        self,
        original_sql: str,
        optimized_sql: str,
    ) -> BenchmarkResult:
        """Benchmark pair and compute checksums for validation.

        Same as benchmark_pair but also computes result checksums
        for equivalence checking.

        Args:
            original_sql: Original SQL query.
            optimized_sql: Optimized SQL query.

        Returns:
            BenchmarkResult with checksums in results.
        """
        from .equivalence_checker import EquivalenceChecker

        result = self.benchmark_pair(original_sql, optimized_sql, capture_results=True)

        # Compute checksums
        checker = EquivalenceChecker()

        if result.original.rows is not None:
            result.original.checksum = checker.compute_checksum(result.original.rows)

        if result.optimized.rows is not None:
            result.optimized.checksum = checker.compute_checksum(result.optimized.rows)

        return result
