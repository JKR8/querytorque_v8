"""Benchmarking harness for MCTS (trimmed mean latency)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from qt_sql.execution.duckdb_executor import DuckDBExecutor


@dataclass
class BenchmarkResult:
    latency_s: float
    timed_out: bool
    raw_timings_s: list[float]


class BenchmarkRunner:
    """Execute queries and compute trimmed-mean latency."""

    def __init__(
        self,
        database: str,
        runs: int = 3,
        dialect: str = "duckdb",
        use_cache: bool = True,
    ):
        self.database = database
        self.runs = runs
        self.dialect = dialect
        self.use_cache = use_cache
        self._cache: dict[tuple[str, Optional[float]], BenchmarkResult] = {}

    def _timed_execute(self, sql: str) -> float:
        start = time.perf_counter()
        with DuckDBExecutor(self.database, read_only=True) as executor:
            executor.execute(sql)
        return time.perf_counter() - start

    def run_query_robust(
        self,
        sql: str,
        timeout_s: Optional[float] = None,
    ) -> BenchmarkResult:
        """Run query multiple times and return trimmed latency.

        Default behavior: 3 runs, discard the first run (warm-up), average the rest.
        """
        if self.use_cache:
            cache_key = (sql, timeout_s)
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        timings: list[float] = []

        for _ in range(self.runs):
            if timeout_s is None:
                timings.append(self._timed_execute(sql))
                continue

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._timed_execute, sql)
                try:
                    elapsed = future.result(timeout=timeout_s)
                    timings.append(elapsed)
                except FuturesTimeoutError:
                    return BenchmarkResult(
                        latency_s=timeout_s,
                        timed_out=True,
                        raw_timings_s=timings,
                    )

        if len(timings) >= 2:
            trimmed = timings[1:]  # discard first run (warm-up)
        else:
            trimmed = timings

        latency = sum(trimmed) / max(len(trimmed), 1)

        result = BenchmarkResult(
            latency_s=latency,
            timed_out=False,
            raw_timings_s=timings,
        )
        if self.use_cache:
            self._cache[cache_key] = result
        return result
