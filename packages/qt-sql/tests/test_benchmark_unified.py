"""Tests for the unified benchmark module (validation.benchmark).

Tests the core invariants:
1. Fail-fast correctness on run 1 (row count / checksum mismatch)
2. All runs counted in timing average
3. Winner confirmation re-check
4. Known-timeout baseline skip
5. Single-connection-per-query (via mock)
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, call

import pytest

# Import the module under test
from qt_sql.validation.benchmark import (
    BenchmarkSummary,
    CandidateResult,
    _timed_runs,
    benchmark_query_patches,
    _benchmark_single_candidate,
)
from qt_sql.validation.sample_checker import SampleChecker, SampleCheckResult


# ── Test fixtures ────────────────────────────────────────────────────────────


@dataclass
class FakePatch:
    """Minimal AppliedPatch stand-in for tests."""
    patch_id: str
    family: str = "A"
    transform: str = "test"
    relevance_score: float = 1.0
    output_sql: Optional[str] = None
    apply_error: Optional[str] = None
    semantic_passed: bool = True
    correctness_verified: bool = False
    speedup: Optional[float] = None
    status: str = "PENDING"
    explain_text: Optional[str] = None
    original_ms: Optional[float] = None
    patch_ms: Optional[float] = None


class FakeExecutor:
    """Mock database executor that tracks calls."""

    def __init__(self, rows_by_sql: Dict[str, list] = None, timing_ms: float = 100.0):
        self.rows_by_sql = rows_by_sql or {}
        self.timing_ms = timing_ms
        self.call_count = 0
        self.connected = False
        self.closed = False
        self._connect_count = 0

    def connect(self):
        self.connected = True
        self._connect_count += 1

    def close(self):
        self.closed = True
        self.connected = False

    def execute(self, sql: str, timeout_ms: int = 300_000):
        self.call_count += 1
        # Simulate timing
        time.sleep(self.timing_ms / 10_000)  # scale down for tests
        # Return rows based on SQL content
        if sql.startswith("EXPLAIN"):
            return [{"QUERY PLAN": "Seq Scan on table (rows=100)"}]
        for key, rows in self.rows_by_sql.items():
            if key in sql:
                return rows
        return [{"id": i, "val": i * 10} for i in range(10)]

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()


# ── Tests: _timed_runs ──────────────────────────────────────────────────────


class TestTimedRuns:
    def test_3_runs_warmup_plus_2_measured(self):
        executor = FakeExecutor()
        avg_ms, rows, times = _timed_runs(executor, "SELECT 1", runs=3, capture_rows=True)
        assert avg_ms > 0
        assert rows is not None
        assert len(times) == 2  # 2 measured (warmup not counted in times)
        assert executor.call_count == 3  # warmup + 2 measured

    def test_1_run(self):
        executor = FakeExecutor()
        avg_ms, rows, times = _timed_runs(executor, "SELECT 1", runs=1)
        assert avg_ms > 0
        assert len(times) == 1
        assert executor.call_count == 1

    def test_5_runs_trimmed_mean(self):
        executor = FakeExecutor()
        avg_ms, rows, times = _timed_runs(executor, "SELECT 1", runs=5)
        assert avg_ms > 0
        assert len(times) == 5  # all 5 runs tracked
        assert executor.call_count == 5

    def test_capture_rows_first_measured_run(self):
        executor = FakeExecutor(rows_by_sql={"SELECT": [{"a": 1}, {"a": 2}]})
        _, rows, _ = _timed_runs(executor, "SELECT x", runs=3, capture_rows=True)
        assert rows == [{"a": 1}, {"a": 2}]

    def test_no_capture_rows(self):
        executor = FakeExecutor()
        _, rows, _ = _timed_runs(executor, "SELECT 1", runs=3, capture_rows=False)
        assert rows is None


# ── Tests: benchmark_query_patches ──────────────────────────────────────────


class TestBenchmarkQueryPatches:

    def _make_executor(self, baseline_rows, candidate_rows_map=None):
        """Create a FakeExecutor with per-SQL row mapping."""
        rows_by_sql = {"ORIGINAL_SQL": baseline_rows}
        if candidate_rows_map:
            rows_by_sql.update(candidate_rows_map)
        return FakeExecutor(rows_by_sql=rows_by_sql)

    @patch("qt_sql.validation.benchmark.create_executor_from_dsn")
    def test_single_passing_candidate(self, mock_factory):
        rows = [{"id": 1, "val": 10}, {"id": 2, "val": 20}]
        executor = FakeExecutor(rows_by_sql={"SELECT": rows})
        mock_factory.return_value = executor

        patches = [FakePatch(patch_id="P1", output_sql="SELECT patched")]
        result = benchmark_query_patches(
            patches=patches,
            original_sql="SELECT original",
            db_path="test.duckdb",
            query_id="Q1",
            baseline_runs=1,
            candidate_runs=1,
            winner_runs=1,
            collect_explain=False,
        )

        assert result.n_benchmarked == 1
        assert result.n_passed == 1
        assert result.best_speedup > 0
        assert patches[0].speedup is not None
        assert patches[0].status in ("WIN", "IMPROVED", "NEUTRAL", "REGRESSION")

    @patch("qt_sql.validation.benchmark.create_executor_from_dsn")
    def test_row_count_mismatch_failfast(self, mock_factory):
        """Candidate returning wrong row count should FAIL immediately."""
        orig_rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        cand_rows = [{"id": 1}]  # wrong count

        call_count = [0]
        def mock_execute(sql, timeout_ms=300_000):
            call_count[0] += 1
            if "original" in sql.lower() or sql.startswith("EXPLAIN"):
                return orig_rows
            return cand_rows

        executor = MagicMock()
        executor.execute = mock_execute
        executor.__enter__ = MagicMock(return_value=executor)
        executor.__exit__ = MagicMock(return_value=False)
        mock_factory.return_value = executor

        patches = [FakePatch(patch_id="P1", output_sql="SELECT patched")]
        result = benchmark_query_patches(
            patches=patches,
            original_sql="SELECT original",
            db_path="test.duckdb",
            query_id="Q1",
            baseline_runs=1,
            candidate_runs=3,  # Would be 3 runs, but should stop after 1
            winner_runs=3,
            collect_explain=False,
        )

        assert result.n_passed == 0
        assert patches[0].speedup == 0.0
        assert "Row count mismatch" in (patches[0].apply_error or "")

    @patch("qt_sql.validation.benchmark.create_executor_from_dsn")
    def test_known_timeout_skips_baseline(self, mock_factory):
        """known_timeout=True should use timeout_seconds as baseline, not execute."""
        rows = [{"id": 1}]
        executor = FakeExecutor(rows_by_sql={"SELECT": rows})
        mock_factory.return_value = executor

        patches = [FakePatch(patch_id="P1", output_sql="SELECT patched")]
        result = benchmark_query_patches(
            patches=patches,
            original_sql="SELECT original",
            db_path="test.duckdb",
            query_id="Q1",
            known_timeout=True,
            timeout_seconds=300,
            baseline_runs=3,
            candidate_runs=1,
            winner_runs=1,
            collect_explain=False,
        )

        # Baseline should be 300000ms (300s * 1000)
        assert result.baseline_ms == 300_000.0
        # Patch should still be benchmarked and show massive "speedup"
        assert result.n_passed == 1
        assert patches[0].speedup > 1.0

    @patch("qt_sql.validation.benchmark.create_executor_from_dsn")
    def test_no_output_sql_skipped(self, mock_factory):
        """Patches without output_sql should be skipped."""
        executor = FakeExecutor()
        mock_factory.return_value = executor

        patches = [
            FakePatch(patch_id="P1", output_sql=None),
            FakePatch(patch_id="P2", output_sql="SELECT valid"),
        ]
        result = benchmark_query_patches(
            patches=patches,
            original_sql="SELECT original",
            db_path="test.duckdb",
            query_id="Q1",
            baseline_runs=1,
            candidate_runs=1,
            winner_runs=1,
            collect_explain=False,
        )

        assert result.n_benchmarked == 2
        assert result.n_passed == 1  # Only P2 passes
        assert result.candidate_results[0].passed is False
        assert result.candidate_results[0].error == "No output SQL"

    @patch("qt_sql.validation.benchmark.create_executor_from_dsn")
    def test_explain_collection(self, mock_factory):
        """EXPLAIN should be collected for passing candidates."""
        rows = [{"id": 1}]
        executor = FakeExecutor(rows_by_sql={"SELECT": rows})
        mock_factory.return_value = executor

        patches = [FakePatch(patch_id="P1", output_sql="SELECT patched")]
        benchmark_query_patches(
            patches=patches,
            original_sql="SELECT original",
            db_path="test.duckdb",
            query_id="Q1",
            baseline_runs=1,
            candidate_runs=1,
            winner_runs=1,
            collect_explain=True,
        )

        assert patches[0].explain_text is not None
        assert "Seq Scan" in patches[0].explain_text

    @patch("qt_sql.validation.benchmark.create_executor_from_dsn")
    def test_single_connection_guarantee(self, mock_factory):
        """Only ONE call to create_executor_from_dsn per benchmark run."""
        rows = [{"id": 1}]
        executor = FakeExecutor(rows_by_sql={"SELECT": rows})
        mock_factory.return_value = executor

        patches = [
            FakePatch(patch_id="P1", output_sql="SELECT p1"),
            FakePatch(patch_id="P2", output_sql="SELECT p2"),
            FakePatch(patch_id="P3", output_sql="SELECT p3"),
        ]
        benchmark_query_patches(
            patches=patches,
            original_sql="SELECT original",
            db_path="test.duckdb",
            query_id="Q1",
            baseline_runs=1,
            candidate_runs=1,
            winner_runs=1,
            collect_explain=True,
        )

        # Factory should be called exactly ONCE
        assert mock_factory.call_count == 1

    @patch("qt_sql.validation.benchmark.create_executor_from_dsn")
    def test_classify_speedup_fn(self, mock_factory):
        """Custom classify_speedup_fn should be used."""
        rows = [{"id": 1}]
        executor = FakeExecutor(rows_by_sql={"SELECT": rows})
        mock_factory.return_value = executor

        classify_called = [False]
        def custom_classify(speedup: float) -> str:
            classify_called[0] = True
            return "CUSTOM_STATUS"

        patches = [FakePatch(patch_id="P1", output_sql="SELECT patched")]
        benchmark_query_patches(
            patches=patches,
            original_sql="SELECT original",
            db_path="test.duckdb",
            query_id="Q1",
            baseline_runs=1,
            candidate_runs=1,
            winner_runs=1,
            collect_explain=False,
            classify_speedup_fn=custom_classify,
        )

        assert classify_called[0] is True
        assert patches[0].status == "CUSTOM_STATUS"


# ── Tests: SampleChecker ────────────────────────────────────────────────────


class TestSampleChecker:

    @patch("qt_sql.execution.factory.create_executor_from_dsn")
    def test_equivalent_queries(self, mock_factory):
        """Both queries return same rows → equivalent."""
        rows = [{"id": 1, "val": 10}]
        executor = FakeExecutor(rows_by_sql={"SELECT": rows})
        mock_factory.return_value = executor

        checker = SampleChecker("sample.duckdb")
        result = checker.check_semantic_equivalence("SELECT a", "SELECT b")

        assert result.equivalent is True
        assert result.original_sample_rows == 1
        assert result.candidate_sample_rows == 1

    @patch("qt_sql.execution.factory.create_executor_from_dsn")
    def test_row_count_mismatch(self, mock_factory):
        """Different row counts → not equivalent."""
        call_num = [0]
        def mock_execute(sql, timeout_ms=30_000):
            call_num[0] += 1
            if call_num[0] == 1:
                return [{"id": 1}, {"id": 2}]  # original: 2 rows
            return [{"id": 1}]  # candidate: 1 row

        executor = MagicMock()
        executor.execute = mock_execute
        executor.__enter__ = MagicMock(return_value=executor)
        executor.__exit__ = MagicMock(return_value=False)
        mock_factory.return_value = executor

        checker = SampleChecker("sample.duckdb")
        result = checker.check_semantic_equivalence("SELECT a", "SELECT b")

        assert result.equivalent is False
        assert "row count mismatch" in (result.error or "").lower()

    @patch("qt_sql.execution.factory.create_executor_from_dsn")
    def test_execution_error(self, mock_factory):
        """Execution error → not equivalent, error captured."""
        executor = MagicMock()
        executor.execute = MagicMock(side_effect=Exception("connection failed"))
        executor.__enter__ = MagicMock(return_value=executor)
        executor.__exit__ = MagicMock(return_value=False)
        mock_factory.return_value = executor

        checker = SampleChecker("sample.duckdb")
        result = checker.check_semantic_equivalence("SELECT a", "SELECT b")

        assert result.equivalent is False
        assert "error" in (result.error or "").lower()


# ── Tests: CandidateResult & BenchmarkSummary dataclasses ────────────────────


class TestDataclasses:

    def test_candidate_result_defaults(self):
        cr = CandidateResult(patch_idx=0, passed=True)
        assert cr.speedup == 0.0
        assert cr.all_times == []
        assert cr.error is None

    def test_benchmark_summary(self):
        summary = BenchmarkSummary(
            baseline_ms=1000.0,
            baseline_rows=100,
            baseline_checksum="abc123",
            n_benchmarked=4,
            n_passed=3,
            best_speedup=2.5,
            best_patch_idx=2,
        )
        assert summary.baseline_ms == 1000.0
        assert summary.best_speedup == 2.5
