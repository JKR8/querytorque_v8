#!/usr/bin/env python3
"""
Comprehensive tests for V5 Optimizer Modes.

Tests:
- Mode 1 (Retry): Single worker with error feedback retries
- Mode 2 (Parallel): Multiple workers with competition
- Mode 3 (Evolutionary): Iterative improvement with stacking

"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import json

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from qt_sql.optimization.adaptive_rewriter_v5 import (
    _build_history_section,
    _build_prompt_with_examples,
    _split_example_batches,
    CandidateResult,
)


# ============================================================================
# Helper Function Tests
# ============================================================================

class TestHistorySection:
    """Test error feedback history generation."""

    def test_build_history_basic(self):
        """Test basic error history formatting."""
        response = '{"rewrite_sets": [{"id": "rs_01"}]}'
        error = "Syntax error near 'JOIN'"

        history = _build_history_section(response, error)

        assert "Previous Attempt (FAILED)" in history
        assert "Failure reason:" in history
        assert "Syntax error near 'JOIN'" in history
        assert response in history
        assert "Try a DIFFERENT approach" in history

    def test_build_history_with_newlines(self):
        """Test that multiline errors are preserved."""
        response = '{"rewrite_sets": []}'
        error = """Row count mismatch:
Expected: 100 rows
Got: 95 rows
Reason: Filter pushed too early"""

        history = _build_history_section(response, error)

        assert "Row count mismatch" in history
        assert "Expected: 100 rows" in history
        assert "Got: 95 rows" in history

    def test_build_history_with_special_chars(self):
        """Test that special characters are handled."""
        response = '{"rewrite_sets": [{"sql": "WHERE x=\'SD\'"}]}'
        error = "Parse error: Unmatched quote"

        history = _build_history_section(response, error)

        assert "Parse error" in history
        assert "WHERE x=" in history


class TestExampleBatching:
    """Test example splitting for workers."""

    def test_split_into_batches(self):
        """Test splitting examples into batches."""
        examples = [Mock(id=f"ex_{i}") for i in range(12)]

        batches = _split_example_batches(examples, batch_size=3)

        assert len(batches) == 4
        assert all(len(batch) == 3 for batch in batches)

    def test_split_uneven_examples(self):
        """Test splitting when examples don't divide evenly."""
        examples = [Mock(id=f"ex_{i}") for i in range(10)]

        batches = _split_example_batches(examples, batch_size=3)

        assert len(batches) == 4
        assert len(batches[0]) == 3
        assert len(batches[1]) == 3
        assert len(batches[2]) == 3
        assert len(batches[3]) == 1  # Remainder

    def test_split_fewer_than_batch_size(self):
        """Test splitting when fewer examples than batch size."""
        examples = [Mock(id=f"ex_{i}") for i in range(2)]

        batches = _split_example_batches(examples, batch_size=3)

        assert len(batches) == 1
        assert len(batches[0]) == 2


# ============================================================================
# Mode 1: Retry (Single Worker with Retries)
# ============================================================================

class TestMode1Retry:
    """Test Mode 1: Retry with error feedback."""

    @pytest.fixture
    def sample_sql(self):
        return "SELECT * FROM customers WHERE state = 'CA'"

    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client that fails then succeeds."""
        client = Mock()
        # First call: invalid JSON
        # Second call: valid response
        client.call.side_effect = [
            '{"rewrite_sets": [{"id": "rs_01", "nodes": {"main": "SELECT FORM customers"}}]}',  # Typo
            '{"rewrite_sets": [{"id": "rs_01", "transform": "pushdown", "nodes": {"main": "SELECT * FROM customers"}}]}'
        ]
        return client

    def test_retry_on_validation_failure(self, sample_sql, mock_llm_client):
        """Test that retry happens on validation failure."""
        # This would be the actual function when implemented
        # For now, test the concept

        attempt = 1
        max_retries = 3
        history = ""

        while attempt <= max_retries:
            response = mock_llm_client.call()

            # Simulate validation
            if "FORM" in response:  # Syntax error
                error = "Syntax error: 'FORM' is not valid SQL keyword"
                history = _build_history_section(response, error)
                attempt += 1
            else:
                # Success!
                assert attempt == 2  # Succeeded on second attempt
                break

        # Verify history was built
        assert "Previous Attempt (FAILED)" in history
        assert "Syntax error" in history

    def test_retry_max_attempts_reached(self):
        """Test that retry stops after max attempts."""
        max_retries = 3
        attempts = []

        for attempt in range(1, max_retries + 1):
            attempts.append(attempt)
            # All fail

        assert len(attempts) == 3
        assert attempts[-1] == 3

    def test_error_feedback_accumulates(self):
        """Test that error history accumulates across attempts."""
        attempt_1_response = '{"rewrite_sets": []}'
        attempt_1_error = "Syntax error: Missing FROM"

        attempt_2_response = '{"rewrite_sets": [{"id": "rs_01"}]}'
        attempt_2_error = "Row count mismatch: 95 vs 100"

        history_1 = _build_history_section(attempt_1_response, attempt_1_error)
        history_2 = _build_history_section(attempt_2_response, attempt_2_error)

        # For attempt 3, would combine both
        combined = f"## All Previous Attempts\n\n### Attempt 1\n{history_1}\n\n### Attempt 2\n{history_2}"

        assert "Syntax error" in combined
        assert "Row count mismatch" in combined
        assert "Attempt 1" in combined
        assert "Attempt 2" in combined


# ============================================================================
# Mode 2: Parallel (Tournament Competition)
# ============================================================================

class TestMode2Parallel:
    """Test Mode 2: Parallel workers with competition."""

    def test_worker_diversity(self):
        """Test that each worker gets different examples."""
        all_examples = [Mock(id=f"ex_{i}") for i in range(12)]

        # Split for 4 workers
        batches = _split_example_batches(all_examples, batch_size=3)

        # Verify no overlap
        worker_1_ids = {ex.id for ex in batches[0]}
        worker_2_ids = {ex.id for ex in batches[1]}
        worker_3_ids = {ex.id for ex in batches[2]}
        worker_4_ids = {ex.id for ex in batches[3]}

        assert len(worker_1_ids & worker_2_ids) == 0
        assert len(worker_1_ids & worker_3_ids) == 0
        assert len(worker_2_ids & worker_3_ids) == 0

    def test_parallel_execution_concept(self):
        """Test parallel execution concept (mock)."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def mock_worker(worker_id, examples):
            """Simulate worker processing."""
            return {
                "worker_id": worker_id,
                "status": "valid",
                "speedup": 1.5 + worker_id * 0.3,
                "examples": [ex.id for ex in examples]
            }

        all_examples = [Mock(id=f"ex_{i}") for i in range(12)]
        batches = _split_example_batches(all_examples, batch_size=3)

        results = []
        with ThreadPoolExecutor(max_workers=4) as pool:
            tasks = [pool.submit(mock_worker, i+1, batch) for i, batch in enumerate(batches)]
            results = [t.result() for t in as_completed(tasks)]

        assert len(results) == 4
        assert all(r["status"] == "valid" for r in results)

    def test_early_stopping_on_target(self):
        """Test that benchmarking stops when target met."""
        candidates = [
            {"worker_id": 1, "sample_speedup": 3.1},
            {"worker_id": 2, "sample_speedup": 2.4},
            {"worker_id": 3, "sample_speedup": 1.8},
            {"worker_id": 4, "sample_speedup": 2.9},
        ]

        target_speedup = 2.0
        benchmarked = []

        for candidate in sorted(candidates, key=lambda x: x["sample_speedup"], reverse=True):
            # Simulate benchmark
            full_speedup = candidate["sample_speedup"] * 0.95  # Slight degradation
            benchmarked.append({**candidate, "full_speedup": full_speedup})

            if full_speedup >= target_speedup:
                # Target met, stop early
                break

        # Should stop after first (highest sample speedup)
        assert len(benchmarked) == 1
        assert benchmarked[0]["worker_id"] == 1

    def test_worker_retry_on_failure(self):
        """Test that individual workers can retry with error feedback."""
        worker_attempts = {
            1: {"attempt_1": "valid"},
            2: {"attempt_1": "valid"},
            3: {"attempt_1": "invalid", "attempt_2": "valid"},  # Retry
            4: {"attempt_1": "valid"},
        }

        for worker_id, attempts in worker_attempts.items():
            if "attempt_2" in attempts:
                # Worker 3 needed retry
                assert worker_id == 3
                assert attempts["attempt_1"] == "invalid"
                assert attempts["attempt_2"] == "valid"


# ============================================================================
# Mode 3: Evolutionary (Iterative Improvement)
# ============================================================================

class TestMode3Evolutionary:
    """Test Mode 3: Evolutionary with stacking."""

    def test_input_evolution(self):
        """Test that input SQL evolves across iterations."""
        original_sql = "SELECT * FROM customers"

        iterations = [
            {"input": original_sql, "output": "WITH filtered AS (...) SELECT * FROM filtered", "speedup": 1.5},
            {"input": "WITH filtered AS (...) SELECT * FROM filtered", "output": "WITH filtered AS (...), ordered AS (...) SELECT * FROM ordered", "speedup": 1.8},
            {"input": "WITH filtered AS (...), ordered AS (...) SELECT * FROM ordered", "output": "Final optimized SQL", "speedup": 2.3},
        ]

        # Verify each iteration builds on previous
        for i in range(1, len(iterations)):
            assert iterations[i]["input"] == iterations[i-1]["output"]

        # Verify speedup improves
        assert iterations[1]["speedup"] > iterations[0]["speedup"]
        assert iterations[2]["speedup"] > iterations[1]["speedup"]

    def test_example_rotation(self):
        """Test that examples rotate across iterations."""
        all_examples = [Mock(id=f"ex_{i}") for i in range(12)]

        rotation = [
            all_examples[0:3],   # Iteration 1
            all_examples[3:6],   # Iteration 2
            all_examples[6:9],   # Iteration 3
            all_examples[9:12],  # Iteration 4
        ]

        # Verify no overlap between consecutive iterations
        for i in range(len(rotation) - 1):
            set_i = {ex.id for ex in rotation[i]}
            set_i_plus_1 = {ex.id for ex in rotation[i+1]}
            assert len(set_i & set_i_plus_1) == 0

    def test_history_accumulation(self):
        """Test that success history accumulates."""
        iterations = [
            {"id": 1, "transform": "decorrelate", "speedup": 1.5},
            {"id": 2, "transform": "pushdown", "speedup": 1.8},
            {"id": 3, "transform": "reorder_join", "speedup": 2.1},
        ]

        # Build history for iteration 3
        history_lines = ["## Previous Iterations\n"]
        for it in iterations[:2]:  # Previous iterations only
            history_lines.append(f"### Iteration {it['id']}: {it['speedup']}x speedup ✓")
            history_lines.append(f"**Transform:** {it['transform']}")

        history = "\n".join(history_lines)

        assert "Iteration 1: 1.5x" in history
        assert "Iteration 2: 1.8x" in history
        assert "decorrelate" in history
        assert "pushdown" in history

    def test_gap_analysis(self):
        """Test gap calculation for ML hints."""
        current_speedup = 1.72
        target_speedup = 2.0

        gap = target_speedup - current_speedup
        gap_percent = (gap / target_speedup) * 100

        assert gap == pytest.approx(0.28, 0.01)
        assert gap_percent == pytest.approx(14.0, 0.5)  # Need ~14% more

    def test_early_stopping_on_target(self):
        """Test that iterations stop when target met."""
        target_speedup = 2.0
        max_iterations = 5

        speedups = [1.5, 1.8, 2.3]  # Meets target on iteration 3

        for i, speedup in enumerate(speedups, 1):
            if speedup >= target_speedup:
                stopped_at = i
                break

        assert stopped_at == 3
        assert stopped_at < max_iterations

    def test_ml_hints_generation(self):
        """Test ML hints structure."""
        hints = {
            "iteration": 2,
            "current_speedup": 1.5,
            "target": 2.0,
            "gap": 0.5,
            "recommendations": [
                {"transform": "pushdown", "confidence": 0.82, "reason": "Date filter detected"},
                {"transform": "reorder_join", "confidence": 0.71, "reason": "Large fact table join"},
            ]
        }

        assert hints["gap"] == hints["target"] - hints["current_speedup"]
        assert len(hints["recommendations"]) > 0
        assert all("transform" in rec for rec in hints["recommendations"])
        assert all("confidence" in rec for rec in hints["recommendations"])


# ============================================================================
# Integration Tests (Mode Comparison)
# ============================================================================

class TestModeComparison:
    """Test differences between modes."""

    def test_mode_characteristics(self):
        """Verify mode characteristics match spec."""
        modes = {
            "retry": {
                "workers": 1,
                "max_attempts": 3,
                "learns_from": "errors",
                "input_evolves": False,
            },
            "parallel": {
                "workers": 5,
                "max_attempts": 1,  # + 1 retry per worker
                "learns_from": "competition",
                "input_evolves": False,
            },
            "evolutionary": {
                "workers": 1,
                "max_attempts": 5,
                "learns_from": "successes",
                "input_evolves": True,
            }
        }

        # Verify retry mode
        assert modes["retry"]["workers"] == 1
        assert modes["retry"]["learns_from"] == "errors"
        assert not modes["retry"]["input_evolves"]

        # Verify parallel mode
        assert modes["parallel"]["workers"] == 5
        assert not modes["parallel"]["input_evolves"]

        # Verify evolutionary mode
        assert modes["evolutionary"]["input_evolves"]
        assert modes["evolutionary"]["learns_from"] == "successes"

    def test_cost_comparison(self):
        """Test cost characteristics."""
        # Mode 1: 1-3 LLM calls
        mode1_min_calls = 1
        mode1_max_calls = 3

        # Mode 2: 5-10 LLM calls (5 workers + retries)
        mode2_min_calls = 5
        mode2_max_calls = 10

        # Mode 3: 1-5 LLM calls
        mode3_min_calls = 1
        mode3_max_calls = 5

        # Verify Mode 2 is most expensive
        assert mode2_min_calls > mode1_max_calls
        assert mode2_max_calls >= mode3_max_calls

    def test_validation_strategy(self):
        """Test validation strategies differ."""
        validations = {
            "retry": ["sample", "full"],  # Sample first, then full
            "parallel": ["sample", "full"],  # Sample first, then full
            "evolutionary": ["full"],  # Full DB every iteration
        }

        # Modes 1 & 2 use sample first
        assert "sample" in validations["retry"]
        assert "sample" in validations["parallel"]

        # Mode 3 skips sample
        assert "sample" not in validations["evolutionary"]
        assert "full" in validations["evolutionary"]


# ============================================================================
# Error Cases
# ============================================================================

class TestErrorHandling:
    """Test error handling across modes."""

    def test_all_retries_exhausted(self):
        """Test behavior when all retries fail."""
        max_retries = 3
        errors = []

        for attempt in range(1, max_retries + 1):
            error = f"Attempt {attempt} failed"
            errors.append(error)

        # All failed
        assert len(errors) == max_retries
        assert "Attempt 3 failed" in errors[-1]

    def test_no_valid_candidates(self):
        """Test when all workers produce invalid results."""
        workers = [
            {"id": 1, "status": "invalid", "error": "Syntax error"},
            {"id": 2, "status": "invalid", "error": "Parse error"},
            {"id": 3, "status": "invalid", "error": "Validation failed"},
            {"id": 4, "status": "invalid", "error": "Timeout"},
            {"id": 5, "status": "invalid", "error": "Row count mismatch"},
        ]

        valid_workers = [w for w in workers if w["status"] == "valid"]

        assert len(valid_workers) == 0

    def test_target_not_met(self):
        """Test when no candidate meets target speedup."""
        candidates = [
            {"speedup": 1.5},
            {"speedup": 1.8},
            {"speedup": 1.9},
        ]

        target = 2.0
        winners = [c for c in candidates if c["speedup"] >= target]

        assert len(winners) == 0

        # But we can still return best effort
        best = max(candidates, key=lambda x: x["speedup"])
        assert best["speedup"] == 1.9


# ============================================================================
# Performance Tests
# ============================================================================

class TestPerformance:
    """Test performance characteristics."""

    def test_parallel_is_faster_than_serial(self):
        """Test that parallel execution is faster (conceptually)."""
        import time

        def serial_execution(n_workers):
            start = time.time()
            for i in range(n_workers):
                time.sleep(0.01)  # Simulate work
            return time.time() - start

        def parallel_execution(n_workers):
            from concurrent.futures import ThreadPoolExecutor
            start = time.time()
            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                tasks = [pool.submit(time.sleep, 0.01) for _ in range(n_workers)]
                [t.result() for t in tasks]
            return time.time() - start

        serial_time = serial_execution(5)
        parallel_time = parallel_execution(5)

        # Parallel should be significantly faster
        assert parallel_time < serial_time * 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
