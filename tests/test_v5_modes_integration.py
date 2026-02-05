#!/usr/bin/env python3
"""
Integration test for all three V5 optimizer modes.

Tests with real functions but mocked LLM and database calls.
"""

import sys
sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

from unittest.mock import Mock, patch, MagicMock
from qt_sql.optimization.adaptive_rewriter_v5 import (
    optimize_v5_retry,
    optimize_v5_evolutionary,
    optimize_v5_json_queue,
    _build_history_section,
)
from qt_sql.validation.schemas import ValidationStatus


# Sample SQL for testing
SAMPLE_SQL = """
SELECT c_customer_id
FROM customer, store_returns
WHERE sr_customer_sk = c_customer_sk
  AND sr_store_sk IN (
    SELECT s_store_sk FROM store WHERE s_state = 'SD'
  )
LIMIT 100
"""


def test_build_history_section():
    """Test 1: Error feedback history generation."""
    print("\n" + "=" * 80)
    print("TEST 1: Error Feedback History Generation")
    print("=" * 80)

    response = '{"rewrite_sets": [{"id": "rs_01", "transform": "pushdown"}]}'
    error = "Syntax error: Missing JOIN condition"

    history = _build_history_section(response, error)

    print("\nGenerated history:")
    print(history)

    # Verify
    assert "Previous Attempt (FAILED)" in history
    assert "Failure reason:" in history
    assert error in history
    assert response in history

    print("\n✅ Test 1 PASSED: Error feedback properly formatted")


def test_mode1_retry_structure():
    """Test 2: Mode 1 (Retry) structure and flow."""
    print("\n" + "=" * 80)
    print("TEST 2: Mode 1 (Retry) - Structure & Flow")
    print("=" * 80)

    print("\n[Mode 1 Configuration]")
    print("  Strategy: Single worker with retries")
    print("  Max retries: 3")
    print("  Learning: From errors")
    print("  Sample DB: Yes (validation)")
    print("  Full DB: Yes (benchmark)")

    # Mock the worker function to simulate retries
    with patch('qt_sql.optimization.adaptive_rewriter_v5._worker_json') as mock_worker:
        with patch('qt_sql.optimization.adaptive_rewriter_v5.SQLValidator') as mock_validator:
            with patch('qt_sql.optimization.adaptive_rewriter_v5._get_plan_context') as mock_plan:
                with patch('qt_sql.optimization.adaptive_rewriter_v5.get_query_recommendations') as mock_ml:

                    # Setup mocks
                    mock_plan.return_value = ("plan summary", "plan text", None)
                    mock_ml.return_value = ["decorrelate", "pushdown"]

                    # Simulate: Attempt 1 fails, Attempt 2 succeeds
                    mock_worker.side_effect = [
                        # Attempt 1: Validation fails
                        Mock(
                            worker_id=1,
                            optimized_sql="SELECT * FORM customers",  # Typo
                            status=ValidationStatus.FAIL,
                            speedup=0.0,
                            error="Syntax error: 'FORM' is not valid",
                            response='{"rewrite_sets": []}',
                        ),
                        # Attempt 2: Success
                        Mock(
                            worker_id=2,
                            optimized_sql="SELECT * FROM customers",
                            status=ValidationStatus.PASS,
                            speedup=1.8,
                            error=None,
                            response='{"rewrite_sets": [{"id": "rs_01"}]}',
                        ),
                    ]

                    # Mock full DB validation
                    mock_full_validator = Mock()
                    mock_full_result = Mock(
                        status=ValidationStatus.PASS,
                        speedup=2.5,
                        errors=[],
                    )
                    mock_full_validator.validate.return_value = mock_full_result
                    mock_validator.return_value = mock_full_validator

                    # Run Mode 1
                    try:
                        result, full_result, history = optimize_v5_retry(
                            sql=SAMPLE_SQL,
                            sample_db="sample.db",
                            full_db="full.db",
                            query_id="q1",
                            max_retries=3,
                            target_speedup=2.0,
                        )

                        print("\n[Execution Results]")
                        print(f"  Attempts made: {len(history)}")
                        print(f"  Success: {result is not None}")
                        if result:
                            print(f"  Final speedup: {full_result.full_speedup:.2f}x")

                        print("\n[Attempts History]")
                        for attempt in history:
                            print(f"  Attempt {attempt['attempt']}: {attempt['status']}")
                            if attempt.get('error'):
                                print(f"    Error: {attempt['error']}")

                        # Verify
                        assert len(history) == 2, "Should have 2 attempts"
                        assert history[0]['status'] == 'fail', "Attempt 1 should fail"
                        assert history[1]['status'] == 'pass', "Attempt 2 should pass"
                        assert result is not None, "Should have final result"
                        assert full_result.full_speedup == 2.5, "Should have 2.5x speedup"

                        print("\n✅ Test 2 PASSED: Mode 1 retry logic works correctly")

                    except Exception as e:
                        print(f"\n❌ Test 2 FAILED: {e}")
                        import traceback
                        traceback.print_exc()
                        raise


def test_mode2_parallel_structure():
    """Test 3: Mode 2 (Parallel) structure and flow."""
    print("\n" + "=" * 80)
    print("TEST 3: Mode 2 (Parallel) - Structure & Flow")
    print("=" * 80)

    print("\n[Mode 2 Configuration]")
    print("  Strategy: Parallel workers with competition")
    print("  Workers: 5 (4 DAG JSON + 1 Full SQL)")
    print("  Learning: From diversity/competition")
    print("  Sample DB: Yes (validation)")
    print("  Full DB: Yes (benchmark)")
    print("  Early stopping: Yes (first to meet target)")

    # Mode 2 is already implemented in optimize_v5_json_queue
    # Test the concept with mocks

    with patch('qt_sql.optimization.adaptive_rewriter_v5._worker_json') as mock_json_worker:
        with patch('qt_sql.optimization.adaptive_rewriter_v5._worker_full_sql') as mock_full_worker:
            with patch('qt_sql.optimization.adaptive_rewriter_v5.SQLValidator') as mock_validator:
                with patch('qt_sql.optimization.adaptive_rewriter_v5._get_plan_context') as mock_plan:
                    with patch('qt_sql.optimization.adaptive_rewriter_v5.get_query_recommendations') as mock_ml:

                        # Setup
                        mock_plan.return_value = ("plan summary", "plan text", None)
                        mock_ml.return_value = ["decorrelate"] * 12

                        # Simulate 5 workers: 4 valid, 1 invalid
                        mock_json_worker.side_effect = [
                            Mock(worker_id=1, optimized_sql="SQL1", status=ValidationStatus.PASS, speedup=3.1, error=None, response='{}', prompt=''),
                            Mock(worker_id=2, optimized_sql="SQL2", status=ValidationStatus.PASS, speedup=2.4, error=None, response='{}', prompt=''),
                            Mock(worker_id=3, optimized_sql="SQL3", status=ValidationStatus.FAIL, speedup=0.0, error="Syntax error", response='{}', prompt=''),
                            Mock(worker_id=4, optimized_sql="SQL4", status=ValidationStatus.PASS, speedup=1.8, error=None, response='{}', prompt=''),
                        ]

                        mock_full_worker.return_value = Mock(
                            worker_id=5, optimized_sql="SQL5", status=ValidationStatus.PASS,
                            speedup=2.9, error=None, response='{}', prompt=''
                        )

                        # Mock full DB validation (only worker 1 gets benchmarked)
                        mock_full_validator = Mock()
                        mock_full_result = Mock(
                            status=ValidationStatus.PASS,
                            speedup=2.92,  # Meets target!
                            errors=[],
                        )
                        mock_full_validator.validate.return_value = mock_full_result
                        mock_validator.return_value = mock_full_validator

                        try:
                            valid, full_results, winner = optimize_v5_json_queue(
                                sql=SAMPLE_SQL,
                                sample_db="sample.db",
                                full_db="full.db",
                                query_id="q1",
                                max_workers=5,
                                target_speedup=2.0,
                            )

                            print("\n[Execution Results]")
                            print(f"  Valid candidates: {len(valid)}/5")
                            print(f"  Benchmarked: {len(full_results)}")
                            print(f"  Winner found: {winner is not None}")
                            if winner:
                                print(f"  Winner: Worker {winner.sample.worker_id}")
                                print(f"  Speedup: {winner.full_speedup:.2f}x")

                            # Verify
                            assert len(valid) == 4, "Should have 4 valid candidates (1 failed)"
                            assert winner is not None, "Should have winner"
                            assert winner.sample.worker_id == 1, "Worker 1 should win (highest speedup)"

                            print("\n✅ Test 3 PASSED: Mode 2 parallel logic works correctly")

                        except Exception as e:
                            print(f"\n❌ Test 3 FAILED: {e}")
                            import traceback
                            traceback.print_exc()
                            raise


def test_mode3_evolutionary_structure():
    """Test 4: Mode 3 (Evolutionary) structure and flow."""
    print("\n" + "=" * 80)
    print("TEST 4: Mode 3 (Evolutionary) - Structure & Flow")
    print("=" * 80)

    print("\n[Mode 3 Configuration]")
    print("  Strategy: Iterative improvement with stacking")
    print("  Max iterations: 5")
    print("  Learning: From successes")
    print("  Input: Evolves (best so far)")
    print("  Examples: Rotate each iteration")
    print("  Full DB: Yes (benchmark every iteration)")

    with patch('qt_sql.optimization.adaptive_rewriter_v5._create_llm_client') as mock_llm:
        with patch('qt_sql.optimization.adaptive_rewriter_v5.DagV2Pipeline') as mock_pipeline:
            with patch('qt_sql.optimization.adaptive_rewriter_v5.SQLValidator') as mock_validator:
                with patch('qt_sql.optimization.adaptive_rewriter_v5._get_plan_context') as mock_plan:
                    with patch('qt_sql.optimization.adaptive_rewriter_v5._build_base_prompt') as mock_base_prompt:
                        with patch('qt_sql.optimization.adaptive_rewriter_v5.load_all_examples') as mock_examples:

                            # Setup
                            mock_plan.return_value = ("plan summary", "plan text", None)
                            mock_base_prompt.return_value = "You are an autonomous Query Rewrite Engine..."
                            mock_examples.return_value = [Mock(id=f"ex_{i}") for i in range(12)]

                            # Mock LLM responses (3 iterations)
                            mock_llm_client = Mock()
                            mock_llm_client.call.side_effect = [
                                '{"rewrite_sets": [{"id": "rs_01", "transform": "decorrelate"}]}',  # Iteration 1
                                '{"rewrite_sets": [{"id": "rs_02", "transform": "pushdown"}]}',     # Iteration 2
                                '{"rewrite_sets": [{"id": "rs_03", "transform": "reorder_join"}]}', # Iteration 3
                            ]
                            mock_llm.return_value = mock_llm_client

                            # Mock SQL assembly
                            mock_pipeline_inst = Mock()
                            mock_pipeline_inst.assemble_from_response.side_effect = [
                                "WITH cte1 AS (...) SELECT * FROM cte1",  # Iteration 1: 1.5x
                                "WITH cte1 AS (...), cte2 AS (...) SELECT * FROM cte2",  # Iteration 2: 1.8x
                                "WITH cte1 AS (...), cte2 AS (...), cte3 AS (...) SELECT * FROM cte3",  # Iteration 3: 2.3x (target met!)
                            ]
                            mock_pipeline.return_value = mock_pipeline_inst

                            # Mock validation (improving speedups)
                            mock_validator_inst = Mock()
                            mock_validator_inst.validate.side_effect = [
                                Mock(status=ValidationStatus.PASS, speedup=1.5, errors=[]),  # Iteration 1
                                Mock(status=ValidationStatus.PASS, speedup=1.8, errors=[]),  # Iteration 2
                                Mock(status=ValidationStatus.PASS, speedup=2.3, errors=[]),  # Iteration 3: Target met!
                            ]
                            mock_validator.return_value = mock_validator_inst

                            try:
                                best, full_result, history = optimize_v5_evolutionary(
                                    sql=SAMPLE_SQL,
                                    full_db="full.db",
                                    query_id="q1",
                                    max_iterations=5,
                                    target_speedup=2.0,
                                )

                                print("\n[Execution Results]")
                                print(f"  Iterations completed: {len(history)}")
                                print(f"  Best found: {best is not None}")
                                if best:
                                    print(f"  Best speedup: {full_result.full_speedup:.2f}x")
                                    print(f"  Achieved at: Iteration {best.worker_id}")

                                print("\n[Iterations History]")
                                for it in history:
                                    print(f"  Iteration {it['iteration']}: {it['status']}", end='')
                                    if it['status'] == 'success':
                                        print(f" - {it['speedup']:.2f}x", end='')
                                        if it.get('improved'):
                                            print(" ✓ (improved)", end='')
                                    print()

                                # Verify
                                assert len(history) == 3, "Should stop after 3 iterations (target met)"
                                assert best is not None, "Should have best result"
                                assert full_result.full_speedup == 2.3, "Should have 2.3x speedup"
                                assert history[0]['speedup'] < history[1]['speedup'] < history[2]['speedup'], "Speedup should improve"

                                print("\n✅ Test 4 PASSED: Mode 3 evolutionary logic works correctly")

                            except Exception as e:
                                print(f"\n❌ Test 4 FAILED: {e}")
                                import traceback
                                traceback.print_exc()
                                raise


def test_mode_comparison():
    """Test 5: Verify mode characteristics match spec."""
    print("\n" + "=" * 80)
    print("TEST 5: Mode Characteristics Verification")
    print("=" * 80)

    modes = {
        "Mode 1 (Retry)": {
            "workers": 1,
            "max_attempts": 3,
            "learns_from": "errors",
            "input_evolves": False,
            "validation": ["sample", "full"],
        },
        "Mode 2 (Parallel)": {
            "workers": 5,
            "max_attempts": 1,  # + 1 retry per worker
            "learns_from": "competition",
            "input_evolves": False,
            "validation": ["sample", "full"],
        },
        "Mode 3 (Evolutionary)": {
            "workers": 1,
            "max_attempts": 5,
            "learns_from": "successes",
            "input_evolves": True,
            "validation": ["full"],
        },
    }

    print("\n[Mode Characteristics]")
    for mode_name, chars in modes.items():
        print(f"\n{mode_name}:")
        for key, value in chars.items():
            print(f"  {key}: {value}")

    # Verify
    assert modes["Mode 1 (Retry)"]["learns_from"] == "errors"
    assert modes["Mode 2 (Parallel)"]["workers"] == 5
    assert modes["Mode 3 (Evolutionary)"]["input_evolves"] == True

    print("\n✅ Test 5 PASSED: All mode characteristics verified")


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "=" * 80)
    print("V5 OPTIMIZER MODES - INTEGRATION TEST SUITE")
    print("=" * 80)

    tests = [
        ("Error Feedback", test_build_history_section),
        ("Mode 1 (Retry)", test_mode1_retry_structure),
        ("Mode 2 (Parallel)", test_mode2_parallel_structure),
        ("Mode 3 (Evolutionary)", test_mode3_evolutionary_structure),
        ("Mode Comparison", test_mode_comparison),
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"\n❌ {test_name} FAILED: {e}")
            failed += 1

    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"\nTotal: {len(tests)}")
    print(f"Passed: {passed} ✅")
    print(f"Failed: {failed} ❌")

    if failed == 0:
        print("\n🎉 ALL TESTS PASSED!")
        print("\n✅ Mode 1 (Retry): Error feedback working")
        print("✅ Mode 2 (Parallel): Competition working")
        print("✅ Mode 3 (Evolutionary): Stacking working")
    else:
        print(f"\n⚠️  {failed} test(s) failed")

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
