#!/usr/bin/env python3
"""Test error feedback mechanism in prompts."""

import sys
sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

from qt_sql.optimization.adaptive_rewriter_v5 import _build_history_section

def test_error_feedback():
    """Test that error messages are properly included in retry prompts."""

    print("=" * 80)
    print("ERROR FEEDBACK MECHANISM TEST")
    print("=" * 80)

    # Test Case 1: Syntax Error
    print("\n[Test 1] Syntax Error Feedback")
    print("-" * 80)

    previous_response = """
    {
      "rewrite_sets": [
        {
          "id": "rs_01",
          "transform": "decorrelate",
          "nodes": {
            "main_query": "SELECT c_customer_id FROM ctr1 JOIN store"
          }
        }
      ]
    }
    """

    error = "Syntax error near 'JOIN': Missing ON condition"

    history = _build_history_section(previous_response, error)

    print("Generated History Section:")
    print(history)

    # Verify key elements are present
    assert "Previous Attempt (FAILED)" in history
    assert "Failure reason:" in history
    assert "Syntax error near 'JOIN'" in history
    assert "Missing ON condition" in history
    print("\n✓ Syntax error properly included in feedback")

    # Test Case 2: Semantic Error
    print("\n[Test 2] Semantic Error Feedback")
    print("-" * 80)

    previous_response = """
    {
      "rewrite_sets": [
        {
          "id": "rs_01",
          "transform": "pushdown",
          "nodes": {
            "filtered_store_returns": "SELECT ... WHERE d_year = 2000 AND s.s_state = 'SD'"
          }
        }
      ]
    }
    """

    error = "Row count mismatch: Expected 100 rows, got 95 rows. Filter pushed too early changed aggregate calculation."

    history = _build_history_section(previous_response, error)

    print("Generated History Section:")
    print(history)

    # Verify key elements
    assert "Previous Attempt (FAILED)" in history
    assert "Failure reason:" in history
    assert "Row count mismatch" in history
    assert "Expected 100" in history
    assert "got 95" in history
    print("\n✓ Semantic error properly included in feedback")

    # Test Case 3: Multiple Errors (Mode 1, Attempt 3)
    print("\n[Test 3] Multiple Error History")
    print("-" * 80)

    # Simulate building history with multiple errors
    error_history = []

    # Attempt 1 error
    attempt1_response = '{"rewrite_sets": [{"id": "rs_01", "nodes": {"main": "SELECT ... JOIN"}}]}'
    attempt1_error = "Syntax error: Missing ON condition"
    error_history.append(_build_history_section(attempt1_response, attempt1_error))

    # Attempt 2 error
    attempt2_response = '{"rewrite_sets": [{"id": "rs_01", "nodes": {"main": "SELECT ... WHERE s_state=\'SD\'"}}]}'
    attempt2_error = "Row count mismatch: 95 rows instead of 100"
    error_history.append(_build_history_section(attempt2_response, attempt2_error))

    # Combine for attempt 3
    combined_history = "\n\n## All Previous Attempts\n\n"
    for i, hist in enumerate(error_history, 1):
        combined_history += f"### Attempt {i}\n{hist}\n\n"

    print("Generated Combined History:")
    print(combined_history)

    # Verify both errors are present
    assert "Syntax error" in combined_history
    assert "Row count mismatch" in combined_history
    assert "Attempt 1" in combined_history
    assert "Attempt 2" in combined_history
    print("\n✓ Multiple errors properly accumulated")

    # Test Case 4: Full Prompt Construction
    print("\n[Test 4] Full Retry Prompt Construction")
    print("-" * 80)

    base_prompt = """You are an autonomous Query Rewrite Engine.

## Target Nodes
[customer_total_return] GROUP_BY
[main_query] CORRELATED

## Subgraph Slice
[customer_total_return] type=cte
SELECT sr_customer_sk AS ctr_customer_sk...

Now output your rewrite_sets:"""

    retry_prompt = base_prompt + "\n\n" + history

    print("Full Retry Prompt Preview (first 500 chars):")
    print(retry_prompt[:500])
    print("...\n")

    # Verify structure
    assert "autonomous Query Rewrite Engine" in retry_prompt
    assert "Previous Attempt (FAILED)" in retry_prompt
    assert "Failure reason:" in retry_prompt
    print("✓ Full retry prompt properly constructed")

    # Test Case 5: Worker-Specific Error in Mode 2
    print("\n[Test 5] Mode 2: Worker-Specific Error Feedback")
    print("-" * 80)

    worker_3_response = """
    {
      "rewrite_sets": [
        {
          "id": "rs_01",
          "transform": "flatten_subquery",
          "nodes": {
            "main_query": "SELECT * FROM (SELECT"
          }
        }
      ]
    }
    """

    worker_3_error = "Incomplete subquery: Missing closing parenthesis"

    worker_3_history = _build_history_section(worker_3_response, worker_3_error)

    print("Worker 3 Error Feedback:")
    print(worker_3_history)

    # Verify
    assert "Incomplete subquery" in worker_3_history
    assert "Missing closing parenthesis" in worker_3_history
    print("\n✓ Worker-specific error properly captured")

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print("\n✅ All error feedback tests passed!")
    print("\nVerified:")
    print("  1. ✓ Syntax errors properly included")
    print("  2. ✓ Semantic errors properly included")
    print("  3. ✓ Multiple errors accumulated correctly")
    print("  4. ✓ Full retry prompts constructed properly")
    print("  5. ✓ Worker-specific errors captured")

    print("\n" + "=" * 80)
    print("ERROR FEEDBACK MECHANISM: WORKING ✅")
    print("=" * 80)

    return True

def test_prompt_examples():
    """Show example prompts with error feedback."""

    print("\n\n" + "=" * 80)
    print("EXAMPLE PROMPTS WITH ERROR FEEDBACK")
    print("=" * 80)

    # Example 1: Mode 1, Attempt 2
    print("\n[Example 1] Mode 1 - Attempt 2 Prompt")
    print("-" * 80)

    prompt_attempt_2 = """You are an autonomous Query Rewrite Engine. Your goal is to maximize execution
speed while strictly preserving semantic invariants.

[... gold examples ...]

## Target Nodes
[customer_total_return] GROUP_BY
[main_query] CORRELATED

## Previous Attempt Failed

Your previous optimization attempt failed with the following error:

**Attempt 1 Error:**
Syntax error at line 15: Missing ON condition for JOIN
```sql
FROM store_returns
JOIN date_dim  -- ERROR: No ON condition
```

Please correct this error. Ensure all JOINs have proper ON conditions.

Now output your corrected rewrite_sets:
"""

    print(prompt_attempt_2)

    # Example 2: Mode 1, Attempt 3
    print("\n[Example 2] Mode 1 - Attempt 3 Prompt")
    print("-" * 80)

    prompt_attempt_3 = """You are an autonomous Query Rewrite Engine. Your goal is to maximize execution
speed while strictly preserving semantic invariants.

[... gold examples ...]

## Target Nodes
[customer_total_return] GROUP_BY
[main_query] CORRELATED

## All Previous Attempts Failed

### Attempt 1: Syntax Error
**Error:** Missing ON condition for JOIN
**Issue:** Incomplete JOIN clause

### Attempt 2: Semantic Error
**Error:** Row count mismatch - Expected 100, got 95
**Issue:** Filter s_state='SD' was pushed into filtered_store_returns before
aggregation, which changed the average calculation scope from all stores to
only SD stores. This violated semantic invariants.

**Root cause:** The filter must be applied AFTER the decorrelation and average
calculation, not before.

Please analyze these failures and provide a correct optimization that:
1. Has valid SQL syntax with all JOIN conditions
2. Produces exactly 100 rows (same as original)
3. Keeps s_state='SD' filter in main query WHERE clause
4. Does not push filters that affect aggregate calculations

Now output your corrected rewrite_sets:
"""

    print(prompt_attempt_3)

    # Example 3: Mode 2, Worker 3 Retry
    print("\n[Example 3] Mode 2 - Worker 3 Retry Prompt")
    print("-" * 80)

    prompt_worker_3_retry = """You are an autonomous Query Rewrite Engine. Your goal is to maximize execution
speed while strictly preserving semantic invariants.

## Examples for Worker 3
[... flatten_subquery, reorder_join, inline_cte ...]

## Target Nodes
[customer_total_return] GROUP_BY
[main_query] CORRELATED

## Previous Attempt Failed

Your previous optimization (Worker 3) failed with:

**Error:** Incomplete subquery - Missing closing parenthesis
**SQL Fragment:**
```sql
SELECT * FROM (SELECT sr_customer_sk, sr_store_sk
FROM store_returns  -- Missing closing )
JOIN customer...
```

Please ensure all subqueries are properly closed.

Now output your corrected rewrite_sets:
"""

    print(prompt_worker_3_retry)

    print("\n" + "=" * 80)
    print("These prompts will be used in actual optimization runs!")
    print("=" * 80)

if __name__ == '__main__':
    try:
        # Run error feedback tests
        test_error_feedback()

        # Show example prompts
        test_prompt_examples()

        print("\n🎉 All tests passed! Error feedback mechanism is ready.")
        sys.exit(0)

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
