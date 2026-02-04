# Q23 Three-Mode Test Plan

**Date:** 2026-02-05
**Status:** Ready for execution

---

## Why Q23?

Q23 is an excellent test case for all three V5 optimizer modes because:

### Previous Results
- ✅ **2.33x speedup** achieved (excellent performance gain)
- ❌ **Failed validation** - semantic error (values mismatch)
- Challenge: Need to achieve speedup while preserving correctness

### Query Complexity
```sql
-- 3 CTEs with aggregations
with frequent_ss_items as (...)  -- GROUP BY with HAVING
 max_store_sales as (...)        -- MAX aggregation
 best_ss_customer as (...)       -- Complex HAVING with subquery

-- UNION ALL
select sum(sales) from (
  select ... from catalog_sales ...  -- Subquery filters
  union all
  select ... from web_sales ...      -- Subquery filters
)
```

**Complexity factors:**
- 3 interdependent CTEs
- Multiple aggregations (COUNT, SUM, MAX)
- UNION ALL
- Subqueries in WHERE clauses (IN clauses)
- Date filtering across multiple years
- Complex HAVING conditions

---

## Test Strategy

### Mode 1: Retry (Corrective Learning)

**Why it's good for Q23:**
- Can learn from semantic errors through error feedback
- If first attempt breaks semantics, retry with error context
- Iteratively corrects mistakes

**Expected behavior:**
1. **Attempt 1:** Might push filters too aggressively → semantic error
2. **Attempt 2:** Error feedback: "Row count mismatch, filter changed aggregation scope"
3. **Attempt 3:** Corrects approach, preserves semantics

**Success criteria:**
- Achieves >=2.0x speedup
- Passes validation (same row count/values as original)
- Shows learning across attempts (different errors each time)

---

### Mode 2: Parallel (Tournament Competition)

**Why it's good for Q23:**
- 5 different workers try different strategies
- One worker might avoid the semantic trap
- Diverse examples increase chance of correct optimization

**Expected behavior:**
- **Worker 1:** Examples 1-3 (e.g., decorrelate, early_filter, or_to_union)
- **Worker 2:** Examples 4-6 (e.g., date_cte_isolate, pushdown, materialize_cte)
- **Worker 3:** Examples 7-9 (e.g., flatten_subquery, reorder_join, inline_cte)
- **Worker 4:** Examples 10-12 (e.g., remove_redundant, multi_push, semantic_rewrite)
- **Worker 5:** Explore mode (full SQL, no examples)

**Success criteria:**
- At least 3/5 workers produce valid results
- Best worker achieves >=2.0x speedup
- Winner passes validation

---

### Mode 3: Evolutionary (Stacking)

**Why it's good for Q23:**
- Can iteratively build on what works
- Each iteration improves on previous best
- Rotating examples try different optimization angles

**Expected behavior:**
1. **Iteration 1:** Apply safe optimization (1.3x)
2. **Iteration 2:** Build on it with additional optimization (1.8x)
3. **Iteration 3:** Further refinement (2.1x, target met!)

**Success criteria:**
- Speedup improves across iterations
- Final result >= 2.0x
- Each iteration builds on previous (input evolves)

---

## Comparison Points

| Aspect | Mode 1 (Retry) | Mode 2 (Parallel) | Mode 3 (Evolutionary) |
|--------|----------------|-------------------|----------------------|
| **Attempts** | 1-3 | 5 (parallel) | 1-5 (sequential) |
| **Learning** | From errors | From diversity | From successes |
| **Cost** | Low (1-3 calls) | High (5 calls) | Medium (1-5 calls) |
| **Time** | Fast | Medium (parallel) | Slow (sequential) |
| **Best for Q23** | Semantic error correction | Finding one correct approach | Iterative improvement |

---

## Expected Outcomes

### Scenario 1: All modes succeed
```
Mode 1 (Retry):        2.3x after 2 attempts (learned from error)
Mode 2 (Parallel):     2.5x (worker 3 found best approach)
Mode 3 (Evolutionary): 2.4x after 3 iterations (stacked improvements)

Winner: Mode 2 (highest speedup, fastest time)
```

### Scenario 2: Only retry succeeds
```
Mode 1 (Retry):        2.1x after 3 attempts (error feedback worked!)
Mode 2 (Parallel):     Failed - all workers made semantic errors
Mode 3 (Evolutionary): Failed - couldn't improve safely

Winner: Mode 1 (only one that succeeded via error correction)
```

### Scenario 3: Only parallel succeeds
```
Mode 1 (Retry):        Failed - same error pattern repeated
Mode 2 (Parallel):     2.3x (worker 5 explore mode found solution)
Mode 3 (Evolutionary): Failed - first iteration broke semantics, couldn't recover

Winner: Mode 2 (diversity found the solution)
```

### Scenario 4: All modes fail
```
Mode 1 (Retry):        Failed - semantic errors persist
Mode 2 (Parallel):     Failed - all 5 workers break semantics
Mode 3 (Evolutionary): Failed - no safe optimization path

Conclusion: Q23 may be too complex for current prompt/examples
Next step: Add Q23-specific gold example
```

---

## Running the Test

### Prerequisites

```bash
# Set API key
export QT_DEEPSEEK_API_KEY=your_api_key

# Set database paths
export QT_SAMPLE_DB=/path/to/tpcds_sf1.duckdb
export QT_FULL_DB=/path/to/tpcds_sf100.duckdb
```

### Execute Test

```bash
python3 test_q23_all_modes.py
```

### Expected Output

```
================================================================================
Q23 THREE-MODE COMPREHENSIVE TEST
================================================================================

[Query Info]
  Query: TPC-DS Q23 (Best customers by sales)
  Complexity: 3 CTEs, UNION ALL, multiple subqueries
  Previous result: 2.33x speedup, but FAILED validation (semantic error)
  Goal: Test if V5 modes can achieve speedup while preserving correctness

================================================================================
Q23 THREE-MODE TEST - Prerequisites Check
================================================================================

✓ API key configured (length: 100)
✓ Sample DB: D:/TPC-DS/tpcds_sf1_sample.duckdb
✓ Full DB: D:/TPC-DS/tpcds_sf100.duckdb

✅ All prerequisites met!

================================================================================
MODE 1: RETRY (Corrective Learning)
================================================================================

[Configuration]
  Strategy: Single worker with retries
  Max retries: 3
  Learning: From errors (error feedback)
  Why good for Q23: Can learn from semantic errors and retry

[Running...]

[Results] (completed in 45.2s)
  Attempts: 3
  ✓ Attempt 1: pass
    Speedup: 1.8x
  ✓ Attempt 2: pass
    Speedup: 2.1x
  ✓ Attempt 3: pass
    Speedup: 2.3x

✅ SUCCESS!
  Sample speedup: 2.1x
  Full DB speedup: 2.3x
  Target met: Yes

[... Mode 2 and Mode 3 results ...]

================================================================================
COMPARISON: All Three Modes
================================================================================

[Summary]

Mode            Success    Speedup    Time       Notes
--------------------------------------------------------------------------------
retry           ✅ Yes     2.30x      45.2s      3 attempts
parallel        ✅ Yes     2.50x      38.1s      4 valid workers
evolutionary    ✅ Yes     2.40x      67.3s      3 iterations

[Analysis]
  Best mode: parallel
  Best speedup: 2.50x
  Time: 38.1s

  ✓ Best SQL saved to: test_results/q23/parallel_optimized.sql
  ✓ Full results saved to: test_results/q23/comparison_results.json

================================================================================
TEST COMPLETE
================================================================================

✅ At least one mode succeeded!
```

---

## Key Insights to Watch For

### 1. Error Feedback Effectiveness (Mode 1)
- Does the LLM understand the semantic error message?
- Does it correct the issue in the next attempt?
- Example error: "Row count mismatch: filter in CTE changed aggregation scope"

### 2. Worker Diversity (Mode 2)
- Do different workers produce different optimizations?
- Which worker finds the winning strategy?
- Worker 5 (explore mode) often finds creative solutions

### 3. Iterative Improvement (Mode 3)
- Does speedup increase monotonically?
- Are optimizations stacked correctly?
- Does input SQL evolve properly?

### 4. Validation Success
- **Most important:** Does the optimization pass validation?
- Q23 previously failed with semantic errors
- Success = speedup AND correctness

---

## Success Metrics

### Must Have
- ✅ At least one mode achieves >=2.0x speedup
- ✅ Optimization passes validation (same results as original)
- ✅ No semantic errors

### Nice to Have
- ✅ All three modes succeed
- ✅ Error feedback visibly improves attempts
- ✅ Mode 3 shows progressive improvement

### Would Learn From
- ❌ All modes fail → Q23 needs specialized gold example
- ❌ Only explore mode (Worker 5) succeeds → Need more diverse examples
- ❌ Semantic errors persist → Prompt needs stronger semantic constraints

---

## Next Steps After Test

### If successful:
1. Run on more complex queries (q33, q35, q38, etc.)
2. Document which mode works best for which query types
3. Create Q23-specific gold example for training

### If partially successful:
1. Analyze which mode worked and why
2. Improve prompts for failed modes
3. Add Q23 patterns to example library

### If all fail:
1. Manual analysis of Q23 optimization challenges
2. Create Q23 gold example with explicit semantic constraints
3. Test individual transforms on Q23 CTEs

---

## Files

**Test script:**
- `test_q23_all_modes.py` - Main test runner (400+ lines)

**Query:**
- `research/archive/queries/q23.sql` - Original Q23 query

**Results:**
- `test_results/q23/retry_optimized.sql` - Mode 1 result
- `test_results/q23/parallel_optimized.sql` - Mode 2 result
- `test_results/q23/evolutionary_optimized.sql` - Mode 3 result
- `test_results/q23/comparison_results.json` - Full comparison

---

## Summary

Q23 is the perfect test for all three modes because:

1. **Complex enough** to be challenging
2. **Known issues** (semantic errors) to fix
3. **High potential** (2.33x speedup possible)
4. **Tests all mode strengths:**
   - Retry: Error correction
   - Parallel: Strategy diversity
   - Evolutionary: Iterative building

Running this test will validate that:
- ✅ V5 implementation works end-to-end
- ✅ Error feedback helps fix semantic issues
- ✅ All three modes are functional
- ✅ We can compare mode effectiveness

**Status:** Ready to run! 🚀
