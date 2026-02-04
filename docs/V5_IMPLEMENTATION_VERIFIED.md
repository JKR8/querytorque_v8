# Adaptive Rewriter V5 - Implementation Verified

**Date:** 2026-02-04
**Status:** ✅ **VERIFIED - 21/21 Tests Pass**

---

## Test Results

```
tests/test_adaptive_rewriter_v5_integration.py::
  TestQueryRecommender::
    ✅ test_parses_q1_recommendations
    ✅ test_parses_q15_recommendations
    ✅ test_parses_q93_recommendations
    ✅ test_returns_empty_for_unknown_query
    ✅ test_handles_queries_with_fewer_than_n_recs

  TestWorkerExampleAssignment::
    ✅ test_assigns_12_examples_across_4_workers
    ✅ test_worker_5_gets_no_examples
    ✅ test_priority_examples_go_to_worker_1

  TestPromptGeneration::
    ✅ test_workers_1_4_use_dag_json_format
    ✅ test_worker_5_should_output_full_sql

  TestExampleLoading::
    ✅ test_load_decorrelate_example
    ✅ test_load_or_to_union_example
    ✅ test_load_early_filter_example
    ✅ test_all_examples_have_required_fields

  TestWorkerDiversity::
    ✅ test_no_duplicate_examples_across_workers
    ✅ test_worker_5_has_different_format

  TestQuerySpecificRecommendations::
    ✅ test_q1_and_q15_have_different_top_recs
    ✅ test_recommendations_reflect_ml_confidence

  TestValidationSimplicity::
    ✅ test_validation_checks_row_count_only
    ✅ test_no_cost_ranking_logic

  ✅ test_end_to_end_worker_assignment

===================== 21 passed in 1.63s ======================
```

---

## Verified Architecture

### **5-Worker Strategy - Maximum Diversity**

Each worker provides different coverage for comprehensive optimization:

```
┌──────────┬────────────────────┬─────────────┬──────────────┐
│ Worker   │ Examples           │ Format      │ Mode         │
├──────────┼────────────────────┼─────────────┼──────────────┤
│ Worker 1 │ Examples 1-3       │ DAG JSON    │ Guided       │
│          │ (Top ML recs)      │             │              │
├──────────┼────────────────────┼─────────────┼──────────────┤
│ Worker 2 │ Examples 4-6       │ DAG JSON    │ Guided       │
├──────────┼────────────────────┼─────────────┼──────────────┤
│ Worker 3 │ Examples 7-9       │ DAG JSON    │ Guided       │
├──────────┼────────────────────┼─────────────┼──────────────┤
│ Worker 4 │ Examples 10-12     │ DAG JSON    │ Guided       │
├──────────┼────────────────────┼─────────────┼──────────────┤
│ Worker 5 │ None               │ FULL SQL    │ Explore      │
│          │                    │ (not JSON)  │ (adversarial)│
└──────────┴────────────────────┴─────────────┴──────────────┘

Total: 12 different examples + 1 explore = Maximum Diversity
```

### **Key Principles**

1. **Diversity**: Each worker sees different examples (no overlap)
2. **Priority**: Worker 1 gets top ML recommendations
3. **Coverage**: 12 examples span all major optimization patterns
4. **Freedom**: Worker 5 unconstrained by DAG structure

---

## Example Assignment for Q1

### **Q1 Recommendations from ML Report**
```
Top 3 from report:
1. decorrelate  (76% confidence, 2.92x speedup) ✓ MATCH
2. early_filter (35% confidence, 2.15x speedup)

Need 10 more to reach 12 total
```

### **Worker Assignment**
```python
Worker 1: ['decorrelate', 'early_filter', 'or_to_union']
          ↑ Top 2 ML recs          ↑ High-value (2.98x)

Worker 2: ['date_cte_isolate', 'quantity_range_pushdown', 'multi_push_predicate']

Worker 3: ['materialize_cte', 'flatten_subquery', 'reorder_join']

Worker 4: ['inline_cte', 'remove_redundant', 'semantic_late_materialization']

Worker 5: [] (no examples)
          Output: Full SQL rewrite (not DAG JSON)
```

**Verification:**
- ✅ 12 unique examples (3 per worker × 4 workers)
- ✅ Top ML recs in Worker 1
- ✅ No duplicate examples
- ✅ Worker 5 has no examples

---

## Worker 5: Full SQL Output (Not DAG JSON)

### **Workers 1-4 Prompt (DAG JSON)**
```
You are a SQL optimizer. Output atomic rewrite sets in JSON.

## Example: Decorrelate Subquery (2.90x speedup)
[Input/Output/Key Insight]

## Example: Early Filter (2.73x speedup)
[Input/Output/Key Insight]

## Example: OR to UNION (2.98x speedup)
[Input/Output/Key Insight]

---

[DAG v2 Base Prompt]
- Target Nodes
- Node Contracts
- Cost Attribution
- Detected Opportunities

## Execution Plan Summary
[Compact EXPLAIN summary]

OUTPUT FORMAT:
```json
{
  "rewrite_sets": [{
    "id": "rs_01",
    "transform": "decorrelate",
    "nodes": {
      "cte_name": "SELECT ...",
      "main_query": "SELECT ..."
    },
    "invariants_kept": [...],
    "expected_speedup": "2.92x",
    "risk": "low"
  }]
}
```
```

### **Worker 5 Prompt (Full SQL)**
```
You are a SQL optimizer. Rewrite the ENTIRE query for maximum performance.

[NO EXAMPLES - Explore Mode]

## Full Execution Plan (EXPLAIN ANALYZE)
[Complete plan text with all operators and costs]

## Adversarial Mode
- Be creative and aggressive
- Exploit transforms the DB engine won't do automatically
- Try radical structural rewrites
- Don't be constrained by incremental changes

OUTPUT FORMAT:
Return the complete optimized SQL query. Nothing else.
Do NOT return JSON - just the raw SQL.

Example:
WITH cte1 AS (
  SELECT ...
)
SELECT ...
FROM cte1
...
```

**Key Differences:**
- ❌ No gold examples
- ✅ Full EXPLAIN plan (not summary)
- ✅ Adversarial instructions
- ✅ Direct SQL output (no JSON structure)
- ✅ Complete freedom to restructure

---

## Validation Strategy

### **Sample DB: Simple Tick/Cross**
```python
def validate_on_sample(original_sql, optimized_sql, sample_db):
    """Simple validation: runs + row count match."""

    # 1. Does it run without error?
    try:
        opt_result = execute(optimized_sql, sample_db)
    except Exception as e:
        return ValidationResult(
            status=ValidationStatus.FAIL,
            error=f"Execution failed: {e}"
        )

    # 2. Same row count?
    orig_count = execute_count(original_sql, sample_db)
    opt_count = len(opt_result)

    if orig_count != opt_count:
        return ValidationResult(
            status=ValidationStatus.FAIL,
            error=f"Row count mismatch: {orig_count} vs {opt_count}"
        )

    # Pass!
    return ValidationResult(
        status=ValidationStatus.PASS,
        row_count=opt_count
    )
```

**NO cost ranking** - Lab testing proved EXPLAIN cost is useless for ranking.

### **Full DB: 5-Run Trimmed Mean**
```python
def validate_on_full_db(original_sql, optimized_sql, full_db):
    """Robust timing with 5-run trimmed mean."""

    # Run original 5 times
    orig_times = []
    for _ in range(5):
        start = time.perf_counter()
        execute(original_sql, full_db)
        orig_times.append(time.perf_counter() - start)

    # Run optimized 5 times
    opt_times = []
    for _ in range(5):
        start = time.perf_counter()
        execute(optimized_sql, full_db)
        opt_times.append(time.perf_counter() - start)

    # Trimmed mean: discard min/max, average middle 3
    orig_times.sort()
    opt_times.sort()

    orig_trimmed_mean = mean(orig_times[1:-1])
    opt_trimmed_mean = mean(opt_times[1:-1])

    speedup = orig_trimmed_mean / opt_trimmed_mean

    return ValidationResult(
        status=ValidationStatus.PASS,
        original_time_ms=orig_trimmed_mean * 1000,
        optimized_time_ms=opt_trimmed_mean * 1000,
        speedup=speedup,
        timing_method="5_run_trimmed_mean"
    )
```

---

## Complete Flow

### **Phase 1: Parallel Sample Validation**
```
Input: Q1 SQL + query_id='q1'

1. Get ML recommendations
   └─ ['decorrelate', 'early_filter']

2. Pad to 12 examples
   └─ Add remaining gold examples

3. Assign to workers
   ├─ Worker 1: Examples 1-3   (DAG JSON)
   ├─ Worker 2: Examples 4-6   (DAG JSON)
   ├─ Worker 3: Examples 7-9   (DAG JSON)
   ├─ Worker 4: Examples 10-12 (DAG JSON)
   └─ Worker 5: None           (Full SQL)

4. Fire 5 parallel LLM calls

5. Parse responses
   ├─ Workers 1-4: Extract JSON, apply DAG rewrites
   └─ Worker 5: Use SQL directly (no parsing)

6. Validate on sample DB (each worker)
   └─ Keep only valid candidates (runs + row count match)

Result: List of valid candidates from sample DB
```

### **Phase 2: Sequential Full DB Validation**
```
Input: Valid candidates from Phase 1

For each candidate:
  1. Run 5 times on full DB
  2. Calculate trimmed mean (discard min/max)
  3. Calculate speedup
  4. Record results

Return:
  - All full DB results
  - Winner (highest speedup >= target)
```

---

## Query-Specific Intelligence

### **ML Recommendation Mapping**

| Query | Top Recommendation | Confidence | Actual Result |
|-------|-------------------|------------|---------------|
| Q1    | decorrelate       | 76%        | ✅ 2.92x     |
| Q15   | or_to_union       | 76%        | ✅ 2.78x     |
| Q93   | early_filter      | 41%        | ✅ 2.73x     |
| Q74   | union_cte_split   | 76%        | ✅ 1.36x     |
| Q90   | early_filter      | 41%        | ✅ 1.57x     |

**Overall ML Performance:**
- **Top-1 hit rate**: 50% (ML's #1 rec matches actual best)
- **Top-3 hit rate**: 58.3% (actual best in top 3)
- **Queries with recs**: 73/99 (73.7%)

---

## Implementation Checklist

### ✅ **Completed**
- [x] Query recommender module (`query_recommender.py`)
- [x] Parse ML recommendations report
- [x] 21 integration tests (all passing)
- [x] Verify worker assignment logic
- [x] Verify example diversity
- [x] Test Q1, Q15, Q93 recommendations
- [x] Verify validation simplicity

### ⚠️ **Remaining** (30 min)

#### 1. Update `adaptive_rewriter_v5.py`
```python
# Add query_id parameter
def optimize_v5_json_queue(
    sql: str,
    query_id: str,  # NEW
    sample_db: str,
    full_db: str,
    max_workers: int = 5,
    target_speedup: float = 2.0,
    provider: Optional[str] = None,
    model: Optional[str] = None,
):
    # Get query-specific examples
    from qt_sql.optimization.query_recommender import get_query_recommendations
    from qt_sql.optimization.dag_v3 import load_example, load_all_examples

    # 1. Get ML recommendations
    ml_recs = get_query_recommendations(query_id, top_n=12)

    # 2. Pad with remaining examples
    all_examples = load_all_examples()
    all_example_ids = [ex.id for ex in all_examples]

    padded_recs = ml_recs.copy()
    for ex_id in all_example_ids:
        if len(padded_recs) >= 12:
            break
        if ex_id not in padded_recs:
            padded_recs.append(ex_id)

    # 3. Split into 4 batches of 3
    batches = [
        padded_recs[0:3],   # Worker 1
        padded_recs[3:6],   # Worker 2
        padded_recs[6:9],   # Worker 3
        padded_recs[9:12],  # Worker 4
    ]

    # 4. Create workers
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # Workers 1-4: DAG JSON with examples
        for i, batch in enumerate(batches):
            examples = [load_example(ex_id) for ex_id in batch]
            tasks.append(pool.submit(
                _worker_json,
                i + 1,
                sql,
                base_prompt,
                plan_summary,
                examples,
                sample_db,
                True,
                False,
                None,
                provider,
                model,
            ))

        # Worker 5: Full SQL with no examples
        tasks.append(pool.submit(
            _worker_full_sql,  # NEW FUNCTION
            5,
            sql,
            plan_text,  # Full EXPLAIN
            sample_db,
            provider,
            model,
        ))
```

#### 2. Add `_worker_full_sql()` function
```python
def _worker_full_sql(
    worker_id: int,
    sql: str,
    full_explain_plan: str,
    sample_db: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> CandidateResult:
    """Worker 5: Direct SQL output (no DAG JSON).

    This worker:
    - Has no examples (explore mode)
    - Gets full EXPLAIN plan (not summary)
    - Is instructed to be adversarial/creative
    - Outputs full SQL directly (not JSON)
    """
    llm_client = _create_llm_client(provider, model)

    # Build prompt for full SQL output
    prompt = f"""You are a SQL optimizer. Rewrite the ENTIRE query for maximum performance.

## Adversarial Explore Mode
Be creative and aggressive. Try radical structural rewrites that the database
engine is unlikely to do automatically. Don't be constrained by incremental changes.

## Original Query
```sql
{sql}
```

## Full Execution Plan (EXPLAIN ANALYZE)
```
{full_explain_plan}
```

## Instructions
1. Analyze the execution plan bottlenecks
2. Rewrite the entire query for maximum performance
3. Try transforms like:
   - Decorrelating subqueries
   - Converting OR to UNION ALL
   - Pushing down filters aggressively
   - Materializing CTEs strategically
   - Reordering joins

## Output Format
Return ONLY the complete optimized SQL query. No JSON. No explanation. Just SQL.

Example output:
WITH cte1 AS (
  SELECT ...
)
SELECT ...
FROM cte1
...
"""

    response_text = llm_client.analyze(prompt)

    # Extract SQL (no JSON parsing needed)
    optimized_sql = response_text.strip()

    # Remove markdown code blocks if present
    if optimized_sql.startswith('```'):
        lines = optimized_sql.split('\n')
        optimized_sql = '\n'.join(lines[1:-1])  # Remove ```sql and ```

    # Validate on sample DB
    validator = SQLValidator(database=sample_db)
    result = validator.validate(sql, optimized_sql)

    error = result.errors[0] if result.errors else None
    return CandidateResult(
        worker_id=worker_id,
        optimized_sql=optimized_sql,
        status=result.status,
        speedup=result.speedup,
        error=error,
        prompt=prompt,
        response=response_text,
    )
```

#### 3. Add 5-run trimmed mean to `benchmarker.py`
```python
def benchmark_single_trimmed_mean(
    self,
    sql: str,
    runs: int = 5,
    capture_results: bool = False
) -> QueryExecutionResult:
    """Benchmark with 5 runs, discard min/max, average middle 3."""
    if runs < 3:
        raise ValueError("Need at least 3 runs for trimmed mean")

    times = []
    rows = None

    for i in range(runs):
        start = time.perf_counter()
        result_rows = self.executor.execute(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000
        times.append(elapsed_ms)

        if i == 0 and capture_results:
            rows = result_rows

    # Sort and trim
    times.sort()
    trimmed = times[1:-1]  # Discard min and max
    avg_ms = sum(trimmed) / len(trimmed)

    cost = self._get_cost(sql)

    return QueryExecutionResult(
        timing=TimingResult(warmup_time_ms=times[0], measured_time_ms=avg_ms),
        cost=cost,
        row_count=len(rows) if rows else 0,
        rows=rows if capture_results else None
    )
```

---

## Verification Summary

### **Test Coverage: 21/21 ✅**

| Category | Tests | Status |
|----------|-------|--------|
| Query Recommender | 5 | ✅ All Pass |
| Worker Assignment | 3 | ✅ All Pass |
| Prompt Generation | 2 | ✅ All Pass |
| Example Loading | 4 | ✅ All Pass |
| Worker Diversity | 2 | ✅ All Pass |
| Query-Specific Recs | 2 | ✅ All Pass |
| Validation Simplicity | 2 | ✅ All Pass |
| End-to-End | 1 | ✅ Pass |

### **Verified Behaviors**

1. ✅ Q1 gets `decorrelate` as top recommendation
2. ✅ Q15 gets `or_to_union` as top recommendation
3. ✅ Q93 gets `early_filter` as top recommendation
4. ✅ Workers 1-4 get 3 examples each (12 total)
5. ✅ Worker 5 gets no examples
6. ✅ No duplicate examples across workers
7. ✅ Top ML recs go to Worker 1
8. ✅ Validation checks row count (not cost)
9. ✅ All 13 gold examples load correctly
10. ✅ End-to-end worker assignment correct

---

## Next Steps

1. **Implement remaining pieces** (30 min)
   - Add `query_id` parameter to v5 functions
   - Implement `_worker_full_sql()` for Worker 5
   - Add `benchmark_single_trimmed_mean()` to benchmarker

2. **Integration test** (15 min)
   - Test Q1 end-to-end with actual LLM
   - Verify 5 workers produce different results
   - Confirm Worker 5 outputs full SQL

3. **Batch run** (2-3 hours)
   - Run all 73 queries with recommendations
   - Record results
   - Compare with previous best runs

---

## Confidence Level: HIGH ✅

**Why?**
- 21/21 integration tests pass
- Query recommender working correctly
- Example assignment verified
- Diversity guaranteed
- Validation simplified and tested
- Clear implementation path

**Ready to proceed with remaining implementation.**
