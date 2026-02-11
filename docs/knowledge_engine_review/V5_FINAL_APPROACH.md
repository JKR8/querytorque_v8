# Adaptive Rewriter V5 - Final Approach

**Date:** 2026-02-04
**Status:** ✅ READY

---

## Confirmed Design Decisions

### 1. ❌ **Cost-Based Ranking Dropped**
- **Lab testing shows**: EXPLAIN cost is useless for ranking
- **New approach**: Simple tick/cross validation
  - ✅ Query runs without error
  - ✅ Same row count as original
  - ❌ Any errors or row count mismatch

### 2. ✅ **Query-Specific Gold Examples**
- **Use ML recommendations from `query_recommendations_report.md`**
- Each query gets Top 3 recommended transforms
- Based on:
  - Pattern detection (GLD-001 to GLD-007)
  - Structural similarity (FAISS)
  - Historical speedup data

**Example - Q1:**
```
Top 3 Recommendations:
1. decorrelate  (76% confidence, 2.92x speedup) ✓ MATCH
2. early_filter (35% confidence, 2.15x speedup)
```

**Example - Q15:**
```
Top 3 Recommendations:
1. or_to_union  (76% confidence, 2.78x speedup) ✓ MATCH
2. early_filter (35% confidence, 2.15x speedup)
3. decorrelate  (18% confidence, 2.92x speedup)
```

### 3. ✅ **Query Recommender Module**
Created `qt_sql/optimization/query_recommender.py`:

```python
from qt_sql.optimization.query_recommender import get_query_recommendations

# Get top 3 recommended examples for Q1
examples = get_query_recommendations('q1', top_n=3)
# Returns: ['decorrelate', 'early_filter']

# Get top 3 for Q15
examples = get_query_recommendations('q15', top_n=3)
# Returns: ['or_to_union', 'early_filter', 'decorrelate']
```

**Features:**
- Parses `query_recommendations_report.md`
- Caches results for fast lookup
- Returns example IDs in priority order

---

## Updated V5 Flow

### **Mode 1: Sequential (1 + 3 retry)**
```
1. Get query-specific gold examples (Top 3 from ML)
2. LLM call with Top 3 examples
3. Validate on sample DB
   └─ If FAIL: Retry up to 3x with error feedback
4. Return first valid candidate
```

### **Mode 2: 5x Parallel**
```
1. Get query-specific gold examples (Top 3 from ML)

2. Fire 5 parallel workers:
   Worker 1: Example #1 only (highest confidence)
   Worker 2: Example #2 only
   Worker 3: Example #3 only
   Worker 4: Examples #1-3 combined
   Worker 5: No examples (explore mode with full EXPLAIN)

3. Each worker validates on sample DB
   └─ Valid = runs without error + same row count

4. Return ALL valid candidates
```

---

## Validation Simplified

### **Sample DB Validation**
```python
# Simple check
valid = (
    query_runs_without_error(optimized_sql) AND
    row_count(original_sql) == row_count(optimized_sql)
)
```

**NO:**
- ❌ EXPLAIN cost ranking
- ❌ Execution time on sample
- ❌ Value-level comparison
- ❌ Checksum validation

**YES:**
- ✅ Query executes
- ✅ Row counts match

### **Full DB Validation**
```python
# 5-run trimmed mean for accurate timing
times = []
for i in range(5):
    times.append(execute_and_time(sql))

times.sort()
trimmed_mean = mean(times[1:-1])  # Discard min/max, avg middle 3
```

**Process:**
1. Run each valid candidate 5 times on full DB
2. Discard highest and lowest times
3. Average middle 3 runs
4. Return candidate with best speedup

---

## Implementation Status

### ✅ **READY NOW**
1. ✅ Query recommender module working
2. ✅ Parses 73 queries from recommendations report
3. ✅ Returns Top N examples per query
4. ✅ Caching for performance
5. ✅ Validation module exists (SQLValidator)
6. ✅ 5-worker parallel mode exists

### ⚠️ **NEEDS UPDATE** (30 min)

#### Change 1: Use Query-Specific Examples
**File:** `adaptive_rewriter_v5.py`

**Current:**
```python
# Generic batching
examples = get_matching_examples(sql)  # All 13 examples
batches = _split_example_batches(examples, batch_size=3)
```

**New:**
```python
# Query-specific examples
from qt_sql.optimization.query_recommender import get_query_recommendations

# Extract query ID from SQL or pass as parameter
query_id = extract_query_id(sql)  # e.g., 'q1', 'q15'
example_ids = get_query_recommendations(query_id, top_n=3)

# Load only those specific examples
examples = [load_example(eid) for eid in example_ids]

# Worker assignment:
# Worker 1: [examples[0]]       # Top recommendation
# Worker 2: [examples[1]]       # 2nd recommendation
# Worker 3: [examples[2]]       # 3rd recommendation
# Worker 4: examples[0:3]       # All 3 combined
# Worker 5: []                  # Explore mode
```

#### Change 2: Simplify Sample Validation
**File:** `adaptive_rewriter_v5.py`

**Current:**
```python
result = validator.validate(sql, optimized_sql)
if result.status == ValidationStatus.PASS:
    # Valid candidate
```

**Keep as is** - SQLValidator already checks row counts

**Remove:**
- Cost-based sorting
- Any logic that ranks by EXPLAIN cost

#### Change 3: Add 5-Run Trimmed Mean
**File:** `qt_sql/validation/benchmarker.py`

**Add method:**
```python
def benchmark_single_trimmed_mean(
    self,
    sql: str,
    runs: int = 5,
    capture_results: bool = False
) -> QueryExecutionResult:
    """5-run trimmed mean: discard min/max, average middle 3."""
    times = []
    rows = None

    for i in range(runs):
        start = time.perf_counter()
        result_rows = self.executor.execute(sql)
        times.append((time.perf_counter() - start) * 1000)

        if i == 0 and capture_results:
            rows = result_rows

    # Sort and trim
    times.sort()
    trimmed = times[1:-1]  # Remove min and max
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

## Usage Examples

### Example 1: Optimize Q1 with Query-Specific Examples
```python
from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_json_queue

sql = "<TPC-DS Q1 SQL>"
sample_db = "D:/TPC-DS/tpcds_sf1_sample.duckdb"
full_db = "D:/TPC-DS/tpcds_sf100.duckdb"

valid_candidates, full_results, winner = optimize_v5_json_queue(
    sql=sql,
    query_id='q1',  # NEW PARAMETER
    sample_db=sample_db,
    full_db=full_db,
    provider="deepseek",
    model="deepseek-chat",
    max_workers=5
)

# valid_candidates: All that passed sample validation
#   - Worker 1: Used 'decorrelate' example
#   - Worker 2: Used 'early_filter' example
#   - Worker 3: Used no example (explore)
#   - Worker 4: Used all 3 examples
#   - Worker 5: Used no examples (explore mode)

# full_results: Each candidate validated on full DB with 5-run trimmed mean

# winner: Best candidate (highest speedup >= 2.0x)
```

### Example 2: Batch Process All Queries
```python
from qt_sql.optimization.query_recommender import get_query_recommendations

# Get all queries with recommendations
query_ids = ['q1', 'q2', 'q3', ..., 'q99']

for query_id in query_ids:
    # Get recommended examples
    examples = get_query_recommendations(query_id, top_n=3)

    if not examples:
        print(f"{query_id}: No recommendations available")
        continue

    print(f"{query_id}: Using examples {examples}")

    # Load SQL
    sql = load_query_sql(query_id)

    # Optimize with query-specific examples
    valid, full, winner = optimize_v5_json_queue(
        sql=sql,
        query_id=query_id,
        sample_db=sample_db,
        full_db=full_db,
        provider="deepseek"
    )

    # Record results
    if winner:
        print(f"  ✓ Winner: {winner.full_speedup:.2f}x speedup")
    else:
        print(f"  ✗ No valid optimizations")
```

---

## Validation Report Format

### Per-Query Results
```json
{
  "query_id": "q1",
  "status": "success",
  "sample_validation": {
    "worker_1": {"example": "decorrelate", "valid": true},
    "worker_2": {"example": "early_filter", "valid": true},
    "worker_3": {"example": null, "valid": false},
    "worker_4": {"examples": ["decorrelate", "early_filter"], "valid": true},
    "worker_5": {"example": null, "valid": true, "explore_mode": true}
  },
  "full_db_results": [
    {
      "worker_id": 1,
      "example": "decorrelate",
      "speedup": 2.92,
      "original_time_ms": 12450.3,
      "optimized_time_ms": 4265.1,
      "timing_method": "5_run_trimmed_mean"
    },
    {
      "worker_id": 2,
      "example": "early_filter",
      "speedup": 1.85,
      "original_time_ms": 12450.3,
      "optimized_time_ms": 6730.2,
      "timing_method": "5_run_trimmed_mean"
    }
  ],
  "winner": {
    "worker_id": 1,
    "example": "decorrelate",
    "speedup": 2.92
  }
}
```

---

## Next Steps

### Immediate (30 min)
1. ✅ Create query_recommender.py (DONE)
2. ⚠️ Update adaptive_rewriter_v5.py to use query-specific examples
3. ⚠️ Add 5-run trimmed mean to benchmarker.py
4. ⚠️ Test on Q1, Q15, Q93

### Testing (1 hour)
1. Run Q1 with new approach
2. Verify Top 3 examples loaded correctly
3. Confirm 5 workers use right examples
4. Validate 5-run trimmed mean on full DB
5. Compare results with previous runs

### Production (Ready after testing)
1. Batch process all 99 TPC-DS queries
2. Record results in `research/experiments/dspy_runs/v5_ml_guided_{timestamp}/`
3. Generate summary report
4. Compare with previous best runs

---

## Benefits of This Approach

### 1. **Query-Specific Intelligence**
- Each query gets examples proven to work on similar queries
- ML model provides confidence scores
- Top-1 hit rate: 50% (ML's #1 rec matches actual best)
- Top-3 hit rate: 58.3% (actual best in top 3)

### 2. **Simplified Validation**
- No wasted time on cost analysis (proven useless in lab)
- Fast tick/cross checks on sample DB
- Robust 5-run trimmed mean on full DB only

### 3. **Better Coverage**
- Worker 1-3: Individual examples (focused)
- Worker 4: Combined examples (comprehensive)
- Worker 5: Explore mode (creative)

### 4. **Maintainable**
- Clear separation: recommender → optimizer → validator
- Cached recommendations (fast lookups)
- Easy to update recommendations as new data arrives

---

## Summary

**Ready to implement:**
1. ✅ Query recommender working
2. ⚠️ Need to wire up query_id → examples in v5
3. ⚠️ Need 5-run trimmed mean
4. ⚠️ Remove cost sorting

**Estimated time:** 30-45 minutes

**Benefits:**
- Query-specific examples (50% hit rate on #1 rec)
- Simplified validation (no useless cost ranking)
- Robust timing (5-run trimmed mean)
- Better results expected

Should I proceed with the implementation?
