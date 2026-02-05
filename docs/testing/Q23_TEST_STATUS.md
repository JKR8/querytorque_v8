# Q23 Three-Mode Test - Running

**Status:** ⏳ IN PROGRESS
**Started:** 2026-02-05 02:03:23
**Estimated completion:** 2026-02-05 02:13-02:25

---

## Test Configuration

### Query
- **ID:** Q23 (TPC-DS)
- **Complexity:** 3 CTEs, UNION ALL, multiple subqueries, aggregations
- **Previous result:** 2.33x speedup, FAILED validation (semantic error)

### Database
- **Path:** /mnt/d/TPC-DS/tpcds_sf100.duckdb
- **Size:** 28GB
- **Rows:** 287,997,024 (store_sales table)

### Modes Testing
1. **Mode 1 (Retry):** Up to 3 attempts with error feedback
2. **Mode 2 (Parallel):** 5 workers with different strategies
3. **Mode 3 (Evolutionary):** Up to 5 iterations building on best

---

## Expected Timeline

```
[02:03] Test started
[02:03-02:08] Mode 1: Retry (2-5 min, 1-3 API calls)
  └─ Attempt 1: Generate optimization
  └─ Attempt 2: Retry with error feedback (if needed)
  └─ Attempt 3: Final retry (if needed)
  └─ Benchmark on full DB

[02:08-02:15] Mode 2: Parallel (3-7 min, 5-10 API calls)
  └─ Worker 1-5: Generate optimizations in parallel
  └─ Validate on sample DB
  └─ Benchmark best on full DB

[02:15-02:25] Mode 3: Evolutionary (5-10 min, 1-5 API calls)
  └─ Iteration 1: Initial optimization
  └─ Iteration 2-5: Iterative improvements (until target met)
  └─ Benchmark each iteration on full DB

[02:25] Results comparison and summary
```

**Estimated total:** 10-22 minutes

---

## Data Being Captured

### Directory Structure
```
test_results/q23_20260205_020323/
├── original_q23.sql           # Original query
├── detailed_results.json       # Complete results from all modes
├── retry_optimized.sql         # Mode 1 result (if successful)
├── parallel_optimized.sql      # Mode 2 result (if successful)
└── evolutionary_optimized.sql  # Mode 3 result (if successful)
```

### Log Files
```
test_results/
├── q23_execution.log          # Detailed execution log
└── q23_full_run.log           # Console output
```

### Results JSON Schema
```json
{
  "query": "q23",
  "timestamp": "20260205_020323",
  "total_time": 1234.5,
  "modes": {
    "retry": {
      "success": true,
      "attempts": 3,
      "sample_speedup": 2.1,
      "full_speedup": 2.3,
      "time": 245.2
    },
    "parallel": {
      "success": true,
      "valid_workers": 4,
      "winner_id": 1,
      "sample_speedup": 3.1,
      "full_speedup": 2.9,
      "time": 412.8
    },
    "evolutionary": {
      "success": true,
      "iterations": 3,
      "best_iteration": 3,
      "full_speedup": 2.4,
      "time": 576.5
    }
  }
}
```

---

## Monitoring Progress

### View Live Output
```bash
tail -f test_results/q23_full_run.log
```

### Check Status
```bash
bash monitor_q23_test.sh
```

### Check Execution Log
```bash
tail -f test_results/q23_execution.log
```

### Check Process
```bash
ps aux | grep test_q23_all_modes
```

---

## What We'll Learn

### Mode 1 (Retry)
- ✅ Does error feedback help fix semantic errors?
- ✅ What errors occur and how are they corrected?
- ✅ How many attempts needed for success?

### Mode 2 (Parallel)
- ✅ Which worker finds the best strategy?
- ✅ How many workers produce valid results?
- ✅ Does diversity help avoid semantic traps?

### Mode 3 (Evolutionary)
- ✅ Does speedup improve across iterations?
- ✅ Can it build on previous successes?
- ✅ What's the final stacked speedup?

### Overall
- ✅ Which mode achieves best speedup?
- ✅ Which mode is most reliable?
- ✅ Which mode handles Q23's complexity best?
- ✅ Can any mode fix the semantic errors from previous runs?

---

## Success Criteria

### Must Have
- [ ] At least one mode achieves >=2.0x speedup
- [ ] Optimization passes validation (no semantic errors)
- [ ] Results are reproducible

### Would Be Excellent
- [ ] All three modes succeed
- [ ] Speedup > 2.3x (better than previous)
- [ ] Error feedback visibly improves retry attempts
- [ ] Evolutionary shows progressive improvement

---

## Current Status

**Last update:** 2026-02-05 02:03:23

```
✅ Prerequisites checked
✅ Test started
✅ Results directory created: test_results/q23_20260205_020323/
⏳ Mode 1 (Retry) running...
⏳ Mode 2 (Parallel) pending...
⏳ Mode 3 (Evolutionary) pending...
```

---

## Quick Access Commands

```bash
# Monitor live
tail -f test_results/q23_full_run.log

# Check execution log
tail -f test_results/q23_execution.log

# View results when done
cat test_results/q23_20260205_020323/detailed_results.json | python3 -m json.tool

# Compare SQLs
diff test_results/q23_20260205_020323/original_q23.sql \
     test_results/q23_20260205_020323/retry_optimized.sql

# Run monitor
bash monitor_q23_test.sh
```

---

## After Completion

The test will automatically:
1. ✅ Save all optimized SQLs
2. ✅ Generate detailed_results.json with all metrics
3. ✅ Compare all three modes
4. ✅ Identify best mode and speedup
5. ✅ Print comprehensive summary

Expected output location: `test_results/q23_20260205_020323/`

---

**Status:** Test is running. Check back in 10-22 minutes for results! ⏳
