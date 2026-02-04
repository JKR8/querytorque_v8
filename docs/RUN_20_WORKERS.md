# V5 Parallel - 20 Workers Test

**20 concurrent API calls on one query with complete output recording**

---

## What This Does

- ✅ **20 concurrent LLM API calls** (not 5)
- ✅ **V5 JSON version** (not DSPy)
- ✅ **Sample DB validation only** (no full DB testing)
- ✅ **All 20 generations saved** with validation results
- ✅ **Incremental saving** (nothing lost if interrupted)

---

## Run Now

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8

# Test query 1 with 20 workers
./scripts/run_v5_20workers.sh 1

# Test other queries
./scripts/run_v5_20workers.sh 15
./scripts/run_v5_20workers.sh 23
```

**API key auto-loaded** from `DeepseekV3.txt`

---

## What Happens

1. **Loads query** from TPC-DS
2. **Analyzes execution plan** on 1% sample DB
3. **Launches 20 workers** in parallel
4. **Each worker**:
   - Makes LLM API call (different examples/prompts)
   - Gets optimized SQL
   - Validates on 1% sample DB
   - Saves immediately (prompt, response, SQL, validation)
5. **Aggregates results** and saves summary

**Expected time**: 30-60 seconds (all 20 calls in parallel)

---

## Output Structure

```
research/experiments/v5_parallel_20/q1_20260204_150000/
├── config.json              # Test configuration
├── original.sql             # Original query
├── plan_summary.txt         # Execution plan summary
├── plan_full.txt            # Full EXPLAIN output
├── plan.json                # Plan JSON
├── base_prompt.txt          # Base DAG prompt
├── summary.json             # Machine-readable results
├── summary.txt              # Human-readable summary
│
├── gen_01/                  # Generation 1
│   ├── optimized.sql        # Generated SQL
│   ├── validation.json      # Validation result
│   ├── prompt.txt           # LLM prompt
│   └── response.txt         # LLM response
│
├── gen_02/  ... gen_20/     # Generations 2-20
```

---

## Validation Results

Each `validation.json`:

```json
{
  "worker_id": 1,
  "status": "pass",           // "pass" or "validation_failed"
  "speedup": 1.87,
  "error": null,
  "original_time": 0.5,
  "optimized_time": 0.27
}
```

**Status**: Simple YES/NO validation on 1% sample DB only

---

## Summary Output

```json
{
  "query_number": 1,
  "total_workers": 20,
  "completed": 20,
  "valid_count": 15,
  "failed_count": 5,
  "valid_workers": [1, 2, 3, 5, 7, 8, 9, 11, 13, 14, 15, 16, 17, 18, 20],
  "speedups": {
    "1": 1.23,
    "2": 1.87,
    "3": 2.15,
    ...
  },
  "best_speedup": 2.45,
  "best_worker": 11,
  "avg_speedup": 1.65
}
```

---

## Console Output

```
======================================================================
V5 Parallel Test - 20 Workers - Query 1
======================================================================

Output directory: research/experiments/v5_parallel_20/q1_20260204_150000

Loading query 1...
✅ Query loaded (1234 chars)

Sample DB: /mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb

Analyzing execution plan...
✅ Plan analyzed

Running 20 workers in parallel...
Making 20 concurrent LLM API calls...

  ✅ Gen 03: pass, 2.15x
  ✅ Gen 07: pass, 1.45x
  ❌ Gen 01: validation_failed, 0.00x
  ✅ Gen 11: pass, 2.45x
  ✅ Gen 05: pass, 1.23x
  ...

======================================================================
RESULTS
======================================================================

Elapsed: 45.3s
Completed: 20/20 workers

✅ Valid: 15/20
❌ Failed: 5/20

Top 10 Speedups:
----------------------------------------------------------------------
  Gen 11: 2.45x
  Gen 03: 2.15x
  Gen 14: 2.08x
  Gen 02: 1.87x
  Gen 17: 1.76x
  Gen 09: 1.65x
  Gen 13: 1.58x
  Gen 07: 1.45x
  Gen 08: 1.34x
  Gen 05: 1.23x

🏆 Best Generation: #11 with 2.45x speedup

Average speedup (valid only): 1.65x

======================================================================
✅ All outputs saved to: research/experiments/v5_parallel_20/q1_20260204_150000
======================================================================
```

---

## Exploring Results

### View Summary

```bash
cat research/experiments/v5_parallel_20/q1_*/summary.txt
```

### Find Best Generation

```bash
# Get best worker ID
BEST=$(jq -r '.best_worker' research/experiments/v5_parallel_20/q1_*/summary.json)

# View best SQL
cat research/experiments/v5_parallel_20/q1_*/gen_$(printf '%02d' $BEST)/optimized.sql

# View best prompt
cat research/experiments/v5_parallel_20/q1_*/gen_$(printf '%02d' $BEST)/prompt.txt

# View best response
cat research/experiments/v5_parallel_20/q1_*/gen_$(printf '%02d' $BEST)/response.txt
```

### Compare All Generations

```bash
# List all speedups
for f in research/experiments/v5_parallel_20/q1_*/gen_*/validation.json; do
    echo "$(basename $(dirname $f)): $(jq -r '.speedup' $f)x ($(jq -r '.status' $f))"
done | sort -t: -k2 -rn

# View all valid SQLs
for f in research/experiments/v5_parallel_20/q1_*/gen_*/validation.json; do
    if [ "$(jq -r '.status' $f)" = "pass" ]; then
        echo "=== $(basename $(dirname $f)) ==="
        cat $(dirname $f)/optimized.sql
        echo ""
    fi
done
```

### Check Failed Generations

```bash
# List failed generations
for f in research/experiments/v5_parallel_20/q1_*/gen_*/validation.json; do
    if [ "$(jq -r '.status' $f)" != "pass" ]; then
        echo "$(basename $(dirname $f)): $(jq -r '.error' $f)"
    fi
done
```

---

## Incremental Saving

**Saves immediately as each worker completes:**

```
Gen 3 completes → ✅ gen_03/ saved
Gen 7 completes → ✅ gen_07/ saved
Gen 11 completes → ✅ gen_11/ saved
[Ctrl+C pressed] → ✅ All completed gens preserved!
```

Even with interruption, completed generations are fully saved.

---

## Testing Different Queries

```bash
# Simple queries
./scripts/run_v5_20workers.sh 1
./scripts/run_v5_20workers.sh 3
./scripts/run_v5_20workers.sh 7

# Medium complexity
./scripts/run_v5_20workers.sh 15
./scripts/run_v5_20workers.sh 23

# Complex queries
./scripts/run_v5_20workers.sh 39
./scripts/run_v5_20workers.sh 74
```

---

## Why 20 Workers?

- **More diverse generations**: 20 different optimization attempts
- **Higher success chance**: More shots at finding good optimizations
- **Concurrent execution**: All 20 run in parallel (~30-60s total)
- **Sample DB only**: Fast validation (no full DB overhead)

**Trade-off**: More API calls, but much faster than sequential + more coverage

---

## Example Analysis

After running, you can:

1. **Find best speedup**: Check `summary.json` for `best_worker`
2. **Compare prompts**: See what different workers got as examples
3. **Study failures**: Check error messages in failed generations
4. **Extract patterns**: Which types of rewrites work best?

---

## Comparison: 5 vs 20 Workers

| Aspect | 5 Workers (v5_queue) | 20 Workers (this) |
|--------|---------------------|-------------------|
| API calls | 5 on sample + validations | 20 on sample only |
| Time | 1-5 min (sequential full DB) | 30-60s (parallel sample only) |
| Coverage | 4 coverage + 1 explore | 20 diverse attempts |
| Validation | Sample + Full DB | Sample only |
| Use case | Production quality | Fast exploration |

---

## Quick Commands

```bash
# Run test
./scripts/run_v5_20workers.sh 1

# View summary
cat research/experiments/v5_parallel_20/q1_*/summary.txt

# Find best
jq -r '.best_worker' research/experiments/v5_parallel_20/q1_*/summary.json

# View best SQL
BEST=$(jq -r '.best_worker' research/experiments/v5_parallel_20/q1_*/summary.json)
cat research/experiments/v5_parallel_20/q1_*/gen_$(printf '%02d' $BEST)/optimized.sql

# List all results
ls -la research/experiments/v5_parallel_20/q1_*/
```

---

**Ready to run 20 concurrent workers?**

```bash
./scripts/run_v5_20workers.sh 1
```

All 20 generations will be saved with validation results! 🚀
