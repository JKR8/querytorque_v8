# V5 Benchmark - All 99 TPC-DS Queries

**Run 20 concurrent workers on ALL 99 TPC-DS queries**

---

## What This Does

- ✅ **All 99 TPC-DS queries** (Q1 through Q99)
- ✅ **20 workers per query** (1,980 total LLM API calls)
- ✅ **V5 JSON version** (not DSPy)
- ✅ **Sample DB validation only** (no full DB)
- ✅ **All generations saved** with validation results
- ✅ **Incremental CSV output** (nothing lost if interrupted)

---

## Run Full Benchmark

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8

./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh
```

**Expected time**: 1-2 hours
**Total API calls**: 1,980 (99 queries × 20 workers)

---

## Partial Run

```bash
# Run queries 1-20
./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh --start-from 1 --end-at 20

# Run queries 50-99
./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh --start-from 50 --end-at 99

# Resume from query 25
./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh --start-from 25
```

---

## Output Structure

```
research/experiments/v5_benchmark_20workers/run_20260204_160000/
│
├── config.json                  # Benchmark configuration
├── results.csv                  # CSV summary (updated incrementally)
├── final_summary.json           # Final summary
│
├── q1/                          # Query 1 results
│   ├── original.sql
│   ├── plan_summary.txt
│   ├── summary.json
│   ├── gen_01/ ... gen_20/      # 20 generations
│
├── q2/  ... q99/                # Same structure for all queries
```

---

## CSV Output

**File**: `results.csv` (updated after each query)

**Columns**:
```csv
query,status,elapsed,completed,valid,failed,best_speedup,best_worker,avg_speedup
1,success,45.3,20,15,5,2.45,11,1.65
2,success,38.7,20,12,8,1.89,7,1.34
3,success,52.1,20,18,2,3.12,14,2.01
...
```

---

## Console Output

```
======================================================================
V5 Benchmark - All Queries - 20 Workers Each
======================================================================

Benchmark directory: research/experiments/v5_benchmark_20workers/run_20260204_160000
Sample DB: /mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb
Queries: 1-99

======================================================================
Query 1
======================================================================
Running 20 workers...
  ✅ Gen 03: pass, 2.15x
  ✅ Gen 07: pass, 1.45x
  ❌ Gen 01: validation_failed, 0.00x
  ...

✅ Q1: 15/20 valid, best=2.45x, elapsed=45.3s

======================================================================
Query 2
======================================================================
Running 20 workers...
  ✅ Gen 02: pass, 1.89x
  ...

✅ Q2: 12/20 valid, best=1.89x, elapsed=38.7s

...

======================================================================
BENCHMARK COMPLETE
======================================================================

Total queries: 99
Successful: 97
Errors: 2

Total workers: 1980
Valid generations: 1485/1980 (75.0%)

Best speedup overall: 3.45x (Q23)
Average best speedup: 2.12x

Total time: 1.3h

Results CSV: research/experiments/v5_benchmark_20workers/run_20260204_160000/results.csv
Full outputs: research/experiments/v5_benchmark_20workers/run_20260204_160000
```

---

## Incremental Saving

**CSV is written after each query** - safe to interrupt!

```
Q1 completes → CSV row written, all 20 gens saved
Q2 completes → CSV row written, all 20 gens saved
Q3 completes → CSV row written, all 20 gens saved
[Ctrl+C pressed] → Q1-Q3 fully preserved in CSV + folders!
```

You can resume from where you left off.

---

## Resuming After Interruption

If interrupted at Q50:

```bash
# Resume from Q51
./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh --start-from 51
```

The new run will create a new directory, but you can merge CSVs later.

---

## Final Summary

After completion, `final_summary.json`:

```json
{
  "total_queries": 99,
  "successful": 97,
  "errored": 2,
  "total_workers": 1980,
  "total_valid": 1485,
  "valid_rate": 75.0,
  "best_overall": {
    "query": 23,
    "best_speedup": 3.45,
    "best_worker": 14
  },
  "avg_best_speedup": 2.12,
  "elapsed_hours": 1.3
}
```

---

## Analysis After Completion

### View CSV

```bash
# View all results
cat research/experiments/v5_benchmark_20workers/run_*/results.csv

# Count valid queries
awk -F, '$3=="success" && $7>2.0 {print $1}' results.csv | wc -l

# Top 10 speedups
sort -t, -k7 -rn results.csv | head -10
```

### Query-Specific Analysis

```bash
# View Q23 results (best overall)
cat research/experiments/v5_benchmark_20workers/run_*/q23/summary.json

# View Q23 best generation
BEST=$(jq -r '.best_worker' research/experiments/v5_benchmark_20workers/run_*/q23/summary.json)
cat research/experiments/v5_benchmark_20workers/run_*/q23/gen_$(printf '%02d' $BEST)/optimized.sql
```

### Aggregate Stats

```bash
# Total valid generations across all queries
jq -s 'map(.valid_count) | add' research/experiments/v5_benchmark_20workers/run_*/q*/summary.json

# Average speedup across all queries
jq -s 'map(.avg_speedup) | add / length' research/experiments/v5_benchmark_20workers/run_*/q*/summary.json

# Distribution of best speedups
jq -r '.best_speedup' research/experiments/v5_benchmark_20workers/run_*/q*/summary.json | \
  awk '{
    if ($1 < 1) low++
    else if ($1 < 2) med++
    else high++
  }
  END {
    print "< 1.0x:", low
    print "1.0-2.0x:", med
    print "> 2.0x:", high
  }'
```

---

## Expected Results

### Per Query

- **20 workers**: All complete in parallel
- **Valid rate**: 60-80% typically
- **Best speedup**: Varies (0.5x to 3x+)
- **Time**: 30-60s per query

### Overall Benchmark

- **Total time**: 1-2 hours (99 queries × ~45s average)
- **Total API calls**: 1,980
- **Expected valid**: ~1,400-1,600 (70-80%)
- **Queries with ≥2x**: 30-50 queries (30-50%)

---

## Monitoring Progress

### Watch CSV

```bash
# In another terminal
watch -n 5 "tail -20 research/experiments/v5_benchmark_20workers/run_*/results.csv"

# Count completed
wc -l research/experiments/v5_benchmark_20workers/run_*/results.csv

# Check last query
tail -1 research/experiments/v5_benchmark_20workers/run_*/results.csv
```

### Check Specific Query

```bash
# See if Q50 started
ls research/experiments/v5_benchmark_20workers/run_*/q50/ 2>/dev/null

# View Q50 progress
ls research/experiments/v5_benchmark_20workers/run_*/q50/gen_* 2>/dev/null | wc -l
```

---

## Resource Usage

### API Calls

- **Total**: 1,980 LLM API calls
- **Parallel**: 20 per query
- **Sequential**: 99 queries

### Disk Space

- **Per generation**: ~50-100 KB (SQL + prompt + response + validation)
- **Per query**: ~1-2 MB (20 generations + metadata)
- **Total**: ~100-200 MB for full benchmark

### Memory

- **Peak**: ~2-4 GB (20 concurrent workers + DuckDB)

---

## Comparison to Original v5

| Aspect | Original v5_queue | This (20 workers) |
|--------|------------------|-------------------|
| Workers per query | 5 | 20 |
| Validation | Sample + Full DB | Sample only |
| Time per query | 1-5 min | 30-60s |
| Total time | 4-6 hours | 1-2 hours |
| Coverage | 5 attempts | 20 attempts |
| Use case | Production quality | Fast exploration |

---

## Quick Commands

```bash
# Run full benchmark
./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh

# Run subset
./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh --start-from 1 --end-at 20

# Resume from Q50
./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh --start-from 50

# View latest results
LATEST=$(ls -td research/experiments/v5_benchmark_20workers/run_* | head -1)
cat $LATEST/results.csv

# View summary
cat $LATEST/final_summary.json | jq
```

---

## After Completion

1. **Review CSV**: Check `results.csv` for overview
2. **Analyze winners**: Queries with best speedups
3. **Study failures**: Why did some queries fail?
4. **Extract patterns**: Which rewrites work best?
5. **Update BENCHMARKS.md**: Record results

---

**Ready to run all 99 queries?**

```bash
./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh
```

**1,980 LLM API calls** across **99 queries** with **20 workers each**! 🚀
