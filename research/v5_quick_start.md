# V5 Benchmark Quick Start Guide

**Status**: ✅ Ready to run (after setup)

---

## TL;DR - Run This

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8

# 1. Set API key
export DEEPSEEK_API_KEY=your_key_here

# 2. Setup and test
./scripts/setup_v5_benchmark.sh

# 3. Run full benchmark (4-6 hours)
./scripts/run_v5_benchmark.sh
```

---

## What Gets Run

### Parallel Sample Optimization (5 workers)

**Workers 1-4**: Coverage optimization
- Use DAG-based rewrites
- Include 3 few-shot examples per worker
- Auto-retry on validation failure

**Worker 5**: Explore mode
- No examples (adversarial)
- Full execution plan details
- Structural rewrites

### Sequential Full Validation

- Tests valid candidates on full SF100 DB
- Stops when first ≥2.0x speedup found
- Returns winner or all results

---

## Output

### CSV File

Location: `research/experiments/benchmarks/v5_parallel_YYYYMMDD_HHMMSS.csv`

Columns:
- `query` - Query number (1-99)
- `valid_sample_count` - Workers that passed validation
- `sample_workers` - Worker IDs (e.g., "1,3,5")
- `sample_speedups` - Speedups (e.g., "0.99;1.87;0.45")
- `sample_best_speedup` - Best sample speedup
- `full_workers` - Workers validated on full DB
- `full_speedups` - Full DB speedups
- `winner_found` - True/False
- `winner_worker` - Winner worker ID
- `winner_full_speedup` - Winner speedup on full DB
- `elapsed_s` - Time in seconds

### Summary File

Location: `research/experiments/benchmarks/v5_parallel_YYYYMMDD_HHMMSS_summary.txt`

Contains:
- Total queries processed
- Winners found
- Win rate
- Top 10 speedups
- Average speedup

---

## Manual Run (Without Scripts)

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8

# Set API key
export DEEPSEEK_API_KEY=your_key_here

# Install packages (if needed)
pip install -e packages/qt-shared packages/qt-sql

# Generate timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Run benchmark
python3 packages/qt-sql/scripts/run_v5_benchmark.py \
  --sample-db /mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb \
  --full-db /mnt/d/TPC-DS/tpcds_sf100.duckdb \
  --queries-dir /mnt/d/TPC-DS/queries_duckdb_converted \
  --output-csv "research/experiments/benchmarks/v5_parallel_${TIMESTAMP}.csv" \
  --max-workers 5 \
  --exclude "2,9"
```

---

## Test Single Query

```bash
python3 << 'EOF'
import sys
sys.path.insert(0, 'packages/qt-sql')
from pathlib import Path
from qt_sql.optimization import optimize_v5_json_queue

sql = Path("/mnt/d/TPC-DS/queries_duckdb_converted/query_1.sql").read_text()
valid, full_results, winner = optimize_v5_json_queue(
    sql=sql,
    sample_db="/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb",
    full_db="/mnt/d/TPC-DS/tpcds_sf100.duckdb",
    max_workers=5,
    target_speedup=2.0,
)

print(f"Valid: {len(valid)}, Full: {len(full_results)}, Winner: {bool(winner)}")
if winner:
    print(f"Speedup: {winner.full_speedup:.2f}x")
EOF
```

---

## Monitor Progress

```bash
# Watch CSV file
watch -n 5 "tail -20 research/experiments/benchmarks/v5_parallel_*.csv"

# Check last query
tail -1 research/experiments/benchmarks/v5_parallel_*.csv

# Count completed
wc -l research/experiments/benchmarks/v5_parallel_*.csv
```

---

## Troubleshooting

### Import Error

```
ModuleNotFoundError: No module named 'qt_sql'
```

**Fix**: Install packages
```bash
pip install -e packages/qt-shared packages/qt-sql
```

### API Key Error

```
No API key found
```

**Fix**: Export key
```bash
export DEEPSEEK_API_KEY=your_key_here
```

### Database Not Found

```
FileNotFoundError: /mnt/d/TPC-DS/...
```

**Fix**: Verify paths
```bash
ls -lh /mnt/d/TPC-DS/*.duckdb
```

---

## Expected Results

### Per Query

- **Runtime**: 1-5 minutes per query
- **Valid candidates**: 0-5 (typically 2-4)
- **Winner**: 0-1 (when speedup ≥2.0x)

### Full Benchmark

- **Total time**: 4-6 hours for 97 queries
- **Win rate**: ~30-50% based on previous runs
- **Average speedup**: 1.5-2.5x for winners

---

## Next Steps After Completion

1. Review CSV: `less research/experiments/benchmarks/v5_parallel_*.csv`
2. View summary: `cat research/experiments/benchmarks/v5_parallel_*_summary.txt`
3. Update BENCHMARKS.md
4. Analyze failures and low-speedup queries
5. Consider re-running Q2 and Q9 for consistency

---

## Files Created

| File | Purpose |
|------|---------|
| `research/v5_benchmark_readiness_report.md` | Detailed analysis and setup guide |
| `research/v5_quick_start.md` | This file - quick reference |
| `scripts/setup_v5_benchmark.sh` | Setup and test script |
| `scripts/run_v5_benchmark.sh` | Full benchmark runner |

---

## Related Documentation

- **Code review**: `research/dspy_v5_review.md`
- **Implementation**: `packages/qt-sql/qt_sql/optimization/adaptive_rewriter_v5.py`
- **Benchmark script**: `packages/qt-sql/scripts/run_v5_benchmark.py`
- **Project context**: `CLAUDE.md`
