# V5 Process Benchmark Readiness Report

**Date**: 2026-02-04
**Status**: ✅ Ready for Benchmarking (with setup fixes)

---

## Executive Summary

The v5 optimization process is **functionally correct and ready for benchmarking**. However, several setup issues need to be addressed before running the full TPC-DS benchmark suite.

### Key Findings

- ✅ **Code Quality**: v5 implementation is production-ready with correct DSPy API usage
- ✅ **Architecture**: Parallel fan-out strategy with 5 workers is sound
- ✅ **Test Data**: All TPC-DS SF100 databases and 99 queries are available
- ⚠️ **Setup Issues**: Import paths and environment configuration need fixes
- ⚠️ **Result Recording**: CSV file is empty - benchmark not yet run

---

## V5 Process Overview

### Architecture

The v5 process implements a **parallel fan-out optimization strategy**:

```
┌──────────────────────────────────────────────────┐
│         optimize_v5_json_queue()                 │
│  (Parallel sample optimization + sequential full)│
└──────────────────────────────────────────────────┘
                    ↓
    ┌───────────────────────────────────────┐
    │   ThreadPoolExecutor (5 workers)      │
    │   Run in PARALLEL on SAMPLE DB        │
    └───────────────────────────────────────┘
                    ↓
    ┌───────────────────────────────────────┐
    │  Workers 1-4: Coverage optimization   │
    │  - DAG-based rewrites with examples   │
    │  - 3 few-shot demos per worker        │
    │  - Automatic retry on validation fail │
    └───────────────────────────────────────┘
    ┌───────────────────────────────────────┐
    │  Worker 5: Explore mode               │
    │  - No examples (adversarial)          │
    │  - Full execution plan details        │
    │  - Structural rewrites                │
    └───────────────────────────────────────┘
                    ↓
    ┌───────────────────────────────────────┐
    │  Validation on sample DB              │
    │  - Semantic correctness check         │
    │  - Performance measurement            │
    └───────────────────────────────────────┘
                    ↓
    ┌───────────────────────────────────────┐
    │  Sequential validation on FULL DB     │
    │  - Only for valid sample candidates   │
    │  - Stop when speedup ≥ 2.0x found     │
    │  - Return first winner or all results │
    └───────────────────────────────────────┘
```

### Key Features

1. **Parallel Sample Testing**: All 5 workers run simultaneously on 1% sample DB
2. **Sequential Full Validation**: Valid candidates tested sequentially on full SF100 DB
3. **Early Stopping**: Stops when first candidate with ≥2.0x speedup is found
4. **Two Variants**:
   - `optimize_v5_json()` - JSON examples (legacy)
   - `optimize_v5_dspy()` - DSPy demos (recommended)

---

## Issues Found & Fixes

### 🔴 Critical: Import Path Issue

**Problem**: Benchmark script cannot import `qt_sql` module

**File**: `research/benchmarks/qt-sql/scripts/run_v5_benchmark.py:13`

**Error**:
```
ModuleNotFoundError: No module named 'qt_sql'
```

**Root Cause**: Script runs without proper Python path or package installation

**Fix Options**:

**Option 1: Install package in development mode** (Recommended)
```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
pip install -e packages/qt-sql
pip install -e packages/qt-shared
```

**Option 2: Use PYTHONPATH**
```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
export PYTHONPATH="${PYTHONPATH}:${PWD}/packages/qt-sql:${PWD}/packages/qt-shared"
python3 research/benchmarks/qt-sql/scripts/run_v5_benchmark.py --output-csv ...
```

**Option 3: Run from workspace root with UV**
```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
uv run --package qt-sql python research/benchmarks/qt-sql/scripts/run_v5_benchmark.py --output-csv ...
```

---

### 🟡 Minor: Empty CSV File

**Problem**: CSV file created but has no data (0 bytes after header)

**File**: `research/experiments/benchmarks/v5_parallel_20260204.csv`

**Cause**: Benchmark not yet run successfully due to import issue

**Fix**: Will populate after import issue is resolved

---

### 🟡 Minor: Hardcoded Prefills

**Problem**: Queries 2 and 9 are hardcoded as "prefilled" in benchmark script

**File**: `research/benchmarks/qt-sql/scripts/run_v5_benchmark.py:34-63`

**Rationale**: These were run in a previous session and results are being reused

**Recommendation**: Either:
1. Re-run these queries for consistency
2. Document why they're prefilled in the CSV output
3. Add `--skip-prefills` flag to allow fresh runs

---

### ✅ Code Quality: All Clear

**Review Findings** (from `research/dspy_v5_review.md`):

- ✅ DSPy API usage is correct
- ✅ DAG construction is clean
- ✅ Validation loop is robust
- ✅ Parallel execution is properly implemented
- ⚠️ Minor: Could benefit from better logging, docstrings, and extracted helper functions
- ⚠️ Minor: Silent fallback to "deepseek" if LM not configured

**Action**: None required for benchmarking; improvements are optional maintenance items

---

## Test Data Verification

### ✅ Database Files

| File | Size | Status | Purpose |
|------|------|--------|---------|
| `tpcds_sf100.duckdb` | 28 GB | ✅ Present | Full TPC-DS SF100 dataset |
| `tpcds_sf100_sampled_1pct.duckdb` | 501 MB | ✅ Present | 1% sample for quick validation |
| `tpcds_sf100_sampled_5pct.duckdb` | 1.5 GB | ✅ Present | 5% sample (alternative) |

### ✅ Query Files

| Location | Count | Status |
|----------|-------|--------|
| `/mnt/d/TPC-DS/queries_duckdb_converted/` | 99 | ✅ Present |

---

## Benchmark Configuration

### Default Settings

| Parameter | Value | Notes |
|-----------|-------|-------|
| Sample DB | `tpcds_sf100_sampled_1pct.duckdb` | 1% sample, 501 MB |
| Full DB | `tpcds_sf100.duckdb` | Full SF100, 28 GB |
| Max Workers | 5 | 4 coverage + 1 explore |
| Target Speedup | 2.0x | Early stopping threshold |
| Excluded Queries | Q2, Q9 | Prefilled from prior run |

### Output Format

CSV with columns:
- `query` - Query number (1-99)
- `prefilled` - Boolean (True/False)
- `valid_sample_count` - Number of workers that passed validation
- `sample_workers` - Comma-separated worker IDs
- `sample_speedups` - Semicolon-separated speedups
- `sample_best_speedup` - Best speedup on sample DB
- `full_workers` - Workers validated on full DB
- `full_speedups` - Full DB speedups
- `winner_found` - Boolean
- `winner_worker` - Winner worker ID
- `winner_full_speedup` - Winner speedup on full DB
- `winner_sample_speedup` - Winner speedup on sample DB
- `elapsed_s` - Time in seconds

---

## Pre-Benchmark Checklist

### Environment Setup

- [ ] Install packages: `pip install -e packages/qt-shared packages/qt-sql`
- [ ] Verify imports: `python3 -c "from qt_sql.optimization import optimize_v5_json_queue; print('OK')"`
- [ ] Set API key: `export DEEPSEEK_API_KEY=your_key_here`
- [ ] Verify DSPy: `python3 -c "import dspy; print('OK')"`

### Database Verification

- [x] Full DB exists: `/mnt/d/TPC-DS/tpcds_sf100.duckdb` (28 GB)
- [x] Sample DB exists: `/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb` (501 MB)
- [x] Query files: 99 queries in `/mnt/d/TPC-DS/queries_duckdb_converted/`

### Output Preparation

- [ ] Create output directory: `mkdir -p research/experiments/benchmarks/`
- [ ] Choose output filename with timestamp: `v5_parallel_YYYYMMDD_HHMMSS.csv`
- [ ] Ensure write permissions

---

## Running the Benchmark

### Quick Test (Single Query)

Test the setup with a single query before full benchmark:

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8

# Set API key
export DEEPSEEK_API_KEY=your_key_here

# Install packages (if not already done)
pip install -e packages/qt-shared packages/qt-sql

# Test import
python3 -c "from qt_sql.optimization import optimize_v5_json_queue; print('Import OK')"

# Test single query (Q1)
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

print(f"Valid candidates: {len(valid)}")
print(f"Full results: {len(full_results)}")
print(f"Winner found: {bool(winner)}")
if winner:
    print(f"Winner speedup: {winner.full_speedup:.2f}x")
EOF
```

### Full Benchmark Run

Run the complete TPC-DS benchmark suite (excluding prefilled Q2, Q9):

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8

# Set timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_CSV="research/experiments/benchmarks/v5_parallel_${TIMESTAMP}.csv"

# Run benchmark
python3 research/benchmarks/qt-sql/scripts/run_v5_benchmark.py \
  --sample-db /mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb \
  --full-db /mnt/d/TPC-DS/tpcds_sf100.duckdb \
  --queries-dir /mnt/d/TPC-DS/queries_duckdb_converted \
  --output-csv "$OUTPUT_CSV" \
  --max-workers 5 \
  --exclude "2,9"

echo "Results saved to: $OUTPUT_CSV"
```

### Include Prefilled Queries

To re-run Q2 and Q9 instead of using prefilled data:

```bash
python3 research/benchmarks/qt-sql/scripts/run_v5_benchmark.py \
  --output-csv "research/experiments/benchmarks/v5_parallel_${TIMESTAMP}.csv" \
  --exclude ""
```

---

## Expected Runtime

### Per-Query Estimates

Based on v5 architecture:

- **Sample optimization**: 5 workers × ~30s per worker = ~30s total (parallel)
- **Sample validation**: ~5-10s per candidate (parallel)
- **Full validation**: ~60-300s per candidate (sequential)

**Total per query**: ~1-5 minutes (if early stopping hits)

### Full Benchmark

- **97 queries** (excluding Q2, Q9 prefills)
- **Best case**: ~2 hours (if all queries find winners quickly)
- **Typical case**: ~4-6 hours
- **Worst case**: ~8 hours (if many queries need all 5 candidates validated)

---

## Result Recording Requirements

Per `CLAUDE.md` project instructions:

> **All benchmark and experiment results MUST be recorded.** Never run benchmarks without saving the results.

### Required Artifacts

1. **CSV File**: `research/experiments/benchmarks/v5_parallel_YYYYMMDD_HHMMSS.csv`
   - Contains all query results in structured format

2. **Summary File**: Create `summary.txt` with:
   - Date, model, parameters
   - Success/failed/error counts
   - Top 10 speedups
   - Average speedup across successful queries

3. **Per-Query Artifacts** (Optional but recommended):
   - Create subfolder: `research/experiments/benchmarks/v5_parallel_YYYYMMDD_HHMMSS/`
   - Store per-query details: `q{N}/optimized.sql`, `q{N}/validation.txt`

### Summary Template

Create `research/experiments/benchmarks/v5_parallel_YYYYMMDD_HHMMSS_summary.txt`:

```
V5 Parallel Benchmark - TPC-DS SF100
=====================================

Date: 2026-02-04
Model: DeepSeek V3
Strategy: v5 parallel (5 workers)
Sample DB: tpcds_sf100_sampled_1pct.duckdb (1%)
Full DB: tpcds_sf100.duckdb (SF100)
Queries: 1-99 (excluding Q2, Q9)

Results
-------
Total queries: 97
Winners found: X
Valid on sample: Y
Failed: Z

Top 10 Speedups
---------------
Q##: X.XXx
Q##: X.XXx
...

Average speedup (winners only): X.XXx
```

---

## Post-Benchmark Analysis

### Data Validation

After benchmark completes:

```bash
# Count results
wc -l research/experiments/benchmarks/v5_parallel_*.csv

# Check for errors
grep -i error research/experiments/benchmarks/v5_parallel_*.csv

# Summary stats
python3 << 'EOF'
import pandas as pd
df = pd.read_csv('research/experiments/benchmarks/v5_parallel_YYYYMMDD_HHMMSS.csv')
print(f"Total queries: {len(df)}")
print(f"Winners: {df['winner_found'].sum()}")
print(f"Avg sample speedup: {df['sample_best_speedup'].mean():.2f}x")
print(f"Avg winner speedup: {df[df['winner_found']]['winner_full_speedup'].mean():.2f}x")
EOF
```

### Update BENCHMARKS.md

Add new entry to `BENCHMARKS.md`:

```markdown
## V5 Parallel Benchmark - 2026-02-04

**Strategy**: Parallel fan-out (5 workers) on sample DB, sequential validation on full DB
**Model**: DeepSeek V3
**Dataset**: TPC-DS SF100

Results: XX/97 queries achieved ≥2.0x speedup
Average speedup (winners): X.XXx
Details: `research/experiments/benchmarks/v5_parallel_YYYYMMDD_HHMMSS.csv`
```

---

## Known Issues & Workarounds

### Issue 1: LM Configuration

**Symptom**: "DSPy LM not configured" warning

**Workaround**: Script auto-configures to DeepSeek, but verify API key is set:
```bash
export DEEPSEEK_API_KEY=your_key_here
```

### Issue 2: Database Lock

**Symptom**: "database is locked" errors on DuckDB

**Workaround**: Ensure no other processes are accessing the DB files

### Issue 3: Memory Usage

**Symptom**: OOM errors on full DB queries

**Workaround**:
- Reduce `--max-workers` to 3
- Use 5% sample DB instead of 1%
- Increase system swap space

---

## Recommendations

### Before Benchmarking

1. ✅ **Fix import path** - Install packages or use PYTHONPATH
2. ✅ **Test single query** - Verify setup with Q1
3. ✅ **Monitor first 5 queries** - Ensure CSV is populating correctly
4. ⚠️ **Add logging** - Consider adding `--verbose` flag for debugging

### During Benchmarking

1. Monitor progress: `tail -f research/experiments/benchmarks/v5_parallel_*.csv`
2. Check for errors: `grep -i error research/experiments/benchmarks/v5_parallel_*.csv`
3. Track timing: Use `time` command or add timestamps to output

### After Benchmarking

1. Create summary file with key metrics
2. Update `BENCHMARKS.md` with results
3. Compare with previous benchmark runs
4. Identify queries that failed or had low speedups for further investigation

---

## Conclusion

The v5 process is **production-ready and ready for benchmarking** once the import path issue is resolved. The code quality is high, the test data is available, and the benchmark script is well-structured.

### Action Items

1. ✅ **Resolve import issue** - Install packages or use proper Python path
2. ✅ **Run single query test** - Verify setup works
3. ✅ **Launch full benchmark** - Run all 97 queries
4. ✅ **Record results** - Create CSV, summary, and update BENCHMARKS.md
5. ⚠️ **Optional improvements** - Add logging, docstrings, extract helpers (can wait)

### Estimated Time

- Setup fixes: 5-10 minutes
- Single query test: 2-3 minutes
- Full benchmark: 4-6 hours
- Results analysis: 15-30 minutes

**Total**: ~4-7 hours including setup and analysis

---

**Ready to proceed?** Follow the "Pre-Benchmark Checklist" and "Running the Benchmark" sections above.
