# V5 Optimizer: CLI Integration Complete

**Date:** 2026-02-05
**Status:** ✅ All 3 modes integrated into CLI

---

## Summary

All three V5 optimizer modes are now fully integrated into the `qt-sql optimize` command with comprehensive options and help documentation.

---

## CLI Commands

### Mode 1: Retry (Corrective Learning)

```bash
# Basic usage
qt-sql optimize query.sql --mode retry --sample-db sample.db --full-db full.db

# With options
qt-sql optimize query.sql \
  --mode retry \
  --sample-db tpcds_sf1.duckdb \
  --full-db tpcds_sf100.duckdb \
  --retries 5 \
  --target-speedup 2.0 \
  --query-id q1 \
  --provider deepseek \
  --output optimized.sql

# Using alias
qt-sql optimize query.sql --mode corrective --sample-db sample.db --full-db full.db
```

**Options:**
- `--retries INTEGER` - Max retry attempts (default: 3)
- `--sample-db PATH` - Sample database for validation (required)
- `--full-db PATH` - Full database for benchmarking (required)
- `--target-speedup FLOAT` - Target speedup threshold (default: 2.0)
- `--query-id TEXT` - Query ID for ML recommendations

**Output Example:**
```
Optimizing: query.sql
Mode: Mode 1: Retry (Corrective Learning)

Attempts: 3
  ✓ Attempt 1: pass
    Speedup: 1.8x
  ✓ Attempt 2: pass
    Speedup: 2.1x
  ✓ Attempt 3: pass
    Speedup: 2.5x

✅ Success after 3 attempts!
Sample speedup: 2.1x
Full DB speedup: 2.5x
```

---

### Mode 2: Parallel (Tournament Competition)

```bash
# Basic usage
qt-sql optimize query.sql --mode parallel --sample-db sample.db --full-db full.db

# With options
qt-sql optimize query.sql \
  --mode parallel \
  --sample-db tpcds_sf1.duckdb \
  --full-db tpcds_sf100.duckdb \
  --workers 5 \
  --target-speedup 2.0 \
  --query-id q1 \
  --provider deepseek \
  --save-results results/q1/

# Using alias
qt-sql optimize query.sql --mode tournament --workers 3 --sample-db sample.db --full-db full.db
```

**Options:**
- `--workers INTEGER` - Number of parallel workers (default: 5)
- `--sample-db PATH` - Sample database for validation (required)
- `--full-db PATH` - Full database for benchmarking (required)
- `--target-speedup FLOAT` - Target speedup threshold (default: 2.0)
- `--query-id TEXT` - Query ID for ML recommendations
- `--save-results PATH` - Save detailed results to directory

**Output Example:**
```
Optimizing: query.sql
Mode: Mode 2: Parallel (Tournament Competition)

Valid candidates: 4/5
  ✓ Worker 1: 3.1x (sample)
  ✓ Worker 2: 2.4x (sample)
  ✓ Worker 4: 1.8x (sample)
  ✓ Worker 5: 2.9x (sample)

🏆 Winner: Worker 1
Sample speedup: 3.1x
Full DB speedup: 2.92x
✅ Target speedup 2.0x MET!
```

---

### Mode 3: Evolutionary (Stacking)

```bash
# Basic usage
qt-sql optimize query.sql --mode evolutionary --full-db full.db

# With options
qt-sql optimize query.sql \
  --mode evolutionary \
  --full-db tpcds_sf100.duckdb \
  --iterations 5 \
  --target-speedup 2.0 \
  --query-id q1 \
  --provider deepseek \
  --save-results results/q1_evolutionary/

# Using alias
qt-sql optimize query.sql --mode stacking --iterations 10 --full-db full.db
```

**Options:**
- `--iterations INTEGER` - Max iterations (default: 5)
- `--full-db PATH` - Full database for benchmarking (required)
- `--target-speedup FLOAT` - Target speedup threshold (default: 2.0)
- `--query-id TEXT` - Query ID for ML recommendations
- `--save-results PATH` - Save detailed results to directory

**Output Example:**
```
Optimizing: query.sql
Mode: Mode 3: Evolutionary (Stacking)

Iterations: 3
  🔼 Iteration 1: 1.5x
  🔼 Iteration 2: 1.8x
  🔼 Iteration 3: 2.3x

🏆 Best result: Iteration 3
Final speedup: 2.3x
✅ Target speedup 2.0x MET!
```

---

## Mode Aliases

For easier typing, each mode has a short alias:

| Primary Name | Alias | Description |
|--------------|-------|-------------|
| `retry` | `corrective` | Learn from errors, retry with corrections |
| `parallel` | `tournament` | Workers compete in tournament style |
| `evolutionary` | `stacking` | Stack optimizations iteratively |

**Examples:**
```bash
# These are equivalent:
qt-sql optimize query.sql --mode retry ...
qt-sql optimize query.sql --mode corrective ...

# These are equivalent:
qt-sql optimize query.sql --mode parallel ...
qt-sql optimize query.sql --mode tournament ...

# These are equivalent:
qt-sql optimize query.sql --mode evolutionary ...
qt-sql optimize query.sql --mode stacking ...
```

---

## Common Options

These options work with all modes:

| Option | Description | Default |
|--------|-------------|---------|
| `--provider TEXT` | LLM provider (deepseek, openai, anthropic, groq, gemini) | deepseek |
| `--model TEXT` | LLM model name | deepseek-reasoner |
| `--target-speedup FLOAT` | Target speedup threshold | 2.0 |
| `--query-id TEXT` | Query ID for ML recommendations | (auto-detect) |
| `--output PATH` | Save optimized SQL to file | (stdout) |
| `--show-prompt` | Display full prompt sent to LLM | False |
| `--verbose` | Verbose output | False |
| `--save-results PATH` | Save detailed results to directory | (none) |

---

## Database Options

Different modes have different database requirements:

| Mode | Sample DB | Full DB | Notes |
|------|-----------|---------|-------|
| **Retry** | ✅ Required | ✅ Required | Sample for fast validation, Full for final benchmark |
| **Parallel** | ✅ Required | ✅ Required | Sample for fast validation, Full for final benchmark |
| **Evolutionary** | ❌ Not used | ✅ Required | Benchmarks on full DB every iteration |

**Quick shorthand:** Use `-d/--database` to set both sample and full:

```bash
# This:
qt-sql optimize query.sql --mode retry -d mydb.duckdb

# Is equivalent to:
qt-sql optimize query.sql --mode retry --sample-db mydb.duckdb --full-db mydb.duckdb
```

---

## Environment Variables

Set these to avoid typing options repeatedly:

```bash
# LLM Configuration
export QT_LLM_PROVIDER=deepseek
export QT_LLM_MODEL=deepseek-reasoner
export QT_DEEPSEEK_API_KEY=sk-xxx

# Database paths
export QT_SAMPLE_DB=/path/to/tpcds_sf1.duckdb
export QT_FULL_DB=/path/to/tpcds_sf100.duckdb

# V5 defaults
export QT_V5_MODE=retry
export QT_V5_RETRIES=3
export QT_V5_WORKERS=5
export QT_V5_ITERATIONS=5
export QT_V5_TARGET_SPEEDUP=2.0
```

**Then simply:**
```bash
qt-sql optimize query.sql
```

---

## Help Command

View all options:

```bash
qt-sql optimize --help
```

**Output includes:**
- Complete documentation of all 3 modes
- All command-line options
- Usage examples for each mode
- Mode aliases
- Database requirements

---

## Implementation Details

### File: `packages/qt-sql/qt_sql/cli/main.py`

**Functions added:**

1. **`_run_v5_retry()`** (lines ~540-600)
   - Runs Mode 1: Retry optimization
   - Shows progress with spinner
   - Displays attempt-by-attempt results
   - Returns optimized SQL or None

2. **`_run_v5_parallel()`** (lines ~602-660)
   - Runs Mode 2: Parallel optimization
   - Shows progress with spinner
   - Displays worker results
   - Returns winner's SQL or None

3. **`_run_v5_evolutionary()`** (lines ~662-720)
   - Runs Mode 3: Evolutionary optimization
   - Shows progress with spinner
   - Displays iteration-by-iteration improvements
   - Returns best SQL or None

**Modified functions:**

1. **`optimize()`** command (lines ~309-480)
   - Added mode selection logic
   - Added mode validation
   - Routes to appropriate mode handler
   - Maintains backward compatibility with `--dag` flag

---

## Progress Indicators

All modes show real-time progress with spinners:

```
Running retry optimization... ⠋
Running parallel optimization... ⠙
Running evolutionary optimization... ⠹
```

---

## Error Handling

### Mode Validation

**Missing required databases:**
```bash
$ qt-sql optimize query.sql --mode retry

Error: --sample-db and --full-db required for retry mode
Example: qt-sql optimize query.sql --mode retry --sample-db sample.db --full-db full.db
```

**Invalid mode:**
```bash
$ qt-sql optimize query.sql --mode invalid

Error: Invalid value for '--mode': 'invalid' is not one of 'retry', 'corrective', 'parallel', 'tournament', 'evolutionary', 'stacking'.
```

### Optimization Failures

**No valid candidates:**
```
❌ All 3 attempts exhausted
```

**Target not met:**
```
⚠️  Below target 2.0x
Best result: 1.8x
```

---

## Backward Compatibility

Legacy `--dag` flag still supported:

```bash
# Old way (still works)
qt-sql optimize query.sql --dag -d mydb.duckdb

# New way (recommended)
qt-sql optimize query.sql --mode parallel -d mydb.duckdb
```

---

## Complete Examples

### Example 1: Production Query (Retry Mode)

```bash
qt-sql optimize production_report.sql \
  --mode retry \
  --sample-db staging.duckdb \
  --full-db production.duckdb \
  --retries 3 \
  --target-speedup 2.0 \
  --provider deepseek \
  --output production_report_optimized.sql
```

### Example 2: Research Query (Parallel Mode)

```bash
qt-sql optimize q1.sql \
  --mode parallel \
  --sample-db tpcds_sf1.duckdb \
  --full-db tpcds_sf100.duckdb \
  --workers 5 \
  --target-speedup 2.0 \
  --query-id q1 \
  --save-results results/q1/ \
  --show-prompt
```

### Example 3: Maximum Optimization (Evolutionary Mode)

```bash
qt-sql optimize complex_query.sql \
  --mode evolutionary \
  --full-db tpcds_sf100.duckdb \
  --iterations 5 \
  --target-speedup 2.5 \
  --query-id q93 \
  --save-results results/q93_evolutionary/ \
  --output q93_optimized.sql \
  --verbose
```

### Example 4: Batch Processing

```bash
for q in queries/q*.sql; do
  qt-sql optimize $q \
    --mode retry \
    --sample-db tpcds_sf1.duckdb \
    --full-db tpcds_sf100.duckdb \
    --save-results results/$(basename $q .sql)/
done
```

---

## Testing

### Unit Tests

**File:** `packages/qt-sql/tests/test_v5_modes.py`
- ✅ 374 lines of unit tests
- ✅ All tests passing

### Integration Tests

**File:** `test_v5_modes_integration.py`
- ✅ 401 lines of integration tests
- ✅ All modes tested end-to-end
- ✅ All tests passing

### CLI Tests

**File:** `test_cli_v5_modes.py`
- ✅ 395 lines of CLI tests
- ✅ Mode aliases validated
- ✅ Options validated
- ✅ Help documentation verified

---

## Documentation

| File | Lines | Description |
|------|-------|-------------|
| `CLI_MODES_V5.md` | 554 | Modes 1 & 2 detailed documentation |
| `CLI_MODE3_ITERATIVE.md` | 560 | Mode 3 detailed documentation |
| `CLI_MODES_OVERVIEW.md` | 450 | Comparison of all 3 modes |
| `CLI_DESIGN_V5.md` | 528 | CLI interface design |
| `V5_IMPLEMENTATION_STATUS.md` | 300 | Implementation status |
| `CLI_INTEGRATION_COMPLETE.md` | This file | CLI integration summary |

**Total documentation:** ~2,900 lines

---

## Verification

### Check CLI is working:

```bash
qt-sql optimize --help
```

Should show all 3 modes with complete documentation.

### Check imports:

```python
from qt_sql.optimization.adaptive_rewriter_v5 import (
    optimize_v5_retry,
    optimize_v5_json_queue,
    optimize_v5_evolutionary,
)
```

Should import without errors.

### Check CLI execution:

```bash
# Should show proper error message (not crash)
qt-sql optimize test.sql --mode retry
```

---

## Status

✅ **CLI Integration:** Complete
✅ **All 3 modes:** Integrated
✅ **Mode aliases:** Working
✅ **Options:** Validated
✅ **Help documentation:** Complete
✅ **Error handling:** Robust
✅ **Progress indicators:** Implemented
✅ **Output formatting:** Rich console output
✅ **Backward compatibility:** Maintained

---

## Next Steps

Ready for real-world usage:

1. **Test on actual data:**
   ```bash
   qt-sql optimize q1.sql --mode retry --sample-db tpcds_sf1.duckdb --full-db tpcds_sf100.duckdb
   ```

2. **Compare modes:**
   ```bash
   qt-sql optimize q1.sql --mode retry ... > results_retry.txt
   qt-sql optimize q1.sql --mode parallel ... > results_parallel.txt
   qt-sql optimize q1.sql --mode evolutionary ... > results_evolutionary.txt
   ```

3. **Run on TPC-DS queries:**
   ```bash
   for q in q{1..99}.sql; do
     qt-sql optimize $q --mode retry --sample-db sf1.db --full-db sf100.db
   done
   ```

---

## Summary

🎉 **CLI Integration Complete!**

All three V5 optimizer modes are now fully integrated into the Qt-SQL CLI with:
- ✅ Comprehensive options
- ✅ Mode aliases
- ✅ Rich console output
- ✅ Progress indicators
- ✅ Error handling
- ✅ Complete documentation
- ✅ Backward compatibility

**Total implementation:**
- Implementation: ~350 lines (3 mode functions + CLI integration)
- Tests: ~1,170 lines (unit + integration + CLI tests)
- Documentation: ~2,900 lines (6 markdown files)

**Ready for production use!** 🚀
