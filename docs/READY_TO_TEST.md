# ✅ V5 Test Ready - Everything Configured

**Status**: Ready to run single-query test with full output recording

---

## What's Been Set Up

### 1. ✅ API Key Loaded

Your DeepSeek API key is in `DeepseekV3.txt` and will be auto-loaded by the test script.

### 2. ✅ Robust Test Script Created

**Script**: `scripts/test_v5_single_query_robust.py`

**Features**:
- Saves ALL worker outputs as they complete
- Nothing lost if interrupted (Ctrl+C safe)
- Records every prompt, response, and SQL
- Saves execution plans and metadata
- Incremental saving - no data loss

### 3. ✅ Easy Runner Script

**Script**: `scripts/run_v5_test.sh`

Auto-loads API key and handles everything.

### 4. ✅ Complete Documentation

**File**: `RUN_V5_TEST.md` - Full guide with examples

---

## Run the Test NOW

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8

# Test query 1
./scripts/run_v5_test.sh 1
```

**That's it!** The script handles everything else.

---

## What Happens

1. **Loads API key** from `DeepseekV3.txt`
2. **Installs packages** if needed
3. **Creates output directory**: `research/experiments/v5_test_runs/q1_YYYYMMDD_HHMMSS/`
4. **Runs 5 workers in parallel** on 1% sample DB
5. **Saves each worker** as it completes (incremental)
6. **Validates on full DB** (sequential, early stopping)
7. **Saves final summary**

**Expected time**: 1-3 minutes

---

## Output Structure

```
research/experiments/v5_test_runs/q1_20260204_143022/
├── config.json                    # Test parameters
├── original.sql                   # Original query
├── plan_summary.txt               # Execution plan summary
├── plan_full.txt                  # Full EXPLAIN output
├── plan.json                      # Plan JSON
├── base_prompt.txt                # DAG prompt
├── summary.json                   # Machine-readable results
├── summary.txt                    # Human-readable summary
│
├── worker_1/
│   ├── sample_optimized.sql       # Generated SQL
│   ├── sample_metadata.json       # Status, speedup
│   ├── sample_prompt.txt          # Full LLM prompt
│   ├── sample_response.txt        # LLM response
│   └── full_metadata.json         # Full DB results
│
├── worker_2/  ... worker_5/       # Same structure
```

**Every generation is saved!** Nothing is lost.

---

## Incremental Saving

**Workers save IMMEDIATELY when done**:

```
[Worker 2 completes] → Files saved to worker_2/
[Worker 4 completes] → Files saved to worker_4/
[Ctrl+C pressed]     → Workers 2 & 4 outputs preserved!
```

Even if you interrupt, completed workers are fully saved.

---

## After Test Completes

### View Summary

```bash
cat research/experiments/v5_test_runs/q1_*/summary.txt
```

### Explore Workers

```bash
# List all workers
ls -la research/experiments/v5_test_runs/q1_*/

# View winner's SQL
cat research/experiments/v5_test_runs/q1_*/worker_*/sample_optimized.sql

# View prompts sent to LLM
cat research/experiments/v5_test_runs/q1_*/worker_*/sample_prompt.txt

# View LLM responses
cat research/experiments/v5_test_runs/q1_*/worker_*/sample_response.txt
```

---

## Test Different Queries

```bash
# Easy queries
./scripts/run_v5_test.sh 1
./scripts/run_v5_test.sh 3
./scripts/run_v5_test.sh 7

# Complex queries
./scripts/run_v5_test.sh 23
./scripts/run_v5_test.sh 39

# Known winner
./scripts/run_v5_test.sh 9   # Previously got 2.05x
```

---

## Interruption Safe

**Press Ctrl+C anytime** - Already-completed workers are fully saved.

Example:
- Workers 1, 2, 3 complete → Fully saved
- Press Ctrl+C → Workers 1-3 outputs preserved
- Workers 4, 5 not started → No output (expected)

You can still analyze the partial results!

---

## Expected Console Output

```
======================================================================
V5 Robust Test - Query 1
======================================================================

Output directory: research/experiments/v5_test_runs/q1_20260204_143022

Loading query 1...
✅ Query loaded (1234 chars)

Running v5 optimization with incremental saving...

Analyzing execution plan...
✅ Plan analyzed

Running 5 workers in parallel on sample DB...
  ✅ Saved worker 2 (sample): pass, 1.23x
  ✅ Saved worker 4 (sample): pass, 1.87x
  ✅ Saved worker 1 (sample): validation_failed, 0.00x
  ✅ Saved worker 5 (sample): pass, 0.98x
  ✅ Saved worker 3 (sample): pass, 2.15x

✅ Sample phase complete: 5/5 workers finished

Running full DB validation on 4 valid candidates...

Validating worker 4 on full DB...
  ✅ Saved worker 4 (full): pass, 2.05x
  🏆 Winner found! Breaking early.

======================================================================
RESULTS
======================================================================

🏆 WINNER FOUND
   Worker: 4
   Sample: 1.87x
   Full: 2.05x

All outputs saved to: research/experiments/v5_test_runs/q1_20260204_143022
```

---

## Files Created for You

| File | Purpose |
|------|---------|
| `RUN_V5_TEST.md` | Complete guide (read this first!) |
| `READY_TO_TEST.md` | This quick start |
| `scripts/test_v5_single_query_robust.py` | Robust test script |
| `scripts/run_v5_test.sh` | Easy runner |
| `V5_REVIEW_SUMMARY.md` | V5 process review |
| `research/v5_quick_start.md` | Quick reference |
| `research/v5_benchmark_readiness_report.md` | Full analysis |

---

## Quick Commands

```bash
# Run test on query 1
./scripts/run_v5_test.sh 1

# View summary after completion
cat research/experiments/v5_test_runs/q1_*/summary.txt

# List all output files
ls -la research/experiments/v5_test_runs/q1_*/

# View all worker SQLs
cat research/experiments/v5_test_runs/q1_*/worker_*/sample_optimized.sql
```

---

## Documentation Hierarchy

1. **THIS FILE** - Quick start (you are here)
2. **RUN_V5_TEST.md** - Detailed test guide
3. **V5_REVIEW_SUMMARY.md** - Process review
4. **research/v5_quick_start.md** - Reference
5. **research/v5_benchmark_readiness_report.md** - Full analysis

---

## Ready?

Just run this command:

```bash
./scripts/run_v5_test.sh 1
```

All outputs will be saved to timestamped directory with full preservation of:
- ✅ All 5 worker outputs
- ✅ All prompts sent to LLM
- ✅ All LLM responses
- ✅ All generated SQL
- ✅ All validation results
- ✅ Execution plans
- ✅ Metadata and summaries

**Nothing will be lost**, even if interrupted!

---

**Go ahead and test!** 🚀
