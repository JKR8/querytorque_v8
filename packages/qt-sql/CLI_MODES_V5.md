# V5 Optimizer: Operational Modes

**Date:** 2026-02-05

---

## Overview

V5 supports three distinct optimization strategies:

1. **Mode 1: Retry** (Single Worker with Retries) - Corrective learning from failures
2. **Mode 2: Parallel** (Parallel Workers with Competition) - Tournament-style diversity
3. **Mode 3: Evolutionary** (Iterative Improvement) - Hill-climbing with stacking

*This document covers Modes 1 and 2. See CLI_MODE3_ITERATIVE.md for Mode 3 details.*

---

## Mode 1: Retry (Single Worker with Retries)

**Memorable name:** `retry` or `corrective`

### Strategy
**Reliability through iteration** - Corrective learning from failures
- 1 worker attempts optimization
- If validation fails, retry up to 3 times total
- Each retry includes the error message from previous attempt
- LLM learns from failures and corrects

### Use Cases
- Production queries where success is critical
- Complex queries that need iterative refinement
- Cost-sensitive scenarios (fewer LLM calls)
- When you want one good result, not multiple options

### CLI Command

```bash
# Primary name
qt-sql optimize q1.sql \
  --mode retry \
  --retries 3 \
  --sample-db tpcds_sf1.duckdb \
  --full-db tpcds_sf100.duckdb

# Alias
qt-sql optimize q1.sql --mode corrective
```

### Execution Flow

```
┌─────────────────────────────────────────────────┐
│ Mode 1: Single Worker with Retries             │
└─────────────────────────────────────────────────┘

Attempt 1:
  ├─ Generate prompt (ML-guided examples)
  ├─ Call LLM → Get rewrite
  ├─ Validate on sample DB
  └─ Result: ✗ Failed (syntax error)

Attempt 2:
  ├─ Generate prompt + error history:
  │    "Previous attempt failed with:
  │     'syntax error near JOIN'"
  ├─ Call LLM → Get corrected rewrite
  ├─ Validate on sample DB
  └─ Result: ✗ Failed (wrong row count)

Attempt 3:
  ├─ Generate prompt + error history:
  │    "Attempt 1 failed: syntax error
  │     Attempt 2 failed: row count mismatch (expected 100, got 95)"
  ├─ Call LLM → Get corrected rewrite
  ├─ Validate on sample DB
  └─ Result: ✓ Success!

Final Step:
  ├─ Benchmark on full DB (5-run trimmed mean)
  └─ Return: Optimized SQL with speedup
```

### Prompt Structure on Retry

**Attempt 1 Prompt:**
```
You are an autonomous Query Rewrite Engine...

[Examples]
[Query DAG]
[Opportunities]

Now output your rewrite_sets:
```

**Attempt 2 Prompt (after failure):**
```
You are an autonomous Query Rewrite Engine...

[Examples]
[Query DAG]
[Opportunities]

## Previous Attempt Failed

Your previous optimization attempt failed with the following error:

**Error:** Syntax error near 'JOIN'
**Location:** Line 15 of assembled SQL
**Attempted SQL:**
WITH filtered_store_returns AS (
  SELECT ... JOIN date_dim ON...  -- Error here
)

Please correct this error and try again. Ensure:
- All JOINs have proper ON conditions
- Column references are valid
- SQL syntax is correct

Now output your corrected rewrite_sets:
```

**Attempt 3 Prompt (after 2 failures):**
```
You are an autonomous Query Rewrite Engine...

[Examples]
[Query DAG]
[Opportunities]

## Previous Attempts Failed

### Attempt 1: Syntax Error
**Error:** Syntax error near 'JOIN'
**Issue:** Missing ON condition

### Attempt 2: Semantic Error
**Error:** Row count mismatch
**Expected:** 100 rows
**Got:** 95 rows
**Issue:** Filter pushed too early changed results

Please analyze these failures and provide a correct optimization that:
1. Has valid SQL syntax
2. Produces exactly 100 rows (same as original)
3. Preserves semantic equivalence

Now output your corrected rewrite_sets:
```

### Output Format

```bash
$ qt-sql optimize q1.sql --mode single

Optimizing q1.sql (Mode: Single Worker with Retries)
Provider: deepseek (deepseek-reasoner)
Max retries: 3

[Attempt 1/3]
  ├─ Generating prompt... ✓
  ├─ Calling LLM... ✓ (3.2s)
  ├─ Assembling SQL... ✓
  ├─ Validating... ✗ Syntax error near 'JOIN'
  └─ Retry with error feedback...

[Attempt 2/3]
  ├─ Generating prompt with error... ✓
  ├─ Calling LLM... ✓ (3.5s)
  ├─ Assembling SQL... ✓
  ├─ Validating... ✗ Row count mismatch (100 expected, 95 got)
  └─ Retry with error feedback...

[Attempt 3/3]
  ├─ Generating prompt with errors... ✓
  ├─ Calling LLM... ✓ (3.8s)
  ├─ Assembling SQL... ✓
  ├─ Validating... ✓ Success!
  └─ Proceeding to benchmark...

[Benchmarking]
  ├─ Original: 12431ms (5-run trimmed mean)
  ├─ Optimized: 4261ms (5-run trimmed mean)
  └─ Speedup: 2.92x ✓

🏆 Success after 3 attempts!
Speedup: 2.92x
Saved to: q1_optimized.sql
```

### Storage Structure

```
results/q1_20260205_103000_single/
├── input/
│   └── ... (standard input files)
│
├── attempts/
│   ├── attempt_1/
│   │   ├── prompt.txt
│   │   ├── llm_response.json
│   │   ├── optimized.sql
│   │   ├── validation.json
│   │   └── error.txt              # "Syntax error near 'JOIN'"
│   ├── attempt_2/
│   │   ├── prompt.txt              # Includes attempt_1 error
│   │   ├── llm_response.json
│   │   ├── optimized.sql
│   │   ├── validation.json
│   │   └── error.txt              # "Row count mismatch"
│   └── attempt_3/
│       ├── prompt.txt              # Includes all previous errors
│       ├── llm_response.json
│       ├── optimized.sql
│       └── validation.json        # Success!
│
├── benchmark/
│   └── full_db/
│       ├── original_runs.json
│       └── optimized_runs.json
│
├── winner/
│   └── optimized.sql
│
└── summary.json
```

### Configuration

```bash
# Default retries (3)
qt-sql optimize q1.sql --mode single

# Custom retry count
qt-sql optimize q1.sql --mode single --retries 5

# No retries (fail fast)
qt-sql optimize q1.sql --mode single --retries 1
```

---

## Mode 2: Parallel (Parallel Workers with Competition)

**Memorable name:** `parallel` or `tournament`

### Strategy
**Diversity through parallelization** - Tournament-style competition
- 5 workers attempt optimization simultaneously
- Each worker uses different examples
- All valid candidates are benchmarked
- Best speedup wins

### Use Cases
- Research and experimentation
- Finding absolute best optimization
- When you have time/budget for multiple attempts
- Comparing different optimization strategies

### CLI Command

```bash
# Primary name
qt-sql optimize q1.sql \
  --mode parallel \
  --workers 5 \
  --sample-db tpcds_sf1.duckdb \
  --full-db tpcds_sf100.duckdb

# Alias
qt-sql optimize q1.sql --mode tournament
```

### Execution Flow

```
┌─────────────────────────────────────────────────┐
│ Mode 2: Parallel Workers with Competition      │
└─────────────────────────────────────────────────┘

Phase 1: Parallel Worker Execution (Attempt 1)
  ├─ Worker 1: Examples 1-3 (DAG JSON) ⏳
  ├─ Worker 2: Examples 4-6 (DAG JSON) ⏳
  ├─ Worker 3: Examples 7-9 (DAG JSON) ⏳
  ├─ Worker 4: Examples 10-12 (DAG JSON) ⏳
  └─ Worker 5: No examples (Full SQL) ⏳

Phase 2: Sample DB Validation (Attempt 1)
  ├─ Worker 1: ✓ Valid
  ├─ Worker 2: ✓ Valid
  ├─ Worker 3: ✗ Invalid (syntax error) → Retry
  ├─ Worker 4: ✓ Valid
  └─ Worker 5: ✓ Valid

Phase 3: Retry Failed Workers (Attempt 2)
  └─ Worker 3: ✓ Valid (corrected with error feedback)

Valid candidates: 5/5

Phase 3: Full DB Benchmark (Sequential)
  ├─ Worker 1: 2.92x ✓ TARGET MET (stop here)
  ├─ Worker 2: [skipped]
  ├─ Worker 4: [skipped]
  └─ Worker 5: [skipped]

Winner: Worker 1 (2.92x speedup)
```

### Output Format

```bash
$ qt-sql optimize q1.sql --mode parallel

Optimizing q1.sql (Mode: Parallel Workers)
Provider: deepseek (deepseek-reasoner)
Workers: 5
Target speedup: 2.0x

[Phase 1/3] Parallel Worker Execution ⏳
  Worker 1 (decorrelate, early_filter, or_to_union): ✓ 3.2s
  Worker 2 (date_cte_isolate, pushdown, materialize): ✓ 2.8s
  Worker 3 (flatten_subquery, reorder_join, inline): ✓ 3.5s
  Worker 4 (remove_redundant, multi_push, semantic): ✓ 3.1s
  Worker 5 (explore mode - full SQL): ✓ 4.1s

[Phase 2/3] Sample DB Validation
  Worker 1: ✓ Valid (3.09x speedup)
  Worker 2: ✓ Valid (2.15x speedup)
  Worker 3: ✗ Invalid (syntax error)
  Worker 4: ✓ Valid (1.89x speedup)
  Worker 5: ✓ Valid (2.87x speedup)

Valid candidates: 4/5

[Phase 3/3] Full DB Benchmark (5-run trimmed mean)
  Worker 1:
    ├─ Original: 12431ms (avg of middle 3)
    ├─ Optimized: 4261ms (avg of middle 3)
    └─ Speedup: 2.92x ✓ TARGET MET!

🏆 Winner: Worker 1 (2.92x speedup)

Transform: decorrelate
Examples used: decorrelate, early_filter, or_to_union
Saved to: q1_optimized.sql
```

### Storage Structure

```
results/q1_20260205_103000_parallel/
├── input/
│   └── ... (standard input files)
│
├── workers/
│   ├── worker_1/
│   │   ├── config.json
│   │   ├── examples.json
│   │   ├── prompt.txt
│   │   ├── llm_response.json
│   │   ├── optimized.sql
│   │   ├── validation_sample.json
│   │   └── benchmark_full.json
│   ├── worker_2/
│   │   └── ... (same structure)
│   ├── worker_3/
│   │   ├── ... (same but validation failed)
│   │   └── error.txt
│   ├── worker_4/
│   │   └── ... (same structure)
│   └── worker_5/
│       └── ... (same structure)
│
├── validation/
│   └── sample_db/
│       ├── original_result.json
│       └── worker_X_result.json
│
├── benchmark/
│   └── full_db/
│       ├── original_runs.json
│       └── worker_1_runs.json  # Only winner benchmarked
│
├── winner/
│   └── optimized.sql
│
└── summary.json
```

### Configuration

```bash
# Default 5 workers
qt-sql optimize q1.sql --mode parallel

# Custom worker count
qt-sql optimize q1.sql --mode parallel --workers 3

# Benchmark all valid candidates (not just first to meet target)
qt-sql optimize q1.sql --mode parallel --benchmark-all
```

---

## Comparison

| Aspect | Mode 1: Retry | Mode 2: Parallel |
|--------|---------------|------------------|
| **Memorable name** | `retry` / `corrective` | `parallel` / `tournament` |
| **Workers** | 1 | 5 |
| **Strategy** | Iterative refinement | Parallel diversity |
| **Retries** | Up to 3 per worker | 1 retry per worker |
| **Error feedback** | ✅ Yes (learns from failures) | ✅ Yes (per worker retry) |
| **LLM calls** | 1-3 total | 5-10 total (parallel) |
| **Validation** | Per attempt | After all workers |
| **Benchmark** | Only successful attempt | All valid candidates |
| **Time** | 10-60s (sequential) | 15-30s (parallel) |
| **Cost** | Lower (1-3 calls) | Higher (5-10 calls) |
| **Best for** | Production, reliability | Research, best result |
| **Success rate** | Higher (multiple tries) | Medium-High (diversity) |

---

## CLI Syntax

### Mode Selection

```bash
# Mode 1: Retry (default)
qt-sql optimize q1.sql --mode retry
qt-sql optimize q1.sql --mode corrective  # alias

# Mode 2: Parallel
qt-sql optimize q1.sql --mode parallel
qt-sql optimize q1.sql --mode tournament  # alias

# Mode 3: Evolutionary (see CLI_MODE3_ITERATIVE.md)
qt-sql optimize q1.sql --mode evolutionary
qt-sql optimize q1.sql --mode stacking  # alias

# Auto-detect (defaults to retry)
qt-sql optimize q1.sql
```

### Full Options

**Mode 1:**
```bash
qt-sql optimize <query.sql> \
  --mode single \
  --retries <1-10> \
  --sample-db <path> \
  --full-db <path> \
  --query-id <id> \
  --target-speedup <float> \
  --provider <provider> \
  --model <model>
```

**Mode 2:**
```bash
qt-sql optimize <query.sql> \
  --mode parallel \
  --workers <1-5> \
  --sample-db <path> \
  --full-db <path> \
  --query-id <id> \
  --target-speedup <float> \
  --benchmark-all \
  --provider <provider> \
  --model <model>
```

---

## Default Behavior

### No Mode Specified

```bash
qt-sql optimize q1.sql
```

**Defaults to Mode 1 (Retry):**
- 1 worker with up to 3 retries
- Most reliable for production use
- Lower cost

### Override Default

```bash
# Set in config (use primary names or aliases)
echo "default_mode: retry" >> ~/.qt-sql/config.yaml
echo "default_mode: parallel" >> ~/.qt-sql/config.yaml
echo "default_mode: evolutionary" >> ~/.qt-sql/config.yaml

# Or environment variable
export QT_V5_MODE=retry        # or: corrective
export QT_V5_MODE=parallel     # or: tournament
export QT_V5_MODE=evolutionary # or: stacking
```

---

## Examples

### Example 1: Production Query (Mode 1)

```bash
qt-sql optimize production_report.sql \
  --mode single \
  --retries 3 \
  --sample-db staging.duckdb \
  --full-db production.duckdb
```

**Expected outcome:** One reliable optimization with iterative refinement.

### Example 2: Research Query (Mode 2)

```bash
qt-sql optimize q1.sql \
  --mode parallel \
  --workers 5 \
  --benchmark-all \
  --save-results research/q1/
```

**Expected outcome:** Multiple optimizations, compare all approaches.

### Example 3: Quick Test (Mode 1, Fast Fail)

```bash
qt-sql optimize test.sql \
  --mode single \
  --retries 1 \
  --sample-db test.duckdb
```

**Expected outcome:** One attempt, fail fast if doesn't work.

### Example 4: Maximum Coverage (Mode 2)

```bash
qt-sql optimize complex_query.sql \
  --mode parallel \
  --workers 5 \
  --benchmark-all \
  --target-speedup 1.5
```

**Expected outcome:** Try everything, benchmark all valid, pick best.

---

## Summary

**Mode 1 (Retry/Corrective):**
- ✅ Iterative learning from errors
- ✅ Lower cost (1-3 LLM calls)
- ✅ Higher success rate
- ✅ Good for production

**Mode 2 (Parallel/Tournament):**
- ✅ Maximum diversity
- ✅ Best possible result
- ✅ Compare strategies
- ✅ Good for research

**Mode 3 (Evolutionary/Stacking):**
- ✅ Cumulative improvement
- ✅ Maximum speedup potential
- ✅ Transform composition
- ✅ Good for optimization research

**All modes:**
- ✅ Use reasoning model by default
- ✅ Store all inputs/outputs
- ✅ 5-run trimmed mean benchmark
- ✅ Complete audit trail

Choose based on your needs:
- **Reliability** → Mode 1 (Retry)
- **Exploration** → Mode 2 (Parallel)
- **Maximum speedup** → Mode 3 (Evolutionary)
