# V5 Optimizer: Three Operational Modes

**Date:** 2026-02-05

---

## Quick Reference

| Mode | Name | Workers | Strategy | Best For |
|------|------|---------|----------|----------|
| **1** | **Retry** | 1 | Learn from errors | Production, reliability |
| **2** | **Parallel** | 5 | Diverse competition | Research, exploration |
| **3** | **Evolutionary** | 1 | Stack successes | Maximum speedup |

---

## Mode 1: Retry (Corrective Learning)

**Memorable name:** `retry` or `corrective`

### Strategy
One worker retries up to 3 times, learning from each failure through error feedback.

### Key Characteristics
- ✅ **Error feedback loop** - LLM receives error messages on retry
- ✅ **Sample DB first** - Fast validation before full benchmark
- ✅ **Cost-effective** - Only 1-3 LLM calls
- ✅ **Reliable** - Multiple chances to get it right
- ❌ Limited exploration - Same worker, same examples

### CLI Commands
```bash
# Default retry mode
qt-sql optimize q1.sql --mode retry

# Custom retry count
qt-sql optimize q1.sql --mode retry --retries 5

# Alias
qt-sql optimize q1.sql --mode corrective
```

### When to Use
- ✅ Production queries (reliability critical)
- ✅ Cost-sensitive scenarios
- ✅ When one good result is enough
- ✅ Queries with common failure modes (syntax, validation)

---

## Mode 2: Parallel (Tournament Competition)

**Memorable name:** `parallel` or `tournament`

### Strategy
Five workers with different examples compete in parallel, best speedup wins.

### Key Characteristics
- ✅ **Maximum diversity** - 5 different optimization strategies
- ✅ **Parallel execution** - All workers run simultaneously
- ✅ **Sample DB first** - Fast validation before benchmarking
- ✅ **1 retry per worker** - Error feedback for failed workers
- ✅ **Early stopping** - Stop when first meets target
- ❌ Higher cost - 5-10 LLM calls

### CLI Commands
```bash
# Default parallel mode
qt-sql optimize q1.sql --mode parallel

# Custom worker count
qt-sql optimize q1.sql --mode parallel --workers 3

# Benchmark all (don't stop early)
qt-sql optimize q1.sql --mode parallel --benchmark-all

# Alias
qt-sql optimize q1.sql --mode tournament
```

### When to Use
- ✅ Research and experimentation
- ✅ Finding absolute best approach
- ✅ Comparing different strategies
- ✅ Complex queries with multiple valid optimizations

---

## Mode 3: Evolutionary (Stacking Optimization)

**Memorable name:** `evolutionary` or `stacking`

### Strategy
One worker iterates up to 5 times, each iteration building on the best result so far with rotating examples and ML hints.

### Key Characteristics
- ✅ **Cumulative improvement** - Each iteration builds on previous best
- ✅ **Input evolves** - Iteration N starts from best SQL of N-1
- ✅ **Rotating examples** - Different strategies each iteration
- ✅ **ML/AST hints** - Updated recommendations each iteration
- ✅ **Full DB benchmark** - Every iteration (no sample DB)
- ✅ **Success history** - Learn from what worked
- ❌ Slowest - Multiple full DB benchmarks
- ❌ Higher cost - Full benchmark every iteration

### CLI Commands
```bash
# Default evolutionary mode
qt-sql optimize q1.sql --mode evolutionary

# Custom iteration count
qt-sql optimize q1.sql --mode evolutionary --iterations 10

# Force all iterations (no early stop)
qt-sql optimize q1.sql --mode evolutionary --no-early-stop

# Alias
qt-sql optimize q1.sql --mode stacking
```

### When to Use
- ✅ Maximum speedup needed
- ✅ Complex queries where optimizations stack well
- ✅ Understanding optimization composition
- ✅ Time and budget allow multiple full benchmarks
- ✅ Research: studying optimization progression

---

## Detailed Comparison

| Aspect | Mode 1: Retry | Mode 2: Parallel | Mode 3: Evolutionary |
|--------|---------------|------------------|---------------------|
| **Memorable name** | Retry / Corrective | Parallel / Tournament | Evolutionary / Stacking |
| **Workers** | 1 | 5 | 1 |
| **Max attempts** | 3 retries | 5 + 1 retry each | 5 iterations |
| **Learning from** | ❌ Errors (failures) | 🔄 Competition | ✅ Successes (speedups) |
| **Input SQL** | Original (static) | Original (static) | Best so far (evolving) |
| **Examples** | Static | Per-worker | Rotate each iteration |
| **Validation** | Sample → Full | Sample → Full | Full only |
| **Benchmark timing** | Once (on success) | All valid | Every iteration |
| **Stops when** | Success or 3 fails | All done or target | Target met or 5 iterations |
| **LLM calls** | 1-3 | 5-10 | 1-5 |
| **DB benchmark runs** | 5 (1 query) | 5 × N valid | 5 × N iterations |
| **Cost** | 💰 Low | 💰💰💰 High | 💰💰 Medium |
| **Time** | ⚡ Fast (10-60s) | ⚡⚡ Medium (15-30s) | 🐌 Slow (30-120s) |
| **Reliability** | ⭐⭐⭐ High | ⭐⭐ Medium | ⭐⭐ Medium |
| **Exploration** | ⭐ Limited | ⭐⭐⭐ High | ⭐⭐ Medium |
| **Max speedup** | ⭐⭐ Good | ⭐⭐⭐ High | ⭐⭐⭐⭐ Maximum |

---

## Execution Flow Comparison

### Mode 1: Retry (Error Correction)
```
Attempt 1: Original SQL → LLM → Validate → ✗ Syntax error
Attempt 2: Original SQL + error → LLM → Validate → ✗ Row count mismatch
Attempt 3: Original SQL + errors → LLM → Validate → ✓ Success!
Benchmark: 2.92x ✅
```

### Mode 2: Parallel (Competition)
```
Worker 1: Original SQL + Examples 1-3 → LLM → Validate → ✓ Valid
Worker 2: Original SQL + Examples 4-6 → LLM → Validate → ✓ Valid
Worker 3: Original SQL + Examples 7-9 → LLM → Validate → ✗ Invalid → Retry → ✓ Valid
Worker 4: Original SQL + Examples 10-12 → LLM → Validate → ✓ Valid
Worker 5: Original SQL (explore) → LLM → Validate → ✓ Valid

Benchmark: Worker 1 → 2.92x ✅ TARGET MET (stop early)
```

### Mode 3: Evolutionary (Stacking)
```
Iteration 1: Original SQL + Examples 1-3 → LLM → Benchmark → 1.5x
Iteration 2: Best SQL (1.5x) + Examples 4-6 + history → LLM → Benchmark → 1.8x
Iteration 3: Best SQL (1.8x) + Examples 7-9 + history → LLM → Benchmark → 2.3x ✅
```

---

## History/Feedback Differences

### Mode 1: Error Feedback
```
## Previous Attempt (FAILED)

Failure reason: Syntax error near 'JOIN': Missing ON condition

Previous rewrites:
```{failed JSON}```

Try a DIFFERENT approach.
```

### Mode 2: Worker-Specific Error Feedback
```
## Previous Attempt (FAILED)

Failure reason: Row count mismatch (expected 100, got 95)

Previous rewrites:
```{worker 3 failed JSON}```

Try a DIFFERENT approach.
```

### Mode 3: Success History
```
## Previous Iterations

### Iteration 1: 1.5x speedup ✓
**Transform:** decorrelate
**Key changes:** Eliminated correlated subquery

### Iteration 2: 1.8x speedup ✓ (improvement: +0.3x)
**Transform:** pushdown
**Key changes:** Pushed date filter earlier

**Current best:** 1.8x
**Target:** 2.0x
**Gap:** 0.2x

Now try to bridge the gap while building on previous successes.
```

---

## Cost Analysis (per query)

| Mode | LLM Calls | DB Benchmarks | Total Cost | Time |
|------|-----------|---------------|------------|------|
| **Retry** | 1-3 | 1 × 5 runs | $0.10-0.30 | 10-60s |
| **Parallel** | 5-10 | 1-5 × 5 runs | $0.50-1.00 | 15-30s |
| **Evolutionary** | 1-5 | 1-5 × 5 runs | $0.20-0.50 | 30-120s |

*Assumes deepseek-reasoner at ~$0.10/call and fast DB benchmarks*

---

## Decision Tree

```
START: Need to optimize SQL query
│
├─ Priority: RELIABILITY & LOW COST
│  └─> Use Mode 1: Retry
│      - Production queries
│      - Budget constraints
│      - Need one good result
│
├─ Priority: EXPLORE ALL STRATEGIES
│  └─> Use Mode 2: Parallel
│      - Research project
│      - Compare approaches
│      - Complex query
│
└─ Priority: MAXIMUM SPEEDUP
   └─> Use Mode 3: Evolutionary
       - Need absolute best
       - Multiple opts stack well
       - Time/cost acceptable
```

---

## Storage Paths

```
results/
├── {query}_retry/              # Mode 1
│   └── attempts/
│       ├── attempt_1/
│       ├── attempt_2/
│       └── attempt_3/
│
├── {query}_parallel/           # Mode 2
│   └── workers/
│       ├── worker_1/
│       ├── worker_2/
│       ├── worker_3/
│       ├── worker_4/
│       └── worker_5/
│
└── {query}_evolutionary/       # Mode 3
    └── iterations/
        ├── iteration_1/
        ├── iteration_2/
        ├── iteration_3/
        ├── iteration_4/
        └── iteration_5/
```

---

## CLI Examples

### Mode 1: Retry (Production)
```bash
# Reliable optimization for production
qt-sql optimize production_report.sql \
  --mode retry \
  --retries 3 \
  --sample-db staging.duckdb \
  --full-db production.duckdb
```

### Mode 2: Parallel (Research)
```bash
# Explore all strategies
qt-sql optimize q1.sql \
  --mode parallel \
  --workers 5 \
  --benchmark-all \
  --save-results research/q1/
```

### Mode 3: Evolutionary (Maximum)
```bash
# Achieve maximum speedup
qt-sql optimize complex_query.sql \
  --mode evolutionary \
  --iterations 5 \
  --full-db tpcds_sf100.duckdb \
  --target-speedup 2.0
```

---

## Summary

**Choose your mode:**

🔄 **Mode 1: Retry** → *"Try, learn from errors, try again"*
- Corrective learning from failures
- Best for: Production, reliability, low cost

⚡ **Mode 2: Parallel** → *"May the best strategy win"*
- Tournament competition between diverse approaches
- Best for: Research, exploration, comparing strategies

🧬 **Mode 3: Evolutionary** → *"Each generation builds on the last"*
- Hill-climbing with cumulative improvements
- Best for: Maximum speedup, stacking optimizations

---

## Default Behavior

If no mode specified:
```bash
qt-sql optimize q1.sql
```

Defaults to **Mode 1: Retry** (most reliable, lowest cost).

Override default:
```bash
# Set in config
echo "default_mode: evolutionary" >> ~/.qt-sql/config.yaml

# Or environment variable
export QT_V5_MODE=parallel
```
