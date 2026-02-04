# CLI Design: V5 Optimizer with Reasoning Models

**Date:** 2026-02-05

---

## Command Structure

### Basic Usage

```bash
qt-sql optimize <query.sql> [options]
```

### V5 Optimizer Options

```bash
qt-sql optimize query.sql \
  --version v5 \
  --provider deepseek \
  --model deepseek-reasoner \
  --sample-db /path/to/sample.duckdb \
  --full-db /path/to/full.duckdb \
  --query-id q1 \
  --target-speedup 2.0 \
  --workers 5
```

---

## CLI Parameters

### Required

| Parameter | Description | Example |
|-----------|-------------|---------|
| `<query.sql>` | Path to SQL file | `queries/q1.sql` |

### Optional

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--version` | `v5` | Optimizer version (`v1`, `v2`, `v5`) |
| `--provider` | `deepseek` | LLM provider (`deepseek`, `openai`, `anthropic`, etc.) |
| `--model` | `deepseek-reasoner` | Model name (auto-detected from provider) |
| `--sample-db` | (required for v5) | Sample database for validation |
| `--full-db` | (optional) | Full database for benchmarking |
| `--query-id` | (auto-detect) | Query ID for ML recommendations (e.g., `q1`) |
| `--target-speedup` | `2.0` | Minimum speedup threshold |
| `--workers` | `5` | Number of parallel workers (1-5) |
| `--output` | `stdout` | Output format (`stdout`, `json`, `file`) |
| `--save-results` | (none) | Save results to directory |

---

## Examples

### Example 1: Basic V5 Optimization

```bash
qt-sql optimize q1.sql \
  --sample-db tpcds_sf1.duckdb \
  --full-db tpcds_sf100.duckdb
```

**Expected output:**
```
Optimizing q1.sql with V5 optimizer...
Provider: deepseek (deepseek-reasoner)
Workers: 5
Target speedup: 2.0x

[1/5] Loading query... ✓
[2/5] Generating worker prompts... ✓ (5 workers)
[3/5] Running LLM optimization... ⏳
  Worker 1: ✓ Valid (3.2s)
  Worker 2: ✓ Valid (2.8s)
  Worker 3: ✗ Invalid (syntax error)
  Worker 4: ✓ Valid (3.5s)
  Worker 5: ✓ Valid (4.1s)
[4/5] Sample DB validation... ✓ 4/5 candidates
[5/5] Full DB benchmark... ⏳
  Worker 1: 2.92x ✓ TARGET MET!

🏆 Winner: Worker 1 (2.92x speedup)

Optimized SQL saved to: q1_optimized.sql
```

### Example 2: Specific Model

```bash
qt-sql optimize q15.sql \
  --provider openai \
  --model o1-preview \
  --sample-db tpcds_sf1.duckdb
```

### Example 3: Save Full Results (ALL inputs/outputs stored)

```bash
qt-sql optimize q93.sql \
  --sample-db tpcds_sf1.duckdb \
  --full-db tpcds_sf100.duckdb \
  --save-results results/q93/ \
  --output json
```

**Automatically saves complete audit trail:**

```
results/q93/
├── input/
│   ├── original.sql                    # Original query
│   ├── query_dag.json                  # Parsed DAG structure
│   ├── explain_plan.txt                # EXPLAIN output
│   └── metadata.json                   # Query ID, size, complexity
│
├── workers/
│   ├── worker_1/
│   │   ├── prompt.txt                  # Full prompt sent to LLM
│   │   ├── examples.json               # Gold examples used
│   │   ├── response.json               # Raw LLM response
│   │   ├── optimized.sql               # Assembled SQL
│   │   ├── validation_sample.json      # Sample DB validation
│   │   └── benchmark_full.json         # Full DB benchmark (if run)
│   ├── worker_2/
│   │   └── ...
│   ├── worker_3/
│   │   └── ...
│   ├── worker_4/
│   │   └── ...
│   └── worker_5/
│       └── ...
│
├── winner/
│   ├── optimized.sql                   # Winning SQL
│   ├── comparison.txt                  # Side-by-side with original
│   ├── speedup.json                    # Detailed metrics
│   └── worker_id.txt                   # Which worker won
│
├── summary.json                        # Complete run summary
├── timeline.json                       # Execution timeline
└── run_config.json                     # CLI parameters used
```

### Example 4: Batch Processing

```bash
for q in queries/q*.sql; do
  qt-sql optimize $q \
    --sample-db tpcds_sf1.duckdb \
    --full-db tpcds_sf100.duckdb \
    --save-results results/$(basename $q .sql)/
done
```

### Example 5: Reasoning vs Chat Comparison

```bash
# Test with reasoning model
qt-sql optimize q1.sql \
  --model deepseek-reasoner \
  --output json > q1_reasoner.json

# Test with chat model
qt-sql optimize q1.sql \
  --model deepseek-chat \
  --output json > q1_chat.json

# Compare results
qt-sql compare q1_reasoner.json q1_chat.json
```

---

## Output Formats

### Default (stdout)

```
🏆 Winner: Worker 1 (2.92x speedup)

WITH filtered_store_returns AS (
  SELECT sr_customer_sk, sr_store_sk, sr_fee
  FROM store_returns
  JOIN date_dim ON sr_returned_date_sk = d_date_sk
  WHERE d_year = 2000
),
...
```

### JSON Output (`--output json`)

```json
{
  "query_id": "q1",
  "provider": "deepseek",
  "model": "deepseek-reasoner",
  "target_speedup": 2.0,
  "workers": {
    "total": 5,
    "valid": 4
  },
  "winner": {
    "worker_id": 1,
    "sample_speedup": 3.1,
    "full_speedup": 2.92,
    "status": "target_met",
    "optimized_sql": "WITH filtered_store_returns AS ..."
  },
  "candidates": [
    {
      "worker_id": 1,
      "status": "valid",
      "sample_speedup": 3.1,
      "full_speedup": 2.92
    },
    ...
  ]
}
```

---

## Environment Variables

The CLI respects these environment variables:

```bash
# Provider configuration
export QT_LLM_PROVIDER=deepseek
export QT_LLM_MODEL=deepseek-reasoner

# API keys
export QT_DEEPSEEK_API_KEY=sk-xxx
export QT_OPENAI_API_KEY=sk-xxx
export QT_ANTHROPIC_API_KEY=sk-ant-xxx

# Database paths (for convenience)
export QT_SAMPLE_DB=/path/to/tpcds_sf1.duckdb
export QT_FULL_DB=/path/to/tpcds_sf100.duckdb

# Default settings
export QT_V5_WORKERS=5
export QT_V5_TARGET_SPEEDUP=2.0
```

**With env vars set:**
```bash
# Short form
qt-sql optimize q1.sql

# Equivalent to:
qt-sql optimize q1.sql \
  --provider deepseek \
  --model deepseek-reasoner \
  --sample-db $QT_SAMPLE_DB \
  --full-db $QT_FULL_DB \
  --workers 5 \
  --target-speedup 2.0
```

---

## Advanced Options

### Debug Mode

```bash
qt-sql optimize q1.sql --debug
```

Shows:
- Full prompts sent to LLM
- Raw LLM responses
- Validation details
- Timing breakdown

### Dry Run

```bash
qt-sql optimize q1.sql --dry-run
```

Shows what would be executed without running LLM calls.

### Worker Selection

```bash
# Use only specific workers
qt-sql optimize q1.sql --workers 1,2,5

# Use only explore mode (worker 5)
qt-sql optimize q1.sql --workers 5
```

### ML Recommendation Override

```bash
# Override ML recommendations with specific examples
qt-sql optimize q1.sql \
  --examples decorrelate,pushdown,or_to_union
```

---

## Interactive Mode

```bash
qt-sql optimize q1.sql --interactive
```

**Workflow:**
1. Shows query analysis
2. Asks for confirmation
3. Shows worker assignments
4. Runs optimization
5. Shows candidates
6. Asks which to benchmark
7. Shows final results

```
Query: q1.sql (TPC-DS Q1 - correlated subquery)
ML recommendations: decorrelate (76%), early_filter (35%)

Worker assignments:
  Worker 1: decorrelate, early_filter, or_to_union
  Worker 2: date_cte_isolate, pushdown, materialize_cte
  Worker 3: flatten_subquery, reorder_join, inline_cte
  Worker 4: remove_redundant, multi_push_predicate, semantic_rewrite
  Worker 5: Explore mode (no examples)

Proceed? [Y/n]: y

Running optimization... ✓

Valid candidates:
  [1] Worker 1: Sample 3.1x
  [2] Worker 2: Sample 2.4x
  [3] Worker 4: Sample 1.8x
  [4] Worker 5: Sample 2.9x

Benchmark which candidates? [1-4/all]: 1,4

Benchmarking... ✓
  Worker 1: 2.92x ✓ TARGET MET
  Worker 4: 1.75x (below target)

Winner: Worker 1 (2.92x)
Save optimized SQL? [Y/n]: y
Saved to: q1_optimized.sql
```

---

## Error Handling

### No API Key

```bash
$ qt-sql optimize q1.sql

❌ Error: No LLM provider configured
Set QT_DEEPSEEK_API_KEY environment variable or use --api-key flag
```

### Database Not Found

```bash
$ qt-sql optimize q1.sql --sample-db missing.duckdb

❌ Error: Sample database not found: missing.duckdb
Use --sample-db to specify path or set QT_SAMPLE_DB environment variable
```

### No Valid Candidates

```bash
$ qt-sql optimize q1.sql

Optimizing q1.sql...
[3/5] Running LLM optimization...
  Worker 1: ✗ Invalid (syntax error)
  Worker 2: ✗ Invalid (semantic error)
  Worker 3: ✗ Invalid (timeout)
  Worker 4: ✗ Invalid (validation failed)
  Worker 5: ✗ Invalid (parse error)

❌ No valid candidates generated
All 5 workers failed validation

Troubleshooting:
  1. Check query syntax
  2. Try different model: --model deepseek-chat
  3. Use --debug to see detailed errors
```

### Below Target Speedup

```bash
$ qt-sql optimize q1.sql --target-speedup 3.0

[5/5] Full DB benchmark...
  Worker 1: 2.92x (below target)
  Worker 2: 2.15x (below target)

⚠️  No candidate met target speedup of 3.0x
Best result: Worker 1 (2.92x)

Save anyway? [y/N]: y
```

---

## Integration with Other Commands

### Optimize → Validate

```bash
qt-sql optimize q1.sql > q1_opt.sql
qt-sql validate q1.sql q1_opt.sql --database tpcds_sf100.duckdb
```

### Audit → Optimize

```bash
qt-sql audit q1.sql --output json | \
  jq '.opportunities[]' | \
  qt-sql optimize q1.sql --hints -
```

---

## Configuration File

`~/.qt-sql/config.yaml`:

```yaml
v5:
  provider: deepseek
  model: deepseek-reasoner
  workers: 5
  target_speedup: 2.0

databases:
  sample: /data/tpcds_sf1.duckdb
  full: /data/tpcds_sf100.duckdb

output:
  format: json
  save_results: true
  results_dir: ./results
```

**Usage:**
```bash
qt-sql optimize q1.sql  # Uses config.yaml settings
```

---

## Help Command

```bash
$ qt-sql optimize --help

Usage: qt-sql optimize <query.sql> [OPTIONS]

Optimize SQL queries using LLM-powered V5 optimizer with reasoning models.

Arguments:
  query.sql                SQL file to optimize

Options:
  --version TEXT           Optimizer version [default: v5]
  --provider TEXT          LLM provider [default: deepseek]
  --model TEXT            Model name [default: deepseek-reasoner]
  --sample-db PATH        Sample database for validation [required]
  --full-db PATH          Full database for benchmarking
  --query-id TEXT         Query ID for ML recommendations
  --target-speedup FLOAT  Minimum speedup threshold [default: 2.0]
  --workers TEXT          Number of workers (1-5 or comma-separated) [default: 5]
  --output TEXT           Output format (stdout|json|file) [default: stdout]
  --save-results PATH     Save detailed results to directory
  --debug                 Enable debug logging
  --dry-run              Show execution plan without running
  --interactive          Interactive mode with prompts
  --help                 Show this message and exit

Examples:
  qt-sql optimize q1.sql --sample-db tpcds_sf1.duckdb
  qt-sql optimize q1.sql --model deepseek-chat --workers 3
  qt-sql optimize q1.sql --save-results results/q1/ --output json

Environment Variables:
  QT_LLM_PROVIDER        Default LLM provider
  QT_LLM_MODEL          Default model name
  QT_DEEPSEEK_API_KEY   DeepSeek API key
  QT_SAMPLE_DB          Default sample database path
  QT_FULL_DB            Default full database path
```

---

## Summary

**Recommended usage:**

```bash
# Set up environment once
export QT_DEEPSEEK_API_KEY=your_key
export QT_SAMPLE_DB=/path/to/tpcds_sf1.duckdb
export QT_FULL_DB=/path/to/tpcds_sf100.duckdb

# Then simple optimization
qt-sql optimize q1.sql

# Reasoning model is default, prompt is updated ✅
```

This design provides:
- ✅ Simple defaults (reasoning model, 5 workers)
- ✅ Full control when needed
- ✅ Clear output and error messages
- ✅ Batch processing support
- ✅ Integration with existing commands
