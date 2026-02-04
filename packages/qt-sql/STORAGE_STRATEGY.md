# V5 Optimizer Storage Strategy

**Date:** 2026-02-05
**Goal:** Complete audit trail of all inputs and outputs

---

## Storage Structure

### Complete Directory Layout

```
results/{query_id}_{timestamp}/
├── input/
│   ├── original.sql                    # Original query as submitted
│   ├── query_dag.json                  # Parsed DAG with contracts
│   ├── explain_plan_sample.txt         # EXPLAIN from sample DB
│   ├── explain_plan_full.txt           # EXPLAIN from full DB (if available)
│   ├── metadata.json                   # Query metadata
│   └── ml_recommendations.json         # ML-selected examples
│
├── workers/
│   ├── worker_1/
│   │   ├── config.json                 # Worker configuration
│   │   ├── examples.json               # Gold examples assigned
│   │   ├── prompt.txt                  # Complete prompt sent to LLM
│   │   ├── llm_request.json            # Request payload
│   │   ├── llm_response.json           # Raw response from LLM
│   │   ├── reasoning.txt               # Reasoning content (if available)
│   │   ├── rewrite_sets.json           # Parsed rewrite_sets
│   │   ├── optimized.sql               # Assembled SQL
│   │   ├── validation_sample.json      # Sample DB validation result
│   │   ├── benchmark_full.json         # Full DB benchmark (if run)
│   │   └── errors.log                  # Any errors encountered
│   ├── worker_2/
│   │   └── ... (same structure)
│   ├── worker_3/
│   │   └── ...
│   ├── worker_4/
│   │   └── ...
│   └── worker_5/
│       ├── config.json                 # No examples (explore mode)
│       ├── prompt.txt                  # Full SQL prompt
│       └── ... (rest same)
│
├── validation/
│   ├── sample_db/
│   │   ├── original_result.json        # Original query result
│   │   ├── original_timing.json        # Timing metrics
│   │   ├── worker_1_result.json        # Each candidate result
│   │   ├── worker_1_timing.json
│   │   ├── worker_2_result.json
│   │   └── ...
│   └── equivalence_checks.json         # Row count, checksum comparisons
│
├── benchmark/
│   ├── full_db/
│   │   ├── original_runs.json          # 5-run trimmed mean data
│   │   ├── worker_1_runs.json          # 5-run trimmed mean data
│   │   ├── worker_2_runs.json
│   │   └── ...
│   └── speedup_calculations.json       # All speedup computations
│
├── winner/
│   ├── optimized.sql                   # Final winning SQL
│   ├── comparison.html                 # Side-by-side diff view
│   ├── comparison.txt                  # Text diff
│   ├── speedup_report.json             # Detailed metrics
│   ├── worker_id.txt                   # Which worker won (e.g., "1")
│   └── verification.json               # Final validation proof
│
├── summary.json                        # High-level summary
├── timeline.json                       # Execution timeline with durations
├── run_config.json                     # Complete CLI configuration
├── system_info.json                    # System, model, versions
└── run.log                             # Complete execution log
```

---

## File Formats

### input/original.sql
```sql
WITH customer_total_return AS (
  SELECT sr_customer_sk AS ctr_customer_sk,
         sr_store_sk AS ctr_store_sk,
         SUM(SR_FEE) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (
    SELECT avg(ctr_total_return)*1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
  )
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'SD'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
```

### input/metadata.json
```json
{
  "query_id": "q1",
  "source_file": "queries/q1.sql",
  "size_bytes": 542,
  "line_count": 18,
  "detected_patterns": [
    "correlated_subquery",
    "aggregate_in_subquery",
    "multiple_joins"
  ],
  "complexity_score": 7.5,
  "timestamp": "2026-02-05T10:30:00Z"
}
```

### input/ml_recommendations.json
```json
{
  "query_id": "q1",
  "recommendations": [
    {
      "rank": 1,
      "example_id": "decorrelate",
      "confidence": 0.76,
      "expected_speedup": "2.92x",
      "rationale": "Correlated subquery detected"
    },
    {
      "rank": 2,
      "example_id": "early_filter",
      "confidence": 0.35,
      "expected_speedup": "2.15x",
      "rationale": "Multiple dimension tables"
    }
  ],
  "total_examples_available": 13,
  "selected_for_workers": 12
}
```

### workers/worker_1/config.json
```json
{
  "worker_id": 1,
  "format": "dag_json",
  "examples": [
    "date_cte_isolate",
    "decorrelate",
    "early_filter"
  ],
  "example_source": "ml_recommendations",
  "prompt_size": 9963,
  "model": "deepseek-reasoner",
  "provider": "deepseek"
}
```

### workers/worker_1/prompt.txt
```
## Example: Date CTE Isolation (DATE_CTE_ISOLATE)
Verified speedup: 1.5-2.5x

### Input:
...

You are an autonomous Query Rewrite Engine. Your goal is to maximize execution
speed while strictly preserving semantic invariants.
...
```

### workers/worker_1/llm_request.json
```json
{
  "model": "deepseek-reasoner",
  "messages": [
    {
      "role": "user",
      "content": "..."
    }
  ],
  "max_tokens": 16384,
  "temperature": 0,
  "timestamp": "2026-02-05T10:30:15Z"
}
```

### workers/worker_1/llm_response.json
```json
{
  "id": "chatcmpl-xyz",
  "model": "deepseek-reasoner",
  "usage": {
    "prompt_tokens": 2450,
    "completion_tokens": 892,
    "total_tokens": 3342
  },
  "choices": [
    {
      "message": {
        "role": "assistant",
        "content": "{\"rewrite_sets\": [...]}",
        "reasoning_content": "Analyzing the query structure..."
      }
    }
  ],
  "duration_ms": 3245,
  "timestamp": "2026-02-05T10:30:18Z"
}
```

### workers/worker_1/reasoning.txt
```
Analyzing the query structure... The correlated subquery computes average
ctr_total_return per store. If I push the s_state='SD' filter into
filtered_store_returns before aggregation, I change the scope of the average
calculation to only SD stores. This violates the semantic invariant that the
average must be computed over all stores. Therefore, I must keep the filter
in the main query WHERE clause after decorrelation.
```

### workers/worker_1/rewrite_sets.json
```json
{
  "rewrite_sets": [
    {
      "id": "rs_01",
      "transform": "decorrelate",
      "nodes": {
        "filtered_store_returns": "...",
        "customer_total_return": "...",
        "store_avg_return": "...",
        "main_query": "..."
      },
      "invariants_kept": [
        "same result rows",
        "same ordering",
        "same column output"
      ],
      "expected_speedup": "2.90x",
      "risk": "low"
    }
  ],
  "explanation": "The correlated subquery computing average..."
}
```

### workers/worker_1/validation_sample.json
```json
{
  "status": "PASS",
  "original": {
    "row_count": 100,
    "execution_time_ms": 234.5,
    "checksum": "a7f3d9e2"
  },
  "optimized": {
    "row_count": 100,
    "execution_time_ms": 75.8,
    "checksum": "a7f3d9e2"
  },
  "speedup": 3.09,
  "equivalence": "exact_match",
  "timestamp": "2026-02-05T10:30:20Z"
}
```

### workers/worker_1/benchmark_full.json
```json
{
  "status": "PASS",
  "runs": 5,
  "method": "trimmed_mean",
  "original": {
    "run_1_ms": 12450,
    "run_2_ms": 12389,
    "run_3_ms": 12512,
    "run_4_ms": 12445,
    "run_5_ms": 12398,
    "trimmed_mean_ms": 12431.3,
    "discarded": ["min: 12389", "max: 12512"]
  },
  "optimized": {
    "run_1_ms": 4265,
    "run_2_ms": 4251,
    "run_3_ms": 4289,
    "run_4_ms": 4268,
    "run_5_ms": 4243,
    "trimmed_mean_ms": 4261.3,
    "discarded": ["min: 4243", "max: 4289"]
  },
  "speedup": 2.92,
  "target_met": true,
  "timestamp": "2026-02-05T10:35:45Z"
}
```

### summary.json
```json
{
  "query_id": "q1",
  "timestamp": "2026-02-05T10:30:00Z",
  "duration_total_ms": 325450,
  "configuration": {
    "provider": "deepseek",
    "model": "deepseek-reasoner",
    "workers": 5,
    "target_speedup": 2.0
  },
  "results": {
    "workers_total": 5,
    "workers_valid": 4,
    "workers_benchmarked": 2,
    "winner": {
      "worker_id": 1,
      "speedup": 2.92,
      "status": "target_met"
    }
  },
  "performance": {
    "original_time_ms": 12431.3,
    "optimized_time_ms": 4261.3,
    "speedup": 2.92,
    "improvement_pct": 65.7
  }
}
```

### timeline.json
```json
{
  "start": "2026-02-05T10:30:00.000Z",
  "end": "2026-02-05T10:35:25.450Z",
  "duration_ms": 325450,
  "phases": [
    {
      "phase": "load_query",
      "start_ms": 0,
      "duration_ms": 125,
      "status": "success"
    },
    {
      "phase": "generate_prompts",
      "start_ms": 125,
      "duration_ms": 450,
      "status": "success"
    },
    {
      "phase": "llm_optimization",
      "start_ms": 575,
      "duration_ms": 18230,
      "workers": [
        {
          "worker_id": 1,
          "start_ms": 575,
          "duration_ms": 3245,
          "status": "success"
        },
        {
          "worker_id": 2,
          "start_ms": 580,
          "duration_ms": 2834,
          "status": "success"
        }
      ]
    },
    {
      "phase": "sample_validation",
      "start_ms": 18805,
      "duration_ms": 1245,
      "status": "success"
    },
    {
      "phase": "full_benchmark",
      "start_ms": 20050,
      "duration_ms": 305400,
      "workers_benchmarked": [1, 4],
      "status": "success"
    }
  ]
}
```

### run_config.json
```json
{
  "cli_version": "0.5.0",
  "command": "qt-sql optimize q1.sql --sample-db tpcds_sf1.duckdb --full-db tpcds_sf100.duckdb",
  "parameters": {
    "query_file": "q1.sql",
    "version": "v5",
    "provider": "deepseek",
    "model": "deepseek-reasoner",
    "sample_db": "tpcds_sf1.duckdb",
    "full_db": "tpcds_sf100.duckdb",
    "query_id": "q1",
    "target_speedup": 2.0,
    "workers": 5,
    "output": "stdout",
    "save_results": "results/q1_20260205_103000/"
  },
  "environment": {
    "QT_LLM_PROVIDER": "deepseek",
    "QT_SAMPLE_DB": "tpcds_sf1.duckdb"
  }
}
```

### system_info.json
```json
{
  "platform": "linux",
  "python_version": "3.11.5",
  "qt_sql_version": "0.5.0",
  "qt_shared_version": "0.5.0",
  "dependencies": {
    "sqlglot": "20.10.0",
    "duckdb": "0.9.2",
    "openai": "1.10.0"
  },
  "hardware": {
    "cpu_cores": 8,
    "memory_gb": 32
  }
}
```

---

## Storage Lifecycle

### Phase 1: Input Capture (Immediate)
```
input/
├── original.sql          ✓ Saved immediately
├── metadata.json         ✓ After parsing
├── query_dag.json        ✓ After DAG building
├── explain_plan_*.txt    ✓ After EXPLAIN
└── ml_recommendations.json ✓ After ML lookup
```

### Phase 2: Worker Execution (Per Worker)
```
workers/worker_X/
├── config.json          ✓ Before LLM call
├── examples.json        ✓ Before LLM call
├── prompt.txt           ✓ Before LLM call
├── llm_request.json     ✓ Before LLM call
├── llm_response.json    ✓ After LLM response
├── reasoning.txt        ✓ After response (if available)
├── rewrite_sets.json    ✓ After JSON parse
├── optimized.sql        ✓ After assembly
├── validation_sample.json ✓ After sample validation
└── benchmark_full.json  ✓ After full benchmark (if run)
```

### Phase 3: Validation & Benchmark
```
validation/
└── sample_db/
    ├── original_result.json   ✓ After original execution
    └── worker_X_result.json   ✓ After each candidate

benchmark/
└── full_db/
    ├── original_runs.json     ✓ After 5 runs
    └── worker_X_runs.json     ✓ After each candidate runs
```

### Phase 4: Winner Selection
```
winner/
├── optimized.sql        ✓ Copy of winning SQL
├── comparison.*         ✓ Generated after selection
├── speedup_report.json  ✓ Generated after selection
└── verification.json    ✓ Final validation proof
```

### Phase 5: Summary
```
summary.json             ✓ After completion
timeline.json            ✓ After completion
run_config.json          ✓ At start (updated at end)
system_info.json         ✓ At start
run.log                  ✓ Continuous append
```

---

## CLI Integration

### Automatic Storage

By default, all runs automatically save to timestamped directories:

```bash
qt-sql optimize q1.sql --sample-db tpcds.duckdb

# Auto-creates:
# results/q1_20260205_103000/
```

### Custom Storage Location

```bash
qt-sql optimize q1.sql \
  --save-results /custom/path/q1_run_1/
```

### Disable Storage (stdout only)

```bash
qt-sql optimize q1.sql --no-save
```

### Storage Level Control

```bash
# Minimal (only summary + winner)
qt-sql optimize q1.sql --save-level minimal

# Standard (summary + winner + worker results)
qt-sql optimize q1.sql --save-level standard

# Full (everything, including prompts/responses)
qt-sql optimize q1.sql --save-level full  # Default
```

---

## Benefits

### 1. Complete Audit Trail
- Every input saved
- Every output saved
- Every intermediate step saved

### 2. Reproducibility
- Exact prompts can be replayed
- LLM responses are preserved
- Timing data is complete

### 3. Debugging
- Easy to identify where failures occurred
- Can inspect exact LLM reasoning
- Can verify semantic correctness

### 4. Analysis
- Compare different runs
- Analyze which examples work best
- Track model performance over time

### 5. Compliance
- Full audit trail for production use
- Proof of optimization correctness
- Track all decisions made

---

## Storage Size Estimates

### Per Query Run

```
input/           ~50 KB
workers/         ~500 KB (5 workers × 100 KB each)
validation/      ~200 KB
benchmark/       ~100 KB
winner/          ~50 KB
summary files    ~50 KB
Total:           ~950 KB per run
```

### 99 TPC-DS Queries

```
99 queries × 950 KB = ~94 MB per complete benchmark run
```

### Compression

```bash
# Compress old runs
tar -czf q1_20260205_103000.tar.gz results/q1_20260205_103000/

# Reduces to ~200 KB (5x compression)
```

---

## Retrieval API

### List Saved Runs

```bash
qt-sql results list
qt-sql results list --query q1
qt-sql results list --date 2026-02-05
```

### View Specific Run

```bash
qt-sql results show q1_20260205_103000
qt-sql results show --worker 1 q1_20260205_103000
```

### Compare Runs

```bash
qt-sql results compare q1_20260205_103000 q1_20260205_143000
```

### Export Run

```bash
qt-sql results export q1_20260205_103000 --format html
qt-sql results export q1_20260205_103000 --format pdf
```

---

## Summary

✅ **Complete storage of all inputs and outputs**
✅ **Timestamped directories for each run**
✅ **Worker-specific subdirectories**
✅ **Full audit trail from input → LLM → output**
✅ **Easy retrieval and analysis**
✅ **Reproducibility guaranteed**

Every run creates a complete, self-contained record of the optimization process! 🎯
