# ADO Learning System

## Overview

The ADO (Adaptive Data Optimization) learning system captures and analyzes three critical data points from every optimization attempt:

1. **Speedup** - Performance improvement ratio (original_time / optimized_time)
2. **Transform** - Which optimization patterns were applied
3. **Error** - Detailed error messages and categories when failures occur

This enables continuous improvement of the optimization system through structured learning records.

## Three Critical Data Points

### 1. Speedup (Performance Metric)

**Captured in**: `validation.json` → `speedup` field
- Float value representing performance ratio
- 1.0 = no improvement
- 2.5 = 2.5x faster
- 0.0 = error/failure

**Storage Path**: `{run_dir}/{query_id}/worker_{id:02d}/validation.json`

```json
{
  "status": "pass",
  "speedup": 2.92,
  "error": null,
  ...
}
```

### 2. Transform (Optimization Pattern)

**Captured in**: `validation.json` → `transforms` field
- List of transformation IDs applied
- Extracted from LLM response
- Includes fallback inference from SQL AST analysis

**Examples**: `["decorrelate"]`, `["or_to_union", "early_filter"]`

**Storage Path**: `{run_dir}/{query_id}/worker_{id:02d}/validation.json`

```json
{
  "transforms": ["decorrelate", "pushdown"],
  ...
}
```

### 3. Error (Diagnostic Information)

**Captured in**: `validation.json` → `error`, `errors`, `error_category` fields

**Three-level error capture**:
- **error**: First error message (for backward compatibility)
- **errors**: Complete list of all error messages
- **error_category**: Classification of error type

**Error Categories**:
- `syntax`: SQL parsing/syntax errors
- `semantic`: Wrong results (row count/value mismatch)
- `timeout`: Query execution timeout
- `execution`: Runtime execution errors
- `unknown`: Uncategorized errors

**Storage Path**: `{run_dir}/{query_id}/worker_{id:02d}/validation.json`

```json
{
  "status": "error",
  "error": "Row count mismatch: original=100, optimized=95 | Value mismatch",
  "errors": [
    "Row count mismatch: original=100, optimized=95",
    "Value mismatch: rows differ"
  ],
  "error_category": "semantic"
}
```

## Learning Records (Structured Journal)

Every optimization attempt creates a structured `LearningRecord` in the learning journal.

### Location
```
research/ado/learning/
├── {query_id}/
│   ├── attempt_01.json
│   ├── attempt_02.json
│   └── ...
└── summary.json
```

### Learning Record Structure

```json
{
  "timestamp": "2026-02-05T15:30:45.123456",
  "query_id": "q1",
  "query_pattern": "subquery_correlate",

  "examples_recommended": ["decorrelate", "cte_isolate"],
  "transforms_recommended": ["decorrelate", "pushdown"],

  "transform_used": "decorrelate",
  "transforms_used": ["decorrelate"],
  "status": "pass",
  "speedup": 2.92,

  "examples_effective": true,
  "transform_effectiveness": 2.92,
  "error_category": null,
  "error_messages": [],

  "worker_id": 1,
  "attempt_number": 1
}
```

### Key Fields

| Field | Purpose | Learning Use |
|-------|---------|--------------|
| `examples_recommended` | Which examples were suggested to LLM | Track example effectiveness |
| `transforms_recommended` | Transforms mentioned in prompt | Validate prompt quality |
| `transforms_used` | Transforms actually applied | Track transform success rates |
| `status` | pass/fail/error | Measure success |
| `speedup` | Performance improvement ratio | Measure effectiveness |
| `error_category` | Type of failure | Identify recoverable errors |
| `examples_effective` | Did examples help? | Improve example selection |

## Learning Summary

The `summary.json` file aggregates all learning records to provide:

### Success Metrics
```json
{
  "total_attempts": 42,
  "pass_rate": 0.67,        // 67% success rate
  "fail_rate": 0.19,        // 19% validation failures
  "error_rate": 0.14,       // 14% errors
  "avg_speedup": 1.58
}
```

### Transform Effectiveness
```json
{
  "decorrelate": {
    "success_rate": 0.75,           // 75% of attempts pass
    "avg_speedup": 2.15,            // Average 2.15x speedup when passes
    "attempts": 20,
    "successful_attempts": 15
  }
}
```

### Example Recommendation Effectiveness
```json
{
  "decorrelate": {
    "effectiveness": 0.80,          // Led to success 80% of the time
    "times_recommended": 15,
    "led_to_success": 12
  }
}
```

### Error Pattern Analysis
```json
{
  "error_patterns": {
    "semantic": {
      "count": 3,
      "messages": [
        "Row count mismatch: original=100, optimized=95",
        "Value mismatch in result set"
      ]
    },
    "syntax": {
      "count": 1,
      "messages": ["Unexpected token: LATERAL"]
    }
  }
}
```

## Using the Learning System

### 1. Creating Learning Records

```python
from ado.learn import Learner

learner = Learner(journal_dir=Path("research/ado/learning"))

# After optimization completes
record = learner.create_learning_record(
    query_id="q1",
    examples_recommended=["decorrelate", "cte_isolate"],
    transforms_recommended=["decorrelate"],
    status="pass",
    speedup=2.92,
    transforms_used=["decorrelate"],
    worker_id=1,
    error_category=None,
    error_messages=[],
)

# Save to journal
learner.save_learning_record(record)
```

### 2. Building Learning Summary

```python
# Aggregate all learning records
summary = learner.build_learning_summary()

# Save summary for analysis
learner.save_learning_summary()

# Use summary to improve
print(f"Overall success rate: {summary['pass_rate']:.1%}")
print(f"Best transform: {max(summary['transform_effectiveness'].items(), key=lambda x: x[1]['success_rate'])}")
```

### 3. Analyzing Failures

```python
summary = learner.build_learning_summary()

# Identify recoverable errors
for error_type, stats in summary['error_patterns'].items():
    print(f"{error_type}: {stats['count']} occurrences")
    for msg in stats['messages']:
        print(f"  - {msg}")
```

## Implementation Details

### Error Message Capture (Fixed)

**Before**: Only first error captured
```python
error_msg = result.errors[0]  # ❌ Loses information
```

**After**: All errors concatenated
```python
error_msg = " | ".join(result.errors)  # ✅ Complete picture
```

### Transform Identification (Fixed)

**Fallback mechanism** when LLM doesn't explicitly name transforms:

1. Parse LLM response for explicit rewrite_sets
2. If no transforms found, analyze SQL AST for patterns:
   - `Subquery reduction` → `decorrelate`
   - `CTE increase` → `materialize_cte`
   - `Union increase` → `or_to_union`
   - `Window increase` → `subquery_to_window`

```python
# Automatic inference from SQL diff
transforms = extract_transforms_from_response(
    response,
    original_sql=sql,
    optimized_sql=optimized_sql
)
```

### Error Categorization (New)

```python
def categorize_error(error_msg: str) -> str:
    if "syntax" in error_msg.lower():
        return "syntax"
    elif "mismatch" in error_msg.lower():
        return "semantic"
    elif "timeout" in error_msg.lower():
        return "timeout"
    # ... etc
    return "unknown"
```

## Files Modified

1. **ado/schemas.py** - Added `errors` list and `error_category` to ValidationResult
2. **ado/validate.py** - Capture all errors, categorize them
3. **ado/learn.py** - Comprehensive learning record system
4. **ado/generate.py** - Pass original/optimized SQL for fallback transform inference
5. **ado/sql_rewriter.py** - Fallback transform identification from SQL AST
6. **ado/runner.py** - Create and save learning records

## Enabling Continuous Improvement

The structured learning records enable:

1. **Transform Effectiveness Analysis**
   - Which transforms have highest success rates?
   - Which transforms consistently outperform others?

2. **Example Quality Assessment**
   - Which examples lead to wins?
   - Which examples should be promoted/demoted?

3. **Error Recovery**
   - Which errors are most common?
   - Can we detect and prevent certain error categories?

4. **Pattern Recognition**
   - What query patterns respond to which transforms?
   - Can we predict transform effectiveness before trying?

5. **System Improvement**
   - Track progress over time
   - A/B test different approaches
   - Identify areas for prompt engineering

## Accessing Learning Data

### From Python
```python
from pathlib import Path
import json

journal = Path("research/ado/learning")
summary = json.loads((journal / "summary.json").read_text())
print(summary)
```

### From Command Line
```bash
# View summary
cat research/ado/learning/summary.json | jq .

# View all attempts for a query
ls -la research/ado/learning/q1/

# Analyze success rates
cat research/ado/learning/summary.json | jq '.pass_rate'
```

## Next Steps for ML Integration

The learning records are designed for:

1. **Training better prompt generators** - Understand what works
2. **Example recommendation** - Predict which examples will help
3. **Transform selection** - Predict which transforms will succeed
4. **Error prediction** - Anticipate failures and avoid them
5. **Feedback loops** - Continuously improve over time
