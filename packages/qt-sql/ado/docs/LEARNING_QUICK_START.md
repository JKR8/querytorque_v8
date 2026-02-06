# ADO Learning System - Quick Start Guide

## The Three Critical Data Points

Every optimization attempt now records:

### 1️⃣ Speedup (Performance)
```json
{
  "speedup": 2.92  // 2.92x faster = success
}
```

### 2️⃣ Transform (What was applied)
```json
{
  "transforms": ["decorrelate", "pushdown"]  // Which optimizations worked
}
```

### 3️⃣ Error (What went wrong)
```json
{
  "error": "Row count mismatch | Value mismatch",
  "errors": [
    "Row count mismatch: original=100, optimized=95",
    "Value mismatch: rows differ"
  ],
  "error_category": "semantic"  // Type of error
}
```

---

## Where Data Is Stored

### Validation Results (Immediate)
```
ado/models/
├── similarity_index.faiss
└── {run_dir}/{query_id}/worker_{id}/
    ├── prompt.txt
    ├── response.txt
    ├── optimized.sql
    └── validation.json  ← Has speedup/transforms/errors
```

### Learning Records (Permanent Journal)
```
research/ado/learning/
├── {query_id}/
│   ├── attempt_01.json  ← Structured learning record
│   ├── attempt_02.json
│   └── ...
└── summary.json  ← Analytics across all attempts
```

---

## Quick Access Patterns

### View Speedup from Last Run
```bash
cat ado/models/{run_dir}/{query_id}/worker_01/validation.json | jq '.speedup'
```

### View Transforms Applied
```bash
cat ado/models/{run_dir}/{query_id}/worker_01/validation.json | jq '.transforms'
```

### View All Errors
```bash
cat ado/models/{run_dir}/{query_id}/worker_01/validation.json | jq '.errors'
```

### View Error Category
```bash
cat ado/models/{run_dir}/{query_id}/worker_01/validation.json | jq '.error_category'
```

---

## Learning Summary Analytics

### Overall Success Rate
```bash
cat research/ado/learning/summary.json | jq '.pass_rate'
# Output: 0.67  (67% success)
```

### Best Performing Transforms
```bash
cat research/ado/learning/summary.json | jq '.transform_effectiveness | to_entries | sort_by(.value.success_rate) | reverse | .[0:3]'
```

### Most Effective Examples
```bash
cat research/ado/learning/summary.json | jq '.example_effectiveness | to_entries | sort_by(.value.effectiveness) | reverse | .[0:5]'
```

### Error Pattern Distribution
```bash
cat research/ado/learning/summary.json | jq '.error_patterns | keys'
# Shows: ["syntax", "semantic", "timeout"]
```

---

## Error Categories Explained

| Category | Meaning | Example |
|----------|---------|---------|
| **syntax** | SQL parsing error | "Unexpected token LATERAL" |
| **semantic** | Wrong results | "Row count mismatch: 100 vs 95" |
| **timeout** | Query too slow | "Execution timeout after 30s" |
| **execution** | Runtime error | "Column not found: foo" |
| **unknown** | Other | "Unknown error type" |

---

## Python API Usage

### Create Learning Record
```python
from ado.learn import Learner

learner = Learner()

# After optimization
record = learner.create_learning_record(
    query_id="q1",
    examples_recommended=["decorrelate"],
    transforms_recommended=["decorrelate"],
    status="pass",
    speedup=2.92,
    transforms_used=["decorrelate"],
    worker_id=1,
)

learner.save_learning_record(record)
```

### View Learning Summary
```python
summary = learner.build_learning_summary()

print(f"Success rate: {summary['pass_rate']:.1%}")
print(f"Avg speedup: {summary['avg_speedup']:.2f}x")
print(f"Transform effectiveness: {summary['transform_effectiveness']}")
print(f"Example effectiveness: {summary['example_effectiveness']}")
print(f"Error patterns: {summary['error_patterns']}")
```

---

## What Gets Recorded Automatically

✅ Every optimization attempt creates a learning record with:
- Query ID
- Examples recommended to LLM
- Transforms in the prompt
- Actual transforms applied
- Performance (speedup)
- Status (pass/fail/error)
- All error messages
- Error categorization

---

## Using Learning Data for Improvement

### 1. Identify Best Transforms
```bash
cat research/ado/learning/summary.json | jq '.transform_effectiveness.decorrelate'
# See: {"success_rate": 0.8, "avg_speedup": 2.15, "attempts": 20}
```

### 2. Find Effective Examples
```bash
cat research/ado/learning/summary.json | jq '.example_effectiveness | map(select(.effectiveness > 0.7))'
# Shows examples that work well
```

### 3. Understand Failures
```bash
cat research/ado/learning/summary.json | jq '.error_patterns.semantic.messages'
# See common semantic errors to fix
```

### 4. Track Progress Over Time
```bash
# Compare old and new summary.json files
diff <(jq .pass_rate old_summary.json) <(jq .pass_rate new_summary.json)
```

---

## Key Metrics to Watch

| Metric | Target | Action if Low |
|--------|--------|--------------|
| **pass_rate** | > 60% | Review prompts, examples |
| **avg_speedup** | > 1.5x | Adjust constraints, prompts |
| **transform success_rate** | > 70% per transform | Remove failing transforms |
| **example effectiveness** | > 60% per example | Replace ineffective examples |
| **error_rate** | < 10% | Debug error categorization |

---

## Common Queries

### Find all failed semantic errors
```bash
cat research/ado/learning/summary.json | jq '.error_patterns.semantic'
```

### Find queries that never passed
```bash
find research/ado/learning -name "*.json" -exec grep -L '"status": "pass"' {} \;
```

### Average speedup by transform
```bash
cat research/ado/learning/summary.json | jq '.transform_effectiveness | map_values(.avg_speedup)'
```

### Top 5 most recommended examples
```bash
cat research/ado/learning/summary.json | jq '.example_effectiveness | to_entries | sort_by(.value.times_recommended) | reverse | .[0:5]'
```

---

## Integration with ML/Training

The learning records are designed for:

1. **Supervised Learning**: Train models to predict which transforms will succeed
2. **Reinforcement Learning**: Reward successful examples, penalize failing ones
3. **Clustering**: Group similar queries and their optimal transforms
4. **Anomaly Detection**: Identify unusual error patterns
5. **Feedback Loops**: Continuously improve prompt generation

Example data format for ML:
```json
{
  "features": {
    "query_pattern": "subquery_correlate",
    "num_joins": 3,
    "has_cte": true,
    "has_aggregation": true
  },
  "examples_used": ["decorrelate", "pushdown"],
  "transforms_applied": ["decorrelate"],
  "label": "success",  // or "fail", "error"
  "speedup": 2.92
}
```

---

## Troubleshooting

### Learning records not appearing
Check:
1. Is `research/ado/learning/` directory writable?
2. Are there any exceptions in ADO logs?
3. Did optimization complete (not timeout)?

### Error categories not set
Ensure:
1. `error_category` is populated in ValidationResult
2. categorize_error() is called for all error paths
3. Error messages are non-empty strings

### Transforms showing as empty list
Check:
1. LLM response has valid rewrite_sets with transform field
2. Fallback AST inference is working (for syntax issues)
3. SQL AST can be parsed by sqlglot

---

## Next Steps

1. **Monitor Learning Summary**: Check `research/ado/learning/summary.json` after running queries
2. **Analyze Effectiveness**: Identify which examples/transforms work best
3. **Improve Prompts**: Use insights to refine prompt generation
4. **Train Models**: Use learning records to train transform recommenders
5. **Close the Loop**: Let recommendations improve based on actual results

See `LEARNING_SYSTEM.md` for complete documentation.
