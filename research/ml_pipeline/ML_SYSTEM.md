# ML Optimization Recommendation System

## Overview

Hybrid ML system combining **AST pattern detection** and **vector similarity search** to recommend SQL optimizations with confidence scores and historical evidence.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      ML Recommender                          │
└─────────────────┬───────────────────────┬───────────────────┘
                  │                       │
      ┌───────────▼──────────┐  ┌────────▼──────────┐
      │  Pattern Detection   │  │  Similarity Search │
      │  (AST Gold Rules)    │  │  (FAISS + Vectors) │
      └───────────┬──────────┘  └────────┬───────────┘
                  │                       │
      ┌───────────▼──────────┐  ┌────────▼───────────┐
      │ Pattern Weight       │  │ Query Vectors      │
      │ Matrix (JSON)        │  │ (90-dim, FAISS)    │
      └──────────────────────┘  └────────────────────┘
```

## Components

### 1. Data Pipeline

**Input**: TPC-DS benchmark results (99 queries)

**Steps**:
1. `generate_ml_training_data.py` → Extract labels, detections, speedups
2. `normalize_sql.py` → Strip semantics (tables→fact_table_N, columns→col_N)
3. `vectorize_queries.py` → Generate 90-dim AST feature vectors

**Outputs**:
- `data/ml_training_data.csv` - Labeled training data
- `data/normalized_queries.json` - Semantic-stripped SQL
- `vectors/query_vectors.npz` - 90-dimensional feature vectors

### 2. Detector Analysis

**Script**: `analyze_detector_effectiveness.py`

**Purpose**: Identify which AST detectors correlate with speedups

**Metrics**:
- **Effectiveness score** = win_rate × avg_speedup × log(occurrences)
- **Win rate** = % of detections in queries with speedup ≥ 1.2x
- **Noise detectors** = high frequency but low win correlation

**Outputs**:
- `analysis/detector_effectiveness.json`
- Recommendations: KEEP / ARCHIVE / DELETE detectors

**Example output**:
```
✓ KEEP (15 detectors)
  GLD-003              - High effectiveness (score=2.45)
  GLD-001              - High effectiveness (score=1.82)

📦 ARCHIVE for other DB testing (23 detectors)
  SQL-JOIN-007         - Low value for DuckDB (win_rate=8%, avg_speedup=1.02x)

🗑️  CONSIDER DELETION (8 detectors)
  SQL-DUCK-018         - Never appears in winning queries (15 occurrences)
```

### 3. Pattern Weight Matrix

**Script**: `train_pattern_weights.py`

**Purpose**: Map gold patterns → transforms with confidence scores

**Algorithm**:
```python
For each winning query (speedup ≥ 1.2x):
  For each gold detection:
    weights[gold_id][transform] += speedup

  If multiple detections:
    combo_key = "GLD-001+GLD-003"
    weights[combo_key][transform] += speedup

Normalize to confidence scores (0-1)
```

**Output**: `models/pattern_weights.json`

**Example**:
```json
{
  "single_patterns": {
    "GLD-003": {
      "early_filter": {
        "count": 4,
        "avg_speedup": 1.75,
        "max_speedup": 2.71,
        "confidence": 0.85
      }
    }
  },
  "pattern_combinations": {
    "GLD-001+GLD-003": {
      "decorrelate": {
        "count": 1,
        "avg_speedup": 2.81,
        "confidence": 1.0
      }
    }
  }
}
```

### 4. FAISS Similarity Index

**Script**: `train_faiss_index.py`

**Purpose**: Enable fast nearest-neighbor search for similar queries

**Algorithm**:
1. Load 90-dim query vectors
2. Normalize vectors (for cosine similarity)
3. Build FAISS IndexFlatL2
4. Store metadata (query_id → speedup, transform)

**Output**:
- `models/similarity_index.faiss` - FAISS index
- `models/similarity_metadata.json` - Query metadata

**Usage**:
```python
# Find 5 most similar queries
distances, indices = index.search(query_vector, k=5)

# Lower distance = more similar
# For normalized vectors: similarity = 1 - (distance^2 / 2)
```

### 5. ML Recommender

**Module**: `qt_sql/optimization/ml_recommender.py`

**Purpose**: Unified interface combining both approaches

**API**:
```python
from qt_sql.optimization.ml_recommender import load_recommender

recommender = load_recommender()

# Generate recommendations
recommendations = recommender.recommend(
    sql=query_text,
    gold_detections=["GLD-003", "GLD-004"],
    top_k=3
)

# Format for prompt inclusion
prompt_text = recommender.format_for_prompt(recommendations)
```

**Output structure**:
```python
{
  "pattern_recommendations": [
    TransformRecommendation(
      transform_name="early_filter",
      confidence=0.85,
      avg_speedup=1.75,
      max_speedup=2.71,
      evidence_type="pattern",
      evidence_details={"pattern": "GLD-003", "count": 4}
    )
  ],
  "similar_queries": [
    SimilarQuery(
      query_id="q93",
      distance=0.234,
      speedup=2.71,
      winning_transform="early_filter",
      similarity_score=0.94
    )
  ],
  "combined_recommendations": [
    {
      "transform": "early_filter",
      "combined_confidence": 0.82,
      "estimated_speedup": 1.68,
      "pattern_confidence": 0.85,
      "pattern_avg_speedup": 1.75,
      "similar_query_count": 2,
      "similar_query_avg_speedup": 1.52,
      "similar_queries": [...]
    }
  ]
}
```

**Ranking algorithm**:
```python
# Combined confidence
combined_confidence = 0.7 * pattern_confidence + 0.3 * (similar_count / 5)

# Estimated speedup
estimated_speedup = 0.7 * pattern_avg + 0.3 * similar_avg

# Final score (for sorting)
score = combined_confidence * estimated_speedup
```

## Installation

### Dependencies

```bash
pip install faiss-cpu numpy
```

### Training Pipeline

```bash
# Run complete training (data + models)
bash scripts/run_ml_training.sh
```

This will:
1. Generate training data (99 queries)
2. Normalize SQL queries
3. Vectorize queries (90-dim)
4. Analyze detector effectiveness
5. Train pattern weight matrix
6. Build FAISS similarity index

**Runtime**: ~2-3 minutes

## Usage

### 1. Standalone Recommender

```python
from qt_sql.optimization.ml_recommender import load_recommender
from qt_sql.analyzers.ast_detector import detect_antipatterns

# Load recommender
recommender = load_recommender()

# Get gold detections
issues = detect_antipatterns(sql, dialect="duckdb")
gold_detections = [i.rule_id for i in issues if i.rule_id.startswith("GLD-")]

# Get recommendations
recs = recommender.recommend(sql, gold_detections, top_k=3)

# Print results
for rec in recs["combined_recommendations"]:
    print(f"{rec['transform']}: {rec['combined_confidence']:.0%} confidence, "
          f"{rec['estimated_speedup']:.2f}x speedup")
```

### 2. Integration with Optimizer

**In `adaptive_rewriter_v5.py`**:

```python
from qt_sql.optimization.ml_recommender import load_recommender

# Load at startup (cache globally)
ml_recommender = load_recommender()

def build_prompt(sql: str, context: OptimizationContext):
    # ... existing prompt building ...

    # Add ML recommendations if available
    if ml_recommender:
        gold_detections = [
            issue.rule_id
            for issue in context.ast_issues
            if issue.rule_id.startswith("GLD-")
        ]

        recommendations = ml_recommender.recommend(sql, gold_detections, top_k=3)
        ml_section = ml_recommender.format_for_prompt(recommendations)

        prompt += "\n" + ml_section

    return prompt
```

### 3. Testing

```bash
# Run tests
cd packages/qt-sql
pytest tests/test_ml_recommender.py -v

# Run demo
python tests/test_ml_recommender.py
```

## Detector Management

### Analysis Results

After running `analyze_detector_effectiveness.py`, review:

```
analysis/detector_effectiveness.json
```

### Archiving Low-Value Detectors

**Criteria for archiving**:
- Win rate < 15% AND avg_speedup < 1.1x
- High frequency but low effectiveness score

**Process**:
1. Move detector to `archived_rules.py`
2. Add comment: "Archived: Low value for DuckDB (win_rate=8%)"
3. Test on PostgreSQL/Oracle before permanent deletion

**Archive location**:
```
packages/qt-sql/qt_sql/analyzers/ast_detector/rules/archived_rules.py
```

### Deleting Useless Detectors

**Criteria for deletion**:
- Never appears in winning queries (10+ occurrences)
- Zero correlation with speedups across ALL databases

**Process**:
1. Test on 2+ databases (DuckDB, PostgreSQL)
2. If still zero value, delete permanently
3. Document deletion reason in git commit

## Performance

### Training Performance

| Step | Runtime | Output Size |
|------|---------|-------------|
| Generate training data | 10s | 10 KB |
| Normalize queries | 5s | 150 KB |
| Vectorize queries | 15s | 50 KB |
| Analyze detectors | 2s | 100 KB |
| Train pattern weights | 1s | 5 KB |
| Build FAISS index | 1s | 40 KB |
| **Total** | **~35s** | **~350 KB** |

### Inference Performance

| Operation | Latency |
|-----------|---------|
| Pattern weight lookup | <1ms |
| FAISS similarity search | <1ms |
| Combined recommendation | <5ms |
| Format for prompt | <1ms |
| **Total overhead** | **<10ms** |

## Metrics & Evaluation

### Cross-Validation

```python
from sklearn.model_selection import KFold

kfold = KFold(n_splits=5, shuffle=True)

for train_idx, test_idx in kfold.split(queries):
    # Train on train_idx
    # Test on test_idx
    # Calculate hit rate
```

### Metrics to Track

1. **Top-1 Hit Rate**: % of queries where top recommendation matches actual winning transform
2. **Top-3 Hit Rate**: % of queries where any of top 3 recommendations match
3. **Precision**: Of recommended transforms, how many succeeded
4. **Recall**: Of successful transforms, how many were recommended
5. **Speedup Correlation**: Correlation between estimated and actual speedup

### Expected Performance

Based on 99 TPC-DS queries:

| Metric | Target |
|--------|--------|
| Top-1 Hit Rate | 60-70% |
| Top-3 Hit Rate | 80-90% |
| Precision | 70-80% |
| Speedup MAE | ±0.3x |

## Future Enhancements

### 1. Active Learning
- Track new benchmark results
- Retrain models weekly/monthly
- Add new gold patterns as discovered

### 2. Multi-Database Support
- Train separate models per database (DuckDB, PostgreSQL, Oracle)
- Database-specific detector effectiveness
- Cross-database pattern transfer learning

### 3. Query Complexity Weighting
- Simple queries: rely more on patterns
- Complex queries: rely more on similarity
- Adaptive weighting based on query features

### 4. Transform Chaining
- Recommend sequences: [early_filter, projection_prune]
- Learn which transforms compose well
- Multi-step optimization paths

### 5. Real-Time Feedback
- Track user-accepted recommendations
- Online learning from production usage
- Personalized recommendations per user/workload

## Troubleshooting

### Models Not Loading

**Symptoms**: `load_recommender()` returns `None`

**Fix**:
```bash
# Rebuild models
bash scripts/run_ml_training.sh
```

### FAISS Import Error

**Symptoms**: `ModuleNotFoundError: No module named 'faiss'`

**Fix**:
```bash
pip install faiss-cpu
```

### Low Recommendation Confidence

**Symptoms**: All recommendations have confidence < 20%

**Possible causes**:
1. No gold patterns detected (need better AST detectors)
2. Query unlike historical queries (expand training set)
3. Models stale (retrain with recent benchmarks)

## References

- FAISS: https://github.com/facebookresearch/faiss
- TPC-DS Benchmark: http://www.tpc.org/tpcds/
- Query Optimization Papers: See `research/papers/`

---

**Last updated**: 2026-02-04
**Maintainer**: QueryTorque ML Team
