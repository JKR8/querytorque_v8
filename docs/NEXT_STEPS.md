# Next Steps: ML System & Detector Cleanup

## ✅ What's Complete

### 1. ML System (Fully Implemented)

**Components**:
- ✅ Detector effectiveness analyzer
- ✅ Pattern weight matrix trainer
- ✅ FAISS similarity index builder
- ✅ Hybrid ML recommender
- ✅ Test suite with demo
- ✅ Complete documentation

**Files**:
```
scripts/
├── analyze_detector_effectiveness.py  ✅
├── train_pattern_weights.py          ✅
├── train_faiss_index.py              ✅
└── run_ml_training.sh                ✅

packages/qt-sql/qt_sql/optimization/
└── ml_recommender.py                 ✅

research/ml_pipeline/
├── ML_SYSTEM.md                      ✅
├── DETECTOR_ANALYSIS_RESULTS.md      ✅
└── models/
    ├── pattern_weights.json          ✅
    ├── similarity_index.faiss        ✅
    └── similarity_metadata.json      ✅
```

**Status**: Ready for integration into optimizer

### 2. Analysis Complete

**Key findings**:
- ✅ 29 detectors identified for archival
- ✅ 22 detectors never in winning queries
- ✅ GLD-003 over-detects (73% false positives)
- ✅ Top 6 effective detectors identified
- ✅ Pattern combinations mapped to transforms

## 🎯 Immediate Actions (Do Today)

### Action 1: Archive Low-Value Detectors

**Create archived rules file**:

```bash
# Create file
touch packages/qt-sql/qt_sql/analyzers/ast_detector/rules/archived_rules.py
```

**Move these 29 detectors** (with comments explaining why):

```python
"""Archived AST detection rules.

These rules showed low effectiveness on DuckDB benchmarks but may be valuable
for other databases. Archive rather than delete to enable cross-database testing.

Archive criteria:
- Win rate < 15% AND avg_speedup < 1.1x on DuckDB
- High frequency (> 50% queries) but low correlation with wins

To restore a rule: move back to appropriate category file.
"""

# Example archived rule:
class AggregateInSubquery(ASTRule):
    """QT-AGG-002: Aggregate function in subquery.

    ARCHIVED: 2026-02-04
    Reason: Low value for DuckDB (win_rate=8.4%, avg_speedup=1.06x)
    Occurrences: 83/99 queries (too generic)
    Next: Test on PostgreSQL before permanent deletion
    """
    rule_id = "QT-AGG-002"
    # ... rest of rule ...
```

**Detectors to archive**:
- QT-AGG-002 (83 occurrences, 8.4% win rate)
- SQL-AGG-006 (21 occurrences, 9.5% win rate)
- SQL-JOIN-007 (17 occurrences, 11.8% win rate)
- SQL-DUCK-002 (44 occurrences, 6.8% win rate)
- QT-CTE-002 (17 occurrences, 5.9% win rate)
- ... (full list in DETECTOR_ANALYSIS_RESULTS.md)

### Action 2: Refine GLD-003 (Early Filter)

**Problem**: Detects in 73/99 queries but only 6 wins (8.2% win rate)

**Fix**: Add selectivity check

```python
# In packages/qt-sql/qt_sql/analyzers/ast_detector/rules/gold_rules.py

class EarlyFilterPushdownGold(ASTRule):
    """GLD-003: Early Filter Pushdown (Dimension Before Fact Join)."""

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # ... existing detection logic ...

        # NEW: Check filter selectivity
        if not self._is_highly_selective_filter(where, dim_tables):
            return  # Skip if filter isn't selective enough

        # Continue with existing reporting...

    def _is_highly_selective_filter(self, where: exp.Where,
                                    dim_tables: list) -> bool:
        """Check if dimension filter is highly selective.

        Heuristics:
        - Equality filters (d_year = 2001) are likely selective
        - IN clauses with < 5 values are selective
        - LIKE with leading constants is selective
        - Skip filters on non-key columns
        """
        for eq in where.find_all(exp.EQ):
            # Check if filter is on dimension table
            for col in eq.find_all(exp.Column):
                col_name = str(col.this).lower()

                # Key indicators of selectivity:
                # - Year/quarter/month filters
                # - Category/type filters
                # - Avoid aggregate/measure columns
                if any(kw in col_name for kw in
                      ['year', 'quarter', 'month', 'type', 'category', 'status']):
                    return True

        return False
```

**Expected result**: Reduce false positives from 73 → ~15 queries

### Action 3: Integrate ML Recommender

**Edit**: `packages/qt-sql/qt_sql/optimization/adaptive_rewriter_v5.py`

```python
from qt_sql.optimization.ml_recommender import load_recommender

# Load once at module level (cached)
_ML_RECOMMENDER = None

def get_ml_recommender():
    global _ML_RECOMMENDER
    if _ML_RECOMMENDER is None:
        _ML_RECOMMENDER = load_recommender()
    return _ML_RECOMMENDER


class AdaptiveRewriterV5:
    def build_prompt(self, sql: str, context: OptimizationContext) -> str:
        """Build optimization prompt with ML recommendations."""

        # ... existing prompt sections ...

        # Add ML recommendations
        recommender = get_ml_recommender()
        if recommender:
            # Extract gold detections
            gold_detections = [
                issue.rule_id
                for issue in context.ast_issues
                if issue.rule_id.startswith("GLD-")
            ]

            if gold_detections:
                # Get recommendations
                recs = recommender.recommend(sql, gold_detections, top_k=3)

                # Format for prompt
                ml_section = recommender.format_for_prompt(recs, max_similar=2)

                # Insert before transform library
                prompt = prompt.replace(
                    "## Transform Library",
                    ml_section + "\n\n## Transform Library"
                )

        return prompt
```

## 📋 Short-term (This Week)

### 1. Test Archived Detectors on PostgreSQL

```bash
# Setup PostgreSQL TPC-DS (if not done)
# Run benchmark with archived detectors enabled
# Compare effectiveness scores

python scripts/benchmark_postgres.py --include-archived

# If still low value → permanent deletion
```

### 2. Expand Training Data

**Current**: 12 winning queries (12.1%)
**Target**: 50+ winning queries

**Actions**:
- Run benchmarks on custom workloads
- Test more aggressive optimizations
- Add queries with known anti-patterns

```bash
# Run on custom queries
python scripts/benchmark_custom_workload.py \
  --queries /path/to/custom/queries \
  --output research/experiments/custom_benchmark_$(date +%Y%m%d)

# Retrain models with new data
bash scripts/run_ml_training.sh
```

### 3. A/B Test ML Recommendations

**Setup**:
- Run 20 queries with ML recommendations
- Run 20 queries without ML recommendations
- Compare success rates

```bash
# With ML
python scripts/benchmark_with_ml.py --queries test_set.txt

# Without ML (baseline)
python scripts/benchmark_baseline.py --queries test_set.txt

# Compare
python scripts/compare_results.py baseline.json with_ml.json
```

**Metrics to track**:
- Top-1 hit rate (ML recommendation = actual best)
- Top-3 hit rate (ML top 3 includes best)
- Speedup prediction error (estimated vs. actual)
- User acceptance rate

### 4. Create Detector Test Suite

```python
# packages/qt-sql/tests/test_detector_accuracy.py

def test_gld003_early_filter_detection():
    """Test GLD-003 detects early filter opportunities."""

    # Should detect
    sql_should_detect = """
        SELECT SUM(ss_sales_price)
        FROM store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE d_year = 2001  -- Highly selective
    """

    issues = detect_antipatterns(sql_should_detect)
    assert any(i.rule_id == "GLD-003" for i in issues)

    # Should NOT detect (not selective)
    sql_should_not_detect = """
        SELECT SUM(ss_sales_price)
        FROM store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE d_date IS NOT NULL  -- Not selective
    """

    issues = detect_antipatterns(sql_should_not_detect)
    assert not any(i.rule_id == "GLD-003" for i in issues)
```

## 🚀 Long-term (Next Month)

### 1. Multi-Database Support

Train separate models for each database:

```bash
# DuckDB (done)
bash scripts/run_ml_training.sh --database duckdb

# PostgreSQL
bash scripts/run_ml_training.sh --database postgres

# Oracle (future)
bash scripts/run_ml_training.sh --database oracle
```

**Database-specific models**:
```
research/ml_pipeline/models/
├── duckdb/
│   ├── pattern_weights.json
│   └── similarity_index.faiss
├── postgres/
│   ├── pattern_weights.json
│   └── similarity_index.faiss
└── oracle/
    ├── pattern_weights.json
    └── similarity_index.faiss
```

### 2. Active Learning Loop

Continuously improve with production data:

```python
# Track recommendations and outcomes
class RecommendationTracker:
    def log_recommendation(self, query_id, recommendations, user_choice):
        """Log what was recommended and what user chose."""

    def log_outcome(self, query_id, chosen_transform, actual_speedup):
        """Log actual speedup achieved."""

    def retrain_if_needed(self):
        """Retrain models weekly with new data."""
```

### 3. Transform Chaining

Recommend sequences of transforms:

```python
# Instead of single transform
recommendation = "early_filter"

# Recommend chain
recommendation = [
    "early_filter",      # Step 1: Filter dimension
    "projection_prune",  # Step 2: Remove unused columns
    "subquery_materialize"  # Step 3: Materialize result
]

# Expected: 1.5x × 1.2x × 1.1x = 1.98x total speedup
```

## 📊 Success Metrics

### Phase 1: Integration (This Week)

- ✅ ML recommender integrated into optimizer
- ✅ No performance regression (<10ms overhead)
- Target: Top-3 hit rate > 70%

### Phase 2: Validation (Next Week)

- ✅ A/B test shows improvement vs. baseline
- ✅ User acceptance rate > 50%
- ✅ Speedup predictions within ±30%

### Phase 3: Production (Next Month)

- ✅ Active learning loop deployed
- ✅ Weekly model retraining automated
- ✅ Multi-database support tested

## 🐛 Known Issues & Limitations

### Issue 1: Small Training Set

**Problem**: Only 12 winning queries (12.1%)
**Impact**: Limited pattern coverage, high variance
**Fix**: Expand to 50+ wins with custom workloads

### Issue 2: GLD-003 Over-Detection

**Problem**: 73% false positive rate
**Impact**: Dilutes pattern confidence scores
**Fix**: Add selectivity check (in progress)

### Issue 3: FAISS Alone Not Predictive

**Problem**: Structural similarity ≠ optimization opportunity
**Impact**: Similarity-only recommendations are 40-50% accurate
**Fix**: Always combine with pattern detection (already done)

### Issue 4: Limited Transform Library

**Problem**: Only 4-5 transforms implemented
**Impact**: Can't capitalize on all detected patterns
**Fix**: Implement remaining gold transforms

## 📚 Documentation

All documentation complete:

- ✅ `ML_IMPLEMENTATION_SUMMARY.md` - Overview
- ✅ `research/ml_pipeline/ML_SYSTEM.md` - Complete technical docs
- ✅ `research/ml_pipeline/DETECTOR_ANALYSIS_RESULTS.md` - Analysis findings
- ✅ This file (`NEXT_STEPS.md`) - Action plan

## 🎓 Usage Examples

### For Developers: Add New Transform

```python
# 1. Implement transform in adaptive_rewriter_v5.py
def apply_projection_prune(sql: str) -> str:
    # ... implementation ...

# 2. Run benchmark with transform
python scripts/benchmark_new_transform.py --transform projection_prune

# 3. Retrain ML models
bash scripts/run_ml_training.sh

# 4. New transform automatically available in recommendations
```

### For Users: Query Optimization

```python
from qt_sql.optimization import optimize_sql

# Optimize with ML recommendations
result = optimize_sql(
    sql=query_text,
    database="duckdb",
    use_ml=True  # Enable ML recommendations
)

print(result.recommendations)
# [
#   {"transform": "early_filter", "confidence": 0.82, "speedup": 1.68},
#   {"transform": "decorrelate", "confidence": 0.76, "speedup": 2.92},
# ]
```

### For Researchers: Analyze New Detectors

```python
# Add new detector
class MyNewDetector(ASTRule):
    rule_id = "GLD-008"
    # ... implementation ...

# Run analysis
python scripts/analyze_detector_effectiveness.py

# Check effectiveness score
cat research/ml_pipeline/analysis/detector_effectiveness.json | grep GLD-008

# If effective → keep
# If not → archive
```

---

## Summary

**Status**: ✅ ML system complete and ready
**Next**: Archive detectors, refine GLD-003, integrate with optimizer
**Timeline**: Archival today, integration this week, validation next week

**Quick Start**:
```bash
# 1. Review detectors to archive
cat research/ml_pipeline/DETECTOR_ANALYSIS_RESULTS.md

# 2. Create archived_rules.py (see Action 1 above)

# 3. Integrate ML recommender (see Action 3 above)

# 4. Test
python packages/qt-sql/tests/test_ml_recommender.py
```

**Questions?** See `research/ml_pipeline/ML_SYSTEM.md` for complete docs.
