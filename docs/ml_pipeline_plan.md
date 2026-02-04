# ML Pipeline for Query Optimization Pattern Matching

**Goal:** Train ML model to predict which optimizations will succeed on new queries by matching to similar historical successes.

---

## Architecture Overview

```
┌─────────────────┐
│  Original SQL   │
└────────┬────────┘
         │
         ├─────────────────────────────────────┐
         │                                     │
         v                                     v
┌────────────────────┐              ┌─────────────────┐
│ AST Detection      │              │ Normalize SQL   │
│ (Rules Detected)   │              │ (Strip Semantics)│
└────────┬───────────┘              └────────┬────────┘
         │                                    │
         │                                    v
         │                          ┌─────────────────┐
         │                          │ Vectorize Query │
         │                          │ (Embeddings)    │
         │                          └────────┬────────┘
         │                                    │
         └─────────────┬──────────────────────┘
                       │
                       v
              ┌────────────────┐
              │  ML Training   │
              │  Data (CSV)    │
              └────────┬───────┘
                       │
                       v
              ┌────────────────┐
              │   ML Model     │
              │  (Classifier)  │
              └────────┬───────┘
                       │
                       v
              ┌────────────────┐
              │ Predict Best   │
              │ Transform      │
              └────────────────┘
```

---

## Phase 1: Training Data Generation ✅

**Script:** `scripts/generate_ml_training_data.py`

**CSV Columns:**
```csv
query_id,speedup,has_win,winning_transform,all_detections,gold_detections,detection_count,gold_count,sql_length,has_cte,has_union,has_subquery
q1,2.810,1,decorrelate,GLD-001|SQL-SUB-003|QT-OPT-007,GLD-001,12,1,2500,1,0,1
q15,2.670,1,or_to_union,GLD-002|SQL-WHERE-001,GLD-002,8,1,1800,0,1,0
...
```

**Features extracted:**
- Query ID
- Speedup factor (numeric)
- Has significant win (binary)
- Winning transform (categorical)
- All detected rules (multi-label)
- Gold rules detected (multi-label)
- Structural features (CTE, UNION, subquery presence)

**Status:** Script created, ready to run

---

## Phase 2: SQL Semantic Normalization

**Goal:** Strip domain-specific semantics to create universal query patterns.

### Normalization Rules

**Table Names:**
```sql
-- BEFORE
FROM store_sales ss
JOIN date_dim d
JOIN customer c

-- AFTER
FROM fact_table_1 ft1
JOIN dimension_table_1 dt1
JOIN dimension_table_2 dt2
```

**Column Names:**
```sql
-- BEFORE
SELECT ss_sales_price, ss_quantity, d_year, c_customer_id

-- AFTER
SELECT fact_col_1, fact_col_2, dim1_col_1, dim2_col_1
```

**Literal Values:**
```sql
-- BEFORE
WHERE d_year = 2001 AND ss_quantity BETWEEN 10 AND 20

-- AFTER
WHERE dim1_col_1 = <INT> AND fact_col_2 BETWEEN <INT> AND <INT>
```

**Table Type Inference:**
- Fact tables: Large volume (sales, orders, returns)
  - Heuristics: Contains "sales", "orders", "returns", OR has many foreign keys
- Dimension tables: Reference data (date, customer, item)
  - Heuristics: Contains "dim", "date", "customer", "item", OR few foreign keys

### Implementation Plan

**Script:** `scripts/normalize_sql.py`

```python
class SQLNormalizer:
    def normalize(self, sql: str) -> tuple[str, dict]:
        """
        Returns:
            - Normalized SQL string
            - Mapping dict (original -> normalized names)
        """

    def _classify_tables(self, ast) -> dict[str, str]:
        """Classify as fact_table_N or dimension_table_N"""

    def _anonymize_columns(self, ast, table_types) -> dict:
        """Replace with fact_col_N or dim_col_N"""

    def _abstract_literals(self, ast) -> str:
        """Replace with <INT>, <STRING>, <DATE>"""
```

**Output:**
- `normalized_queries.json`: Mapping of query_id → normalized SQL
- Preserves structural patterns while removing domain specifics

---

## Phase 3: Query Vectorization

**Goal:** Convert normalized SQL to fixed-size vectors for similarity matching.

### Approach A: AST-Based Structural Features (Fast, Interpretable)

**Features to extract:**
1. **Node type counts** (40 features)
   - SELECT, JOIN, WHERE, GROUP BY, UNION, CTE, etc.

2. **Depth metrics** (10 features)
   - Max nesting depth
   - Subquery depth
   - JOIN chain length

3. **Cardinality features** (20 features)
   - Number of tables
   - Number of columns in SELECT
   - Number of WHERE conditions
   - Number of JOINs

4. **Pattern indicators** (30 features)
   - Has UNION (ALL)?
   - Has CTE?
   - Has correlated subquery?
   - Has window functions?
   - Has date filters?
   - Has aggregation?

**Total:** ~100-dimensional feature vector

**Script:** `scripts/vectorize_queries_ast.py`

```python
class ASTVectorizer:
    def vectorize(self, normalized_sql: str) -> np.ndarray:
        """Convert SQL to 100-dim feature vector"""

    def _extract_node_counts(self, ast) -> np.ndarray:
        """Count each node type"""

    def _extract_structural_features(self, ast) -> np.ndarray:
        """Depth, cardinality, complexity metrics"""

    def _extract_pattern_features(self, ast) -> np.ndarray:
        """Binary indicators for known patterns"""
```

### Approach B: Embedding-Based (Semantic, Better Generalization)

**Use pre-trained code embedding model:**
- CodeBERT, GraphCodeBERT, or SQL-specific transformer
- Input: Normalized SQL text
- Output: 768-dim embedding vector

**Advantages:**
- Captures semantic similarity
- Better generalization to unseen queries
- Learns from SQL syntax patterns

**Disadvantages:**
- Requires GPU for inference
- Less interpretable
- Larger model footprint

**Script:** `scripts/vectorize_queries_embed.py`

```python
from transformers import AutoTokenizer, AutoModel

class SQLEmbedder:
    def __init__(self, model_name="microsoft/codebert-base"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)

    def embed(self, normalized_sql: str) -> np.ndarray:
        """Convert to 768-dim embedding"""
        inputs = self.tokenizer(normalized_sql, return_tensors="pt")
        outputs = self.model(**inputs)
        return outputs.last_hidden_state[:, 0, :].detach().numpy()
```

### Hybrid Approach (Recommended)

Concatenate both:
- AST features (100-dim) - Structural patterns
- Embeddings (768-dim) - Semantic similarity
- **Total: 868-dim vector**

Best of both worlds: Interpretable structure + semantic learning

---

## Phase 4: Similarity Matching

**Goal:** Find historical queries similar to new query.

### Vector Similarity Search

**Index building:**
```python
import faiss

# Build FAISS index from training vectors
dimension = 868  # or 100 for AST-only
index = faiss.IndexFlatL2(dimension)
index.add(training_vectors)  # All 99 TPC-DS queries
```

**Query matching:**
```python
def find_similar_queries(new_query_vector, k=5):
    """Find k most similar historical queries"""
    distances, indices = index.search(new_query_vector, k)
    return [
        {
            "query_id": query_ids[idx],
            "distance": dist,
            "speedup": speedups[idx],
            "winning_transform": transforms[idx]
        }
        for dist, idx in zip(distances[0], indices[0])
    ]
```

**Recommendation strategy:**
1. Find top-K similar queries
2. Filter to only queries with speedup ≥ 1.2x
3. Recommend most common winning transform
4. Weight by similarity (closer queries = higher weight)

### Distance Metrics

**L2 Distance (Euclidean):**
- Good for embeddings
- Sensitive to magnitude

**Cosine Similarity:**
- Better for sparse features
- Angle-based, ignores magnitude

**Combined metric:**
```python
score = 0.7 * cosine_sim + 0.3 * (1 - normalized_l2)
```

---

## Phase 5: ML Model Training

### Model Architecture Options

#### Option A: Multi-Output Classifier (Simple, Fast)

**Input:** Query vector (868-dim)

**Outputs:**
1. Binary classifier: "Will optimization help?" (yes/no)
2. Multi-class classifier: "Which transform?" (11 classes)
3. Regression: "Expected speedup" (continuous)

**Architecture:**
```python
Input (868)
  ↓
Dense(512, ReLU)
  ↓
Dropout(0.3)
  ↓
Dense(256, ReLU)
  ↓
Dropout(0.3)
  ↓
┌─────────┬──────────┬──────────┐
│         │          │          │
Dense(2)  Dense(11)  Dense(1)
Sigmoid   Softmax    Linear
│         │          │
Will      Which      Expected
Help?     Transform? Speedup
```

**Training:**
```python
model.compile(
    optimizer='adam',
    loss={
        'will_help': 'binary_crossentropy',
        'transform': 'categorical_crossentropy',
        'speedup': 'mse'
    },
    loss_weights={'will_help': 1.0, 'transform': 2.0, 'speedup': 1.0}
)
```

#### Option B: Gradient Boosting (Better for Small Data)

**Models to try:**
- XGBoost
- LightGBM
- CatBoost

**Advantages:**
- Works well with <100 training examples
- Handles missing features
- Feature importance analysis
- Less prone to overfitting

**Pipeline:**
```python
from xgboost import XGBClassifier, XGBRegressor

# Transform classifier
transform_model = XGBClassifier(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1
)

# Speedup predictor
speedup_model = XGBRegressor(
    n_estimators=100,
    max_depth=4,
    learning_rate=0.1
)
```

#### Option C: Ensemble (Best Accuracy)

Combine multiple approaches:
1. **Similarity-based recommendation** (60% weight)
   - Find top-5 similar queries
   - Vote on best transform

2. **Rule-based heuristics** (20% weight)
   - If GLD-003 detected → recommend early_filter
   - If GLD-006 detected → recommend union_cte_split

3. **ML classifier** (20% weight)
   - XGBoost on query vectors

**Final prediction:**
```python
prediction = (
    0.6 * similarity_vote +
    0.2 * rule_heuristic +
    0.2 * ml_classifier
)
```

---

## Phase 6: Evaluation & Metrics

### Train/Test Split Strategy

**Challenge:** Only 99 TPC-DS queries, 9 with proven wins

**Approach: Leave-One-Out Cross-Validation**
```
For each query Q in winning_queries:
    Train on: 98 other queries
    Test on: Q
    Measure: Did we recommend the winning transform?
```

**Metrics:**
- **Precision@1:** Did we recommend the winning transform as #1?
- **Recall@3:** Is winning transform in top-3 recommendations?
- **Speedup correlation:** Does predicted speedup correlate with actual?
- **False positive rate:** How often do we recommend transforms that fail?

### Baseline Comparisons

1. **Random guess:** 1/11 accuracy (~9%)
2. **Most common transform:** Always recommend "early_filter" (~44% of wins)
3. **Rule-based only:** Only use gold detections
4. **Our ML model:** Should beat all baselines

---

## Implementation Timeline

### Week 1: Data Pipeline
- ✅ Generate training CSV
- [ ] Build SQL normalizer
- [ ] Test normalization on 10 queries

### Week 2: Vectorization
- [ ] Implement AST vectorizer
- [ ] (Optional) Setup embedding model
- [ ] Generate vectors for all 99 queries

### Week 3: Similarity Matching
- [ ] Build FAISS index
- [ ] Implement similarity search
- [ ] Test retrieval accuracy

### Week 4: ML Training
- [ ] Train XGBoost classifier
- [ ] Train speedup regressor
- [ ] Implement ensemble model

### Week 5: Evaluation
- [ ] Run cross-validation
- [ ] Measure metrics
- [ ] Compare to baselines

---

## Expected Outcomes

**Success Criteria:**
- Precision@1 > 60% (vs 9% random)
- Recall@3 > 85%
- Speedup prediction MAE < 0.5x

**Deliverables:**
1. Training dataset (CSV)
2. Normalized queries (JSON)
3. Query vectors (numpy array)
4. Trained models (pickle/h5)
5. Inference API (FastAPI)
6. Evaluation report (markdown)

---

## Future Enhancements

1. **Active Learning**
   - User provides feedback on recommendations
   - Retrain model with new examples

2. **Multi-Database Support**
   - Train separate models for Postgres, Snowflake, etc.
   - Transfer learning between databases

3. **Explain Recommendations**
   - Show which features influenced decision
   - Display similar historical queries

4. **Online Learning**
   - Continuously improve from user feedback
   - A/B test different recommendation strategies

---

## Technology Stack

**Core:**
- Python 3.10+
- sqlglot (SQL parsing)
- scikit-learn (ML basics)
- xgboost/lightgbm (models)

**Vectorization:**
- numpy, pandas
- faiss-cpu (similarity search)
- transformers (optional, for embeddings)

**ML Framework:**
- TensorFlow/PyTorch (optional, for neural nets)
- optuna (hyperparameter tuning)

**Serving:**
- FastAPI (REST API)
- Redis (caching)
- Docker (deployment)

---

## Next Steps

1. Run `scripts/generate_ml_training_data.py` to create CSV ✓
2. Review data quality and feature distribution
3. Implement SQL normalizer (Phase 2)
4. Choose vectorization approach (Phase 3)
5. Build similarity search (Phase 4)
6. Train baseline models (Phase 5)

---

*Plan created: 2026-02-04*
*Status: Phase 1 complete, ready for Phase 2*
