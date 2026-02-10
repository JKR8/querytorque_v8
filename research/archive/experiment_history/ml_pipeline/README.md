# ML Pipeline for Query Optimization

This directory contains the complete ML pipeline for predicting optimal query transformations.

## Directory Structure

```
ml_pipeline/
├── data/                    # Training data and preprocessed queries
│   ├── ml_training_data.csv      # Training dataset (99 queries)
│   └── normalized_queries.json   # Semantic-stripped SQL queries
├── vectors/                 # Query vector representations
│   ├── query_vectors.npz         # 90-dim feature vectors (compressed)
│   └── query_vectors_metadata.json  # Feature names and statistics
└── models/                  # Trained ML models (future)
    ├── similarity_index.faiss    # FAISS index for nearest-neighbor search
    ├── transform_classifier.pkl  # XGBoost model for transform prediction
    └── speedup_regressor.pkl     # Model for speedup estimation
```

## Data Files

### ml_training_data.csv
Training dataset with 99 TPC-DS queries.

**Columns:**
- `query_id`: Query identifier (q1-q99)
- `speedup`: Actual speedup factor (1.0 = no change)
- `has_win`: Binary indicator for significant speedup (≥1.2x)
- `winning_transform`: Name of successful optimization
- `all_detections`: All AST rules detected (pipe-separated)
- `gold_detections`: Gold rules detected (pipe-separated)
- `detection_count`: Number of rules detected
- `gold_count`: Number of gold rules detected
- `sql_length`: Character count of SQL query
- `has_cte`, `has_union`, `has_subquery`: Structural features

**Statistics:**
- Total queries: 99
- Queries with wins: 12 (12.1%)
- Queries with gold detections: 81 (81.8%)

### normalized_queries.json
Queries with domain-specific semantics removed.

**Transformations applied:**
- Table names: `store_sales` → `fact_table_1`, `date_dim` → `dimension_table_1`
- Column names: `d_year` → `dim_col_1`, `ss_quantity` → `fact_col_2`
- Literals: `2001` → `<INT>`, `'Store'` → `<STRING>`

**Purpose:** Create universal query patterns for similarity matching.

**Statistics:**
- Queries normalized: 99
- Average length reduction: 14.1%
- Unique table patterns: 44

### query_vectors.npz
Compressed numpy array of 90-dimensional feature vectors.

**Features (90 total):**
1. **Node counts (40):** SELECT, JOIN, WHERE, aggregates, etc.
2. **Depth metrics (5):** Max depth, subquery depth, join depth
3. **Cardinality (10):** Number of tables, columns, conditions
4. **Patterns (30):** Binary indicators (has_cte, has_union, etc.)
5. **Complexity (5):** Total nodes, branching factor, complexity scores

**Statistics:**
- Dimensions: 90
- Average non-zero features: 41.5
- Sparsity: 53.88%

## Scripts

Generate the pipeline data:

```bash
# From repository root:
source .venv/bin/activate

# Step 1: Generate training CSV (99 queries with labels)
python3 scripts/generate_ml_training_data.py

# Step 2: Normalize queries (strip semantics)
python3 scripts/normalize_sql.py

# Step 3: Vectorize queries (create feature vectors)
python3 scripts/vectorize_queries.py
```

## Next Steps

1. **Build similarity index:** Use FAISS to enable fast nearest-neighbor search
2. **Train classifier:** XGBoost model to predict winning transforms
3. **Create API:** REST endpoint for optimization recommendations

## Winning Transforms

From benchmark analysis, these transforms have proven speedups:

| Transform | Count | Max Speedup | Queries |
|-----------|-------|-------------|---------|
| early_filter | 4 | 2.71x | Q93, Q90, Q80, Q27 |
| decorrelate | 1 | 2.81x | Q1 |
| or_to_union | 1 | 2.67x | Q15 |
| union_cte_split | 1 | 1.42x | Q74 |
| subquery_materialize | 1 | 1.24x | Q73 |
| projection_prune | 1 | 1.21x | Q78 |

---

*Generated from TPC-DS SF100 benchmark results*
*Last updated: 2026-02-04*
