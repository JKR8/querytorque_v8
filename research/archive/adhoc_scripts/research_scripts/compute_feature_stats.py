#!/usr/bin/env python3
"""Compute feature statistics (mean, std) across all 99 TPC-DS queries.

These statistics are used for z-score normalization to ensure all features
contribute equally to similarity matching, regardless of their natural scale.

Without standardization, high-variance features like `total_nodes` (variance ~5500)
dominate similarity calculations, while low-variance features are ignored.
"""

import json
import sys
import numpy as np
from pathlib import Path

# Check virtual environment
if not (hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)):
    print("WARNING: Not running in virtual environment!")
    print("Consider running: source .venv/bin/activate")

BASE = Path(__file__).parent.parent

# Query files
QUERY_DIR = BASE / "packages" / "qt-sql" / "tests" / "fixtures" / "tpcds"

# Normalized queries (preferred)
NORMALIZED_QUERIES_FILE = BASE / "research" / "ml_pipeline" / "data" / "normalized_queries.json"


def load_normalized_queries() -> dict:
    """Load normalized queries from JSON file."""
    if NORMALIZED_QUERIES_FILE.exists():
        with open(NORMALIZED_QUERIES_FILE) as f:
            return json.load(f)
    return {}


def load_query_sql(query_id: str, normalized_queries: dict) -> str:
    """Load SQL for a query ID, preferring normalized version."""
    if query_id in normalized_queries:
        return normalized_queries[query_id]["normalized_sql"]

    # Fallback to raw SQL
    num = int(query_id[1:])
    filename = f"query_{num:02d}.sql"
    query_file = QUERY_DIR / filename

    if query_file.exists():
        return query_file.read_text()

    return None


def compute_feature_statistics():
    """Compute mean and std for each of the 90 features across all TPC-DS queries."""
    # Import vectorizer
    sys.path.insert(0, str(BASE / "scripts"))
    from vectorize_queries import ASTVectorizer

    vectorizer = ASTVectorizer()
    normalized_queries = load_normalized_queries()

    print("=" * 60)
    print("Computing Feature Statistics from All TPC-DS Queries")
    print("=" * 60)

    # Collect vectors for all 99 queries
    vectors = []
    query_ids = []

    for i in range(1, 100):
        query_id = f"q{i}"
        sql = load_query_sql(query_id, normalized_queries)

        if sql is None:
            print(f"  Skipping {query_id}: file not found")
            continue

        try:
            vector = vectorizer.vectorize(sql, dialect="duckdb")
            vectors.append(vector)
            query_ids.append(query_id)
        except Exception as e:
            print(f"  Skipping {query_id}: vectorization failed - {e}")

    print(f"\nVectorized {len(vectors)} queries")

    if len(vectors) == 0:
        print("ERROR: No vectors computed!")
        sys.exit(1)

    # Stack into matrix
    vectors_array = np.vstack(vectors).astype('float64')
    print(f"Vector matrix shape: {vectors_array.shape}")

    # Compute statistics for each feature
    means = np.mean(vectors_array, axis=0)
    stds = np.std(vectors_array, axis=0)

    # Handle zero-std features (constant across all queries)
    # Set std to 1 to avoid division by zero, feature will become 0 after centering
    zero_std_mask = stds == 0
    stds[zero_std_mask] = 1.0

    print(f"\nFeature Statistics:")
    print(f"  Features with zero std (constant): {np.sum(zero_std_mask)}")
    print(f"  Features with non-zero std: {np.sum(~zero_std_mask)}")

    # Show top variance features
    variances = stds ** 2
    top_var_indices = np.argsort(variances)[::-1][:10]

    print(f"\nTop 10 highest-variance features:")
    feature_names = _get_feature_names()
    for idx in top_var_indices:
        name = feature_names[idx] if idx < len(feature_names) else f"feature_{idx}"
        print(f"  [{idx:2d}] {name:30s}: variance={variances[idx]:10.2f}, std={stds[idx]:8.2f}, mean={means[idx]:8.2f}")

    # Save statistics
    output_dir = BASE / "research" / "ml_pipeline" / "models"
    output_dir.mkdir(parents=True, exist_ok=True)

    stats_file = output_dir / "feature_stats.json"

    stats = {
        "mean": means.tolist(),
        "std": stds.tolist(),
        "zero_std_indices": np.where(zero_std_mask)[0].tolist(),
        "num_queries": len(vectors),
        "num_features": len(means),
        "description": "Z-score normalization statistics computed from all 99 TPC-DS queries"
    }

    print(f"\nSaving feature statistics to: {stats_file}")
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)

    # Verify by showing standardized variance
    print("\n" + "=" * 60)
    print("Verification: Variance after standardization")
    print("=" * 60)

    standardized = (vectors_array - means) / stds
    std_variances = np.var(standardized, axis=0)

    print(f"  All feature variances should be ~1.0 after standardization")
    print(f"  Mean variance: {np.mean(std_variances):.4f}")
    print(f"  Min variance:  {np.min(std_variances):.4f}")
    print(f"  Max variance:  {np.max(std_variances):.4f}")

    # Show that total_nodes is now normalized
    if 85 < len(feature_names):
        print(f"\n  total_nodes (index 85):")
        print(f"    Before: variance={variances[85]:.2f}")
        print(f"    After:  variance={std_variances[85]:.4f}")

    print("\nDone!")
    return stats


def _get_feature_names() -> list:
    """Get feature names for the 90-dimensional vector."""
    names = []

    # Node counts (40 features, indices 0-39)
    node_types = [
        'Select', 'From', 'Join', 'Where', 'Group', 'Having', 'Order', 'Limit',
        'Union', 'Intersect', 'Except', 'Subquery', 'CTE', 'Window', 'Case',
        'Cast', 'Coalesce', 'Nullif', 'In', 'Between', 'Like', 'Exists',
        'All', 'Any', 'Some', 'And', 'Or', 'Not', 'Eq', 'Neq', 'Gt', 'Gte',
        'Lt', 'Lte', 'Add', 'Sub', 'Mul', 'Div', 'Mod', 'Neg'
    ]
    for nt in node_types:
        names.append(f"node_{nt.lower()}")

    # Depth metrics (5 features, indices 40-44)
    names.extend(['max_depth', 'avg_depth', 'subquery_depth', 'cte_depth', 'join_depth'])

    # Cardinality (10 features, indices 45-54)
    names.extend([
        'num_tables', 'num_columns', 'num_predicates', 'num_aggregates',
        'num_group_cols', 'num_order_cols', 'num_ctes', 'num_subqueries',
        'num_joins', 'num_unions'
    ])

    # Binary patterns (30 features, indices 55-84)
    patterns = [
        'has_subquery', 'has_correlated_subquery', 'has_cte', 'has_recursive_cte',
        'has_window', 'has_union', 'has_intersect', 'has_except', 'has_distinct',
        'has_group_by', 'has_having', 'has_order_by', 'has_limit', 'has_offset',
        'has_inner_join', 'has_left_join', 'has_right_join', 'has_full_join',
        'has_cross_join', 'has_self_join', 'has_case', 'has_cast', 'has_coalesce',
        'has_between', 'has_in_list', 'has_in_subquery', 'has_exists', 'has_like',
        'has_or_predicate', 'has_function_call'
    ]
    for p in patterns:
        names.append(p)

    # Complexity (5 features, indices 85-89)
    names.extend(['total_nodes', 'total_literals', 'total_identifiers', 'ast_width', 'ast_height'])

    return names


if __name__ == "__main__":
    compute_feature_statistics()
