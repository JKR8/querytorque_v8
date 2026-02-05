#!/usr/bin/env python3
"""Rebuild FAISS index with ONLY the 7 verified training queries.

This script creates a FAISS index containing only queries with VERIFIED speedups:
- Q1:  decorrelate      (2.92x)
- Q6:  date_cte_isolate (4.00x)
- Q9:  pushdown         (2.11x)
- Q15: or_to_union      (3.17x)
- Q74: union_cte_split  (1.36x)
- Q93: early_filter     (4.00x)
- Q95: materialize_cte  (1.37x)

The resulting index enables pure vector similarity matching - ANY query can be
vectorized and compared against these 7 training examples to get transform
recommendations.

IMPORTANT: Uses NORMALIZED SQL (from normalized_queries.json) for consistent
vectorization. Normalization includes:
- Table/column renaming to generic names (fact_table_N, col_N)
- Literal abstraction (<INT>, <STRING>, etc.)
- Predicate alphabetization (AND/OR conditions sorted)

IMPORTANT: Applies Z-SCORE STANDARDIZATION to vectors before L2 normalization.
This ensures all 90 features contribute equally to similarity matching,
preventing high-variance features (like total_nodes) from dominating.
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

# The 8 verified training queries with their transforms and speedups
VERIFIED_TRAINING_DATA = {
    "q1": {
        "transform": "decorrelate",
        "speedup": 2.92,
        "description": "Decorrelate subquery to CTE with GROUP BY",
    },
    "q6": {
        "transform": "date_cte_isolate",
        "speedup": 4.00,
        "description": "Extract date filtering into separate CTE",
    },
    "q9": {
        "transform": "pushdown",
        "speedup": 2.11,
        "description": "Push predicates into CTEs/subqueries",
    },
    "q14": {
        "transform": "intersect_to_exists",
        "speedup": 1.83,
        "description": "Convert INTERSECT to multiple EXISTS for better join planning",
    },
    "q15": {
        "transform": "or_to_union",
        "speedup": 3.17,
        "description": "Split OR conditions into UNION ALL branches",
    },
    "q74": {
        "transform": "union_cte_split",
        "speedup": 1.36,
        "description": "Split generic UNION CTE into year-specific CTEs",
    },
    "q93": {
        "transform": "early_filter",
        "speedup": 4.00,
        "description": "Filter dimension tables FIRST before fact joins",
    },
    "q95": {
        "transform": "materialize_cte",
        "speedup": 1.37,
        "description": "Extract repeated subquery into CTE",
    },
}

# Normalized queries file
NORMALIZED_QUERIES_FILE = BASE / "research" / "ml_pipeline" / "data" / "normalized_queries.json"

# Feature statistics for z-score normalization
FEATURE_STATS_FILE = BASE / "research" / "ml_pipeline" / "models" / "feature_stats.json"

# Raw query files (fallback)
QUERY_DIR = BASE / "packages" / "qt-sql" / "tests" / "fixtures" / "tpcds"

# Cache for normalized queries
_NORMALIZED_QUERIES = None

# Cache for feature statistics
_FEATURE_STATS = None


def load_feature_stats() -> dict:
    """Load feature statistics for z-score normalization."""
    global _FEATURE_STATS
    if _FEATURE_STATS is None:
        if FEATURE_STATS_FILE.exists():
            with open(FEATURE_STATS_FILE) as f:
                _FEATURE_STATS = json.load(f)
        else:
            print(f"WARNING: Feature stats file not found: {FEATURE_STATS_FILE}")
            print("         Run compute_feature_stats.py first!")
            _FEATURE_STATS = {}
    return _FEATURE_STATS


def apply_zscore_normalization(vectors: np.ndarray) -> np.ndarray:
    """Apply z-score normalization to vectors.

    Z-score: (x - mean) / std

    This ensures all features contribute equally to similarity matching,
    preventing high-variance features from dominating.

    Args:
        vectors: Shape (n, 90) array of raw feature vectors

    Returns:
        Standardized vectors with mean=0 and std=1 for each feature
    """
    stats = load_feature_stats()
    if not stats:
        print("  WARNING: No feature stats available, skipping z-score normalization")
        return vectors

    mean = np.array(stats["mean"], dtype='float32')
    std = np.array(stats["std"], dtype='float32')

    # Apply z-score: (x - mean) / std
    standardized = (vectors - mean) / std

    return standardized


def load_normalized_queries() -> dict:
    """Load normalized queries from JSON file."""
    global _NORMALIZED_QUERIES
    if _NORMALIZED_QUERIES is None:
        if NORMALIZED_QUERIES_FILE.exists():
            with open(NORMALIZED_QUERIES_FILE) as f:
                _NORMALIZED_QUERIES = json.load(f)
        else:
            _NORMALIZED_QUERIES = {}
    return _NORMALIZED_QUERIES


def load_query_sql(query_id: str, use_normalized: bool = True) -> str:
    """Load SQL for a query ID.

    Args:
        query_id: Query ID (e.g., 'q1')
        use_normalized: If True, use normalized SQL from normalized_queries.json
                       If False, use raw SQL from fixtures

    Returns:
        SQL string (normalized or raw)
    """
    if use_normalized:
        normalized = load_normalized_queries()
        if query_id in normalized:
            return normalized[query_id]["normalized_sql"]
        print(f"  WARNING: {query_id} not in normalized_queries.json, using raw SQL")

    # Fallback to raw SQL
    num = int(query_id[1:])
    filename = f"query_{num:02d}.sql"
    query_file = QUERY_DIR / filename

    if not query_file.exists():
        raise FileNotFoundError(f"Query file not found: {query_file}")

    return query_file.read_text()


def rebuild_faiss_index(use_normalized: bool = True):
    """Rebuild FAISS index with only verified training queries.

    Args:
        use_normalized: If True, use normalized SQL for vectorization
    """
    try:
        import faiss
    except ImportError:
        print("ERROR: faiss not installed. Install with: pip install faiss-cpu")
        sys.exit(1)

    # Import vectorizer
    sys.path.insert(0, str(BASE / "scripts"))
    from vectorize_queries import ASTVectorizer

    vectorizer = ASTVectorizer()

    print("=" * 60)
    print("Rebuilding FAISS index with 8 verified training queries")
    print(f"Using: {'NORMALIZED' if use_normalized else 'RAW'} SQL")
    print("=" * 60)

    # Output paths
    output_dir = BASE / "research" / "ml_pipeline" / "models"
    output_dir.mkdir(parents=True, exist_ok=True)

    index_file = output_dir / "similarity_index.faiss"
    metadata_file = output_dir / "similarity_metadata.json"

    # Vectorize each verified query
    vectors = []
    query_metadata = {}

    for idx, (query_id, data) in enumerate(VERIFIED_TRAINING_DATA.items()):
        print(f"\nProcessing {query_id}: {data['transform']} ({data['speedup']}x)")

        # Load SQL (normalized or raw)
        try:
            sql = load_query_sql(query_id, use_normalized=use_normalized)
        except FileNotFoundError as e:
            print(f"  ERROR: {e}")
            continue

        # Vectorize
        vector = vectorizer.vectorize(sql, dialect="duckdb")
        vectors.append(vector)

        # Store metadata
        query_metadata[query_id] = {
            "vector_index": idx,
            "speedup": data["speedup"],
            "winning_transform": data["transform"],
            "description": data["description"],
            "has_win": True,
        }

        print(f"  Vector shape: {vector.shape}")
        print(f"  Non-zero features: {np.count_nonzero(vector)}/{len(vector)}")

    if len(vectors) != 8:
        print(f"\nWARNING: Expected 8 queries, got {len(vectors)}")

    # Stack vectors into matrix
    vectors_array = np.vstack(vectors).astype('float32')

    print(f"\n--- Applying Z-Score Normalization ---")
    print(f"  Before standardization:")
    print(f"    Mean of total_nodes (idx 85): {np.mean(vectors_array[:, 85]):.2f}")
    print(f"    Std of total_nodes (idx 85):  {np.std(vectors_array[:, 85]):.2f}")

    # Apply z-score normalization FIRST
    vectors_array = apply_zscore_normalization(vectors_array)

    print(f"  After standardization:")
    print(f"    Mean of total_nodes (idx 85): {np.mean(vectors_array[:, 85]):.2f}")
    print(f"    Std of total_nodes (idx 85):  {np.std(vectors_array[:, 85]):.2f}")

    # Then apply L2 normalization for cosine similarity
    faiss.normalize_L2(vectors_array)
    print(f"  After L2 normalization: all vectors have unit norm")

    # Create FAISS index
    dimension = vectors_array.shape[1]  # 90
    index = faiss.IndexFlatL2(dimension)
    index.add(vectors_array)

    print(f"\n" + "=" * 60)
    print(f"FAISS Index Statistics")
    print(f"=" * 60)
    print(f"Total vectors: {index.ntotal}")
    print(f"Dimensions: {dimension}")
    print(f"Index type: IndexFlatL2 (cosine similarity via normalization)")

    # Save index
    print(f"\nSaving FAISS index to: {index_file}")
    faiss.write_index(index, str(index_file))

    # Build full metadata
    full_metadata = {
        "query_metadata": query_metadata,
        "index_stats": {
            "total_vectors": 8,
            "dimensions": dimension,
            "index_type": "IndexFlatL2 (cosine similarity via normalization)",
            "normalized_sql": use_normalized,
            "zscore_normalized": True,
            "feature_stats_file": "feature_stats.json",
        }
    }

    # Save metadata
    print(f"Saving metadata to: {metadata_file}")
    with open(metadata_file, 'w') as f:
        json.dump(full_metadata, f, indent=2)

    print(f"\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # Verify by doing a self-search
    print("\nSelf-search test (each query should find itself as #1):")
    for query_id, meta in query_metadata.items():
        idx = meta["vector_index"]
        query_vector = vectors_array[idx:idx+1]
        distances, indices = index.search(query_vector, 3)

        top_matches = []
        query_ids = list(query_metadata.keys())
        for i, (dist, match_idx) in enumerate(zip(distances[0], indices[0])):
            match_id = query_ids[match_idx]
            match_transform = query_metadata[match_id]["winning_transform"]
            top_matches.append(f"{match_id}:{match_transform} (d={dist:.4f})")

        print(f"  {query_id} -> {', '.join(top_matches)}")

    print(f"\n{'=' * 60}")
    print("Done! The FAISS index now contains ONLY the 8 verified queries.")
    print("Any input query will be compared against these 8 training examples.")
    print("=" * 60)


if __name__ == "__main__":
    # Parse command line args
    use_normalized = "--raw" not in sys.argv
    rebuild_faiss_index(use_normalized=use_normalized)
