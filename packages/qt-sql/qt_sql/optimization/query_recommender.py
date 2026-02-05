"""Query-specific transform recommender using LIVE FAISS vector similarity.

Uses AST vectorization + FAISS similarity search to find similar verified queries
and recommend their winning transforms. NO query ID matching - pure vector similarity.

The 7 verified transforms with proven TPC-DS speedups:
- decorrelate:      2.92x (Q1)
- date_cte_isolate: 4.00x (Q6)
- pushdown:         2.11x (Q9)
- or_to_union:      3.17x (Q15)
- union_cte_split:  1.36x (Q74)
- early_filter:     4.00x (Q93)
- materialize_cte:  1.37x (Q95)
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Lazy imports for optional dependencies
faiss = None
ASTVectorizer = None


# The 8 verified transforms with proven TPC-DS speedups
VERIFIED_TRANSFORMS = frozenset([
    "decorrelate",         # 2.92x Q1
    "or_to_union",         # 3.17x Q15
    "early_filter",        # 4.00x Q93
    "pushdown",            # 2.11x Q9
    "date_cte_isolate",    # 4.00x Q6
    "intersect_to_exists", # 1.83x Q14
    "union_cte_split",     # 1.36x Q74
    "materialize_cte",     # 1.37x Q95
])


@dataclass
class SimilarQuery:
    """A similar verified query found by FAISS search."""
    query_id: str
    distance: float
    similarity_score: float  # 0-1, higher is more similar
    winning_transform: str
    speedup: float


@dataclass
class QueryRecommendation:
    """A single recommendation from ML system."""
    transform: str
    confidence_pct: float
    estimated_speedup: float
    similar_queries: Optional[List[str]] = None


class LiveFAISSRecommender:
    """LIVE FAISS similarity recommender using 7 verified training queries."""

    def __init__(self, models_dir: Optional[Path] = None):
        """Initialize recommender with FAISS index.

        Args:
            models_dir: Path to models directory. If None, uses default location.
        """
        if models_dir is None:
            # Default to research/ml_pipeline/models
            base = Path(__file__).parent.parent.parent.parent.parent
            models_dir = base / "research" / "ml_pipeline" / "models"

        self.models_dir = models_dir
        self.faiss_index = None
        self.faiss_metadata = None
        self.vectorizer = None
        self.feature_stats = None  # Z-score normalization statistics
        self._initialized = False

        # Lazy load on first use
        self._load_faiss_index()

    def _load_faiss_index(self):
        """Load FAISS similarity index and vectorizer."""
        global faiss, ASTVectorizer

        index_file = self.models_dir / "similarity_index.faiss"
        metadata_file = self.models_dir / "similarity_metadata.json"

        if not index_file.exists() or not metadata_file.exists():
            logger.debug(f"FAISS index not found at {index_file}")
            return

        try:
            # Lazy import faiss
            if faiss is None:
                import faiss as faiss_module
                faiss = faiss_module

            # Load index
            self.faiss_index = faiss.read_index(str(index_file))

            # Load metadata
            with open(metadata_file) as f:
                self.faiss_metadata = json.load(f)

            # Load vectorizer from ado.faiss_builder
            if ASTVectorizer is None:
                import sys
                ado_dir = Path(__file__).parent.parent.parent / "ado"
                if str(ado_dir.parent) not in sys.path:
                    sys.path.insert(0, str(ado_dir.parent))
                from ado.faiss_builder import ASTVectorizer as ASTVectorizerClass
                ASTVectorizer = ASTVectorizerClass

            self.vectorizer = ASTVectorizer()

            # Load feature statistics for z-score normalization
            self._load_feature_stats()

            self._initialized = True

            num_vectors = self.faiss_metadata.get("index_stats", {}).get("total_vectors", "?")
            zscore_status = "with z-score" if self.feature_stats else "without z-score"
            logger.info(f"Loaded FAISS index with {num_vectors} verified training vectors ({zscore_status})")

        except ImportError as e:
            logger.debug(f"FAISS not available: {e}")
            self.faiss_index = None
        except Exception as e:
            logger.warning(f"Failed to load FAISS index: {e}")
            self.faiss_index = None

    def _load_feature_stats(self):
        """Load feature statistics for z-score normalization.

        The FAISS index was built with z-score normalized vectors.
        We must apply the same normalization to query vectors.
        """
        # Check if index was built with z-score normalization
        index_stats = self.faiss_metadata.get("index_stats", {})
        if not index_stats.get("zscore_normalized", False):
            logger.debug("FAISS index was not built with z-score normalization")
            return

        stats_file = self.models_dir / "feature_stats.json"
        if not stats_file.exists():
            logger.warning(f"Feature stats file not found: {stats_file}")
            return

        try:
            with open(stats_file) as f:
                self.feature_stats = json.load(f)
            logger.debug(f"Loaded feature stats for {self.feature_stats.get('num_features', '?')} features")
        except Exception as e:
            logger.warning(f"Failed to load feature stats: {e}")
            self.feature_stats = None

    def _apply_zscore_normalization(self, vector):
        """Apply z-score normalization to a query vector.

        Z-score: (x - mean) / std

        Args:
            vector: Raw feature vector (90,) or (1, 90)

        Returns:
            Standardized vector with same shape
        """
        if self.feature_stats is None:
            return vector

        import numpy as np

        mean = np.array(self.feature_stats["mean"], dtype='float32')
        std = np.array(self.feature_stats["std"], dtype='float32')

        # Apply z-score: (x - mean) / std
        original_shape = vector.shape
        if vector.ndim == 1:
            vector = vector.reshape(1, -1)

        standardized = (vector - mean) / std

        if original_shape != standardized.shape:
            standardized = standardized.reshape(original_shape)

        return standardized

    def _normalize_sql(self, sql: str, dialect: str = "duckdb") -> str:
        """Normalize SQL for consistent vectorization.

        Applies Percona-style fingerprinting:
        - Replace all literals (strings, numbers, dates) with $N placeholders
        - Normalize identifiers to lowercase
        - Normalize whitespace

        This ensures structurally similar queries produce similar vectors
        regardless of specific literal values or identifier casing.
        """
        try:
            import re
            import sqlglot
            from sqlglot import exp
            from sqlglot.optimizer import normalize_identifiers

            # Parse SQL
            ast = sqlglot.parse_one(sql, dialect=dialect)

            # Replace all literals with placeholders
            placeholder_counter = [0]  # Use list for mutable closure

            def replace_literals(node):
                if isinstance(node, exp.Literal):
                    placeholder_counter[0] += 1
                    return exp.Placeholder(this=f"${placeholder_counter[0]}")
                if isinstance(node, exp.Null):
                    return exp.Placeholder(this="$NULL")
                return node

            ast = ast.transform(replace_literals)

            # Normalize identifiers to lowercase
            ast = normalize_identifiers.normalize_identifiers(ast, dialect=dialect)

            # Generate normalized SQL
            normalized = ast.sql(dialect=dialect)

            # Additional whitespace normalization
            normalized = re.sub(r'\s+', ' ', normalized).strip()

            return normalized

        except Exception as e:
            logger.debug(f"SQL normalization failed, using raw SQL: {e}")
            # Fallback: basic whitespace normalization
            import re
            return re.sub(r'\s+', ' ', sql).strip()

    def find_similar_queries(self, sql: str, k: int = 5) -> List[SimilarQuery]:
        """Find similar verified training queries using LIVE FAISS search.

        Args:
            sql: The SQL query text to find similar queries for
            k: Number of similar queries to return

        Returns:
            List of SimilarQuery objects ordered by similarity (most similar first)
        """
        if not self.faiss_index or not self.vectorizer:
            return []

        try:
            import numpy as np

            # Vectorize directly - AST features (node counts, depth, patterns)
            # are structural and don't depend on literal values
            query_vector = self.vectorizer.vectorize(sql, dialect="duckdb")
            query_vector = query_vector.reshape(1, -1).astype('float32')

            # Apply z-score normalization FIRST (same as training data)
            query_vector = self._apply_zscore_normalization(query_vector)

            # Then L2 normalize for cosine similarity
            faiss.normalize_L2(query_vector)

            # Search against the 7 verified training vectors
            distances, indices = self.faiss_index.search(query_vector, k)

            # Build results
            similar_queries = []
            query_ids = list(self.faiss_metadata["query_metadata"].keys())

            for dist, idx in zip(distances[0], indices[0]):
                if idx < 0 or idx >= len(query_ids):
                    continue

                qid = query_ids[idx]
                meta = self.faiss_metadata["query_metadata"][qid]

                # Convert L2 distance to similarity score (0-1)
                # For normalized vectors: similarity = 1 - (distance^2 / 2)
                similarity = max(0.0, 1.0 - (float(dist) ** 2) / 2)

                similar_queries.append(SimilarQuery(
                    query_id=qid,
                    distance=float(dist),
                    similarity_score=similarity,
                    winning_transform=meta.get("winning_transform", ""),
                    speedup=meta.get("speedup", 1.0),
                ))

            return similar_queries

        except Exception as e:
            logger.warning(f"Similarity search failed: {e}")
            return []

    def get_recommendations(self, sql: str, top_n: int = 3) -> List[str]:
        """Get transform recommendations based on vector similarity.

        NO query ID matching - pure vector similarity only.

        Args:
            sql: The SQL query to optimize (ANY query, not just TPC-DS)
            top_n: Number of recommendations to return

        Returns:
            List of transform IDs ordered by similarity to verified training data
        """
        similar = self.find_similar_queries(sql, k=top_n * 2)

        # Extract unique transforms from similar queries, ordered by similarity
        transforms = []
        for sq in similar:
            if sq.winning_transform and sq.winning_transform not in transforms:
                # Only include verified transforms
                if sq.winning_transform in VERIFIED_TRANSFORMS:
                    transforms.append(sq.winning_transform)

        return transforms[:top_n]


# Global singleton for lazy initialization
_RECOMMENDER: Optional[LiveFAISSRecommender] = None


def _get_recommender() -> Optional[LiveFAISSRecommender]:
    """Get or create the global FAISS recommender."""
    global _RECOMMENDER
    if _RECOMMENDER is None:
        _RECOMMENDER = LiveFAISSRecommender()
    return _RECOMMENDER


def get_recommendations_for_sql(sql: str, top_n: int = 3) -> List[str]:
    """Get transform recommendations based on LIVE vector similarity.

    This is the PRIMARY API for ML recommendations. It vectorizes the input SQL
    and performs FAISS similarity search against the 7 verified training queries.

    NO query ID matching - works on ANY SQL query.

    Args:
        sql: The SQL query text to optimize
        top_n: Number of recommendations to return

    Returns:
        List of verified transform IDs ordered by similarity

    Example:
        >>> sql = "SELECT * FROM orders WHERE status IN ('A', 'B', 'C')"
        >>> get_recommendations_for_sql(sql)
        ['or_to_union', 'early_filter', 'pushdown']
    """
    recommender = _get_recommender()
    if recommender is None or not recommender._initialized:
        logger.debug("FAISS recommender not available, returning empty recommendations")
        return []

    return recommender.get_recommendations(sql, top_n=top_n)


def get_similar_queries_for_sql(sql: str, k: int = 5) -> List[SimilarQuery]:
    """Get similar verified queries for any SQL input.

    Args:
        sql: The SQL query text
        k: Number of similar queries to return

    Returns:
        List of SimilarQuery objects with similarity scores and transforms
    """
    recommender = _get_recommender()
    if recommender is None or not recommender._initialized:
        return []

    return recommender.find_similar_queries(sql, k=k)


def get_recommendation_details_for_sql(sql: str, top_n: int = 3) -> List[QueryRecommendation]:
    """Get detailed ML recommendations for any SQL input.

    Returns full recommendation objects with confidence, speedup estimates,
    and evidence (similar queries found).

    Args:
        sql: The SQL query text
        top_n: Number of recommendations to return

    Returns:
        List of QueryRecommendation objects with full details
    """
    recommender = _get_recommender()
    if recommender is None or not recommender._initialized:
        return []

    similar = recommender.find_similar_queries(sql, k=top_n * 2)

    # Aggregate by transform
    transform_data: Dict[str, dict] = {}
    for sq in similar:
        transform = sq.winning_transform
        if not transform or transform not in VERIFIED_TRANSFORMS:
            continue

        if transform not in transform_data:
            transform_data[transform] = {
                "total_similarity": 0.0,
                "max_speedup": 0.0,
                "similar_queries": [],
            }

        data = transform_data[transform]
        data["total_similarity"] += sq.similarity_score
        data["max_speedup"] = max(data["max_speedup"], sq.speedup)
        data["similar_queries"].append(sq.query_id)

    # Build recommendation objects
    recommendations = []
    for transform, data in sorted(
        transform_data.items(),
        key=lambda x: x[1]["total_similarity"],
        reverse=True
    ):
        # Confidence based on similarity score
        confidence = min(100.0, data["total_similarity"] * 100)

        recommendations.append(QueryRecommendation(
            transform=transform,
            confidence_pct=confidence,
            estimated_speedup=data["max_speedup"],
            similar_queries=data["similar_queries"],
        ))

    return recommendations[:top_n]


# =============================================================================
# LEGACY API (for backward compatibility)
# =============================================================================

def get_query_recommendations(query_id: str, top_n: int = 3) -> List[str]:
    """DEPRECATED: Use get_recommendations_for_sql() instead.

    This function exists for backward compatibility. It loads the query SQL
    from fixtures and delegates to get_recommendations_for_sql().

    Args:
        query_id: Query ID (e.g., 'q1', 'q15')
        top_n: Number of recommendations to return

    Returns:
        List of verified transform IDs
    """
    # Try to load query SQL from fixtures
    base = Path(__file__).parent.parent.parent.parent.parent
    query_dir = base / "packages" / "qt-sql" / "tests" / "fixtures" / "tpcds"

    query_id_lower = query_id.lower()
    if query_id_lower.startswith('q'):
        num = int(query_id_lower[1:])
        query_file = query_dir / f"query_{num:02d}.sql"

        if query_file.exists():
            sql = query_file.read_text()
            return get_recommendations_for_sql(sql, top_n=top_n)

    # Fallback: return empty if query not found
    logger.debug(f"Query {query_id} not found in fixtures")
    return []


def get_query_recommendation_details(query_id: str) -> List[QueryRecommendation]:
    """DEPRECATED: Use get_recommendation_details_for_sql() instead.

    Args:
        query_id: Query ID (e.g., 'q1', 'q15')

    Returns:
        List of QueryRecommendation objects with full details
    """
    # Try to load query SQL from fixtures
    base = Path(__file__).parent.parent.parent.parent.parent
    query_dir = base / "packages" / "qt-sql" / "tests" / "fixtures" / "tpcds"

    query_id_lower = query_id.lower()
    if query_id_lower.startswith('q'):
        num = int(query_id_lower[1:])
        query_file = query_dir / f"query_{num:02d}.sql"

        if query_file.exists():
            sql = query_file.read_text()
            return get_recommendation_details_for_sql(sql)

    return []
