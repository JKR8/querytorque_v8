"""FAISS index builder for ADO PostgreSQL DSB knowledge base.

Builds a FAISS similarity index from gold examples in ado/examples/.
Each example's before_sql (or input_slice) is vectorized and indexed.

Usage:
    python -m ado.faiss_builder          # Build index
    python -m ado.faiss_builder --stats  # Show index stats
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).resolve().parent
EXAMPLES_DIR = BASE_DIR / "examples"
MODELS_DIR = BASE_DIR / "models"
INDEX_FILE = MODELS_DIR / "similarity_index.faiss"
METADATA_FILE = MODELS_DIR / "similarity_metadata.json"
FEATURE_STATS_FILE = MODELS_DIR / "feature_stats.json"


# =============================================================================
# SQL Normalizer (fingerprinting for similarity)
# =============================================================================

class SQLNormalizer:
    """Normalize SQL queries for similarity comparison.

    Applies transformations based on Percona fingerprinting:
    - Replace all literals (strings, numbers, dates) with placeholders
    - Normalize identifiers to lowercase
    - Remove comments
    - Normalize whitespace

    This ensures structurally similar queries produce similar vectors
    regardless of specific literal values or identifier casing.
    """

    def __init__(self):
        self._placeholder_counter = 0

    def normalize(self, sql: str, dialect: str = "postgres") -> str:
        """Normalize SQL query for similarity comparison.

        Args:
            sql: Raw SQL query
            dialect: SQL dialect for parsing

        Returns:
            Normalized SQL with literals replaced by placeholders
        """
        try:
            import sqlglot
            from sqlglot import exp
            from sqlglot.optimizer import normalize_identifiers

            # Parse SQL
            ast = sqlglot.parse_one(sql, dialect=dialect)

            # Reset placeholder counter for each query
            self._placeholder_counter = 0

            # Replace all literals with placeholders
            ast = ast.transform(self._replace_literals)

            # Normalize identifiers to lowercase
            ast = normalize_identifiers.normalize_identifiers(ast, dialect=dialect)

            # Generate normalized SQL
            normalized = ast.sql(dialect=dialect)

            # Additional whitespace normalization
            import re
            normalized = re.sub(r'\s+', ' ', normalized).strip()

            return normalized

        except Exception as e:
            logger.warning(f"SQL normalization failed: {e}")
            # Fallback: basic whitespace normalization
            import re
            return re.sub(r'\s+', ' ', sql).strip()

    def _replace_literals(self, node):
        """Replace literal values with placeholders."""
        from sqlglot import exp

        if isinstance(node, exp.Literal):
            self._placeholder_counter += 1
            # Use $N placeholder style (PostgreSQL compatible)
            return exp.Placeholder(this=f"${self._placeholder_counter}")

        # Also handle NULL as a literal
        if isinstance(node, exp.Null):
            return exp.Placeholder(this="$NULL")

        return node


# =============================================================================
# AST Vectorizer (inline for self-containment)
# =============================================================================

class ASTVectorizer:
    """Convert SQL queries to feature vectors using AST analysis.

    This is a copy of scripts/vectorize_queries.py ASTVectorizer for
    self-containment within the ado/ module.
    """

    NODE_TYPES = [
        'Select', 'From', 'Where', 'Join', 'Group', 'Having', 'Order', 'Limit',
        'With', 'CTE', 'Union', 'Subquery', 'Column', 'Table', 'Alias',
        'EQ', 'GT', 'LT', 'GTE', 'LTE', 'NEQ', 'In', 'Like', 'Between',
        'And', 'Or', 'Not', 'Is', 'IsNull',
        'Sum', 'Count', 'Avg', 'Min', 'Max', 'StdDev',
        'Cast', 'Case', 'Window', 'RowNumber', 'Rank'
    ]

    def __init__(self):
        self.feature_names = self._build_feature_names()

    def _build_feature_names(self) -> List[str]:
        features = []
        # Node type counts (40)
        features.extend([f"node_{nt.lower()}" for nt in self.NODE_TYPES])
        # Depth metrics (5)
        features.extend(["max_depth", "max_subquery_depth", "max_join_depth", "cte_depth", "union_depth"])
        # Cardinality (10)
        features.extend([
            "num_tables", "num_select_cols", "num_where_conditions", "num_joins",
            "num_ctes", "num_subqueries", "num_aggregates", "num_window_functions",
            "num_unions", "num_case_statements"
        ])
        # Pattern indicators (30)
        features.extend([
            "has_cte", "has_union", "has_union_all", "has_subquery", "has_correlated_subquery",
            "has_aggregation", "has_group_by", "has_having", "has_window_function",
            "has_self_join", "has_cross_join", "has_left_join", "has_outer_join",
            "has_distinct", "has_limit", "has_order_by", "has_multiple_tables",
            "has_date_filter", "has_in_clause", "has_exists", "has_not_exists",
            "has_like", "has_between", "has_case_when", "has_cast", "has_null_check",
            "has_complex_predicate", "has_nested_aggregation", "has_multiple_ctes", "has_recursive_cte"
        ])
        # Complexity (5)
        features.extend([
            "total_nodes", "avg_branching_factor", "predicate_complexity",
            "select_complexity", "join_complexity"
        ])
        return features

    @property
    def num_features(self) -> int:
        return len(self.feature_names)

    def vectorize(self, sql: str, dialect: str = "postgres") -> np.ndarray:
        """Convert SQL query to feature vector."""
        try:
            import sqlglot
            from sqlglot import exp
            from collections import Counter

            ast = sqlglot.parse_one(sql, dialect=dialect)
            features = []

            # 1. Node type counts
            node_counts = Counter()
            for node in ast.walk():
                node_type = type(node).__name__
                if node_type in self.NODE_TYPES:
                    node_counts[node_type] += 1
            for nt in self.NODE_TYPES:
                features.append(float(node_counts.get(nt, 0)))

            # 2. Depth metrics
            max_depth = max_subquery_depth = max_join_depth = cte_depth = union_depth = 0

            def traverse(node, depth=0, sq_depth=0, j_depth=0):
                nonlocal max_depth, max_subquery_depth, max_join_depth, cte_depth, union_depth
                max_depth = max(max_depth, depth)
                if isinstance(node, exp.Subquery):
                    sq_depth += 1
                    max_subquery_depth = max(max_subquery_depth, sq_depth)
                if isinstance(node, exp.Join):
                    j_depth += 1
                    max_join_depth = max(max_join_depth, j_depth)
                if isinstance(node, exp.CTE):
                    cte_depth = max(cte_depth, depth)
                if isinstance(node, exp.Union):
                    union_depth = max(union_depth, depth)
                for child in node.iter_expressions():
                    traverse(child, depth + 1, sq_depth, j_depth)

            traverse(ast)
            features.extend([float(max_depth), float(max_subquery_depth), float(max_join_depth),
                           float(cte_depth), float(union_depth)])

            # 3. Cardinality
            num_tables = len(list(ast.find_all(exp.Table)))
            num_select_cols = sum(len(s.expressions) if s.expressions else 0 for s in ast.find_all(exp.Select))
            num_where_conditions = 0
            for where in ast.find_all(exp.Where):
                num_where_conditions += len(list(where.find_all((
                    exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ, exp.In, exp.Like, exp.Between, exp.Is
                ))))
            num_joins = len(list(ast.find_all(exp.Join)))
            num_ctes = len(list(ast.find_all(exp.CTE)))
            num_subqueries = len(list(ast.find_all(exp.Subquery)))
            num_aggregates = len(list(ast.find_all(exp.AggFunc)))
            num_window_functions = len(list(ast.find_all(exp.Window)))
            num_unions = len(list(ast.find_all(exp.Union)))
            num_case_statements = len(list(ast.find_all(exp.Case)))
            features.extend([float(num_tables), float(num_select_cols), float(num_where_conditions),
                           float(num_joins), float(num_ctes), float(num_subqueries),
                           float(num_aggregates), float(num_window_functions),
                           float(num_unions), float(num_case_statements)])

            # 4. Pattern indicators (30 binary features)
            table_names = [str(t.this).lower() for t in ast.find_all(exp.Table) if t.this]
            patterns = [
                1.0 if node_counts.get('With', 0) > 0 else 0.0,  # has_cte
                1.0 if node_counts.get('Union', 0) > 0 else 0.0,  # has_union
                1.0 if any(u.args.get('distinct') is False for u in ast.find_all(exp.Union)) else 0.0,  # has_union_all
                1.0 if node_counts.get('Subquery', 0) > 0 else 0.0,  # has_subquery
                1.0 if any(sq.find(exp.Column) for sq in ast.find_all(exp.Subquery)) else 0.0,  # has_correlated
                1.0 if node_counts.get('Sum', 0) + node_counts.get('Count', 0) > 0 else 0.0,  # has_aggregation
                1.0 if node_counts.get('Group', 0) > 0 else 0.0,  # has_group_by
                1.0 if node_counts.get('Having', 0) > 0 else 0.0,  # has_having
                1.0 if node_counts.get('Window', 0) > 0 else 0.0,  # has_window_function
                1.0 if len(table_names) != len(set(table_names)) else 0.0,  # has_self_join
                1.0 if any('cross' in str(j).lower() for j in ast.find_all(exp.Join)) else 0.0,  # has_cross_join
                1.0 if any('left' in str(j).lower() for j in ast.find_all(exp.Join)) else 0.0,  # has_left_join
                1.0 if any('outer' in str(j).lower() for j in ast.find_all(exp.Join)) else 0.0,  # has_outer_join
                1.0 if ast.find(exp.Distinct) else 0.0,  # has_distinct
                1.0 if node_counts.get('Limit', 0) > 0 else 0.0,  # has_limit
                1.0 if node_counts.get('Order', 0) > 0 else 0.0,  # has_order_by
                1.0 if len(table_names) > 1 else 0.0,  # has_multiple_tables
                1.0 if any('date' in str(c).lower() for c in ast.find_all(exp.Column)) else 0.0,  # has_date_filter
                1.0 if node_counts.get('In', 0) > 0 else 0.0,  # has_in_clause
                1.0 if ast.find(exp.Exists) else 0.0,  # has_exists
                1.0 if any(n.find(exp.Exists) for n in ast.find_all(exp.Not)) else 0.0,  # has_not_exists
                1.0 if node_counts.get('Like', 0) > 0 else 0.0,  # has_like
                1.0 if node_counts.get('Between', 0) > 0 else 0.0,  # has_between
                1.0 if node_counts.get('Case', 0) > 0 else 0.0,  # has_case_when
                1.0 if node_counts.get('Cast', 0) > 0 else 0.0,  # has_cast
                1.0 if node_counts.get('IsNull', 0) + node_counts.get('Is', 0) > 0 else 0.0,  # has_null_check
                1.0 if node_counts.get('And', 0) > 2 or node_counts.get('Or', 0) > 1 else 0.0,  # has_complex_predicate
                1.0 if any(a.find(exp.AggFunc) for a in ast.find_all(exp.AggFunc)) else 0.0,  # has_nested_aggregation
                1.0 if node_counts.get('CTE', 0) > 1 else 0.0,  # has_multiple_ctes
                1.0 if any(c.find(exp.Union) for c in ast.find_all(exp.CTE)) else 0.0,  # has_recursive_cte
            ]
            features.extend(patterns)

            # 5. Complexity metrics
            total_nodes = sum(1 for _ in ast.walk())
            branching_factors = [len(list(n.iter_expressions())) for n in ast.walk() if list(n.iter_expressions())]
            avg_branching = np.mean(branching_factors) if branching_factors else 0.0
            predicate_complexity = node_counts.get('And', 0) + node_counts.get('Or', 0) + node_counts.get('Not', 0)
            select_complexity = sum(len(s.expressions) if s.expressions else 0 for s in ast.find_all(exp.Select))
            join_complexity = sum(len(list(j.find_all((exp.EQ, exp.And)))) for j in ast.find_all(exp.Join))
            features.extend([float(total_nodes), float(avg_branching), float(predicate_complexity),
                           float(select_complexity), float(join_complexity)])

            return np.array(features, dtype=np.float32)

        except Exception as e:
            logger.warning(f"Failed to vectorize SQL: {e}")
            return np.zeros(self.num_features, dtype=np.float32)


# =============================================================================
# Example Loading
# =============================================================================

# ADO is PostgreSQL-focused - only use ado/examples/ (DSB catalog rules)
# Do NOT load qt_sql/optimization/examples/ - those are DuckDB TPC-DS gold examples


def _clean_sql_markers(sql: str) -> str:
    """Remove [xxx]: markers from example SQL and extract main query."""
    import re

    # Remove lines with [xxx]: markers
    lines = sql.split('\n')
    clean_lines = []
    for line in lines:
        # Skip pure marker lines like "[customer_total_return] CORRELATED:"
        if re.match(r'^\s*\[[\w_]+\].*:\s*$', line):
            continue
        # Skip "[main_query]:" lines
        if re.match(r'^\s*\[main_query\]:\s*$', line):
            continue
        clean_lines.append(line)

    cleaned = '\n'.join(clean_lines).strip()

    # If there are multiple SQL statements, try to find the main SELECT
    if 'SELECT' in cleaned.upper():
        # Find the last/main SELECT statement
        parts = re.split(r'\n\s*\n', cleaned)  # Split by blank lines
        for part in reversed(parts):
            if 'SELECT' in part.upper() and 'FROM' in part.upper():
                return part.strip()

    return cleaned


def load_examples_for_indexing() -> List[Tuple[str, str, Dict]]:
    """Load examples from multiple directories for FAISS indexing.

    Loads from:
    - ado/examples/ (generic PostgreSQL patterns)
    - qt_sql/optimization/examples/ (verified TPC-DS gold examples)

    Returns:
        List of (example_id, sql_text, metadata) tuples
    """
    examples = []

    # Load only from ado/examples/ (PostgreSQL DSB catalog rules)
    for example_dir in [EXAMPLES_DIR]:
        if not example_dir.exists():
            continue

        for path in sorted(example_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text())
                example_id = data.get("id", path.stem)

                # Get SQL to vectorize - try multiple fields
                example_data = data.get("example", {})
                sql_text = (
                    example_data.get("before_sql") or
                    example_data.get("input_slice") or
                    example_data.get("original_sql") or
                    ""
                )

                if not sql_text:
                    logger.warning(f"No SQL found in example {example_id}")
                    continue

                # Clean SQL by removing [xxx]: markers
                sql_text = _clean_sql_markers(sql_text)

                if not sql_text:
                    logger.warning(f"Empty SQL after cleaning in {example_id}")
                    continue

                # Extract metadata
                transforms = example_data.get("transforms", [])
                if not transforms and example_data.get("opportunity"):
                    transforms = [example_data["opportunity"].lower()]

                # Get transform from rewrite_sets
                output = example_data.get("output", {})
                rewrite_sets = output.get("rewrite_sets", [])
                if rewrite_sets and not transforms:
                    transforms = [rs.get("transform", "") for rs in rewrite_sets if rs.get("transform")]

                # Get speedup from example or data level
                speedup = data.get("verified_speedup", "unknown")

                metadata = {
                    "name": data.get("name", example_id),
                    "description": data.get("description", ""),
                    "verified_speedup": speedup,
                    "transforms": transforms,
                    "key_insight": example_data.get("key_insight", ""),
                    "benchmark_queries": data.get("benchmark_queries", []),
                }

                examples.append((example_id, sql_text, metadata))

            except Exception as e:
                logger.warning(f"Failed to load example {path}: {e}")

    return examples


# =============================================================================
# FAISS Index Building
# =============================================================================

def build_faiss_index(
    examples: List[Tuple[str, str, Dict]],
    dialect: str = "postgres",
) -> Tuple[Optional[object], Dict, Dict]:
    """Build FAISS index from examples.

    Args:
        examples: List of (example_id, sql_text, metadata) tuples
        dialect: SQL dialect for parsing

    Returns:
        (faiss_index, metadata_dict, feature_stats_dict)
    """
    try:
        import faiss
    except ImportError:
        logger.error("FAISS not installed. Run: pip install faiss-cpu")
        return None, {}, {}

    if not examples:
        logger.warning("No examples to index")
        return None, {}, {}

    vectorizer = ASTVectorizer()
    vectors = []
    query_metadata = {}

    print(f"Vectorizing {len(examples)} examples...")
    print("  (AST features are literal-independent, no SQL normalization needed)")

    for i, (example_id, sql_text, meta) in enumerate(examples):
        # Vectorize directly - AST features (node counts, depth, patterns)
        # are structural and don't depend on literal values
        vector = vectorizer.vectorize(sql_text, dialect=dialect)
        vectors.append(vector)

        query_metadata[example_id] = {
            "vector_index": i,
            "name": meta.get("name", example_id),
            "description": meta.get("description", ""),
            "verified_speedup": meta.get("verified_speedup", "unknown"),
            "transforms": meta.get("transforms", []),
            "winning_transform": meta.get("transforms", [""])[0] if meta.get("transforms") else "",
            "key_insight": meta.get("key_insight", ""),
        }

        print(f"  [{i+1}/{len(examples)}] {example_id}: {np.count_nonzero(vector)} non-zero features")

    # Stack vectors
    vectors_array = np.vstack(vectors).astype('float32')

    # Compute feature statistics for z-score normalization
    mean = np.mean(vectors_array, axis=0)
    std = np.std(vectors_array, axis=0)
    std[std == 0] = 1.0  # Avoid division by zero

    feature_stats = {
        "mean": mean.tolist(),
        "std": std.tolist(),
        "num_features": len(mean),
    }

    # Apply z-score normalization
    normalized = (vectors_array - mean) / std

    # L2 normalize for cosine similarity
    faiss.normalize_L2(normalized)

    # Build index
    dimension = normalized.shape[1]
    index = faiss.IndexFlatL2(dimension)
    index.add(normalized)

    # Build metadata
    metadata = {
        "query_metadata": query_metadata,
        "index_stats": {
            "total_vectors": len(examples),
            "dimensions": dimension,
            "index_type": "IndexFlatL2 (cosine similarity via normalization)",
            "normalized_sql": True,
            "zscore_normalized": True,
            "feature_stats_file": "feature_stats.json",
            "dialect": dialect,
            "normalization": "literals replaced with $N placeholders, identifiers lowercased",
        }
    }

    print(f"\nBuilt FAISS index: {len(examples)} vectors, {dimension} dimensions")

    return index, metadata, feature_stats


def save_index(
    index,
    metadata: Dict,
    feature_stats: Dict,
) -> None:
    """Save FAISS index and metadata to ado/models/."""
    import faiss

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # Save index
    faiss.write_index(index, str(INDEX_FILE))
    print(f"Saved index to {INDEX_FILE}")

    # Save metadata
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to {METADATA_FILE}")

    # Save feature stats
    with open(FEATURE_STATS_FILE, 'w') as f:
        json.dump(feature_stats, f, indent=2)
    print(f"Saved feature stats to {FEATURE_STATS_FILE}")


def show_index_stats() -> None:
    """Show statistics about the current FAISS index."""
    if not METADATA_FILE.exists():
        print("No FAISS index found. Run: python -m ado.faiss_builder")
        return

    with open(METADATA_FILE) as f:
        metadata = json.load(f)

    stats = metadata.get("index_stats", {})
    query_meta = metadata.get("query_metadata", {})

    print("=" * 60)
    print("ADO FAISS Index Statistics")
    print("=" * 60)
    print(f"Total vectors:    {stats.get('total_vectors', 0)}")
    print(f"Dimensions:       {stats.get('dimensions', 0)}")
    print(f"Index type:       {stats.get('index_type', 'unknown')}")
    print(f"Z-score norm:     {stats.get('zscore_normalized', False)}")
    print(f"Dialect:          {stats.get('dialect', 'postgres')}")
    print()
    print("Indexed Examples:")
    print("-" * 60)

    for ex_id, meta in sorted(query_meta.items()):
        transforms = meta.get("transforms", [])
        speedup = meta.get("verified_speedup", "unknown")
        print(f"  {ex_id}")
        print(f"    transforms: {transforms}")
        print(f"    speedup: {speedup}")


def rebuild_index() -> bool:
    """Rebuild FAISS index from ado/examples/.

    Returns:
        True if successful, False otherwise
    """
    print("=" * 60)
    print("Building ADO FAISS Index for PostgreSQL DSB")
    print("=" * 60)

    # Load examples
    examples = load_examples_for_indexing()
    if not examples:
        print("\nNo examples found in ado/examples/")
        print("Add example JSON files with 'before_sql' or 'input_slice' fields")
        return False

    print(f"\nFound {len(examples)} examples")

    # Build index
    index, metadata, feature_stats = build_faiss_index(examples, dialect="postgres")

    if index is None:
        print("\nFailed to build index")
        return False

    # Save
    save_index(index, metadata, feature_stats)

    print("\n" + "=" * 60)
    print("FAISS index built successfully!")
    print("=" * 60)

    return True


# =============================================================================
# CLI
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build ADO FAISS index")
    parser.add_argument("--stats", action="store_true", help="Show index statistics")
    parser.add_argument("--rebuild", action="store_true", help="Force rebuild index")

    args = parser.parse_args()

    if args.stats:
        show_index_stats()
    else:
        rebuild_index()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
