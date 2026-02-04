#!/usr/bin/env python3
"""Vectorize SQL queries for similarity matching.

Converts normalized SQL to fixed-size feature vectors using AST-based features.
"""

import json
import sys
import numpy as np
from pathlib import Path
from typing import Dict, List
import sqlglot
from sqlglot import exp
from collections import Counter

# Check virtual environment
if not (hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)):
    print("❌ ERROR: Not running in virtual environment!")
    print("Please run: source .venv/bin/activate")
    sys.exit(1)

BASE = Path(__file__).parent.parent


class ASTVectorizer:
    """Convert SQL queries to feature vectors using AST analysis."""

    # SQL node types to track
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
        """Build list of all feature names."""
        features = []

        # 1. Node type counts (40 features)
        features.extend([f"node_{nt.lower()}" for nt in self.NODE_TYPES])

        # 2. Depth metrics (5 features)
        features.extend([
            "max_depth",
            "max_subquery_depth",
            "max_join_depth",
            "cte_depth",
            "union_depth"
        ])

        # 3. Cardinality features (10 features)
        features.extend([
            "num_tables",
            "num_select_cols",
            "num_where_conditions",
            "num_joins",
            "num_ctes",
            "num_subqueries",
            "num_aggregates",
            "num_window_functions",
            "num_unions",
            "num_case_statements"
        ])

        # 4. Pattern indicators (binary, 30 features)
        features.extend([
            "has_cte",
            "has_union",
            "has_union_all",
            "has_subquery",
            "has_correlated_subquery",
            "has_aggregation",
            "has_group_by",
            "has_having",
            "has_window_function",
            "has_self_join",
            "has_cross_join",
            "has_left_join",
            "has_outer_join",
            "has_distinct",
            "has_limit",
            "has_order_by",
            "has_multiple_tables",
            "has_date_filter",
            "has_in_clause",
            "has_exists",
            "has_not_exists",
            "has_like",
            "has_between",
            "has_case_when",
            "has_cast",
            "has_null_check",
            "has_complex_predicate",
            "has_nested_aggregation",
            "has_multiple_ctes",
            "has_recursive_cte"
        ])

        # 5. Complexity metrics (5 features)
        features.extend([
            "total_nodes",
            "avg_branching_factor",
            "predicate_complexity",
            "select_complexity",
            "join_complexity"
        ])

        return features

    def vectorize(self, sql: str, dialect: str = "duckdb") -> np.ndarray:
        """Convert SQL query to feature vector."""

        try:
            ast = sqlglot.parse_one(sql, dialect=dialect)
            features = self._extract_features(ast)
            return np.array(features, dtype=np.float32)

        except Exception as e:
            print(f"Warning: Failed to vectorize query: {e}")
            # Return zero vector on failure
            return np.zeros(len(self.feature_names), dtype=np.float32)

    def _extract_features(self, ast: exp.Expression) -> List[float]:
        """Extract all features from AST."""
        features = []

        # 1. Node type counts
        node_counts = self._count_nodes(ast)
        for node_type in self.NODE_TYPES:
            features.append(float(node_counts.get(node_type, 0)))

        # 2. Depth metrics
        features.extend(self._compute_depth_metrics(ast))

        # 3. Cardinality features
        features.extend(self._compute_cardinality(ast))

        # 4. Pattern indicators (binary)
        features.extend(self._detect_patterns(ast, node_counts))

        # 5. Complexity metrics
        features.extend(self._compute_complexity(ast, node_counts))

        return features

    def _count_nodes(self, ast: exp.Expression) -> Dict[str, int]:
        """Count occurrences of each node type."""
        counts = Counter()

        for node in ast.walk():
            node_type = type(node).__name__
            if node_type in self.NODE_TYPES:
                counts[node_type] += 1

        return dict(counts)

    def _compute_depth_metrics(self, ast: exp.Expression) -> List[float]:
        """Compute depth-related metrics."""

        max_depth = 0
        max_subquery_depth = 0
        max_join_depth = 0
        cte_depth = 0
        union_depth = 0

        def traverse(node, depth=0, subquery_depth=0, join_depth=0):
            nonlocal max_depth, max_subquery_depth, max_join_depth, cte_depth, union_depth

            max_depth = max(max_depth, depth)

            if isinstance(node, exp.Subquery):
                subquery_depth += 1
                max_subquery_depth = max(max_subquery_depth, subquery_depth)

            if isinstance(node, exp.Join):
                join_depth += 1
                max_join_depth = max(max_join_depth, join_depth)

            if isinstance(node, exp.CTE):
                cte_depth = max(cte_depth, depth)

            if isinstance(node, exp.Union):
                union_depth = max(union_depth, depth)

            for child in node.iter_expressions():
                traverse(child, depth + 1, subquery_depth, join_depth)

        traverse(ast)

        return [
            float(max_depth),
            float(max_subquery_depth),
            float(max_join_depth),
            float(cte_depth),
            float(union_depth)
        ]

    def _compute_cardinality(self, ast: exp.Expression) -> List[float]:
        """Count various query elements."""

        num_tables = len(list(ast.find_all(exp.Table)))
        num_select_cols = 0
        num_where_conditions = 0
        num_joins = len(list(ast.find_all(exp.Join)))
        num_ctes = len(list(ast.find_all(exp.CTE)))
        num_subqueries = len(list(ast.find_all(exp.Subquery)))
        num_aggregates = len(list(ast.find_all(exp.AggFunc)))
        num_window_functions = len(list(ast.find_all(exp.Window)))
        num_unions = len(list(ast.find_all(exp.Union)))
        num_case_statements = len(list(ast.find_all(exp.Case)))

        # Count SELECT expressions
        for select in ast.find_all(exp.Select):
            if select.expressions:
                num_select_cols += len(select.expressions)

        # Count WHERE conditions (approximate by counting comparison operators)
        for where in ast.find_all(exp.Where):
            num_where_conditions += len(list(where.find_all((
                exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ,
                exp.In, exp.Like, exp.Between, exp.Is
            ))))

        return [
            float(num_tables),
            float(num_select_cols),
            float(num_where_conditions),
            float(num_joins),
            float(num_ctes),
            float(num_subqueries),
            float(num_aggregates),
            float(num_window_functions),
            float(num_unions),
            float(num_case_statements)
        ]

    def _detect_patterns(self, ast: exp.Expression, node_counts: Dict) -> List[float]:
        """Detect binary pattern indicators."""

        patterns = []

        # Basic structural patterns
        patterns.append(1.0 if node_counts.get('With', 0) > 0 else 0.0)
        patterns.append(1.0 if node_counts.get('Union', 0) > 0 else 0.0)

        # Check for UNION ALL vs UNION
        has_union_all = False
        for union in ast.find_all(exp.Union):
            if union.args.get('distinct') is False:
                has_union_all = True
                break
        patterns.append(1.0 if has_union_all else 0.0)

        # Subquery patterns
        has_subquery = node_counts.get('Subquery', 0) > 0
        patterns.append(1.0 if has_subquery else 0.0)

        # Correlated subquery (approximate by checking for column references)
        has_correlated = False
        for subquery in ast.find_all(exp.Subquery):
            # This is a simplified check
            if subquery.find(exp.Column):
                has_correlated = True
                break
        patterns.append(1.0 if has_correlated else 0.0)

        # Aggregation patterns
        patterns.append(1.0 if node_counts.get('Sum', 0) + node_counts.get('Count', 0) > 0 else 0.0)
        patterns.append(1.0 if node_counts.get('Group', 0) > 0 else 0.0)
        patterns.append(1.0 if node_counts.get('Having', 0) > 0 else 0.0)

        # Window functions
        patterns.append(1.0 if node_counts.get('Window', 0) > 0 else 0.0)

        # Join patterns
        has_self_join = False
        table_names = [str(t.this).lower() for t in ast.find_all(exp.Table) if t.this]
        has_self_join = len(table_names) != len(set(table_names))
        patterns.append(1.0 if has_self_join else 0.0)

        patterns.append(1.0 if any('cross' in str(j).lower() for j in ast.find_all(exp.Join)) else 0.0)
        patterns.append(1.0 if any('left' in str(j).lower() for j in ast.find_all(exp.Join)) else 0.0)
        patterns.append(1.0 if any('outer' in str(j).lower() for j in ast.find_all(exp.Join)) else 0.0)

        # Other patterns
        patterns.append(1.0 if ast.find(exp.Distinct) else 0.0)
        patterns.append(1.0 if node_counts.get('Limit', 0) > 0 else 0.0)
        patterns.append(1.0 if node_counts.get('Order', 0) > 0 else 0.0)
        patterns.append(1.0 if len(table_names) > 1 else 0.0)

        # Check for date-related columns (heuristic)
        has_date = any('date' in str(c).lower() or 'd_year' in str(c).lower()
                       for c in ast.find_all(exp.Column))
        patterns.append(1.0 if has_date else 0.0)

        # Predicate patterns
        patterns.append(1.0 if node_counts.get('In', 0) > 0 else 0.0)
        patterns.append(1.0 if ast.find(exp.Exists) else 0.0)

        # NOT EXISTS check
        not_exists = False
        for not_node in ast.find_all(exp.Not):
            if not_node.find(exp.Exists):
                not_exists = True
                break
        patterns.append(1.0 if not_exists else 0.0)

        patterns.append(1.0 if node_counts.get('Like', 0) > 0 else 0.0)
        patterns.append(1.0 if node_counts.get('Between', 0) > 0 else 0.0)
        patterns.append(1.0 if node_counts.get('Case', 0) > 0 else 0.0)
        patterns.append(1.0 if node_counts.get('Cast', 0) > 0 else 0.0)

        # NULL checks
        has_null_check = node_counts.get('IsNull', 0) > 0 or node_counts.get('Is', 0) > 0
        patterns.append(1.0 if has_null_check else 0.0)

        # Complex predicates (AND/OR combinations)
        has_complex = node_counts.get('And', 0) > 2 or node_counts.get('Or', 0) > 1
        patterns.append(1.0 if has_complex else 0.0)

        # Nested aggregation (aggregate of aggregate)
        nested_agg = False
        for agg in ast.find_all(exp.AggFunc):
            if agg.find(exp.AggFunc):
                nested_agg = True
                break
        patterns.append(1.0 if nested_agg else 0.0)

        # Multiple CTEs
        patterns.append(1.0 if node_counts.get('CTE', 0) > 1 else 0.0)

        # Recursive CTE (simplified check)
        recursive_cte = False
        for cte in ast.find_all(exp.CTE):
            if cte.find(exp.Union):
                recursive_cte = True  # Likely recursive if CTE has UNION
                break
        patterns.append(1.0 if recursive_cte else 0.0)

        return patterns

    def _compute_complexity(self, ast: exp.Expression, node_counts: Dict) -> List[float]:
        """Compute complexity metrics."""

        # Total nodes in AST
        total_nodes = sum(1 for _ in ast.walk())

        # Average branching factor (children per node)
        branching_factors = []
        for node in ast.walk():
            children = list(node.iter_expressions())
            if children:
                branching_factors.append(len(children))
        avg_branching = np.mean(branching_factors) if branching_factors else 0.0

        # Predicate complexity (number of logical operators)
        predicate_complexity = node_counts.get('And', 0) + node_counts.get('Or', 0) + node_counts.get('Not', 0)

        # SELECT complexity (number of expressions)
        select_complexity = sum(
            len(s.expressions) if s.expressions else 0
            for s in ast.find_all(exp.Select)
        )

        # JOIN complexity (number of join conditions)
        join_complexity = sum(
            len(list(j.find_all((exp.EQ, exp.And))))
            for j in ast.find_all(exp.Join)
        )

        return [
            float(total_nodes),
            float(avg_branching),
            float(predicate_complexity),
            float(select_complexity),
            float(join_complexity)
        ]


def vectorize_benchmark_queries():
    """Vectorize all normalized TPC-DS queries."""

    NORMALIZED_QUERIES = BASE / "research" / "ml_pipeline" / "data" / "normalized_queries.json"
    OUTPUT_VECTORS = BASE / "research" / "ml_pipeline" / "vectors" / "query_vectors.npz"
    OUTPUT_METADATA = BASE / "research" / "ml_pipeline" / "vectors" / "query_vectors_metadata.json"

    if not NORMALIZED_QUERIES.exists():
        print("Error: normalized_queries.json not found")
        print("Run: python scripts/normalize_sql.py first")
        return

    print("Loading normalized queries...")
    with open(NORMALIZED_QUERIES) as f:
        queries = json.load(f)

    vectorizer = ASTVectorizer()

    print(f"Vectorizing {len(queries)} queries...")
    print(f"Feature dimensions: {len(vectorizer.feature_names)}")
    print("=" * 60)

    vectors = []
    query_ids = []
    metadata = {
        "feature_names": vectorizer.feature_names,
        "feature_count": len(vectorizer.feature_names),
        "queries": {}
    }

    for query_id, data in sorted(queries.items()):
        normalized_sql = data["normalized_sql"]

        # Vectorize
        vector = vectorizer.vectorize(normalized_sql)
        vectors.append(vector)
        query_ids.append(query_id)

        # Store metadata
        metadata["queries"][query_id] = {
            "vector_index": len(vectors) - 1,
            "feature_stats": {
                "mean": float(np.mean(vector)),
                "std": float(np.std(vector)),
                "nonzero_count": int(np.count_nonzero(vector))
            }
        }

        if len(vectors) <= 3:
            print(f"\n{query_id}:")
            print(f"  Vector shape: {vector.shape}")
            print(f"  Non-zero features: {np.count_nonzero(vector)}/{len(vector)}")
            print(f"  Sample features (first 5): {vector[:5]}")

    # Convert to numpy array
    vectors_array = np.vstack(vectors)

    # Save vectors
    print(f"\nSaving {len(vectors)} vectors to {OUTPUT_VECTORS}")
    np.savez_compressed(
        OUTPUT_VECTORS,
        vectors=vectors_array,
        query_ids=np.array(query_ids)
    )

    # Save metadata
    print(f"Saving metadata to {OUTPUT_METADATA}")
    with open(OUTPUT_METADATA, 'w') as f:
        json.dump(metadata, f, indent=2)

    # Print statistics
    print("\n" + "=" * 60)
    print("Vectorization Statistics")
    print("=" * 60)
    print(f"Total queries: {len(vectors)}")
    print(f"Vector dimensions: {vectors_array.shape[1]}")
    print(f"Average non-zero features: {np.mean([np.count_nonzero(v) for v in vectors]):.1f}")
    print(f"Sparsity: {1 - np.count_nonzero(vectors_array) / vectors_array.size:.2%}")

    print(f"\n✓ Query vectors saved:")
    print(f"  - {OUTPUT_VECTORS}")
    print(f"  - {OUTPUT_METADATA}")


if __name__ == "__main__":
    vectorize_benchmark_queries()
