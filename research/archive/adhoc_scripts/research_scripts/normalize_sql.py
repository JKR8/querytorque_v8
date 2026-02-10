#!/usr/bin/env python3
"""Normalize SQL queries by stripping domain-specific semantics.

Converts:
    FROM store_sales JOIN date_dim WHERE d_year = 2001
To:
    FROM fact_table_1 JOIN dimension_table_1 WHERE dim_col_1 = <INT>

This creates universal query patterns for similarity matching.
"""

import json
import re
import sys
from pathlib import Path
from typing import Dict, Tuple
import sqlglot
from sqlglot import exp

# Check virtual environment
if not (hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)):
    print("❌ ERROR: Not running in virtual environment!")
    print("Please run: source .venv/bin/activate")
    sys.exit(1)

# Known fact tables (high cardinality, transactional)
FACT_TABLES = {
    'store_sales', 'catalog_sales', 'web_sales',
    'store_returns', 'catalog_returns', 'web_returns',
    'inventory', 'orders', 'lineitem', 'sales', 'transactions'
}

# Known dimension tables (low cardinality, reference data)
DIMENSION_TABLES = {
    'store', 'customer', 'item', 'customer_address', 'customer_demographics',
    'household_demographics', 'promotion', 'reason', 'ship_mode', 'warehouse',
    'web_site', 'web_page', 'catalog_page', 'call_center', 'income_band',
    'time_dim', 'date_dim'
}


class SQLNormalizer:
    """Normalize SQL queries to universal patterns."""

    def __init__(self):
        self.fact_counter = 0
        self.dim_counter = 0
        self.col_counters = {}  # table_type -> counter
        self.table_mapping = {}  # original_name -> normalized_name
        self.column_mapping = {}  # (table, col) -> normalized_name

    def normalize(self, sql: str, dialect: str = "duckdb") -> Tuple[str, Dict]:
        """
        Normalize SQL query.

        Returns:
            - Normalized SQL string
            - Mapping dict with original -> normalized names
        """
        self._reset_counters()

        try:
            # Parse SQL
            ast = sqlglot.parse_one(sql, dialect=dialect)

            # Step 1: Alphabetize predicates FIRST (before renaming)
            # This ensures column names are assigned in consistent order
            ast = self._alphabetize_predicates(ast)

            # Step 2: Classify and rename tables
            self._normalize_tables(ast)

            # Step 3: Rename columns (now in alphabetized order)
            self._normalize_columns(ast)

            # Step 4: Abstract literals
            normalized_ast = self._abstract_literals(ast)

            # Generate normalized SQL
            normalized_sql = normalized_ast.sql(dialect=dialect, pretty=True)

            # Build mapping
            mapping = {
                "tables": self.table_mapping.copy(),
                "columns": {f"{t}.{c}": n for (t, c), n in self.column_mapping.items()}
            }

            return normalized_sql, mapping

        except Exception as e:
            # If parsing fails, return original with empty mapping
            print(f"Warning: Failed to normalize query: {e}")
            return sql, {}

    def _reset_counters(self):
        """Reset internal counters for new query."""
        self.fact_counter = 0
        self.dim_counter = 0
        self.col_counters = {}
        self.table_mapping = {}
        self.column_mapping = {}

    def _normalize_tables(self, ast: exp.Expression):
        """Classify and rename tables to fact_table_N or dimension_table_N."""

        # Collect all tables
        for table_node in ast.find_all(exp.Table):
            if not table_node.this:
                continue

            table_name = str(table_node.this).lower()

            # Skip if already mapped
            if table_name in self.table_mapping:
                table_node.set("this", exp.to_identifier(self.table_mapping[table_name]))
                continue

            # Classify table
            if table_name in FACT_TABLES or self._is_fact_table(table_name):
                table_type = "fact"
                self.fact_counter += 1
                normalized_name = f"fact_table_{self.fact_counter}"
            else:
                # Default to dimension
                table_type = "dimension"
                self.dim_counter += 1
                normalized_name = f"dimension_table_{self.dim_counter}"

            # Store mapping
            self.table_mapping[table_name] = normalized_name
            self.col_counters[normalized_name] = 0

            # Update AST
            table_node.set("this", exp.to_identifier(normalized_name))

            # Also update alias if present
            if table_node.alias:
                alias = str(table_node.alias)
                if alias.lower() != table_name:
                    # Keep aliases separate from table names in mapping
                    self.table_mapping[alias.lower()] = normalized_name

    def _normalize_columns(self, ast: exp.Expression):
        """Rename columns to table_type_col_N."""

        for col_node in ast.find_all(exp.Column):
            if not col_node.this:
                continue

            col_name = str(col_node.this).lower()

            # Get table context (if qualified)
            table_ref = None
            if col_node.table:
                table_ref = str(col_node.table).lower()

            # Skip if already mapped
            if (table_ref, col_name) in self.column_mapping:
                normalized = self.column_mapping[(table_ref, col_name)]
                col_node.set("this", exp.to_identifier(normalized.split('.')[-1]))
                continue

            # Determine normalized table name
            if table_ref and table_ref in self.table_mapping:
                normalized_table = self.table_mapping[table_ref]
            else:
                # Unqualified column - use generic name
                normalized_table = "col"

            # Generate normalized column name
            if normalized_table not in self.col_counters:
                self.col_counters[normalized_table] = 0

            self.col_counters[normalized_table] += 1
            col_suffix = self.col_counters[normalized_table]

            # Create normalized name based on table type
            if normalized_table.startswith("fact"):
                normalized_col = f"fact_col_{col_suffix}"
            elif normalized_table.startswith("dimension"):
                normalized_col = f"dim_col_{col_suffix}"
            else:
                normalized_col = f"col_{col_suffix}"

            # Store mapping
            self.column_mapping[(table_ref, col_name)] = f"{normalized_table}.{normalized_col}"

            # Update AST
            col_node.set("this", exp.to_identifier(normalized_col))

    def _abstract_literals(self, ast: exp.Expression) -> exp.Expression:
        """Replace literal values with type placeholders."""

        for literal in ast.find_all(exp.Literal):
            value = literal.this

            # Determine type
            if isinstance(value, bool):
                placeholder = "<BOOL>"
            elif isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
                placeholder = "<INT>"
            elif isinstance(value, float) or self._is_float(value):
                placeholder = "<FLOAT>"
            elif self._is_date(value):
                placeholder = "<DATE>"
            else:
                placeholder = "<STRING>"

            # Replace with placeholder
            literal.set("this", placeholder)
            literal.set("is_string", True)  # Treat as string to preserve quotes

        return ast

    def _alphabetize_predicates(self, ast: exp.Expression) -> exp.Expression:
        """Sort AND/OR predicates alphabetically for canonical form.

        This ensures:
            WHERE a=1 AND b=2
            WHERE b=2 AND a=1
        Both become:
            WHERE a=1 AND b=2
        """

        def sort_binary_op(node: exp.Expression) -> exp.Expression:
            """Recursively sort AND/OR operands alphabetically."""
            if isinstance(node, (exp.And, exp.Or)):
                # Collect all operands of the same type
                operands = []
                op_type = type(node)

                def collect_operands(n):
                    if isinstance(n, op_type):
                        collect_operands(n.left)
                        collect_operands(n.right)
                    else:
                        # Recursively sort nested expressions first
                        sorted_n = sort_binary_op(n)
                        operands.append(sorted_n)

                collect_operands(node)

                # Sort operands by their SQL string representation
                operands.sort(key=lambda x: x.sql())

                # Rebuild the tree from sorted operands
                if len(operands) == 0:
                    return node
                elif len(operands) == 1:
                    return operands[0]
                else:
                    result = operands[0]
                    for operand in operands[1:]:
                        result = op_type(this=result, expression=operand)
                    return result
            else:
                # For non-AND/OR nodes, recurse into children
                for key, value in node.args.items():
                    if isinstance(value, exp.Expression):
                        node.set(key, sort_binary_op(value))
                    elif isinstance(value, list):
                        node.set(key, [sort_binary_op(v) if isinstance(v, exp.Expression) else v for v in value])
                return node

        return sort_binary_op(ast)

    def _is_fact_table(self, name: str) -> bool:
        """Heuristic to detect fact tables from name."""
        fact_keywords = ['sales', 'orders', 'transactions', 'returns', 'inventory', 'fact']
        return any(kw in name.lower() for kw in fact_keywords)

    def _is_float(self, value) -> bool:
        """Check if value represents a float."""
        if isinstance(value, str):
            try:
                float(value)
                return '.' in value
            except ValueError:
                return False
        return False

    def _is_date(self, value) -> bool:
        """Check if value looks like a date."""
        if isinstance(value, str):
            # Simple date pattern matching
            date_pattern = r'\d{4}-\d{2}-\d{2}'
            return bool(re.match(date_pattern, value))
        return False


def normalize_benchmark_queries():
    """Normalize all TPC-DS benchmark queries."""

    BASE = Path(__file__).parent.parent
    BENCHMARK_DIR = BASE / "research" / "experiments" / "benchmarks" / "kimi_benchmark_20260202_221828"
    OUTPUT_FILE = BASE / "research" / "ml_pipeline" / "data" / "normalized_queries.json"

    normalizer = SQLNormalizer()
    results = {}

    print("Normalizing TPC-DS queries...")
    print("=" * 60)

    for qnum in range(1, 100):
        query_dir = BENCHMARK_DIR / f"q{qnum}"
        original_sql = query_dir / "original.sql"

        if not original_sql.exists():
            continue

        sql = original_sql.read_text()

        # Normalize
        normalized_sql, mapping = normalizer.normalize(sql, dialect="duckdb")

        # Store result
        results[f"q{qnum}"] = {
            "original_length": len(sql),
            "normalized_length": len(normalized_sql),
            "normalized_sql": normalized_sql,
            "mapping": mapping
        }

        # Print sample
        if qnum <= 3 or qnum in [15, 74, 93]:
            print(f"\nQ{qnum}:")
            print(f"  Tables: {len(mapping.get('tables', {}))} -> {list(mapping.get('tables', {}).values())[:3]}")
            print(f"  Columns: {len(mapping.get('columns', {}))}")
            print(f"  Normalized SQL (first 150 chars):")
            print(f"    {normalized_sql[:150]}...")

    # Save results
    print(f"\nSaving {len(results)} normalized queries to {OUTPUT_FILE}")

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2)

    # Statistics
    print("\n" + "=" * 60)
    print("Normalization Statistics")
    print("=" * 60)

    total = len(results)
    avg_reduction = sum(
        1 - (r["normalized_length"] / r["original_length"])
        for r in results.values()
    ) / total * 100

    print(f"Total queries normalized: {total}")
    print(f"Average length reduction: {avg_reduction:.1f}%")

    # Count unique patterns
    table_patterns = set()
    for r in results.values():
        tables = tuple(sorted(r["mapping"].get("tables", {}).values()))
        table_patterns.add(tables)

    print(f"Unique table patterns: {len(table_patterns)}")
    print(f"\n✓ Normalized queries saved: {OUTPUT_FILE}")


if __name__ == "__main__":
    normalize_benchmark_queries()
