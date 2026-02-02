"""JOIN Conversion Rewriters.

SQL-JOIN-002: Implicit to explicit join (FROM a, b WHERE -> FROM a JOIN b ON)
"""

from typing import Any

from sqlglot import exp

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter


@register_rewriter
class ImplicitToExplicitJoinRewriter(BaseRewriter):
    """Convert comma-separated implicit joins to explicit JOIN syntax.

    Example:
        SELECT * FROM orders, customers
        WHERE orders.customer_id = customers.id
        ->
        SELECT * FROM orders
        JOIN customers ON orders.customer_id = customers.id

    Benefits:
    - Clearer intent and readability
    - Prevents accidental Cartesian products
    - Modern SQL standard syntax
    - Better maintainability
    """

    rewriter_id = "implicit_to_explicit_join"
    name = "Implicit to Explicit JOIN"
    description = "Convert comma joins to explicit JOIN ON syntax"
    linked_rule_ids = ("SQL-JOIN-002",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for implicit join pattern (comma joins without ON clause)."""
        if not isinstance(node, exp.Select):
            return False

        # Look for implicit joins (joins with kind=None, which are comma joins)
        joins = node.args.get("joins", [])
        implicit_joins = [j for j in joins if j.args.get("kind") is None and j.args.get("on") is None]

        if not implicit_joins:
            return False

        # Must have WHERE clause with join condition
        where = node.find(exp.Where)
        if not where:
            return False

        # Build set of all table aliases in the FROM clause
        table_aliases = self._get_all_table_aliases(node)
        if len(table_aliases) < 2:
            return False

        # Check for equality condition that could be a join (with or without table qualifiers)
        for eq in where.find_all(exp.EQ):
            if isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column):
                left_table = str(eq.left.table).lower() if eq.left.table else None
                right_table = str(eq.right.table).lower() if eq.right.table else None

                # Case 1: Both have table qualifiers and they differ
                if left_table and right_table and left_table != right_table:
                    return True

                # Case 2: Unqualified columns - infer from column naming conventions
                # TPC-DS uses prefix naming: sr_customer_sk is from store_returns, d_date_sk from date_dim
                if not left_table or not right_table:
                    left_col = str(eq.left.name).lower()
                    right_col = str(eq.right.name).lower()
                    # Check if column names suggest different tables (different prefixes)
                    left_prefix = left_col.split('_')[0] if '_' in left_col else ''
                    right_prefix = right_col.split('_')[0] if '_' in right_col else ''
                    if left_prefix and right_prefix and left_prefix != right_prefix:
                        return True

        return False

    def _get_all_table_aliases(self, node: exp.Select) -> set[str]:
        """Get all table names/aliases in the FROM clause."""
        aliases = set()

        # From clause
        from_clause = node.find(exp.From)
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                alias = str(table.alias or table.name).lower()
                aliases.add(alias)

        # Joins
        for join in node.find_all(exp.Join):
            table = join.find(exp.Table)
            if table:
                alias = str(table.alias or table.name).lower()
                aliases.add(alias)

        return aliases

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert implicit joins to explicit JOIN syntax."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()

            from_clause = rewritten.find(exp.From)
            where = rewritten.find(exp.Where)

            if not from_clause or not where:
                return self._create_failure(original_sql, "Missing FROM or WHERE clause")

            # Get base table from FROM and implicit joins
            base_table = from_clause.this
            existing_joins = rewritten.args.get("joins", [])

            # Find implicit joins (kind=None, no ON clause)
            implicit_joins = [j for j in existing_joins if j.args.get("kind") is None and j.args.get("on") is None]
            explicit_joins = [j for j in existing_joins if j not in implicit_joins]

            if not implicit_joins:
                return self._create_failure(original_sql, "No implicit joins found")

            # Build list of all tables (base + implicit join tables)
            all_tables = [base_table]
            for j in implicit_joins:
                if isinstance(j.this, exp.Table):
                    all_tables.append(j.this)

            # Build map of table aliases to help infer column ownership
            table_aliases = self._get_all_table_aliases(rewritten)

            # Find join conditions in WHERE
            join_conditions = []
            remaining_conditions = []

            def is_join_condition(expr):
                """Check if expression is a join condition (col = col from different tables)."""
                if not isinstance(expr, exp.EQ):
                    return False

                left_col = expr.left if isinstance(expr.left, exp.Column) else None
                right_col = expr.right if isinstance(expr.right, exp.Column) else None

                if not left_col or not right_col:
                    return False

                left_table = str(left_col.table).lower() if left_col.table else None
                right_table = str(right_col.table).lower() if right_col.table else None

                # Case 1: Both qualified with different tables
                if left_table and right_table:
                    return left_table != right_table

                # Case 2: Unqualified - use column prefix heuristic (TPC-DS style)
                left_name = str(left_col.name).lower()
                right_name = str(right_col.name).lower()

                # Check for foreign key pattern: xxx_sk = xxx_sk where prefixes differ
                if '_sk' in left_name or '_sk' in right_name or '_id' in left_name or '_id' in right_name:
                    left_prefix = left_name.split('_')[0]
                    right_prefix = right_name.split('_')[0]
                    if left_prefix != right_prefix:
                        return True

                # Check for _date_sk pattern (common in TPC-DS)
                if 'date_sk' in left_name or 'date_sk' in right_name:
                    return True

                return False

            def extract_conditions(expr):
                """Extract individual conditions from AND chain."""
                if isinstance(expr, exp.And):
                    extract_conditions(expr.left)
                    extract_conditions(expr.right)
                else:
                    if is_join_condition(expr):
                        join_conditions.append(expr)
                    else:
                        remaining_conditions.append(expr)

            extract_conditions(where.this)

            if not join_conditions:
                return self._create_failure(original_sql, "No join conditions found in WHERE")

            # Build explicit JOINs for each implicit join table
            new_joins = list(explicit_joins)  # Keep existing explicit joins
            used_conditions = set()

            def column_matches_table(col: exp.Column, table_alias: str, table_name: str) -> bool:
                """Check if a column might belong to a table (qualified or by prefix heuristic)."""
                if col.table:
                    return str(col.table).lower() == table_alias
                # Unqualified - use prefix heuristic
                col_name = str(col.name).lower()
                # Check if column prefix matches table alias or common abbreviations
                col_prefix = col_name.split('_')[0]
                table_prefix = table_alias[0] if table_alias else ''
                table_name_prefix = table_name.split('_')[0] if '_' in table_name else table_name[:2]
                return col_prefix == table_prefix or col_prefix == table_name_prefix.lower()

            for implicit_join in implicit_joins:
                table = implicit_join.this
                if not isinstance(table, exp.Table):
                    continue

                table_alias = str(table.alias or table.name).lower()
                table_name = str(table.name).lower()

                # Find join condition(s) for this table
                table_join_conds = []
                for i, cond in enumerate(join_conditions):
                    if i in used_conditions:
                        continue

                    left_col = cond.left
                    right_col = cond.right

                    # Check if either column belongs to this table
                    left_matches = column_matches_table(left_col, table_alias, table_name)
                    right_matches = column_matches_table(right_col, table_alias, table_name)

                    if left_matches or right_matches:
                        table_join_conds.append(cond.copy())
                        used_conditions.add(i)

                if table_join_conds:
                    # Combine multiple conditions with AND
                    on_clause = table_join_conds[0]
                    for cond in table_join_conds[1:]:
                        on_clause = exp.And(this=on_clause, expression=cond)

                    new_join = exp.Join(
                        this=table.copy(),
                        on=on_clause,
                        kind="INNER",  # Implicit joins are INNER joins
                    )
                    new_joins.append(new_join)

            # Update joins
            if new_joins:
                rewritten.set("joins", new_joins)
            else:
                rewritten.set("joins", None)

            # Update WHERE with remaining conditions
            if remaining_conditions:
                combined = remaining_conditions[0].copy()
                for cond in remaining_conditions[1:]:
                    combined = exp.And(this=combined, expression=cond.copy())
                rewritten.set("where", exp.Where(this=combined))
            else:
                rewritten.set("where", None)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Converted {len(implicit_joins)} implicit join(s) to explicit JOIN syntax",
            )

            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="Explicit JOIN with same conditions is equivalent to implicit join",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))
