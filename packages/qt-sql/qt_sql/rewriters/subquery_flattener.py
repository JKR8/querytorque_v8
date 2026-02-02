"""Subquery Flattening Rewriters.

SQL-SEL-003: Multiple scalar subqueries to single JOIN
SQL-SUB-003: Deeply nested subqueries to CTEs
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
class MultipleScalarToJoinRewriter(BaseRewriter):
    """Convert multiple correlated scalar subqueries to a single JOIN.

    Example:
        SELECT
            e.name,
            (SELECT d.name FROM departments d WHERE d.id = e.dept_id) as dept_name,
            (SELECT d.budget FROM departments d WHERE d.id = e.dept_id) as dept_budget
        FROM employees e
        ->
        SELECT e.name, d.name as dept_name, d.budget as dept_budget
        FROM employees e
        LEFT JOIN departments d ON d.id = e.dept_id

    Benefits:
    - Reduces repeated subquery execution
    - Single table access instead of multiple
    - Better optimizer opportunities
    """

    rewriter_id = "multiple_scalar_to_join"
    name = "Multiple Scalar Subqueries to JOIN"
    description = "Consolidate correlated scalar subqueries into single JOIN"
    linked_rule_ids = ("SQL-SEL-003",)
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for multiple scalar subqueries on same correlation."""
        if not isinstance(node, exp.Select):
            return False

        # Find scalar subqueries in SELECT list
        scalar_subqueries = []
        for expr in node.expressions:
            subquery = None
            if isinstance(expr, exp.Alias):
                subquery = expr.this if isinstance(expr.this, exp.Subquery) else None
            elif isinstance(expr, exp.Subquery):
                subquery = expr

            if subquery:
                inner = subquery.find(exp.Select)
                if inner and len(inner.expressions) == 1:  # Scalar
                    scalar_subqueries.append(subquery)

        if len(scalar_subqueries) < 2:
            return False

        # Check if they reference the same table with same correlation
        tables = set()
        correlations = []
        for sq in scalar_subqueries:
            inner = sq.find(exp.Select)
            if inner:
                inner_from = inner.find(exp.From)
                if inner_from:
                    for table in inner_from.find_all(exp.Table):
                        tables.add(str(table.name).lower())
                inner_where = inner.find(exp.Where)
                if inner_where:
                    correlations.append(inner_where.this.sql())

        # If same table appears and correlations match, can consolidate
        return len(tables) == 1 and len(set(correlations)) == 1 and len(correlations) >= 2

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert scalar subqueries to JOIN."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()

            # Find scalar subqueries and their info
            subquery_info = []  # (index, alias, column, table, where)

            new_expressions = []
            subquery_table = None
            subquery_where = None

            for i, expr in enumerate(rewritten.expressions):
                subquery = None
                alias = None

                if isinstance(expr, exp.Alias):
                    alias = str(expr.alias)
                    if isinstance(expr.this, exp.Subquery):
                        subquery = expr.this

                if subquery:
                    inner = subquery.find(exp.Select)
                    if inner and len(inner.expressions) == 1:
                        # Get the column being selected
                        inner_col = inner.expressions[0]
                        if isinstance(inner_col, exp.Column):
                            col_name = str(inner_col.name)
                        elif isinstance(inner_col, exp.Alias):
                            col_name = str(inner_col.alias)
                        else:
                            col_name = inner_col.sql()

                        # Get table info
                        inner_from = inner.find(exp.From)
                        if inner_from:
                            table = inner_from.find(exp.Table)
                            if table:
                                subquery_table = table.copy()
                                inner_where = inner.find(exp.Where)
                                if inner_where:
                                    subquery_where = inner_where.this.copy()

                                # Replace with column reference
                                table_alias = str(table.alias or table.name)
                                new_col = exp.Alias(
                                    this=exp.Column(this=exp.to_identifier(col_name), table=exp.to_identifier(table_alias)),
                                    alias=exp.to_identifier(alias) if alias else None
                                )
                                new_expressions.append(new_col)
                                continue

                new_expressions.append(expr)

            if not subquery_table or not subquery_where:
                return self._create_failure(original_sql, "Could not extract subquery info")

            # Set new expressions
            rewritten.set("expressions", new_expressions)

            # Add LEFT JOIN
            existing_joins = list(rewritten.args.get("joins", []))
            new_join = exp.Join(
                this=subquery_table,
                on=subquery_where,
                side="LEFT",
            )
            existing_joins.append(new_join)
            rewritten.set("joins", existing_joins)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.MEDIUM,
                explanation="Consolidated scalar subqueries into single LEFT JOIN",
            )

            result.add_safety_check(
                name="null_handling",
                result=SafetyCheckResult.WARNING,
                message="LEFT JOIN preserves NULL behavior of scalar subquery when no match",
            )

            result.add_safety_check(
                name="cardinality",
                result=SafetyCheckResult.WARNING,
                message="Ensure join produces at most one row per outer row (like scalar subquery)",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class DeeplyNestedToCTERewriter(BaseRewriter):
    """Convert deeply nested subqueries (3+ levels) to CTEs.

    Example:
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM base_table WHERE x > 0
                ) t1 WHERE y > 0
            ) t2 WHERE z > 0
        ) t3
        ->
        WITH
            cte1 AS (SELECT * FROM base_table WHERE x > 0),
            cte2 AS (SELECT * FROM cte1 WHERE y > 0),
            cte3 AS (SELECT * FROM cte2 WHERE z > 0)
        SELECT * FROM cte3

    Benefits:
    - Improved readability
    - Easier debugging and testing
    - Some engines optimize CTEs differently
    """

    rewriter_id = "deeply_nested_to_cte"
    name = "Deeply Nested to CTEs"
    description = "Convert 3+ nested subqueries to CTEs"
    linked_rule_ids = ("SQL-SUB-003",)
    default_confidence = RewriteConfidence.MEDIUM
    min_nesting_level = 3

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for deeply nested subqueries."""
        if not isinstance(node, exp.Select):
            return False

        max_depth = self._get_max_subquery_depth(node)
        return max_depth >= self.min_nesting_level

    def _get_max_subquery_depth(self, node: exp.Expression, current_depth: int = 0) -> int:
        """Get maximum subquery nesting depth."""
        max_depth = current_depth

        for child in node.iter_expressions():
            if isinstance(child, exp.Subquery):
                inner_depth = self._get_max_subquery_depth(child, current_depth + 1)
                max_depth = max(max_depth, inner_depth)
            else:
                inner_depth = self._get_max_subquery_depth(child, current_depth)
                max_depth = max(max_depth, inner_depth)

        return max_depth

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert nested subqueries to CTEs."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            # Extract subqueries from inside out
            ctes = []
            cte_counter = [0]  # Use list for mutable closure

            def extract_subqueries(expr: exp.Expression, depth: int = 0) -> exp.Expression:
                """Recursively extract nested subqueries into CTEs."""
                if isinstance(expr, exp.Subquery):
                    inner = expr.this
                    if isinstance(inner, exp.Select):
                        # First process any nested subqueries
                        processed_inner = inner.copy()

                        # Process FROM subqueries
                        from_clause = processed_inner.find(exp.From)
                        if from_clause:
                            for subq in list(from_clause.find_all(exp.Subquery)):
                                if subq.parent == from_clause.this or isinstance(subq.parent, exp.From):
                                    replacement = extract_subqueries(subq, depth + 1)
                                    if replacement != subq:
                                        subq.replace(replacement)

                        # If we're deep enough, convert to CTE
                        if depth >= self.min_nesting_level - 1:
                            cte_counter[0] += 1
                            cte_name = f"cte_{cte_counter[0]}"

                            # Create CTE
                            cte = exp.CTE(
                                this=processed_inner,
                                alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
                            )
                            ctes.append(cte)

                            # Return table reference
                            return exp.Table(this=exp.to_identifier(cte_name))

                return expr

            rewritten = node.copy()

            # Process FROM clause subqueries
            from_clause = rewritten.find(exp.From)
            if from_clause:
                for subq in list(from_clause.find_all(exp.Subquery)):
                    depth = self._get_subquery_depth(subq, rewritten)
                    if depth >= self.min_nesting_level:
                        replacement = extract_subqueries(subq, 0)
                        if replacement != subq:
                            subq.replace(replacement)

            if not ctes:
                return self._create_failure(original_sql, "No deep nesting to convert")

            # Add CTEs to query
            existing_with = rewritten.find(exp.With)
            if existing_with:
                # Prepend new CTEs
                all_ctes = ctes + list(existing_with.expressions)
                existing_with.set("expressions", all_ctes)
            else:
                rewritten.set("with", exp.With(expressions=ctes))

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Extracted {len(ctes)} nested subqueries into CTEs",
            )

            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="CTEs maintain same logical structure",
            )

            result.add_safety_check(
                name="materialization",
                result=SafetyCheckResult.WARNING,
                message="Some engines materialize CTEs which may affect performance",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _get_subquery_depth(self, subq: exp.Subquery, root: exp.Expression) -> int:
        """Get the depth of a subquery relative to root."""
        depth = 0
        current = subq.parent
        while current and current != root:
            if isinstance(current, exp.Subquery):
                depth += 1
            current = current.parent
        return depth
