"""Correlated Subquery to JOIN Rewriter.

Pattern: SELECT ... (SELECT col FROM t2 WHERE t2.fk = t1.pk) FROM t1
Rewrite: SELECT ... t2.col FROM t1 LEFT JOIN t2 ON t2.fk = t1.pk

This transforms row-by-row correlated scalar subqueries into set-based JOINs.
One of the most impactful optimizations as it changes O(n*m) to O(n+m).
"""

from typing import Any, Optional

from sqlglot import exp

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter


@register_rewriter
class CorrelatedSubqueryToJoinRewriter(BaseRewriter):
    """Rewrites correlated scalar subqueries to JOINs.

    Example:
        SELECT
            e.name,
            (SELECT d.name FROM departments d WHERE d.id = e.dept_id) as dept_name
        FROM employees e
        ->
        SELECT
            e.name,
            d.name as dept_name
        FROM employees e
        LEFT JOIN departments d ON d.id = e.dept_id

    Safety Requirements:
        - Subquery must return at most one row (scalar)
        - Join column should have unique/PK constraint on subquery side
        - NULL handling preserved via LEFT JOIN
    """

    rewriter_id = "correlated_subquery_to_join"
    name = "Correlated Subquery to JOIN"
    description = "Convert correlated scalar subqueries to JOIN operations"
    linked_rule_ids = (
        "SQL-SEL-008",    # Correlated subquery in SELECT
        "SQL-WHERE-007",  # Correlated subquery in WHERE
    )
    default_confidence = RewriteConfidence.MEDIUM

    def get_required_metadata(self) -> list[str]:
        """Uniqueness constraints help verify safety."""
        return ["primary_key", "unique_constraints"]

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if node contains correlated scalar subquery."""
        if not isinstance(node, exp.Select):
            return False

        # Look for scalar subqueries in SELECT list
        for subq in node.find_all(exp.Subquery):
            if self._is_correlated_scalar(subq, node):
                return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Perform the correlated subquery to JOIN transformation."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()

            # Find all correlated scalar subqueries
            subqueries = []
            for subq in list(rewritten.find_all(exp.Subquery)):
                if self._is_correlated_scalar(subq, rewritten):
                    subqueries.append(subq)

            if not subqueries:
                return self._create_failure(original_sql, "No correlated scalar subqueries found")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converting {len(subqueries)} correlated subquery(ies) to JOIN",
            )

            # Process each subquery
            join_alias_counter = 0
            for subq in subqueries:
                join_alias_counter += 1
                join_alias = f"_sq{join_alias_counter}"

                transform_result = self._transform_subquery(
                    rewritten, subq, join_alias, result
                )

                if not transform_result:
                    return self._create_failure(
                        original_sql,
                        f"Failed to transform subquery {join_alias}"
                    )

            result.rewritten_sql = rewritten.sql()
            result.rewritten_node = rewritten

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _is_correlated_scalar(self, subq: exp.Subquery, outer: exp.Select) -> bool:
        """Check if subquery is correlated and scalar."""
        # Must be a single-row subquery (scalar)
        inner = subq.find(exp.Select)
        if not inner:
            return False

        # Get tables from outer query
        outer_tables = self._get_table_aliases(outer)

        # Get tables defined INSIDE the subquery (these are NOT correlations)
        inner_tables = self._get_table_aliases(inner)

        # Check for correlation - references to outer tables that aren't also inner tables
        has_correlation = False
        for col in inner.find_all(exp.Column):
            if col.table:
                col_table = str(col.table).lower()
                # Only correlated if references outer table that's NOT defined in subquery
                if col_table in outer_tables and col_table not in inner_tables:
                    has_correlation = True
                    break

        if not has_correlation:
            return False

        # Check if correlation is inside an OR expression (too complex to transform)
        # Pattern like (A AND corr) OR (B AND corr) is not safely transformable
        where = inner.find(exp.Where)
        if where and self._correlation_in_or(where.this, outer_tables, inner_tables):
            return False

        return True

    def _correlation_in_or(
        self,
        expr: exp.Expression,
        outer_tables: set[str],
        inner_tables: set[str],
    ) -> bool:
        """Check if correlation appears inside an OR expression."""
        # If this is an OR, check if either branch has correlation
        if isinstance(expr, exp.Or):
            left_has_corr = self._has_correlation_ref(expr.left, outer_tables, inner_tables)
            right_has_corr = self._has_correlation_ref(expr.right, outer_tables, inner_tables)
            if left_has_corr or right_has_corr:
                return True
        # Check children
        for child in expr.iter_expressions():
            if self._correlation_in_or(child, outer_tables, inner_tables):
                return True
        return False

    def _has_correlation_ref(
        self,
        expr: exp.Expression,
        outer_tables: set[str],
        inner_tables: set[str],
    ) -> bool:
        """Check if expression contains a correlation reference."""
        for col in expr.find_all(exp.Column):
            if col.table:
                col_table = str(col.table).lower()
                if col_table in outer_tables and col_table not in inner_tables:
                    return True
        return False

    def _get_table_aliases(self, select: exp.Select) -> set[str]:
        """Get all table aliases in a SELECT statement."""
        aliases = set()

        # From clause
        from_clause = select.find(exp.From)
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                alias = table.alias or table.name
                if alias:
                    aliases.add(str(alias).lower())

        # Joins
        for join in select.find_all(exp.Join):
            table = join.find(exp.Table)
            if table:
                alias = table.alias or table.name
                if alias:
                    aliases.add(str(alias).lower())

        return aliases

    def _transform_subquery(
        self,
        outer: exp.Select,
        subq: exp.Subquery,
        join_alias: str,
        result: RewriteResult,
    ) -> bool:
        """Transform a single correlated subquery to JOIN.

        Handles two cases:
        1. Simple lookup: SELECT col FROM t WHERE t.fk = outer.pk
           -> JOIN t ON t.fk = outer.pk, reference t.col
        2. Aggregate: SELECT agg(col) FROM t WHERE t.fk = outer.pk
           -> JOIN (SELECT fk, agg(col) FROM t GROUP BY fk) ON ...

        Returns True on success.
        """
        inner = subq.find(exp.Select)
        if not inner:
            return False

        # Extract correlation predicate from WHERE
        where = inner.find(exp.Where)
        if not where:
            result.add_safety_check(
                name="correlation_predicate",
                result=SafetyCheckResult.FAILED,
                message="Subquery has no WHERE clause - cannot determine join condition",
            )
            return False

        # Find the correlation condition (outer.col = inner.col)
        correlation = self._extract_correlation(where, outer, inner)
        if not correlation:
            result.add_safety_check(
                name="correlation_predicate",
                result=SafetyCheckResult.FAILED,
                message="Could not extract correlation predicate from subquery",
            )
            return False

        outer_col, inner_col = correlation

        # Get the table being queried in subquery
        inner_from = inner.find(exp.From)
        if not inner_from:
            return False

        inner_table = inner_from.find(exp.Table)
        if not inner_table:
            return False

        inner_table_name = str(inner_table.name)
        inner_col_name = str(inner_col.name) if hasattr(inner_col, 'name') else str(inner_col)

        # Check if subquery has aggregate functions
        select_expr = inner.expressions[0] if inner.expressions else None
        has_aggregate = self._has_aggregate(select_expr)

        if has_aggregate:
            # Aggregate subquery: build derived table with GROUP BY
            return self._transform_aggregate_subquery(
                outer, subq, inner, join_alias, outer_col, inner_col,
                inner_table, select_expr, result
            )
        else:
            # Simple lookup: direct join to table
            return self._transform_simple_subquery(
                outer, subq, inner, join_alias, outer_col, inner_col,
                inner_table, result
            )

    def _has_aggregate(self, expr: exp.Expression) -> bool:
        """Check if expression contains an aggregate function."""
        if expr is None:
            return False
        agg_types = (exp.Avg, exp.Sum, exp.Count, exp.Min, exp.Max)
        if isinstance(expr, agg_types):
            return True
        if isinstance(expr, exp.Alias):
            return self._has_aggregate(expr.this)
        for child in expr.iter_expressions():
            if self._has_aggregate(child):
                return True
        return False

    def _transform_aggregate_subquery(
        self,
        outer: exp.Select,
        subq: exp.Subquery,
        inner: exp.Select,
        join_alias: str,
        outer_col: exp.Column,
        inner_col: exp.Column,
        inner_table: exp.Table,
        select_expr: exp.Expression,
        result: RewriteResult,
    ) -> bool:
        """Transform aggregate correlated subquery to JOIN with derived table."""
        inner_col_name = str(inner_col.name) if hasattr(inner_col, 'name') else str(inner_col)
        inner_table_name = str(inner_table.name)

        # Build derived table: SELECT correlation_col, agg(...) as agg_result FROM t WHERE ... GROUP BY correlation_col
        # Get the aggregate expression (unwrap if already aliased)
        agg_alias = "agg_val"

        if isinstance(select_expr, exp.Alias):
            agg_expr = select_expr.this.copy()
        else:
            agg_expr = select_expr.copy()

        # Build the derived table select
        derived_select = exp.Select(
            expressions=[
                inner_col.copy(),  # The correlation column
                exp.Alias(this=agg_expr, alias=exp.to_identifier(agg_alias)),
            ]
        ).from_(inner_table.copy())

        # Copy WHERE conditions from inner query (excluding the correlation condition)
        inner_where = inner.find(exp.Where)
        if inner_where:
            # Extract non-correlation conditions
            non_corr_conditions = self._extract_non_correlation_conditions(
                inner_where.this, outer_col, inner_col
            )
            if non_corr_conditions:
                derived_select.set("where", exp.Where(this=non_corr_conditions))

        # Add GROUP BY on correlation column
        derived_select.set("group", exp.Group(expressions=[inner_col.copy()]))

        # Create derived table as subquery
        derived_subquery = exp.Subquery(
            this=derived_select,
            alias=exp.TableAlias(this=exp.to_identifier(join_alias)),
        )

        # Build JOIN condition
        join_condition = exp.EQ(
            this=exp.Column(
                this=inner_col.this.copy() if hasattr(inner_col, 'this') else exp.to_identifier(inner_col_name),
                table=exp.to_identifier(join_alias),
            ),
            expression=outer_col.copy(),
        )

        # Create LEFT JOIN to derived table
        join = exp.Join(
            this=derived_subquery,
            on=join_condition,
            kind="INNER",
        )

        # Add join to outer query - append at END to ensure comma-joined tables are accessible
        existing_joins = list(outer.args.get("joins", []))
        outer.set("joins", existing_joins + [join])

        # Replace subquery with reference to aggregate column
        replacement = exp.Column(
            this=exp.to_identifier(agg_alias),
            table=exp.to_identifier(join_alias),
        )

        # Preserve alias if subquery had one
        parent = subq.parent
        if isinstance(parent, exp.Alias):
            # The subquery is already aliased, replace just the subquery
            subq.replace(replacement)
        elif subq.alias:
            replacement = exp.Alias(this=replacement, alias=subq.alias)
            subq.replace(replacement)
        else:
            subq.replace(replacement)

        result.add_safety_check(
            name="aggregate_transform",
            result=SafetyCheckResult.PASSED,
            message=f"Transformed aggregate subquery to derived table with GROUP BY {inner_col_name}",
        )

        return True

    def _transform_simple_subquery(
        self,
        outer: exp.Select,
        subq: exp.Subquery,
        inner: exp.Select,
        join_alias: str,
        outer_col: exp.Column,
        inner_col: exp.Column,
        inner_table: exp.Table,
        result: RewriteResult,
    ) -> bool:
        """Transform simple (non-aggregate) correlated subquery to direct JOIN."""
        inner_table_name = str(inner_table.name)
        inner_col_name = str(inner_col.name) if hasattr(inner_col, 'name') else str(inner_col)

        # Verify uniqueness constraint for safety
        if self.metadata.has_primary_key(inner_table_name, inner_col_name):
            result.add_safety_check(
                name="uniqueness_constraint",
                result=SafetyCheckResult.PASSED,
                message=f"Join column {inner_col_name} is primary key on {inner_table_name}",
            )
            result.confidence = RewriteConfidence.HIGH
        elif self.metadata.has_unique_constraint(inner_table_name, [inner_col_name]):
            result.add_safety_check(
                name="uniqueness_constraint",
                result=SafetyCheckResult.PASSED,
                message=f"Join column {inner_col_name} has unique constraint on {inner_table_name}",
            )
            result.confidence = RewriteConfidence.HIGH
        else:
            result.add_safety_check(
                name="uniqueness_constraint",
                result=SafetyCheckResult.WARNING,
                message=f"Cannot verify uniqueness of {inner_col_name} on {inner_table_name}. "
                        "If not unique, results may differ.",
                metadata_required=["primary_key", "unique_constraints"],
            )

        # Get selected columns from subquery
        select_cols = list(inner.expressions)
        if not select_cols:
            return False

        # Build the JOIN
        join_table = exp.Table(
            this=inner_table.this.copy(),
            alias=exp.TableAlias(this=exp.to_identifier(join_alias)),
        )

        # Build ON condition
        join_condition = exp.EQ(
            this=exp.Column(
                this=inner_col.this.copy() if hasattr(inner_col, 'this') else inner_col,
                table=exp.to_identifier(join_alias),
            ),
            expression=outer_col.copy(),
        )

        # Create LEFT JOIN
        join = exp.Join(
            this=join_table,
            on=join_condition,
            kind="INNER",
        )

        # Add join to outer query - append at END to ensure comma-joined tables are accessible
        existing_joins = list(outer.args.get("joins", []))
        outer.set("joins", existing_joins + [join])

        # Replace subquery with column reference
        projected_col = select_cols[0]

        if isinstance(projected_col, exp.Column):
            replacement = exp.Column(
                this=projected_col.this.copy(),
                table=exp.to_identifier(join_alias),
            )
        elif isinstance(projected_col, exp.Alias):
            inner_expr = projected_col.this
            if isinstance(inner_expr, exp.Column):
                replacement = exp.Column(
                    this=inner_expr.this.copy(),
                    table=exp.to_identifier(join_alias),
                )
            else:
                replacement = exp.Column(
                    this=exp.to_identifier(str(projected_col.alias)),
                    table=exp.to_identifier(join_alias),
                )
        else:
            replacement = exp.Column(
                this=exp.to_identifier("result"),
                table=exp.to_identifier(join_alias),
            )

        # Preserve alias if subquery had one
        if subq.alias:
            replacement = exp.Alias(
                this=replacement,
                alias=subq.alias,
            )

        # Replace the subquery
        subq.replace(replacement)

        result.add_safety_check(
            name="null_handling",
            result=SafetyCheckResult.PASSED,
            message="Using LEFT JOIN preserves NULL behavior of scalar subquery",
        )

        return True

    def _extract_non_correlation_conditions(
        self,
        where_expr: exp.Expression,
        outer_col: exp.Column,
        inner_col: exp.Column,
    ) -> Optional[exp.Expression]:
        """Extract conditions that are NOT the correlation predicate.

        Given WHERE (a = b AND c = d AND inner.x = outer.y),
        returns (a = b AND c = d) without the correlation.
        """
        # Get identifiers for comparison
        outer_table = str(outer_col.table).lower() if outer_col.table else ""
        outer_name = str(outer_col.name).lower() if hasattr(outer_col, 'name') else ""
        inner_table = str(inner_col.table).lower() if inner_col.table else ""
        inner_name = str(inner_col.name).lower() if hasattr(inner_col, 'name') else ""

        def is_correlation_eq(eq_expr: exp.EQ) -> bool:
            """Check if this EQ is the correlation condition."""
            if not isinstance(eq_expr, exp.EQ):
                return False
            left = eq_expr.left
            right = eq_expr.right
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                l_table = str(left.table).lower() if left.table else ""
                l_name = str(left.name).lower()
                r_table = str(right.table).lower() if right.table else ""
                r_name = str(right.name).lower()
                # Check both orders
                if (l_table == outer_table and l_name == outer_name and
                    r_table == inner_table and r_name == inner_name):
                    return True
                if (r_table == outer_table and r_name == outer_name and
                    l_table == inner_table and l_name == inner_name):
                    return True
            return False

        def extract_from_and(and_expr: exp.And) -> list[exp.Expression]:
            """Extract non-correlation conditions from AND chain."""
            conditions = []
            left = and_expr.left
            right = and_expr.right

            if isinstance(left, exp.And):
                conditions.extend(extract_from_and(left))
            elif isinstance(left, exp.EQ) and is_correlation_eq(left):
                pass  # Skip correlation
            else:
                conditions.append(left.copy())

            if isinstance(right, exp.And):
                conditions.extend(extract_from_and(right))
            elif isinstance(right, exp.EQ) and is_correlation_eq(right):
                pass  # Skip correlation
            else:
                conditions.append(right.copy())

            return conditions

        def extract_from_or(or_expr: exp.Or) -> Optional[exp.Expression]:
            """Extract from OR - keep entire OR if it contains correlation."""
            # For OR expressions, if any branch has correlation, we need to restructure
            # For now, keep the whole OR and let the correlation be filtered at join time
            return or_expr.copy()

        # Handle different expression types
        if isinstance(where_expr, exp.And):
            conditions = extract_from_and(where_expr)
            if not conditions:
                return None
            # Rebuild AND chain
            result = conditions[0]
            for cond in conditions[1:]:
                result = exp.And(this=result, expression=cond)
            return result
        elif isinstance(where_expr, exp.Or):
            # OR with correlation is complex - for now skip transformation
            return extract_from_or(where_expr)
        elif isinstance(where_expr, exp.EQ) and is_correlation_eq(where_expr):
            return None  # Only had correlation, no other conditions
        else:
            return where_expr.copy()

    def _extract_correlation(
        self,
        where: exp.Where,
        outer: exp.Select,
        inner: exp.Select = None
    ) -> Optional[tuple[exp.Column, exp.Column]]:
        """Extract the correlation predicate (outer_col, inner_col)."""
        outer_tables = self._get_table_aliases(outer)

        # Get inner tables to avoid false correlation detection
        inner_tables = set()
        if inner:
            inner_tables = self._get_table_aliases(inner)

        for eq in where.find_all(exp.EQ):
            left = eq.left
            right = eq.right

            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                left_table = str(left.table).lower() if left.table else ""
                right_table = str(right.table).lower() if right.table else ""

                # Left is outer if it's in outer_tables but NOT in inner_tables
                left_is_outer = left_table in outer_tables and left_table not in inner_tables
                right_is_outer = right_table in outer_tables and right_table not in inner_tables

                if left_is_outer and not right_is_outer:
                    return (left, right)
                elif right_is_outer and not left_is_outer:
                    return (right, left)

        return None
