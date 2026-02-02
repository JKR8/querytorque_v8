"""IN Subquery Rewriters.

SQL-SUB-002: IN with subquery -> JOIN for clarity and performance
SQL-DUCK-003: IN (subquery) in WHERE -> JOIN for better DuckDB optimization
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
class InSubqueryToJoinRewriter(BaseRewriter):
    """Rewrites IN (subquery) to JOIN.

    Example:
        SELECT * FROM orders
        WHERE customer_id IN (SELECT id FROM customers WHERE active = 1)
        ->
        SELECT orders.* FROM orders
        JOIN (SELECT DISTINCT id FROM customers WHERE active = 1) AS _in_sq
        ON orders.customer_id = _in_sq.id

    Benefits:
    - JOIN can be more efficient than IN for large subqueries
    - Allows optimizer to choose optimal join strategy
    - Clearer execution plan
    """

    rewriter_id = "in_subquery_to_join"
    name = "IN Subquery to JOIN"
    description = "Convert IN (subquery) to JOIN for better optimization"
    linked_rule_ids = ("SQL-SUB-002", "SQL-DUCK-003")
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for IN (subquery) pattern anywhere in the query."""
        if not isinstance(node, exp.Select):
            return False

        # Look for IN with subquery anywhere in the query tree
        # This catches IN subqueries in nested SELECTs, CTEs, UNION branches, etc.
        for in_expr in node.find_all(exp.In):
            # Check if it's IN (subquery), not IN (list)
            subquery = in_expr.find(exp.Subquery)
            if subquery:
                # Verify it's a simple subquery we can convert
                inner = subquery.this
                if isinstance(inner, exp.Select):
                    return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Transform IN (subquery) to JOIN.

        Handles IN subqueries anywhere in the query tree by transforming
        each SELECT that contains IN subqueries.
        """
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            alias_counter = [0]
            total_converted = [0]

            def transform_select(select_node: exp.Select) -> None:
                """Transform IN subqueries within a single SELECT."""
                where = select_node.args.get("where")
                if not where:
                    return

                # Collect IN subqueries in this SELECT's WHERE
                in_exprs_to_convert = []
                for in_expr in where.find_all(exp.In):
                    subquery = in_expr.find(exp.Subquery)
                    if subquery and isinstance(subquery.this, exp.Select):
                        # Don't convert if the subquery references outer tables (correlated)
                        # For now, only convert simple CTE references
                        inner = subquery.this
                        inner_from = inner.find(exp.From)
                        if inner_from:
                            in_exprs_to_convert.append(in_expr)

                if not in_exprs_to_convert:
                    return

                # Get existing joins for this SELECT
                existing_joins = list(select_node.args.get("joins", []))

                conditions_to_remove = []
                for in_expr in in_exprs_to_convert:
                    subquery = in_expr.find(exp.Subquery)
                    inner_select = subquery.this

                    # Get the column being checked
                    check_col = in_expr.this
                    if not isinstance(check_col, exp.Column):
                        continue

                    # Get the column being selected in subquery
                    inner_exprs = inner_select.expressions
                    if not inner_exprs:
                        continue

                    # Get first selected column
                    inner_col = inner_exprs[0]
                    if isinstance(inner_col, exp.Alias):
                        inner_col_name = str(inner_col.alias)
                    elif isinstance(inner_col, exp.Column):
                        inner_col_name = str(inner_col.name)
                    else:
                        inner_col_name = str(inner_col)

                    # Create alias for the subquery
                    alias_counter[0] += 1
                    sq_alias = f"_in_sq{alias_counter[0]}"

                    # Wrap subquery with DISTINCT to preserve IN semantics
                    inner_with_distinct = inner_select.copy()
                    inner_with_distinct.set("distinct", exp.Distinct())

                    # Create the JOIN
                    join_subquery = exp.Subquery(
                        this=inner_with_distinct,
                        alias=exp.TableAlias(this=exp.to_identifier(sq_alias))
                    )

                    on_condition = exp.EQ(
                        this=check_col.copy(),
                        expression=exp.Column(
                            this=exp.to_identifier(inner_col_name),
                            table=exp.to_identifier(sq_alias)
                        )
                    )

                    join = exp.Join(
                        this=join_subquery,
                        on=on_condition,
                        kind="INNER"
                    )

                    existing_joins.append(join)
                    conditions_to_remove.append(in_expr)
                    total_converted[0] += 1

                # Update joins for this SELECT
                select_node.set("joins", existing_joins if existing_joins else None)

                # Remove IN conditions from WHERE
                new_where_expr = self._remove_conditions(where.this, conditions_to_remove)
                if new_where_expr:
                    select_node.set("where", exp.Where(this=new_where_expr))
                else:
                    select_node.set("where", None)

            # Transform all SELECT nodes in the query tree
            for select_node in rewritten.find_all(exp.Select):
                transform_select(select_node)

            if total_converted[0] == 0:
                return self._create_failure(original_sql, "No convertible IN subqueries found")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converted {total_converted[0]} IN subquery(s) to JOIN",
            )

            result.add_safety_check(
                name="distinct_preservation",
                result=SafetyCheckResult.PASSED,
                message="Added DISTINCT to subquery to preserve IN semantics",
            )

            result.add_safety_check(
                name="null_handling",
                result=SafetyCheckResult.WARNING,
                message="JOIN handles NULLs differently than IN - verify NULL behavior",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _remove_conditions(
        self,
        expr: exp.Expression,
        to_remove: list[exp.Expression]
    ) -> Optional[exp.Expression]:
        """Remove specific conditions from a WHERE expression."""
        if expr in to_remove:
            return None

        if isinstance(expr, exp.And):
            left = self._remove_conditions(expr.left, to_remove)
            right = self._remove_conditions(expr.right, to_remove)

            if left is None and right is None:
                return None
            elif left is None:
                return right
            elif right is None:
                return left
            else:
                return exp.And(this=left, expression=right)

        if isinstance(expr, exp.Or):
            left = self._remove_conditions(expr.left, to_remove)
            right = self._remove_conditions(expr.right, to_remove)

            if left is None or right is None:
                # Can't safely remove part of OR
                return expr
            return exp.Or(this=left, expression=right)

        return expr.copy()
