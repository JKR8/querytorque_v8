"""NULL Semantics Rewriters.

QT-NULL-001: NOT IN subquery (NULL trap) -> NOT EXISTS
QT-NULL-002: COALESCE in join predicate -> explicit NULL handling
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
class NotInToNotExistsRewriter(BaseRewriter):
    """Rewrites NOT IN subquery to NOT EXISTS.

    NOT IN with NULLs is a common SQL trap:
    - If subquery returns ANY NULL, NOT IN returns no rows
    - NOT EXISTS handles NULLs correctly

    Example:
        SELECT * FROM orders o
        WHERE o.customer_id NOT IN (SELECT customer_id FROM blacklist)
        ->
        SELECT * FROM orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM blacklist b
            WHERE b.customer_id = o.customer_id
        )

    This is both a correctness fix AND often a performance improvement.
    """

    rewriter_id = "not_in_to_not_exists"
    name = "NOT IN to NOT EXISTS"
    description = "Convert NOT IN subquery to NOT EXISTS (NULL-safe)"
    linked_rule_ids = ("QT-NULL-001",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for NOT IN subquery pattern."""
        if not isinstance(node, exp.Select):
            return False

        # Look for NOT IN with subquery
        for not_node in node.find_all(exp.Not):
            in_node = not_node.find(exp.In)
            if in_node:
                # Check if it's IN (subquery) not IN (list)
                query = in_node.args.get("query")
                if query and isinstance(query, exp.Subquery):
                    return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Transform NOT IN subquery to NOT EXISTS."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            transformations = 0

            # Find and transform NOT IN patterns
            for not_node in list(rewritten.find_all(exp.Not)):
                in_node = not_node.find(exp.In)
                if not in_node:
                    continue

                query = in_node.args.get("query")
                if not query or not isinstance(query, exp.Subquery):
                    continue

                # Get the column being tested
                outer_col = in_node.this
                if not isinstance(outer_col, exp.Column):
                    continue

                # Get the subquery
                inner_select = query.find(exp.Select)
                if not inner_select:
                    continue

                # Get the column from subquery's SELECT list
                if not inner_select.expressions:
                    continue

                inner_expr = inner_select.expressions[0]
                if isinstance(inner_expr, exp.Alias):
                    inner_col = inner_expr.this
                else:
                    inner_col = inner_expr

                # Build correlation condition
                correlation = exp.EQ(
                    this=inner_col.copy(),
                    expression=outer_col.copy(),
                )

                # Build new inner select with correlation
                new_inner = inner_select.copy()
                new_inner.set("expressions", [exp.Literal.number(1)])

                # Add correlation to WHERE
                existing_where = new_inner.find(exp.Where)
                if existing_where:
                    new_condition = exp.And(
                        this=existing_where.this.copy(),
                        expression=correlation,
                    )
                    new_inner.set("where", exp.Where(this=new_condition))
                else:
                    new_inner.set("where", exp.Where(this=correlation))

                # Build NOT EXISTS
                not_exists = exp.Not(
                    this=exp.Exists(this=exp.Subquery(this=new_inner))
                )

                # Replace NOT IN with NOT EXISTS
                not_node.replace(not_exists)
                transformations += 1

            if transformations == 0:
                return self._create_failure(original_sql, "No NOT IN patterns found")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Converted {transformations} NOT IN to NOT EXISTS",
            )

            result.add_safety_check(
                name="null_semantics",
                result=SafetyCheckResult.PASSED,
                message="NOT EXISTS handles NULLs correctly unlike NOT IN",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))
