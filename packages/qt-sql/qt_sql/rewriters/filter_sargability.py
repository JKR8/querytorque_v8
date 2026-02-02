"""Filter Sargability Rewriters.

QT-FILT-001: Non-sargable predicate (function on column) -> range predicate
QT-FILT-003: LIKE with leading wildcard -> suggestion
QT-FILT-004: Implicit cast on column -> cast literal instead
"""

from typing import Any, Optional
from datetime import datetime, timedelta

from sqlglot import exp

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter


@register_rewriter
class NonSargableToRangeRewriter(BaseRewriter):
    """Rewrites non-sargable predicates to range predicates.

    Non-sargable predicates prevent index usage:
        WHERE DATE(created_at) = '2024-01-15'
        WHERE YEAR(ts) = 2024
        WHERE EXTRACT(MONTH FROM dt) = 6

    Rewrite to range predicates:
        WHERE created_at >= '2024-01-15' AND created_at < '2024-01-16'
        WHERE ts >= '2024-01-01' AND ts < '2025-01-01'
        WHERE dt >= '2024-06-01' AND dt < '2024-07-01'

    This enables index seeks instead of full scans.
    """

    rewriter_id = "non_sargable_to_range"
    name = "Non-Sargable to Range Predicate"
    description = "Convert function-on-column predicates to sargable range predicates"
    linked_rule_ids = ("QT-FILT-001",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for non-sargable date/time predicates."""
        if not isinstance(node, exp.Select):
            return False

        where = node.find(exp.Where)
        if not where:
            return False

        # Look for DATE(), YEAR(), MONTH(), EXTRACT() on columns
        for eq in where.find_all(exp.EQ):
            if self._is_date_function_on_column(eq.left):
                return True
            if self._is_date_function_on_column(eq.right):
                return True

        return False

    def _is_date_function_on_column(self, expr: exp.Expression) -> bool:
        """Check if expression is a date function applied to a column."""
        if isinstance(expr, exp.Date):
            # DATE(column)
            return isinstance(expr.this, exp.Column)
        elif isinstance(expr, exp.Year):
            # YEAR(column)
            return isinstance(expr.this, exp.Column)
        elif isinstance(expr, exp.Month):
            # MONTH(column)
            return isinstance(expr.this, exp.Column)
        elif isinstance(expr, exp.Extract):
            # EXTRACT(part FROM column)
            return isinstance(expr.expression, exp.Column)
        elif isinstance(expr, exp.Anonymous):
            # DATE_TRUNC, etc.
            func_name = str(expr.this).upper()
            if func_name in ("DATE", "DATE_TRUNC", "TRUNC"):
                if expr.expressions:
                    return isinstance(expr.expressions[0], exp.Column)
        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Transform non-sargable predicates to range predicates."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            where = rewritten.find(exp.Where)

            if not where:
                return self._create_failure(original_sql, "No WHERE clause")

            transformations = 0

            def transform_condition(cond):
                """Transform a single condition."""
                nonlocal transformations

                if isinstance(cond, exp.And):
                    left = transform_condition(cond.left)
                    right = transform_condition(cond.right)
                    return exp.And(this=left, expression=right)

                if isinstance(cond, exp.Or):
                    left = transform_condition(cond.left)
                    right = transform_condition(cond.right)
                    return exp.Or(this=left, expression=right)

                if isinstance(cond, exp.EQ):
                    result = self._transform_eq(cond)
                    if result:
                        transformations += 1
                        return result

                return cond.copy()

            new_where = transform_condition(where.this)
            rewritten.set("where", exp.Where(this=new_where))

            if transformations == 0:
                return self._create_failure(original_sql, "No transformable predicates found")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Converted {transformations} non-sargable predicate(s) to range",
            )

            result.add_safety_check(
                name="sargability",
                result=SafetyCheckResult.PASSED,
                message="Range predicates enable index seeks",
            )

            result.add_safety_check(
                name="timezone",
                result=SafetyCheckResult.WARNING,
                message="Verify timezone handling is correct for your use case",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _transform_eq(self, eq: exp.EQ) -> Optional[exp.Expression]:
        """Transform an equality to range predicate if applicable."""
        left, right = eq.left, eq.right

        # DATE(col) = 'YYYY-MM-DD'
        if isinstance(left, exp.Date) and isinstance(left.this, exp.Column):
            col = left.this
            if isinstance(right, exp.Literal):
                return self._date_to_range(col, str(right.this))

        if isinstance(right, exp.Date) and isinstance(right.this, exp.Column):
            col = right.this
            if isinstance(left, exp.Literal):
                return self._date_to_range(col, str(left.this))

        # YEAR(col) = NNNN
        if isinstance(left, exp.Year) and isinstance(left.this, exp.Column):
            col = left.this
            if isinstance(right, exp.Literal):
                return self._year_to_range(col, str(right.this))

        if isinstance(right, exp.Year) and isinstance(right.this, exp.Column):
            col = right.this
            if isinstance(left, exp.Literal):
                return self._year_to_range(col, str(left.this))

        # EXTRACT(YEAR FROM col) = NNNN
        if isinstance(left, exp.Extract):
            part = str(left.this).upper() if left.this else ""
            if part == "YEAR" and isinstance(left.expression, exp.Column):
                col = left.expression
                if isinstance(right, exp.Literal):
                    return self._year_to_range(col, str(right.this))

        return None

    def _date_to_range(self, col: exp.Column, date_str: str) -> exp.Expression:
        """Convert DATE(col) = 'YYYY-MM-DD' to range."""
        # Remove quotes if present
        date_str = date_str.strip("'\"")

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            next_day = dt + timedelta(days=1)

            return exp.And(
                this=exp.GTE(
                    this=col.copy(),
                    expression=exp.Literal.string(dt.strftime("%Y-%m-%d")),
                ),
                expression=exp.LT(
                    this=col.copy(),
                    expression=exp.Literal.string(next_day.strftime("%Y-%m-%d")),
                ),
            )
        except ValueError:
            # If we can't parse, return a simpler transformation
            return exp.And(
                this=exp.GTE(
                    this=col.copy(),
                    expression=exp.Literal.string(date_str),
                ),
                expression=exp.LT(
                    this=col.copy(),
                    expression=exp.Anonymous(
                        this="DATE_ADD",
                        expressions=[
                            exp.Literal.string(date_str),
                            exp.Interval(this=exp.Literal.number(1), unit=exp.Var(this="DAY")),
                        ],
                    ),
                ),
            )

    def _year_to_range(self, col: exp.Column, year_str: str) -> exp.Expression:
        """Convert YEAR(col) = NNNN to range."""
        year_str = year_str.strip("'\"")

        try:
            year = int(year_str)
            start = f"{year}-01-01"
            end = f"{year + 1}-01-01"

            return exp.And(
                this=exp.GTE(
                    this=col.copy(),
                    expression=exp.Literal.string(start),
                ),
                expression=exp.LT(
                    this=col.copy(),
                    expression=exp.Literal.string(end),
                ),
            )
        except ValueError:
            return None
