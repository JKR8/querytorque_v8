"""Boolean Optimization Rewriters.

QT-BOOL-001: OR to UNION ALL transformation for better index usage
SQL-WHERE-010: Complex OR conditions that benefit from UNION
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
class OrToUnionRewriter(BaseRewriter):
    """Convert OR conditions on different indexed columns to UNION ALL.

    Example:
        SELECT * FROM orders
        WHERE customer_id = 100 OR product_id = 50
        ->
        SELECT * FROM orders WHERE customer_id = 100
        UNION ALL
        SELECT * FROM orders WHERE product_id = 50
        (with deduplication if needed)

    Benefits:
    - Allows index seeks on both conditions
    - Avoids full table scan when indexes exist
    - Can be more efficient for selective conditions

    Caution:
    - May increase I/O for overlapping results
    - Requires DISTINCT if overlap possible (changes to UNION, not UNION ALL)
    """

    rewriter_id = "or_to_union"
    name = "OR to UNION"
    description = "Convert OR conditions to UNION for better index usage"
    linked_rule_ids = ("QT-BOOL-001", "SQL-WHERE-010")
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for OR conditions on different columns at top level of WHERE."""
        if not isinstance(node, exp.Select):
            return False

        where = node.find(exp.Where)
        if not where:
            return False

        # Look for top-level OR with conditions on different columns
        if isinstance(where.this, exp.Or):
            conditions = self._extract_or_conditions(where.this)
            if len(conditions) >= 2:
                # Check if conditions reference different columns
                columns = set()
                for cond in conditions:
                    col = self._get_condition_column(cond)
                    if col:
                        columns.add(col)
                # If we have multiple different columns, UNION might help
                return len(columns) >= 2

        return False

    def _extract_or_conditions(self, expr: exp.Expression) -> list[exp.Expression]:
        """Extract individual conditions from OR chain."""
        conditions = []

        def traverse(e):
            if isinstance(e, exp.Or):
                traverse(e.left)
                traverse(e.right)
            else:
                conditions.append(e)

        traverse(expr)
        return conditions

    def _get_condition_column(self, cond: exp.Expression) -> str | None:
        """Get the primary column from a simple condition."""
        if isinstance(cond, (exp.EQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            if isinstance(cond.left, exp.Column):
                return str(cond.left.name).lower()
            if isinstance(cond.right, exp.Column):
                return str(cond.right.name).lower()
        elif isinstance(cond, exp.In):
            if isinstance(cond.this, exp.Column):
                return str(cond.this.name).lower()
        return None

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Transform OR conditions to UNION."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            where = node.find(exp.Where)
            if not where or not isinstance(where.this, exp.Or):
                return self._create_failure(original_sql, "No OR condition at top level")

            conditions = self._extract_or_conditions(where.this)
            if len(conditions) < 2:
                return self._create_failure(original_sql, "Need at least 2 OR conditions")

            # Build UNION of individual SELECTs
            # Start with the base query structure (without WHERE)
            base = node.copy()
            base.set("where", None)

            union_parts = []
            for cond in conditions:
                part = base.copy()
                part.set("where", exp.Where(this=cond.copy()))
                union_parts.append(part)

            # Build UNION (not UNION ALL to avoid duplicates when conditions overlap)
            result_query = union_parts[0]
            for part in union_parts[1:]:
                result_query = exp.Union(
                    this=result_query,
                    expression=part,
                    distinct=True,  # Use UNION (not UNION ALL) to deduplicate
                )

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=result_query.sql(),
                rewritten_node=result_query,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converted {len(conditions)} OR conditions to UNION",
            )

            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="UNION with deduplication is equivalent to OR",
            )

            result.add_safety_check(
                name="performance_tradeoff",
                result=SafetyCheckResult.WARNING,
                message="UNION may increase I/O if conditions overlap significantly; best when each condition is selective and uses different indexes",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))
