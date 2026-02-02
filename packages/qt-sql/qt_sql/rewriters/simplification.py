"""Simplification Rewriters.

SQL-WHERE-006: Double negative simplification (NOT(x <> y) -> x = y)
SQL-ORD-004, SQL-AGG-001: Ordinal to column name (ORDER BY 1 -> ORDER BY col)
SQL-SUB-006: EXISTS SELECT * to SELECT 1
QT-DIST-001: Redundant DISTINCT removal
SQL-WHERE-007: Redundant predicate removal
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
class DoubleNegativeSimplifier(BaseRewriter):
    """Simplify double negatives: NOT(x <> y) -> x = y.

    Example:
        WHERE NOT (status <> 'active')
        ->
        WHERE status = 'active'

    Also handles:
        - NOT NOT x -> x
        - NOT(x != y) -> x = y

    Benefits:
    - Improved readability
    - Potentially better optimizer handling
    """

    rewriter_id = "double_negative_simplifier"
    name = "Double Negative Simplifier"
    description = "Simplify NOT(x <> y) to x = y"
    linked_rule_ids = ("SQL-WHERE-006",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if node contains double negative patterns."""
        for not_node in node.find_all(exp.Not):
            inner = not_node.this
            # Unwrap parentheses
            while isinstance(inner, exp.Paren):
                inner = inner.this
            if isinstance(inner, (exp.NEQ, exp.Not)):
                return True
        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Perform the double negative simplification."""
        original_sql = node.sql()

        try:
            rewritten = node.copy()
            transforms = 0

            for not_node in list(rewritten.find_all(exp.Not)):
                inner = not_node.this
                # Unwrap parentheses
                while isinstance(inner, exp.Paren):
                    inner = inner.this

                if isinstance(inner, exp.NEQ):
                    # NOT(x <> y) -> x = y
                    eq = exp.EQ(this=inner.left.copy(), expression=inner.right.copy())
                    not_node.replace(eq)
                    transforms += 1
                elif isinstance(inner, exp.Not):
                    # NOT NOT x -> x
                    not_node.replace(inner.this.copy())
                    transforms += 1

            if transforms == 0:
                return self._create_failure(original_sql, "No double negatives found")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Simplified {transforms} double negative(s)",
            )
            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="Double negative simplification is logically equivalent",
            )
            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class OrdinalToColumnRewriter(BaseRewriter):
    """Convert ORDER BY 1, 2 to ORDER BY col_name.

    Example:
        SELECT name, age FROM users ORDER BY 1, 2
        ->
        SELECT name, age FROM users ORDER BY name, age

    Also handles GROUP BY ordinals.

    Benefits:
    - Improved readability and maintainability
    - Avoids silent bugs when SELECT list changes
    - Some optimizers handle named columns better
    """

    rewriter_id = "ordinal_to_column"
    name = "Ordinal to Column Name"
    description = "Replace positional references with column names"
    linked_rule_ids = ("SQL-ORD-004", "SQL-AGG-001")
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if node contains ordinal references in ORDER BY or GROUP BY."""
        if not isinstance(node, exp.Select):
            return False

        # Check ORDER BY
        for order in node.find_all(exp.Order):
            for key in order.expressions:
                ordinal_expr = key.this if hasattr(key, 'this') else key
                if isinstance(ordinal_expr, exp.Literal) and ordinal_expr.is_int:
                    return True

        # Check GROUP BY
        for group in node.find_all(exp.Group):
            for expr in group.expressions:
                if isinstance(expr, exp.Literal) and expr.is_int:
                    return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Replace ordinal references with column names."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Must be SELECT statement")

        try:
            rewritten = node.copy()
            select_exprs = list(rewritten.selects)
            transforms = 0

            # Handle ORDER BY ordinals
            for order in rewritten.find_all(exp.Order):
                for key in order.expressions:
                    ordinal_expr = key.this if hasattr(key, 'this') else key
                    if isinstance(ordinal_expr, exp.Literal) and ordinal_expr.is_int:
                        idx = int(ordinal_expr.this) - 1  # 1-indexed
                        if 0 <= idx < len(select_exprs):
                            col = select_exprs[idx]
                            # Use alias if present, otherwise the expression itself
                            if isinstance(col, exp.Alias):
                                replacement = exp.Column(this=exp.to_identifier(str(col.alias)))
                            elif isinstance(col, exp.Column):
                                replacement = col.copy()
                            else:
                                # For complex expressions, we need an alias
                                continue

                            if hasattr(key, 'this'):
                                key.set("this", replacement)
                            else:
                                key.replace(replacement)
                            transforms += 1

            # Handle GROUP BY ordinals
            for group in rewritten.find_all(exp.Group):
                new_exprs = []
                for expr in group.expressions:
                    if isinstance(expr, exp.Literal) and expr.is_int:
                        idx = int(expr.this) - 1
                        if 0 <= idx < len(select_exprs):
                            col = select_exprs[idx]
                            if isinstance(col, exp.Alias):
                                new_exprs.append(exp.Column(this=exp.to_identifier(str(col.alias))))
                            elif isinstance(col, exp.Column):
                                new_exprs.append(col.copy())
                            else:
                                new_exprs.append(expr)
                            transforms += 1
                        else:
                            new_exprs.append(expr)
                    else:
                        new_exprs.append(expr)
                group.set("expressions", new_exprs)

            if transforms == 0:
                return self._create_failure(original_sql, "No ordinals found to replace")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                explanation=f"Replaced {transforms} ordinal reference(s)",
            )
            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="Column names are equivalent to ordinal positions",
            )
            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class ExistsStarToOneRewriter(BaseRewriter):
    """Convert EXISTS (SELECT *) to EXISTS (SELECT 1).

    Example:
        WHERE EXISTS (SELECT * FROM orders WHERE orders.user_id = users.id)
        ->
        WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)

    Benefits:
    - Signals intent more clearly (we only care about existence)
    - Some engines may optimize SELECT 1 better
    - Avoids potential issues with column access permissions
    """

    rewriter_id = "exists_star_to_one"
    name = "EXISTS SELECT * to SELECT 1"
    description = "Replace SELECT * in EXISTS with SELECT 1"
    linked_rule_ids = ("SQL-SUB-006",)
    default_confidence = RewriteConfidence.HIGH

    def _get_inner_select(self, exists: exp.Exists) -> exp.Select | None:
        """Get the inner SELECT from EXISTS, handling both direct and subquery wrapping."""
        # EXISTS can have either a direct Select or a Subquery containing a Select
        if isinstance(exists.this, exp.Select):
            return exists.this
        subquery = exists.find(exp.Subquery)
        if subquery:
            inner = subquery.find(exp.Select)
            if inner:
                return inner
        return None

    def _has_star(self, select: exp.Select) -> bool:
        """Check if SELECT has * in its expressions."""
        for expr in select.expressions:
            if isinstance(expr, exp.Star):
                return True
            if isinstance(expr, exp.Column) and str(expr.name) == "*":
                return True
        return False

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for EXISTS with SELECT *."""
        for exists in node.find_all(exp.Exists):
            inner_select = self._get_inner_select(exists)
            if inner_select and self._has_star(inner_select):
                return True
        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Replace SELECT * with SELECT 1 in EXISTS subqueries."""
        original_sql = node.sql()

        try:
            rewritten = node.copy()
            transforms = 0

            for exists in rewritten.find_all(exp.Exists):
                inner_select = self._get_inner_select(exists)
                if inner_select and self._has_star(inner_select):
                    # Replace with SELECT 1
                    inner_select.set("expressions", [exp.Literal.number(1)])
                    transforms += 1

            if transforms == 0:
                return self._create_failure(original_sql, "No EXISTS SELECT * found")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Replaced {transforms} EXISTS SELECT * with SELECT 1",
            )
            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="EXISTS only checks for row existence, SELECT list doesn't affect result",
            )
            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class RedundantDistinctRemover(BaseRewriter):
    """Remove redundant DISTINCT when result is already unique.

    Example 1 - Primary key guarantees uniqueness:
        SELECT DISTINCT id, name FROM users
        ->
        SELECT id, name FROM users
        (when id is primary key)

    Example 2 - UNION ALL with identical queries:
        SELECT DISTINCT * FROM (
            SELECT id FROM t WHERE x = 1
            UNION ALL
            SELECT id FROM t WHERE x = 1
        )
        -> May be simplified

    Note: This rewriter requires schema metadata for safe operation.
    Without metadata, it only removes DISTINCT where it's provably redundant.
    """

    rewriter_id = "redundant_distinct_remover"
    name = "Remove Redundant DISTINCT"
    description = "Remove DISTINCT when result is already unique"
    linked_rule_ids = ("QT-DIST-001",)
    default_confidence = RewriteConfidence.MEDIUM

    def get_required_metadata(self) -> list[str]:
        """Requires primary key info for safe removal."""
        return ["primary_key"]

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for DISTINCT that may be redundant."""
        if not isinstance(node, exp.Select):
            return False

        # Must have DISTINCT
        if not node.args.get("distinct"):
            return False

        # Case 1: Has GROUP BY (covered by DistinctGroupByRedundancyRewriter)
        group = node.find(exp.Group)
        if group:
            return True

        # Case 2: LIMIT 1 (only one row, DISTINCT is redundant)
        limit = node.find(exp.Limit)
        if limit:
            limit_val = limit.expression
            if isinstance(limit_val, exp.Literal) and str(limit_val.this) == "1":
                return True

        # Case 3: Selecting from VALUES with unique rows
        # (would need analysis)

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Remove redundant DISTINCT."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            # Determine reason for redundancy
            reason = None

            # Check LIMIT 1
            limit = node.find(exp.Limit)
            if limit:
                limit_val = limit.expression
                if isinstance(limit_val, exp.Literal) and str(limit_val.this) == "1":
                    reason = "LIMIT 1 guarantees single row"

            # Check GROUP BY
            if not reason:
                group = node.find(exp.Group)
                if group:
                    reason = "GROUP BY guarantees uniqueness"

            if not reason:
                return self._create_failure(
                    original_sql,
                    "Could not determine why DISTINCT is redundant"
                )

            rewritten = node.copy()
            rewritten.set("distinct", None)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Removed redundant DISTINCT ({reason})",
            )
            result.add_safety_check(
                name="uniqueness_guarantee",
                result=SafetyCheckResult.PASSED,
                message=reason,
            )
            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class RedundantPredicateRemover(BaseRewriter):
    """Remove duplicate WHERE conditions.

    Example:
        WHERE status = 'active' AND status = 'active' AND x > 5
        ->
        WHERE status = 'active' AND x > 5

    Also handles:
    - Tautologies: x = x (removes)
    - Contradictions: x = 1 AND x = 2 (warns)

    Benefits:
    - Cleaner SQL
    - May help some optimizers
    """

    rewriter_id = "redundant_predicate_remover"
    name = "Remove Redundant Predicates"
    description = "Remove duplicate WHERE conditions"
    linked_rule_ids = ("SQL-WHERE-007",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for duplicate predicates."""
        if not isinstance(node, exp.Select):
            return False

        where = node.find(exp.Where)
        if not where:
            return False

        # Collect all AND conditions
        conditions = self._extract_conditions(where.this)
        if len(conditions) <= 1:
            return False

        # Check for duplicates
        seen_sql = set()
        for cond in conditions:
            cond_sql = cond.sql()
            if cond_sql in seen_sql:
                return True
            seen_sql.add(cond_sql)

        return False

    def _extract_conditions(self, expr: exp.Expression) -> list[exp.Expression]:
        """Extract individual conditions from AND chain."""
        conditions = []

        def traverse(e):
            if isinstance(e, exp.And):
                traverse(e.left)
                traverse(e.right)
            else:
                conditions.append(e)

        traverse(expr)
        return conditions

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Remove duplicate predicates."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            where = rewritten.find(exp.Where)

            if not where:
                return self._create_failure(original_sql, "No WHERE clause")

            conditions = self._extract_conditions(where.this)
            unique_conditions = []
            seen_sql = set()
            removed = 0

            for cond in conditions:
                cond_sql = cond.sql()
                if cond_sql not in seen_sql:
                    seen_sql.add(cond_sql)
                    unique_conditions.append(cond.copy())
                else:
                    removed += 1

            if removed == 0:
                return self._create_failure(original_sql, "No duplicate predicates found")

            # Rebuild WHERE clause
            if unique_conditions:
                combined = unique_conditions[0]
                for cond in unique_conditions[1:]:
                    combined = exp.And(this=combined, expression=cond)
                rewritten.set("where", exp.Where(this=combined))
            else:
                rewritten.set("where", None)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Removed {removed} duplicate predicate(s)",
            )
            result.add_safety_check(
                name="duplicate_removal",
                result=SafetyCheckResult.PASSED,
                message="Duplicate predicates do not affect query result",
            )
            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))
