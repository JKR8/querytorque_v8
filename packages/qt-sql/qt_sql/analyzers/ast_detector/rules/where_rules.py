"""WHERE clause anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


# Functions that prevent index usage when applied to columns
NON_SARGABLE_FUNCTIONS = (
    exp.Year, exp.Month, exp.Day,
    exp.Upper, exp.Lower, exp.Trim,
    exp.Substring, exp.Left, exp.Right,
    exp.Cast, exp.TryCast,
    exp.Coalesce,
    exp.DateTrunc,
    exp.Abs, exp.Round, exp.Floor, exp.Ceil,
    exp.Length, exp.Replace,
)


class FunctionOnColumnRule(ASTRule):
    """SQL-WHERE-001: Detect function applied to column in WHERE clause.

    Applying a function to a column prevents index usage (non-sargable):
        WHERE YEAR(created_at) = 2024  -- Full table scan
        WHERE created_at >= '2024-01-01'  -- Can use index

    This rule detects functions wrapping columns in WHERE/HAVING clauses.

    False Positive Prevention:
    - Only flags when function wraps a Column node directly
    - Functions on literals are fine (optimizer can fold them)
    - Functions in SELECT list are not performance issues
    """

    rule_id = "SQL-WHERE-001"
    name = "Function on Column in WHERE"
    severity = "high"
    category = "where_clause"
    penalty = 15
    description = "Function on column prevents index usage (non-sargable)"
    suggestion = "Rewrite to apply function to the literal value instead"

    target_node_types = NON_SARGABLE_FUNCTIONS

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check if we're inside WHERE or HAVING clause
        if not (context.in_where or context.in_having):
            return

        # Check if this function directly wraps a column
        if self._wraps_column(node):
            func_name = type(node).__name__.upper()
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{func_name}() on column prevents index usage",
                matched_text=node.sql()[:80],
            )

    def _wraps_column(self, node: exp.Expression) -> bool:
        """Check if function's first argument is a column reference."""
        # Most functions have 'this' as the main argument
        if hasattr(node, 'this'):
            arg = node.this
            if isinstance(arg, exp.Column):
                return True
            # Handle nested: UPPER(TRIM(column))
            if isinstance(arg, NON_SARGABLE_FUNCTIONS):
                return self._wraps_column(arg)

        # Some functions use 'expressions' list
        if hasattr(node, 'expressions') and node.expressions:
            first_arg = node.expressions[0]
            if isinstance(first_arg, exp.Column):
                return True

        return False


class NotInSubqueryRule(ASTRule):
    """SQL-WHERE-005: Detect NOT IN with subquery.

    NOT IN with subquery has dangerous NULL handling:
        WHERE id NOT IN (SELECT user_id FROM banned_users)

    If the subquery returns ANY NULL value, the entire NOT IN returns
    no rows (NULL comparison propagates). This is almost never intended.

    Better alternatives:
    - NOT EXISTS (handles NULLs correctly)
    - LEFT JOIN ... WHERE right.id IS NULL

    Detection:
    - sqlglot normalizes NOT IN (subquery) to: col <> ALL (subquery)
    - So we detect NEQ node where expression is All with subquery
    - Also handles the rare case of NOT wrapping IN with subquery
    """

    rule_id = "SQL-WHERE-005"
    name = "NOT IN with Subquery"
    severity = "high"
    category = "where_clause"
    penalty = 15
    description = "NOT IN with subquery may return no rows if NULLs exist"
    suggestion = "Use NOT EXISTS instead for correct NULL handling"

    # sqlglot normalizes NOT IN (subquery) to NEQ with ALL
    target_node_types = (exp.NEQ,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # sqlglot normalizes: NOT IN (SELECT ...) -> col <> ALL (SELECT ...)
        # Check if NEQ has an ALL expression containing a subquery
        expression = node.args.get('expression')
        if isinstance(expression, exp.All):
            # All contains the subquery
            if self._has_subquery(expression):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="NOT IN with subquery - NULLs cause unexpected empty results",
                    matched_text=node.sql()[:80],
                )

    def _has_subquery(self, node: exp.Expression) -> bool:
        """Check if node contains a subquery."""
        return bool(node.find(exp.Subquery) or node.find(exp.Select))


class LeadingWildcardRule(ASTRule):
    """SQL-WHERE-003: Detect LIKE with leading wildcard.

    LIKE '%value' cannot use an index - requires full table scan:
        WHERE name LIKE '%smith'  -- Full scan
        WHERE name LIKE 'smith%'  -- Can use index

    False Positive Prevention:
    - Only checks exp.Like nodes (no regex on strings)
    - Validates the pattern is a string literal
    - Correctly identifies % at start of pattern
    """

    rule_id = "SQL-WHERE-003"
    name = "Leading Wildcard in LIKE"
    severity = "high"
    category = "where_clause"
    penalty = 15
    description = "LIKE with leading wildcard cannot use index"
    suggestion = "Use full-text search or redesign the query"

    target_node_types = (exp.Like, exp.ILike)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get the pattern (right side of LIKE)
        pattern = node.expression  # This is the pattern in LIKE col LIKE pattern

        if pattern is None:
            return

        # Check if pattern is a literal string
        if isinstance(pattern, exp.Literal):
            pattern_value = pattern.this
            if isinstance(pattern_value, str) and pattern_value.startswith('%'):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"LIKE '{pattern_value}' - leading wildcard prevents index",
                    matched_text=node.sql()[:80],
                )
        # Handle concatenation: LIKE '%' || value
        elif isinstance(pattern, exp.Concat):
            first_part = pattern.expressions[0] if pattern.expressions else None
            if isinstance(first_part, exp.Literal):
                if str(first_part.this).startswith('%'):
                    yield RuleMatch(
                        node=node,
                        context=context,
                        message="LIKE with leading wildcard prevents index",
                        matched_text=node.sql()[:80],
                    )


class ImplicitTypeConversionRule(ASTRule):
    """SQL-WHERE-002: Detect implicit type conversion in comparisons.

    Comparing columns to mismatched types forces conversion:
        WHERE varchar_code = 12345  -- Converts every row
        WHERE int_id = '12345'  -- Quoted number to int column

    Detection:
    - Find EQ comparisons in WHERE
    - Check if one side is column, other is mismatched literal type
    """

    rule_id = "SQL-WHERE-002"
    name = "Implicit Type Conversion"
    severity = "high"
    category = "where_clause"
    penalty = 15
    description = "Type mismatch forces implicit conversion - prevents index"
    suggestion = "Match literal type to column type"

    target_node_types = (exp.EQ,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not (context.in_where or context.in_having):
            return

        left = node.args.get('this')
        right = node.args.get('expression')

        # Check for common mismatches
        mismatch = self._detect_mismatch(left, right)
        if mismatch:
            yield RuleMatch(
                node=node,
                context=context,
                message=mismatch,
                matched_text=node.sql()[:80],
            )

    def _detect_mismatch(self, left, right) -> str:
        """Detect type mismatches between column and literal."""
        # Column compared to number without quotes (potential varchar issue)
        if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
            col_name = str(left.this).lower() if left.this else ""
            # Heuristic: columns ending in _code, _num, _ref are often varchar
            if right.is_number and any(col_name.endswith(s) for s in ['_code', '_num', '_ref', '_name']):
                return f"Possible type mismatch: {col_name} compared to unquoted number"

            # Heuristic: _id columns compared to quoted string
            if right.is_string and col_name.endswith('_id'):
                return f"Possible type mismatch: {col_name} compared to quoted string"

        return ""


class OrInsteadOfInRule(ASTRule):
    """SQL-WHERE-004: Detect multiple OR conditions that could be IN.

    Multiple OR on same column is less readable than IN:
        WHERE status = 'a' OR status = 'b' OR status = 'c'

    Better:
        WHERE status IN ('a', 'b', 'c')

    Detection:
    - Find OR chains in WHERE
    - Check if they compare same column to different values
    """

    rule_id = "SQL-WHERE-004"
    name = "OR Instead of IN"
    severity = "low"
    category = "where_clause"
    penalty = 5
    description = "Multiple OR conditions could be simplified to IN"
    suggestion = "Use IN clause for readability"

    target_node_types = (exp.Or,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Count OR chain length
        or_count = self._count_or_chain(node)
        if or_count >= 3:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{or_count} OR conditions - consider using IN",
                matched_text=node.sql()[:80],
            )

    def _count_or_chain(self, node: exp.Expression) -> int:
        """Count consecutive OR conditions."""
        count = 0
        current = node
        while isinstance(current, exp.Or):
            count += 1
            current = current.args.get('this')
        return count + 1  # +1 for the last condition


class DoubleNegativeRule(ASTRule):
    """SQL-WHERE-006: Detect double negative logic.

    Double negatives are confusing and may confuse optimizer:
        WHERE NOT (status <> 'complete')  -- Double negative

    Better:
        WHERE status = 'complete'

    Detection:
    - Find NOT containing <> or !=
    - Or NOT NOT patterns
    """

    rule_id = "SQL-WHERE-006"
    name = "Double Negative"
    severity = "medium"
    category = "where_clause"
    penalty = 10
    description = "Double negative logic is confusing"
    suggestion = "Simplify to positive logic"

    target_node_types = (exp.Not,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        inner = node.this

        # NOT (x <> y) or NOT (x != y)
        if isinstance(inner, (exp.NEQ, exp.Paren)):
            if isinstance(inner, exp.Paren):
                inner_expr = inner.this
                if isinstance(inner_expr, exp.NEQ):
                    yield RuleMatch(
                        node=node,
                        context=context,
                        message="Double negative: NOT with <> - simplify",
                        matched_text=node.sql()[:60],
                    )
                    return

            if isinstance(inner, exp.NEQ):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="Double negative: NOT with <> - simplify",
                    matched_text=node.sql()[:60],
                )

        # NOT NOT x
        if isinstance(inner, exp.Not):
            yield RuleMatch(
                node=node,
                context=context,
                message="Double negative: NOT NOT - remove both",
                matched_text=node.sql()[:60],
            )


class CoalesceInFilterRule(ASTRule):
    """SQL-WHERE-008: Detect COALESCE/ISNULL in filter predicates.

    COALESCE/ISNULL in WHERE prevents index usage:
        WHERE COALESCE(ship_date, '1900-01-01') > '2024-01-01'

    Better:
        WHERE ship_date > '2024-01-01' OR ship_date IS NULL

    Detection:
    - Find COALESCE/ISNULL in WHERE/HAVING
    """

    rule_id = "SQL-WHERE-008"
    name = "COALESCE/ISNULL in Filter"
    severity = "medium"
    category = "where_clause"
    penalty = 10
    description = "COALESCE/ISNULL in filter prevents index usage"
    suggestion = "Handle NULL explicitly with OR ... IS NULL"

    target_node_types = (exp.Coalesce,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not (context.in_where or context.in_having):
            return

        # Check if COALESCE wraps a column
        if hasattr(node, 'this') and isinstance(node.this, exp.Column):
            yield RuleMatch(
                node=node,
                context=context,
                message="COALESCE on column in filter - prevents index",
                matched_text=node.sql()[:60],
            )


class RedundantPredicateRule(ASTRule):
    """SQL-WHERE-007: Detect redundant predicates.

    Redundant predicates waste evaluation time and indicate unclear requirements:
        WHERE x > 5 AND x > 3  -- x > 3 is redundant (or reversed)
        WHERE status = 'A' AND status = 'A'  -- Duplicate
        WHERE id = 5 AND id = 5  -- Exact duplicate

    This rule detects:
    - Exact duplicate predicates (same condition twice)
    - For complex range overlap, this is harder to detect reliably

    Detection:
    - Find AND conditions with duplicate predicates
    - Compare SQL representation for equality
    """

    rule_id = "SQL-WHERE-007"
    name = "Redundant Predicate"
    severity = "low"
    category = "where_clause"
    penalty = 5
    description = "Redundant or duplicate predicate in WHERE clause"
    suggestion = "Remove the redundant condition"

    target_node_types = (exp.And,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Collect all predicates from AND chain
        predicates = []
        self._collect_predicates(node, predicates)

        # Check for duplicates by SQL text
        seen = set()
        for pred in predicates:
            pred_sql = pred.sql().strip()
            if pred_sql in seen:
                yield RuleMatch(
                    node=pred,
                    context=context,
                    message=f"Duplicate predicate: {pred_sql[:40]}",
                    matched_text=pred_sql[:60],
                )
                return  # Only report once
            seen.add(pred_sql)

    def _collect_predicates(
        self, node: exp.Expression, predicates: list
    ) -> None:
        """Collect all predicates from AND chain."""
        if isinstance(node, exp.And):
            self._collect_predicates(node.this, predicates)
            self._collect_predicates(node.expression, predicates)
        else:
            predicates.append(node)


class NonSargableDateRule(ASTRule):
    """SQL-WHERE-009: Detect non-sargable date comparisons.

    DATEDIFF/DATEADD on columns prevents index usage:
        WHERE DATEDIFF(day, order_date, GETDATE()) <= 30

    Better:
        WHERE order_date >= DATEADD(day, -30, GETDATE())

    Detection:
    - Find DATEDIFF in WHERE comparisons
    - Find DATEADD wrapping column (not literal)
    """

    rule_id = "SQL-WHERE-009"
    name = "Non-Sargable Date Comparison"
    severity = "high"
    category = "where_clause"
    penalty = 15
    description = "Date function on column prevents index usage"
    suggestion = "Apply date arithmetic to the literal side"

    target_node_types = (exp.DateDiff,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not (context.in_where or context.in_having):
            return

        # DATEDIFF on column is non-sargable
        if node.find(exp.Column):
            yield RuleMatch(
                node=node,
                context=context,
                message="DATEDIFF on column - rewrite to compare date directly",
                matched_text=node.sql()[:60],
            )


class OrPreventsIndexRule(ASTRule):
    """SQL-WHERE-010: OR on different columns prevents index usage.

    STRUCTURAL REWRITE: Optimizer cannot convert OR to UNION automatically
    because the semantics may differ (duplicate handling).

    Problem - OR across different indexed columns = full table scan:
        SELECT * FROM orders
        WHERE customer_id = 123 OR product_id = 456

    The optimizer cannot use both indexes efficiently. Solutions:

    1. UNION ALL decomposition (if no duplicates possible):
        SELECT * FROM orders WHERE customer_id = 123
        UNION ALL
        SELECT * FROM orders WHERE product_id = 456

    2. UNION with deduplication (if duplicates possible):
        SELECT * FROM orders WHERE customer_id = 123
        UNION
        SELECT * FROM orders WHERE product_id = 456

    Detection:
    - Find OR in WHERE with different columns on each side
    - Both columns appear to be indexed (heuristic: _id suffix, pk name)
    """

    rule_id = "SQL-WHERE-010"
    name = "OR Prevents Index Usage"
    severity = "high"
    category = "where_clause"
    penalty = 15
    description = "OR on different columns prevents efficient index usage"
    suggestion = "Consider UNION ALL decomposition for index utilization"

    target_node_types = (exp.Or,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Skip if nested (we want top-level OR)
        if isinstance(node.parent, exp.Or):
            return

        # Collect columns from each side of OR
        left_cols = self._get_columns(node.this)
        right_cols = self._get_columns(node.expression)

        # If different columns on each side, index can't be used efficiently
        if left_cols and right_cols and not left_cols.intersection(right_cols):
            # Check if either side has indexed-looking columns
            all_cols = left_cols.union(right_cols)
            indexed_cols = [c for c in all_cols if self._looks_indexed(c)]

            if len(indexed_cols) >= 2:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="OR on different indexed columns - consider UNION ALL",
                    matched_text=f"OR across {', '.join(sorted(all_cols)[:3])}",
                )

    def _get_columns(self, node: exp.Expression) -> set:
        """Extract column names from expression."""
        cols = set()
        if node:
            for col in node.find_all(exp.Column):
                col_name = str(col.this).lower() if col.this else ""
                if col_name:
                    cols.add(col_name)
        return cols

    def _looks_indexed(self, col_name: str) -> bool:
        """Heuristic: does column name suggest it's indexed?"""
        indexed_patterns = ['_id', 'id', '_key', '_code', '_date', '_at', 'pk', 'fk']
        return any(col_name.endswith(p) or col_name == p for p in indexed_patterns)


class NotInNullRiskRule(ASTRule):
    """SQL-WHERE-011: NOT IN with nullable column.

    STRUCTURAL REWRITE: Optimizer cannot auto-convert NOT IN to NOT EXISTS
    because they have different NULL semantics.

    Problem - NOT IN returns no rows if subquery contains NULL:
        SELECT * FROM orders
        WHERE customer_id NOT IN (SELECT id FROM inactive_customers)
        -- If inactive_customers.id contains NULL, returns NOTHING

    Solution - Use NOT EXISTS (handles NULLs correctly):
        SELECT * FROM orders o
        WHERE NOT EXISTS (
            SELECT 1 FROM inactive_customers ic
            WHERE ic.id = o.customer_id
        )

    Or use explicit NULL handling:
        WHERE customer_id NOT IN (
            SELECT id FROM inactive_customers WHERE id IS NOT NULL
        )

    Detection:
    - Find NOT IN with subquery
    - Flag because we can't know if subquery contains NULLs
    """

    rule_id = "SQL-WHERE-011"
    name = "NOT IN NULL Risk"
    severity = "high"
    category = "where_clause"
    penalty = 15
    description = "NOT IN with subquery - NULL values cause unexpected empty results"
    suggestion = "Use NOT EXISTS instead (handles NULLs correctly)"

    target_node_types = (exp.Not,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Check if NOT contains IN with subquery
        in_expr = node.find(exp.In)
        if not in_expr:
            return

        # Check for subquery in IN
        has_subquery = in_expr.find(exp.Subquery) or in_expr.find(exp.Select)
        if has_subquery:
            yield RuleMatch(
                node=node,
                context=context,
                message="NOT IN subquery - NULLs cause empty results, use NOT EXISTS",
                matched_text="NOT IN (SELECT ...)",
            )
