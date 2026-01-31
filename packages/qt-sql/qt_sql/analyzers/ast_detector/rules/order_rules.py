"""ORDER BY anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class OrderByInSubqueryRule(ASTRule):
    """SQL-ORD-001: Detect ORDER BY in subquery without TOP/LIMIT.

    ORDER BY in subquery without TOP/LIMIT is ignored by most databases:
        SELECT * FROM (SELECT * FROM users ORDER BY name)  -- ORDER ignored

    The ORDER BY adds overhead but has no effect on the result.

    Exceptions:
    - Subquery with TOP/LIMIT - ORDER BY determines which rows kept
    - Window functions may need ORDER BY
    - Some dialects (like SQL Server) require ORDER BY for OFFSET

    Detection:
    - Find ORDER BY inside Subquery
    - Check if there's a LIMIT/TOP/OFFSET clause
    """

    rule_id = "SQL-ORD-001"
    name = "ORDER BY in Subquery"
    severity = "medium"
    category = "order_by"
    penalty = 10
    description = "ORDER BY in subquery without TOP is ignored"
    suggestion = "Remove ORDER BY or add TOP clause"

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Find SELECT inside subquery
        select = node.find(exp.Select)
        if not select:
            return

        # Check if there's an ORDER BY
        order = select.find(exp.Order)
        if not order:
            return

        # Check if there's a LIMIT/TOP/OFFSET (which would make ORDER BY meaningful)
        if select.find(exp.Limit) or select.find(exp.Fetch) or select.find(exp.Offset):
            return

        # Check for TOP in SQL Server style (part of Select)
        if hasattr(select, 'args') and select.args.get('limit'):
            return

        yield RuleMatch(
            node=order,
            context=context,
            message="ORDER BY in subquery without LIMIT/TOP - will be ignored",
            matched_text=order.sql()[:60],
        )


class OrderByWithoutLimitRule(ASTRule):
    """SQL-ORD-003: Detect ORDER BY without LIMIT on main query.

    Sorting entire result set without limiting rows:
        SELECT * FROM large_table ORDER BY created_at

    This can be expensive for large tables but only a human can determine
    whether all rows are needed. This is a review warning, not an
    auto-fixable anti-pattern — adding LIMIT would change result semantics.

    Detection:
    - Find ORDER BY on main query (not subquery)
    - Check if there's no LIMIT/TOP
    """

    rule_id = "SQL-ORD-003"
    name = "ORDER BY Without LIMIT"
    severity = "info"
    category = "order_by"
    penalty = 0
    description = "Sorting entire result set without row limit — review whether all rows are needed"
    suggestion = "Review: consider adding TOP/LIMIT if you don't need all rows"

    target_node_types = (exp.Order,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check ORDER BY in main query, not subqueries
        if context.in_subquery:
            return

        # Find parent SELECT
        parent = node.parent
        while parent and not isinstance(parent, exp.Select):
            parent = parent.parent

        if not parent:
            return

        select = parent

        # Check if there's a LIMIT/TOP
        if select.find(exp.Limit) or select.find(exp.Fetch) or select.find(exp.Offset):
            return

        if hasattr(select, 'args') and select.args.get('limit'):
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="ORDER BY without LIMIT - sorting entire result set",
            matched_text=node.sql()[:60],
        )


class OrderByExpressionRule(ASTRule):
    """SQL-ORD-002: Detect ORDER BY on expression/function.

    ORDER BY with function prevents index-based sorting:
        SELECT * FROM orders ORDER BY YEAR(order_date)

    Better:
        SELECT * FROM orders ORDER BY order_date  -- Can use index

    Detection:
    - Find function calls in ORDER BY expressions
    """

    rule_id = "SQL-ORD-002"
    name = "ORDER BY on Expression"
    severity = "medium"
    category = "order_by"
    penalty = 10
    description = "ORDER BY on expression prevents index sort"
    suggestion = "Order by column directly or use computed column"

    target_node_types = (exp.Order,)

    # Common functions that indicate expression in ORDER BY
    EXPRESSION_FUNCS = (
        exp.Year, exp.Month, exp.Day, exp.DateTrunc,
        exp.Upper, exp.Lower, exp.Trim,
        exp.Cast, exp.Abs, exp.Concat,
    )

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check order expressions
        if not hasattr(node, 'expressions'):
            return

        for expr in node.expressions:
            # Check for Ordered wrapper
            actual_expr = expr.this if isinstance(expr, exp.Ordered) else expr

            # Check for function
            if isinstance(actual_expr, self.EXPRESSION_FUNCS):
                func_name = type(actual_expr).__name__.upper()
                yield RuleMatch(
                    node=expr,
                    context=context,
                    message=f"ORDER BY {func_name}() - cannot use index",
                    matched_text=expr.sql()[:60],
                )
                return  # Only report once


class OffsetPaginationRule(ASTRule):
    """SQL-ORD-005: OFFSET pagination becomes slow at scale.

    STRUCTURAL REWRITE: Database cannot auto-convert OFFSET to keyset pagination
    because they have different semantics (keyset can't jump to arbitrary page).

    Problem - OFFSET scans and discards rows:
        SELECT * FROM products ORDER BY created_at DESC LIMIT 10 OFFSET 50000
        -- Must scan 50,010 rows to return 10!

    Solution - Keyset/seek pagination (requires tracking last seen value):
        SELECT * FROM products
        WHERE (created_at, id) < (@last_created_at, @last_id)
        ORDER BY created_at DESC, id DESC
        LIMIT 10

    Speedup: 100x+ at high offsets (5.8ms vs 0.1ms benchmarked)

    Detection:
    - Find OFFSET with large value or OFFSET without context
    """

    rule_id = "SQL-ORD-005"
    name = "OFFSET Pagination"
    severity = "medium"
    category = "order_by"
    penalty = 10
    description = "OFFSET pagination scans and discards rows - slow at scale"
    suggestion = "Use keyset/seek pagination: WHERE (col, id) < (last_val, last_id) LIMIT N"

    target_node_types = (exp.Offset,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get offset value if it's a literal
        offset_val = None
        if hasattr(node, 'this') and isinstance(node.this, exp.Literal):
            try:
                offset_val = int(node.this.this)
            except (ValueError, TypeError):
                pass

        # Always flag OFFSET as potential issue, higher severity for large values
        if offset_val is not None and offset_val > 1000:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"OFFSET {offset_val} scans {offset_val}+ rows - use keyset pagination",
                matched_text=f"OFFSET {offset_val}",
            )
        else:
            yield RuleMatch(
                node=node,
                context=context,
                message="OFFSET pagination degrades at scale - consider keyset pagination",
                matched_text=node.sql()[:40],
            )


class OrderByOrdinalRule(ASTRule):
    """SQL-ORD-004: Detect ORDER BY using column ordinal.

    ORDER BY using position instead of name is fragile:
        SELECT name, age FROM users ORDER BY 2  -- Fragile

    If SELECT columns change, ORDER BY breaks silently.

    Detection:
    - Find numeric literals in ORDER BY clause
    """

    rule_id = "SQL-ORD-004"
    name = "ORDER BY Ordinal"
    severity = "low"
    category = "order_by"
    penalty = 5
    description = "ORDER BY using column position instead of name"
    suggestion = "Use explicit column names for clarity"

    target_node_types = (exp.Order,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not hasattr(node, 'expressions'):
            return

        for expr in node.expressions:
            # Handle Ordered wrapper
            actual_expr = expr.this if isinstance(expr, exp.Ordered) else expr

            # Check for numeric literal
            if isinstance(actual_expr, exp.Literal) and actual_expr.is_int:
                yield RuleMatch(
                    node=expr,
                    context=context,
                    message=f"ORDER BY {actual_expr.this} - use column name",
                    matched_text=node.sql()[:60],
                )
                return  # Only report once
