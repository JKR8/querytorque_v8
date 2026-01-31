"""Aggregation anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class GroupByOrdinalRule(ASTRule):
    """SQL-AGG-001: Detect GROUP BY using column ordinal.

    GROUP BY using position instead of name is fragile:
        SELECT dept, COUNT(*) FROM emp GROUP BY 1  -- Fragile

    If SELECT columns change, GROUP BY breaks silently.

    Better:
        SELECT dept, COUNT(*) FROM emp GROUP BY dept

    Detection:
    - Find numeric literals in GROUP BY clause
    """

    rule_id = "SQL-AGG-001"
    name = "GROUP BY Ordinal"
    severity = "low"
    category = "aggregation"
    penalty = 5
    description = "GROUP BY using column position instead of name"
    suggestion = "Use explicit column names for clarity"

    target_node_types = (exp.Group,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check expressions in GROUP BY
        if not hasattr(node, 'expressions'):
            return

        for expr in node.expressions:
            # Check for numeric literal
            if isinstance(expr, exp.Literal) and expr.is_int:
                yield RuleMatch(
                    node=expr,
                    context=context,
                    message=f"GROUP BY {expr.this} - use column name instead",
                    matched_text=node.sql()[:60],
                )
                return  # Only report once


class HavingWithoutAggregateRule(ASTRule):
    """SQL-AGG-003: Detect HAVING clause without aggregate function.

    HAVING filtering on non-aggregate is less efficient than WHERE:
        SELECT dept FROM emp GROUP BY dept HAVING dept = 'Sales'  -- Wrong

    This should be:
        SELECT dept FROM emp WHERE dept = 'Sales' GROUP BY dept  -- Better

    HAVING is for filtering aggregates:
        HAVING COUNT(*) > 10  -- Correct use

    Detection:
    - Find HAVING clause
    - Check if it contains aggregate functions
    """

    rule_id = "SQL-AGG-003"
    name = "HAVING Without Aggregate"
    severity = "medium"
    category = "aggregation"
    penalty = 10
    description = "HAVING clause filtering on non-aggregate column"
    suggestion = "Move non-aggregate conditions to WHERE clause"

    target_node_types = (exp.Having,)

    AGGREGATE_FUNCS = (
        exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
        exp.ArrayAgg, exp.GroupConcat, exp.Variance, exp.Stddev,
    )

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if HAVING contains any aggregate function
        has_aggregate = any(node.find_all(*self.AGGREGATE_FUNCS))

        if has_aggregate:
            return  # Correct usage

        yield RuleMatch(
            node=node,
            context=context,
            message="HAVING without aggregate - move to WHERE",
            matched_text=node.sql()[:60],
        )


class GroupByExpressionRule(ASTRule):
    """SQL-AGG-002: Detect GROUP BY on expression/function.

    GROUP BY with function prevents index usage:
        SELECT YEAR(order_date), COUNT(*) FROM orders
        GROUP BY YEAR(order_date)  -- Function evaluated per row

    Better:
        SELECT order_year, COUNT(*) FROM orders GROUP BY order_year

    Detection:
    - Find function calls in GROUP BY expressions
    """

    rule_id = "SQL-AGG-002"
    name = "GROUP BY on Expression"
    severity = "medium"
    category = "aggregation"
    penalty = 10
    description = "GROUP BY on expression prevents index usage"
    suggestion = "Use computed column or pre-calculate"

    target_node_types = (exp.Group,)

    # Common functions that indicate expression in GROUP BY
    EXPRESSION_FUNCS = (
        exp.Year, exp.Month, exp.Day, exp.DateTrunc,
        exp.Cast, exp.Upper, exp.Lower, exp.Trim,
        exp.Substring, exp.Concat,
    )

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not hasattr(node, 'expressions'):
            return

        for expr in node.expressions:
            # Check for function in GROUP BY
            if isinstance(expr, self.EXPRESSION_FUNCS):
                func_name = type(expr).__name__.upper()
                yield RuleMatch(
                    node=expr,
                    context=context,
                    message=f"GROUP BY {func_name}() - consider computed column",
                    matched_text=expr.sql()[:60],
                )
                return  # Only report once


class DistinctInsideAggregateRule(ASTRule):
    """SQL-AGG-004: Detect DISTINCT inside aggregate function.

    DISTINCT in aggregates is expensive:
        SELECT COUNT(DISTINCT customer_id) FROM orders

    May be necessary, but:
    - SUM(DISTINCT) and AVG(DISTINCT) often indicate join problems
    - COUNT(DISTINCT) on high cardinality is expensive

    Detection:
    - Find aggregate functions with DISTINCT modifier
    """

    rule_id = "SQL-AGG-004"
    name = "DISTINCT Inside Aggregate"
    severity = "medium"
    category = "aggregation"
    penalty = 10
    description = "DISTINCT in aggregate is expensive on large data"
    suggestion = "Review if DISTINCT is necessary - may indicate join issue"

    # Target aggregate functions
    target_node_types = (exp.Count, exp.Sum, exp.Avg)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check for DISTINCT modifier
        if node.args.get('distinct'):
            agg_name = type(node).__name__.upper()

            # SUM/AVG with DISTINCT is usually wrong
            if isinstance(node, (exp.Sum, exp.Avg)):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"{agg_name}(DISTINCT) - likely indicates join problem",
                    matched_text=node.sql()[:60],
                )
            elif isinstance(node, exp.Count):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="COUNT(DISTINCT) - expensive on high cardinality",
                    matched_text=node.sql()[:60],
                )


class MissingGroupByColumnRule(ASTRule):
    """SQL-AGG-005: Detect non-aggregated columns not in GROUP BY.

    Selecting columns not in GROUP BY or aggregated:
        SELECT dept, name, COUNT(*) FROM emp GROUP BY dept
        -- 'name' is not in GROUP BY or aggregated

    Some databases allow this (pick arbitrary value), others error.

    Detection:
    - Parse SELECT columns
    - Check if non-aggregated columns are in GROUP BY
    """

    rule_id = "SQL-AGG-005"
    name = "Missing GROUP BY Column"
    severity = "medium"
    category = "aggregation"
    penalty = 10
    description = "Column in SELECT not in GROUP BY or aggregate"
    suggestion = "Add to GROUP BY or wrap in aggregate function"

    target_node_types = (exp.Select,)

    AGGREGATE_FUNCS = (
        exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
        exp.ArrayAgg, exp.GroupConcat,
    )

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Must have GROUP BY
        group_by = node.args.get('group')
        if not group_by:
            return

        # Get GROUP BY columns
        group_cols = set()
        for expr in group_by.expressions if hasattr(group_by, 'expressions') else []:
            if isinstance(expr, exp.Column):
                group_cols.add(str(expr.this).lower())

        # Check SELECT columns
        for expr in node.expressions:
            # Skip if entire expression is aggregate
            if isinstance(expr, self.AGGREGATE_FUNCS):
                continue
            # Skip if wrapped in aggregate
            if expr.find(*self.AGGREGATE_FUNCS):
                continue
            # Skip aliases pointing to aggregates
            if isinstance(expr, exp.Alias) and isinstance(expr.this, self.AGGREGATE_FUNCS):
                continue

            # Check for bare columns
            for col in expr.find_all(exp.Column):
                col_name = str(col.this).lower()
                if col_name and col_name not in group_cols:
                    # Check if inside aggregate
                    if not self._is_inside_aggregate(col):
                        yield RuleMatch(
                            node=col,
                            context=context,
                            message=f"Column '{col_name}' not in GROUP BY",
                            matched_text=col.sql(),
                        )
                        return  # Only report once

    def _is_inside_aggregate(self, node: exp.Expression) -> bool:
        """Check if column is inside an aggregate function."""
        parent = node.parent
        while parent:
            if isinstance(parent, self.AGGREGATE_FUNCS):
                return True
            if isinstance(parent, exp.Select):
                return False
            parent = parent.parent
        return False


class RepeatedAggregationRule(ASTRule):
    """SQL-AGG-006: Same aggregation computed multiple times.

    STRUCTURAL REWRITE: Optimizer cannot identify repeated aggregation
    patterns across different parts of a query.

    Problem - Same aggregation in multiple places:
        SELECT
            SUM(amount) as total,
            SUM(amount) * 0.1 as tax,
            SUM(amount) * 0.9 as net,
            COUNT(*) as cnt,
            SUM(amount) / COUNT(*) as avg_manual
        FROM orders

    Better - Compute once with CTE or subquery:
        WITH aggs AS (
            SELECT SUM(amount) as total, COUNT(*) as cnt FROM orders
        )
        SELECT
            total,
            total * 0.1 as tax,
            total * 0.9 as net,
            cnt,
            total / cnt as avg_manual
        FROM aggs

    Detection:
    - Find same aggregate expression appearing multiple times
    """

    rule_id = "SQL-AGG-006"
    name = "Repeated Aggregation"
    severity = "medium"
    category = "aggregation"
    penalty = 10
    description = "Same aggregation computed multiple times - extract to CTE"
    suggestion = "Compute aggregation once in CTE, reference multiple times"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Collect all aggregate expressions
        agg_exprs = {}
        for agg in node.find_all((exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)):
            agg_sql = agg.sql()
            if agg_sql in agg_exprs:
                agg_exprs[agg_sql] += 1
            else:
                agg_exprs[agg_sql] = 1

        # Flag if any aggregate appears 2+ times
        repeated = [(sql, cnt) for sql, cnt in agg_exprs.items() if cnt >= 2]
        if repeated:
            agg_name, count = repeated[0]
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Aggregation '{agg_name[:30]}' computed {count} times - use CTE",
                matched_text=f"Repeated: {agg_name[:40]}",
            )


class AggregateOfAggregateRule(ASTRule):
    """SQL-AGG-007: Aggregate of aggregate requiring two-pass.

    STRUCTURAL REWRITE: Optimizer cannot automatically split
    multi-level aggregations into efficient staged computation.

    Problem - Nested aggregation (average of sums):
        SELECT region, AVG(customer_total)
        FROM (
            SELECT region, customer_id, SUM(amount) as customer_total
            FROM orders GROUP BY region, customer_id
        ) t
        GROUP BY region

    This pattern is fine, but can be simplified in some cases.
    More importantly, detecting this helps identify when
    pre-aggregation tables would help:

        -- Materialized pre-aggregation:
        CREATE TABLE customer_totals AS
        SELECT region, customer_id, SUM(amount) as total
        FROM orders GROUP BY region, customer_id;

    Detection:
    - Find aggregate function on column that comes from subquery aggregate
    """

    rule_id = "SQL-AGG-007"
    name = "Aggregate of Aggregate"
    severity = "low"
    category = "aggregation"
    penalty = 5
    description = "Multi-level aggregation - consider materialized pre-aggregation"
    suggestion = "For repeated queries, pre-compute base aggregations"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Check if FROM is a subquery
        from_clause = node.args.get('from')
        if not from_clause:
            return

        subquery = from_clause.find(exp.Subquery)
        if not subquery:
            return

        # Check if subquery has GROUP BY (aggregation)
        inner_select = subquery.find(exp.Select)
        if not inner_select or not inner_select.args.get('group'):
            return

        # Check if outer query also has aggregation
        has_outer_agg = bool(node.find((exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max)))
        has_outer_group = bool(node.args.get('group'))

        if has_outer_agg and has_outer_group:
            yield RuleMatch(
                node=node,
                context=context,
                message="Aggregate of aggregate - consider pre-aggregation table",
                matched_text="GROUP BY on pre-grouped subquery",
            )


class GroupByWithHavingCountRule(ASTRule):
    """SQL-AGG-008: HAVING COUNT could be JOIN.

    STRUCTURAL REWRITE: Optimizer cannot convert certain HAVING patterns
    to more efficient JOIN-based filtering.

    Problem - Filter groups by count using HAVING:
        SELECT customer_id, COUNT(*) as order_count
        FROM orders
        GROUP BY customer_id
        HAVING COUNT(*) >= 10

    For very selective HAVING, a semi-join may be faster:
        SELECT customer_id, COUNT(*) as order_count
        FROM orders
        WHERE customer_id IN (
            SELECT customer_id FROM orders
            GROUP BY customer_id HAVING COUNT(*) >= 10
        )
        GROUP BY customer_id

    Or with CTE for clarity:
        WITH active_customers AS (
            SELECT customer_id FROM orders
            GROUP BY customer_id HAVING COUNT(*) >= 10
        )
        SELECT o.customer_id, COUNT(*) as order_count
        FROM orders o
        JOIN active_customers ac ON o.customer_id = ac.customer_id
        GROUP BY o.customer_id

    Detection:
    - Find HAVING with selective COUNT condition
    """

    rule_id = "SQL-AGG-008"
    name = "HAVING COUNT Filter"
    severity = "low"
    category = "aggregation"
    penalty = 5
    description = "HAVING COUNT filter - consider semi-join for large datasets"
    suggestion = "For selective filters, pre-filter IDs with subquery/CTE"

    target_node_types = (exp.Having,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check for COUNT in HAVING
        count_expr = node.find(exp.Count)
        if not count_expr:
            return

        # Check for comparison (>=, >, =)
        comparison = node.find((exp.GTE, exp.GT, exp.EQ, exp.LTE, exp.LT))
        if not comparison:
            return

        # Check if comparing to a literal number
        literal = comparison.find(exp.Literal)
        if literal and literal.is_number:
            yield RuleMatch(
                node=node,
                context=context,
                message="HAVING COUNT filter - consider CTE/semi-join for large data",
                matched_text=node.sql()[:60],
            )


class LargeCountDistinctRule(ASTRule):
    """SQL-AGG-009: COUNT(DISTINCT) on high-cardinality columns.

    STRUCTURAL REWRITE: Optimizer cannot convert exact COUNT(DISTINCT) to
    approximate counting (HyperLogLog) because semantics differ (~2% error).

    Problem - Exact distinct count is memory-intensive:
        SELECT date, COUNT(DISTINCT user_id) as unique_users
        FROM page_views
        GROUP BY date
        -- Must track ALL distinct values in memory

    Solutions:
    1. APPROX_COUNT_DISTINCT (if database supports it):
        SELECT date, APPROX_COUNT_DISTINCT(user_id) as unique_users
        FROM page_views GROUP BY date

    2. HyperLogLog functions for reaggregatable sketches:
        SELECT date, HLL_COUNT_DISTINCT(user_id) as unique_users
        FROM page_views GROUP BY date

    Speedup: 10-1000x with ~2% accuracy trade-off

    Note: This is a suggestion rule - only human can accept accuracy trade-off.

    Detection:
    - Find COUNT(DISTINCT ...) in queries
    - Flag as optimization opportunity (not error)
    """

    rule_id = "SQL-AGG-009"
    name = "COUNT DISTINCT Optimization"
    severity = "info"
    category = "aggregation"
    penalty = 0
    description = "COUNT(DISTINCT) is memory-intensive - consider approximate counting if exact precision not required"
    suggestion = "If ~2% error acceptable, use APPROX_COUNT_DISTINCT or HyperLogLog for 10-1000x speedup"

    target_node_types = (exp.Count,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check for DISTINCT modifier - sqlglot wraps it as count.this = exp.Distinct
        has_distinct = node.args.get('distinct') or isinstance(node.this, exp.Distinct)
        if not has_distinct:
            return

        # Check if this is inside GROUP BY (aggregation query)
        parent_select = node.parent
        while parent_select and not isinstance(parent_select, exp.Select):
            parent_select = parent_select.parent

        if not parent_select:
            return

        # Only flag if there's a GROUP BY (analytics use case)
        has_group_by = bool(parent_select.args.get('group'))

        if has_group_by:
            yield RuleMatch(
                node=node,
                context=context,
                message="COUNT(DISTINCT) in GROUP BY - consider APPROX_COUNT_DISTINCT if exact precision not needed",
                matched_text=node.sql()[:60],
            )
