"""Subquery anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class CorrelatedSubqueryInWhereRule(ASTRule):
    """SQL-SUB-001: Detect correlated subquery in WHERE clause.

    Correlated subqueries in WHERE execute once per outer row:
        SELECT * FROM orders o
        WHERE o.total > (
            SELECT AVG(total) FROM orders o2
            WHERE o2.customer_id = o.customer_id
        )

    This is row-by-row processing - often rewritable as JOIN or CTE.

    Detection:
    - Find subqueries in WHERE
    - Check if they reference outer query columns
    """

    rule_id = "SQL-SUB-001"
    name = "Correlated Subquery in WHERE"
    severity = "high"
    category = "subqueries"
    penalty = 15
    description = "Correlated subquery in WHERE executes per row"
    suggestion = "Rewrite as JOIN with CTE or derived table"

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Must be in WHERE context
        if not context.in_where:
            return

        # Check if correlated
        if self._is_correlated(node):
            yield RuleMatch(
                node=node,
                context=context,
                message="Correlated subquery in WHERE - runs per row",
                matched_text=node.sql()[:80],
            )

    def _is_correlated(self, subquery: exp.Expression) -> bool:
        """Check if subquery references outer query columns."""
        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return False

        # Get tables in subquery
        inner_tables = set()
        for table in inner_select.find_all(exp.Table):
            alias = str(table.alias or table.this).lower()
            inner_tables.add(alias)

        # Check for references to outer tables
        for col in inner_select.find_all(exp.Column):
            table_ref = str(col.table).lower() if col.table else ""
            if table_ref and table_ref not in inner_tables:
                return True

        return False


class SubqueryInsteadOfJoinRule(ASTRule):
    """SQL-SUB-002: Detect IN/= subquery that could be JOIN.

    Subqueries that return single column for filtering:
        WHERE customer_id IN (SELECT id FROM customers WHERE region = 'WEST')

    Better as JOIN:
        INNER JOIN customers c ON o.customer_id = c.id WHERE c.region = 'WEST'

    Detection:
    - Find IN with subquery in WHERE
    - Or = with subquery returning single row
    """

    rule_id = "SQL-SUB-002"
    name = "Subquery Instead of JOIN"
    severity = "medium"
    category = "subqueries"
    penalty = 10
    description = "IN subquery may be less efficient than JOIN"
    suggestion = "Consider rewriting as JOIN"

    target_node_types = (exp.In,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Check if IN has subquery (not literal list)
        query = node.args.get('query')
        if query or self._has_subquery(node):
            yield RuleMatch(
                node=node,
                context=context,
                message="IN with subquery - consider JOIN for clarity",
                matched_text=node.sql()[:80],
            )

    def _has_subquery(self, node: exp.Expression) -> bool:
        """Check for subquery in IN expression."""
        return bool(node.find(exp.Subquery) or node.find(exp.Select))


class DeeplyNestedSubqueryRule(ASTRule):
    """SQL-SUB-003: Detect deeply nested subqueries.

    Subqueries nested more than 3 levels are hard to read:
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM t
                ) a
            ) b
        ) c

    Detection:
    - Track subquery depth during traversal
    - Flag if exceeds threshold
    """

    rule_id = "SQL-SUB-003"
    name = "Deeply Nested Subqueries"
    severity = "medium"
    category = "subqueries"
    penalty = 10
    description = "Subqueries nested too deeply - hard to optimize"
    suggestion = "Refactor using CTEs or temp tables"

    target_node_types = (exp.Subquery,)

    WARNING_DEPTH = 3
    CRITICAL_DEPTH = 5

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        depth = context.subquery_depth

        if depth >= self.CRITICAL_DEPTH:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Subquery depth {depth} - critical complexity",
                matched_text=f"Subquery at depth {depth}",
            )
        elif depth >= self.WARNING_DEPTH:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Subquery depth {depth} - consider refactoring",
                matched_text=f"Subquery at depth {depth}",
            )


class RepeatedSubqueryRule(ASTRule):
    """SQL-SUB-004: Detect identical subqueries repeated in query.

    Same subquery logic appearing multiple times:
        SELECT
            (SELECT COUNT(*) FROM orders WHERE status = 'new'),
            (SELECT COUNT(*) FROM orders WHERE status = 'new') * 2

    Should be CTE or derived table.

    Detection:
    - Hash subquery SQL text
    - Flag duplicates
    """

    rule_id = "SQL-SUB-004"
    name = "Repeated Subquery"
    severity = "medium"
    category = "subqueries"
    penalty = 10
    description = "Same subquery repeated - may execute multiple times"
    suggestion = "Extract to CTE and reference once"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check top-level SELECT
        if context.in_subquery:
            return

        # Collect all subqueries
        subqueries = list(node.find_all(exp.Subquery))
        if len(subqueries) < 2:
            return

        # Get SQL text for each (normalized)
        seen = {}
        for sq in subqueries:
            sql_text = sq.sql()
            if sql_text in seen:
                yield RuleMatch(
                    node=sq,
                    context=context,
                    message="Repeated subquery - extract to CTE",
                    matched_text=sql_text[:60],
                )
                return  # Only report once
            seen[sql_text] = sq


class ScalarSubqueryToLateralRule(ASTRule):
    """SQL-SUB-005: Multiple scalar subqueries from same table.

    STRUCTURAL REWRITE: Optimizer cannot combine scalar subqueries that
    access the same table - each executes independently.

    Problem - Multiple scalar subqueries hit same table repeatedly:
        SELECT
            o.id,
            (SELECT MAX(amount) FROM order_items WHERE order_id = o.id) as max_amt,
            (SELECT MIN(amount) FROM order_items WHERE order_id = o.id) as min_amt,
            (SELECT AVG(amount) FROM order_items WHERE order_id = o.id) as avg_amt
        FROM orders o

    This runs 3 separate subqueries per row. Solutions:

    1. LATERAL JOIN (Postgres/DuckDB):
        SELECT o.id, aggs.max_amt, aggs.min_amt, aggs.avg_amt
        FROM orders o
        LEFT JOIN LATERAL (
            SELECT MAX(amount) as max_amt, MIN(amount) as min_amt, AVG(amount) as avg_amt
            FROM order_items WHERE order_id = o.id
        ) aggs ON TRUE

    2. CTE with aggregation:
        WITH item_aggs AS (
            SELECT order_id, MAX(amount) as max_amt, MIN(amount) as min_amt, AVG(amount) as avg_amt
            FROM order_items GROUP BY order_id
        )
        SELECT o.id, a.max_amt, a.min_amt, a.avg_amt
        FROM orders o LEFT JOIN item_aggs a ON o.id = a.order_id

    Detection:
    - Find 2+ scalar subqueries in SELECT
    - Check if they reference same table
    """

    rule_id = "SQL-SUB-005"
    name = "Multiple Scalar Subqueries"
    severity = "high"
    category = "subqueries"
    penalty = 20
    description = "Multiple scalar subqueries from same table - combine with LATERAL or CTE"
    suggestion = "Use LATERAL JOIN or CTE to query table once"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check top-level SELECT
        if context.in_subquery:
            return

        # Find scalar subqueries in SELECT list
        select_exprs = node.expressions if hasattr(node, 'expressions') else []
        scalar_subqueries = []

        for expr in select_exprs:
            for sq in expr.find_all(exp.Subquery):
                scalar_subqueries.append(sq)

        if len(scalar_subqueries) < 2:
            return

        # Check if they reference same tables
        table_counts = {}
        for sq in scalar_subqueries:
            for table in sq.find_all(exp.Table):
                table_name = str(table.this).lower() if table.this else ""
                if table_name:
                    table_counts[table_name] = table_counts.get(table_name, 0) + 1

        # Flag if same table accessed multiple times
        repeated_tables = [t for t, c in table_counts.items() if c >= 2]
        if repeated_tables:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{len(scalar_subqueries)} scalar subqueries hit '{repeated_tables[0]}' - use LATERAL/CTE",
                matched_text=f"Multiple subqueries from {repeated_tables[0]}",
            )


class ExistsWithSelectStarRule(ASTRule):
    """SQL-SUB-006: EXISTS with SELECT * instead of SELECT 1.

    STRUCTURAL REWRITE: While most optimizers handle this, some don't optimize
    SELECT * in EXISTS - it may fetch all columns unnecessarily.

    Problem:
        WHERE EXISTS (SELECT * FROM orders WHERE customer_id = c.id)

    Better (explicit that we only check existence):
        WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id)

    Detection:
    - Find EXISTS with SELECT *
    """

    rule_id = "SQL-SUB-006"
    name = "EXISTS with SELECT *"
    severity = "low"
    category = "subqueries"
    penalty = 5
    description = "EXISTS with SELECT * - use SELECT 1 for clarity"
    suggestion = "Use SELECT 1 in EXISTS for explicit intent"

    target_node_types = (exp.Exists,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Find inner SELECT
        inner_select = node.find(exp.Select)
        if not inner_select:
            return

        # Check for SELECT *
        has_star = any(isinstance(e, exp.Star) for e in inner_select.expressions)
        if has_star:
            yield RuleMatch(
                node=node,
                context=context,
                message="EXISTS with SELECT * - use SELECT 1",
                matched_text="EXISTS (SELECT * ...)",
            )


class CorrelatedSubqueryCouldBeWindowRule(ASTRule):
    """SQL-SUB-007: Correlated subquery that could be window function.

    STRUCTURAL REWRITE: Optimizers cannot convert correlated aggregate
    subqueries to window functions - this requires semantic understanding.

    Problem - Correlated subquery for running total/ranking:
        SELECT o.*,
            (SELECT SUM(amount) FROM orders o2
             WHERE o2.customer_id = o.customer_id
             AND o2.order_date <= o.order_date) as running_total
        FROM orders o

    Better - Window function (single pass):
        SELECT o.*,
            SUM(amount) OVER (
                PARTITION BY customer_id
                ORDER BY order_date
                ROWS UNBOUNDED PRECEDING
            ) as running_total
        FROM orders o

    Detection:
    - Find correlated subquery with aggregate
    - Check for inequality on ordering column (<=, <)
    """

    rule_id = "SQL-SUB-007"
    name = "Correlated Subquery Could Be Window"
    severity = "high"
    category = "subqueries"
    penalty = 20
    description = "Correlated aggregate subquery - rewrite as window function"
    suggestion = "Use window function with OVER (PARTITION BY ... ORDER BY ...)"

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Must be in SELECT list (scalar context)
        if context.in_where or context.in_having:
            return

        inner_select = node.find(exp.Select)
        if not inner_select:
            return

        # Check for aggregate function
        has_aggregate = bool(inner_select.find(exp.AggFunc))
        if not has_aggregate:
            return

        # Check for inequality comparison (suggests running aggregate)
        has_inequality = bool(inner_select.find((exp.LTE, exp.LT, exp.GTE, exp.GT)))

        # Check if correlated
        is_correlated = self._is_correlated(node)

        if is_correlated and has_inequality:
            yield RuleMatch(
                node=node,
                context=context,
                message="Correlated aggregate with inequality - use window function",
                matched_text="Running total/cumulative pattern",
            )

    def _is_correlated(self, subquery: exp.Expression) -> bool:
        """Check if subquery references outer query columns."""
        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return False

        inner_tables = set()
        for table in inner_select.find_all(exp.Table):
            alias = str(table.alias or table.this).lower()
            inner_tables.add(alias)

        for col in inner_select.find_all(exp.Column):
            table_ref = str(col.table).lower() if col.table else ""
            if table_ref and table_ref not in inner_tables:
                return True

        return False
