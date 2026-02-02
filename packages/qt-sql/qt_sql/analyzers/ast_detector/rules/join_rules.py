"""JOIN anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class CartesianJoinRule(ASTRule):
    """SQL-JOIN-001: Detect Cartesian/Cross joins.

    Cartesian products multiply rows between tables, often accidentally:
        FROM users, orders  -- Implicit cross join (old syntax)
        FROM users CROSS JOIN orders  -- Explicit cross join

    This produces N*M rows which is usually unintended.

    Detection Strategy:
    1. CROSS JOIN keyword -> Always flag
    2. Comma syntax in FROM -> Flag (old SQL-89 style)
    3. JOIN without ON clause -> Flag (except for lateral/cross apply)

    False Positive Prevention:
    - CROSS APPLY / OUTER APPLY are valid patterns
    - Lateral joins are intentional
    - Small dimension tables crossed intentionally are acceptable
      (but we can't know table sizes, so we flag anyway with suggestion)
    """

    rule_id = "SQL-JOIN-001"
    name = "Cartesian Join"
    severity = "info"  # Demoted: high false positive rate on comma joins with WHERE predicates
    category = "joins"
    penalty = 0  # Info only - not actionable by optimizer
    description = "Possible Cartesian product - rows multiply between tables"
    suggestion = "Add explicit JOIN with ON clause, or confirm CROSS JOIN is intentional"

    target_node_types = (exp.Join, exp.From)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if isinstance(node, exp.Join):
            yield from self._check_join(node, context)
        elif isinstance(node, exp.From):
            yield from self._check_from(node, context)

    def _check_join(self, node: exp.Join, context: ASTContext) -> Iterator[RuleMatch]:
        """Check JOIN nodes for cross join patterns."""
        # Get join kind (INNER, LEFT, CROSS, etc.)
        join_kind = getattr(node, 'kind', '') or ''
        join_kind_upper = str(join_kind).upper()

        # CROSS JOIN is explicit Cartesian
        if 'CROSS' in join_kind_upper:
            # Skip CROSS APPLY - that's a valid lateral pattern
            if 'APPLY' in join_kind_upper:
                return

            yield RuleMatch(
                node=node,
                context=context,
                message="CROSS JOIN creates Cartesian product",
                matched_text=node.sql()[:100],
            )
            return

        # Check for JOIN without ON clause (implicit cross)
        on_clause = node.args.get('on')
        using_clause = node.args.get('using')
        method = node.args.get('method', '')

        if not on_clause and not using_clause:
            # Natural join doesn't need ON (method='NATURAL')
            if 'NATURAL' in join_kind_upper or str(method).upper() == 'NATURAL':
                return

            # LATERAL is intentional
            if self._is_lateral_join(node):
                return

            # Check if this is a comma-join with WHERE predicate
            # If WHERE has predicates linking this table to others, it's not a true Cartesian
            if self._has_where_join_predicate(node, context):
                return  # Skip - WHERE provides the join condition

            yield RuleMatch(
                node=node,
                context=context,
                message="JOIN without ON clause - possible Cartesian product",
                matched_text=node.sql()[:100],
            )

    def _check_from(self, node: exp.From, context: ASTContext) -> Iterator[RuleMatch]:
        """Check FROM clause for comma-separated tables (old join syntax)."""
        # Get all table expressions in FROM
        tables = list(node.find_all(exp.Table))

        # If there are multiple direct table references under FROM (not via JOIN),
        # this might be comma syntax
        # sqlglot typically wraps comma joins, so we check differently

        # Look for comma-separated tables by checking the parent structure
        # In modern sqlglot, comma FROM is parsed as nested Joins or as From with multiple
        # We check by looking at the FROM's expression structure

        from_expr = node.this
        if isinstance(from_expr, exp.Table):
            # Single table - OK
            return

        # Check for implicit cross join patterns
        # sqlglot parses "FROM a, b" as Join with kind=None or empty
        for join in node.find_all(exp.Join):
            join_kind = getattr(join, 'kind', None)
            on_clause = join.args.get('on')
            using_clause = join.args.get('using')

            # Empty kind with no ON = comma join
            if not join_kind and not on_clause and not using_clause:
                # Check if WHERE clause provides the join predicate
                if self._has_where_join_predicate(join, context):
                    return  # Skip - WHERE provides the join condition

                yield RuleMatch(
                    node=join,
                    context=context,
                    message="Comma-separated tables in FROM (implicit CROSS JOIN)",
                    matched_text=join.sql()[:100],
                )
                return  # Only report once per FROM

    def _has_where_join_predicate(self, join_node: exp.Join, context: ASTContext) -> bool:
        """Check if WHERE clause has a predicate linking this comma-joined table.

        For comma-style joins like 'FROM a, b WHERE a.id = b.id',
        the WHERE clause provides the join condition. We should not flag
        these as Cartesian joins.
        """
        # Get the table being joined
        joined_table = join_node.this
        if not isinstance(joined_table, exp.Table):
            return False

        table_name = joined_table.name
        table_alias = joined_table.alias or table_name

        # Find the parent SELECT to get the WHERE clause
        parent = join_node.parent
        while parent and not isinstance(parent, exp.Select):
            parent = parent.parent

        if not parent:
            return False

        where_clause = parent.find(exp.Where)
        if not where_clause:
            return False

        # Look for equality conditions in WHERE that reference this table
        for eq in where_clause.find_all(exp.EQ):
            left = eq.left
            right = eq.right

            # Check if both sides are columns from different tables
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                left_table = left.table or ""
                right_table = right.table or ""

                # Check if this join's table is involved
                if table_alias in (left_table, right_table) or table_name in (left_table, right_table):
                    # And the other side is a different table
                    if left_table != right_table:
                        return True

        return False

    def _is_lateral_join(self, node: exp.Join) -> bool:
        """Check if this is a lateral join."""
        # Check for LATERAL keyword
        if node.args.get('lateral'):
            return True

        # Check the joined expression for Lateral wrapper
        joined = node.this
        if isinstance(joined, exp.Lateral):
            return True

        return False


class ImplicitJoinRule(ASTRule):
    """SQL-JOIN-002: Detect implicit join (comma-separated FROM).

    Legacy ANSI-89 join syntax using comma-separated tables:
        FROM orders o, customers c
        WHERE o.customer_id = c.id

    While functionally equivalent to explicit JOIN, this pattern:
    - Makes it easy to accidentally create Cartesian products
    - Mixes join logic with filter logic in WHERE
    - Is harder to maintain

    Detection:
    - Find comma-separated tables in FROM clause
    - Different from JOIN-001 in that it focuses on the syntax issue,
      not the potential Cartesian product
    """

    rule_id = "SQL-JOIN-002"
    name = "Implicit Join Syntax"
    severity = "info"  # Demoted: style preference only, not a performance issue
    category = "joins"
    penalty = 0  # Info only - not actionable by optimizer
    description = "Legacy comma-join syntax - use explicit JOIN instead"
    suggestion = "Rewrite using explicit INNER JOIN with ON clause"

    # Target Join nodes directly (they're siblings of From in sqlglot's parse tree)
    target_node_types = (exp.Join,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if this Join has empty kind (comma syntax)
        join_kind = getattr(node, 'kind', None)
        on_clause = node.args.get('on')
        using_clause = node.args.get('using')

        # Empty kind = comma join (even if WHERE provides the condition)
        if not join_kind and not on_clause and not using_clause:
            yield RuleMatch(
                node=node,
                context=context,
                message="Implicit comma-join syntax - prefer explicit JOIN",
                matched_text=node.sql()[:80],
            )


class FunctionInJoinRule(ASTRule):
    """SQL-JOIN-003: Detect function in JOIN condition.

    Functions in JOIN ON clause prevent index usage:
        ON UPPER(a.name) = UPPER(b.name)  -- Can't use index
        ON a.name = b.name  -- Can use index

    Similar to non-sargable WHERE conditions.

    Detection:
    - Find function calls inside JOIN ON conditions
    - Function must wrap a column reference
    """

    rule_id = "SQL-JOIN-003"
    name = "Function in JOIN Condition"
    severity = "high"
    category = "joins"
    penalty = 15
    description = "Function in JOIN prevents index usage"
    suggestion = "Fix data at source or add computed column with index"

    target_node_types = (exp.Join,)

    # Functions that prevent index usage
    NON_SARGABLE_FUNCS = (
        exp.Upper, exp.Lower, exp.Trim,
        exp.Cast, exp.TryCast,
        exp.Coalesce,
        exp.Substring, exp.Left, exp.Right,
        exp.Replace, exp.Concat,
    )

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        on_clause = node.args.get('on')
        if not on_clause:
            return

        # Find functions in ON clause that wrap columns
        for func in on_clause.find_all(*self.NON_SARGABLE_FUNCS):
            if self._wraps_column(func):
                func_name = type(func).__name__.upper()
                yield RuleMatch(
                    node=func,
                    context=context,
                    message=f"{func_name}() in JOIN ON prevents index usage",
                    matched_text=func.sql()[:60],
                )
                return  # Only report once per JOIN

    def _wraps_column(self, node: exp.Expression) -> bool:
        """Check if function wraps a column."""
        if hasattr(node, 'this') and isinstance(node.this, exp.Column):
            return True
        if hasattr(node, 'expressions'):
            for arg in node.expressions:
                if isinstance(arg, exp.Column):
                    return True
        return False


class OrInJoinRule(ASTRule):
    """SQL-JOIN-005: Detect OR in JOIN condition.

    OR in JOIN condition prevents efficient index usage:
        ON a.id = b.id OR a.alt_id = b.alt_id

    The optimizer often can't use indexes effectively with OR.

    Better alternatives:
    - UNION of separate JOINs
    - Normalize data to avoid OR conditions

    Detection:
    - Find OR expressions inside JOIN ON clause
    """

    rule_id = "SQL-JOIN-005"
    name = "OR in JOIN Condition"
    severity = "high"
    category = "joins"
    penalty = 15
    description = "OR in JOIN condition prevents efficient index usage"
    suggestion = "Use UNION of separate JOINs or normalize data"

    target_node_types = (exp.Join,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        on_clause = node.args.get('on')
        if not on_clause:
            return

        # Find OR in ON clause
        if on_clause.find(exp.Or):
            yield RuleMatch(
                node=node,
                context=context,
                message="OR in JOIN condition prevents efficient index usage",
                matched_text=on_clause.sql()[:80],
            )


class ExpressionInJoinRule(ASTRule):
    """SQL-JOIN-004: Detect expression/arithmetic in JOIN condition.

    Arithmetic or string expressions in JOIN prevent index usage:
        ON a.id + 1 = b.id  -- Arithmetic
        ON CONCAT(a.region, a.code) = b.full_code  -- Concatenation

    Detection:
    - Find arithmetic operators (+, -, *, /) in JOIN ON
    - Find string concatenation (||, CONCAT) in JOIN ON
    """

    rule_id = "SQL-JOIN-004"
    name = "Expression in JOIN Condition"
    severity = "high"
    category = "joins"
    penalty = 15
    description = "Expression in JOIN prevents index usage"
    suggestion = "Store derived key explicitly or restructure query"

    target_node_types = (exp.Join,)

    # Arithmetic and string expression types
    EXPRESSION_TYPES = (
        exp.Add, exp.Sub, exp.Mul, exp.Div,
        exp.Concat, exp.DPipe,  # String concatenation
    )

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        on_clause = node.args.get('on')
        if not on_clause:
            return

        # Find expressions in ON clause
        for expr in on_clause.find_all(*self.EXPRESSION_TYPES):
            # Check if expression involves a column
            if expr.find(exp.Column):
                expr_type = type(expr).__name__
                yield RuleMatch(
                    node=expr,
                    context=context,
                    message=f"Expression ({expr_type}) in JOIN prevents index usage",
                    matched_text=expr.sql()[:60],
                )
                return  # Only report once per JOIN


class InequalityJoinRule(ASTRule):
    """SQL-JOIN-006: Detect unbounded inequality JOIN.

    Range joins without bounds create large intermediate results:
        ON a.date > b.date  -- Every row matches many rows

    Better with bounds:
        ON a.date > b.date AND a.date <= b.date + INTERVAL 30 DAY

    Detection:
    - Find inequality comparisons (>, <, >=, <=) in JOIN ON
    - Check if there's a corresponding bound on the same columns
    """

    rule_id = "SQL-JOIN-006"
    name = "Unbounded Inequality JOIN"
    severity = "high"
    category = "joins"
    penalty = 15
    description = "Range join without bounds creates large intermediate results"
    suggestion = "Add bounds to inequality or use equality join"

    target_node_types = (exp.Join,)

    INEQUALITY_OPS = (exp.GT, exp.GTE, exp.LT, exp.LTE)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        on_clause = node.args.get('on')
        if not on_clause:
            return

        # Find inequality comparisons
        inequalities = list(on_clause.find_all(*self.INEQUALITY_OPS))
        if not inequalities:
            return

        # Check if there's also an equality condition (common in valid range joins)
        has_equality = bool(on_clause.find(exp.EQ))

        # If only inequalities and no equality, likely problematic
        if not has_equality and len(inequalities) == 1:
            ineq = inequalities[0]
            # Verify it compares columns from both sides (not column to literal)
            if self._compares_two_columns(ineq):
                yield RuleMatch(
                    node=ineq,
                    context=context,
                    message="Unbounded inequality join - may create large result",
                    matched_text=ineq.sql()[:60],
                )

    def _compares_two_columns(self, node: exp.Expression) -> bool:
        """Check if comparison is between two column references."""
        left = node.args.get('this')
        right = node.args.get('expression')
        return (
            (isinstance(left, exp.Column) or (left and left.find(exp.Column))) and
            (isinstance(right, exp.Column) or (right and right.find(exp.Column)))
        )


class TooManyJoinsRule(ASTRule):
    """SQL-JOIN-007: Detect queries with too many JOINs.

    Queries with many JOINs have exponential optimizer search space:
        FROM a JOIN b JOIN c JOIN d JOIN e JOIN f JOIN g JOIN h ...

    This can lead to:
    - Long compilation times
    - Suboptimal plans
    - Hard to maintain queries

    Detection:
    - Count JOIN clauses in query
    - Flag if exceeds threshold
    """

    rule_id = "SQL-JOIN-007"
    name = "Too Many JOINs"
    severity = "medium"
    category = "joins"
    penalty = 10
    description = "Query has excessive JOINs - optimizer may struggle"
    suggestion = "Consider splitting query or materializing intermediates"

    target_node_types = (exp.Select,)

    WARNING_THRESHOLD = 8
    CRITICAL_THRESHOLD = 15

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check top-level SELECT (not subqueries)
        if context.in_subquery or context.in_cte:
            return

        # Count JOINs in this SELECT
        join_count = len(list(node.find_all(exp.Join)))

        if join_count >= self.CRITICAL_THRESHOLD:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{join_count} JOINs - critical complexity",
                matched_text=f"Query with {join_count} JOINs",
            )
        elif join_count >= self.WARNING_THRESHOLD:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{join_count} JOINs - consider simplifying",
                matched_text=f"Query with {join_count} JOINs",
            )


class SelfJoinCouldBeWindowRule(ASTRule):
    """SQL-JOIN-008: Self-join that could be window function.

    STRUCTURAL REWRITE: Optimizer cannot convert self-joins to window
    functions - this requires semantic understanding.

    Problem - Self-join to compare rows within same table:
        SELECT a.id, a.value, b.value as prev_value
        FROM data a
        LEFT JOIN data b ON a.id = b.id + 1

        Or comparing to previous row in time:
        SELECT curr.*, prev.value as prev_value
        FROM events curr
        LEFT JOIN events prev
            ON curr.user_id = prev.user_id
            AND prev.timestamp = (
                SELECT MAX(timestamp) FROM events
                WHERE user_id = curr.user_id AND timestamp < curr.timestamp
            )

    Better - Window functions (single scan):
        SELECT id, value, LAG(value) OVER (ORDER BY id) as prev_value
        FROM data

    Detection:
    - Find JOIN where both sides are same table
    - Especially with offset/ordering patterns in condition
    """

    rule_id = "SQL-JOIN-008"
    name = "Self-Join Could Be Window"
    severity = "high"
    category = "joins"
    penalty = 15
    description = "Self-join pattern - consider LAG/LEAD window functions"
    suggestion = "Use LAG/LEAD OVER (ORDER BY ...) for previous/next row access"

    target_node_types = (exp.Join,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get join table
        join_table = node.this
        if not isinstance(join_table, exp.Table):
            return

        join_table_name = str(join_table.this).lower() if join_table.this else ""

        # Get from table (need to walk up to SELECT)
        select = node.find_ancestor(exp.Select)
        if not select:
            return

        from_clause = select.args.get('from')
        if not from_clause:
            return

        from_table = from_clause.this
        if not isinstance(from_table, exp.Table):
            return

        from_table_name = str(from_table.this).lower() if from_table.this else ""

        # Check if same table (self-join)
        if join_table_name != from_table_name:
            return

        # Check for offset pattern in ON clause (id + 1, id - 1, etc.)
        on_clause = node.args.get('on')
        if on_clause:
            has_offset = (
                on_clause.find(exp.Add) or
                on_clause.find(exp.Sub) or
                on_clause.find((exp.LT, exp.GT, exp.LTE, exp.GTE))
            )
            if has_offset:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"Self-join on '{join_table_name}' with offset - use LAG/LEAD",
                    matched_text="Self-join (previous/next row pattern)",
                )


class TriangleJoinPatternRule(ASTRule):
    """SQL-JOIN-009: Triangle join pattern (A-B, B-C, A-C).

    STRUCTURAL REWRITE: Optimizer may not recognize triangle patterns
    where the A-C join is redundant due to transitivity.

    Problem - Redundant join in triangle:
        SELECT *
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        JOIN order_items i ON o.id = i.order_id
        JOIN customers c2 ON i.customer_id = c2.id  -- Redundant if i.customer_id = o.customer_id

    The C2 join is redundant if order_items.customer_id is always
    equal to orders.customer_id (denormalized).

    Detection:
    - Find A-B-C chain where A is also joined to C
    - Flag for review (may be intentional or may be redundant)
    """

    rule_id = "SQL-JOIN-009"
    name = "Triangle Join Pattern"
    severity = "low"
    category = "joins"
    penalty = 5
    description = "Triangle join - verify third join is necessary"
    suggestion = "Review if transitive join is redundant"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Get all tables in query
        tables = {}
        from_clause = node.args.get('from')
        if from_clause and from_clause.this:
            t = from_clause.this
            if isinstance(t, exp.Table):
                name = str(t.alias or t.this).lower()
                tables[name] = t

        for join in node.find_all(exp.Join):
            t = join.this
            if isinstance(t, exp.Table):
                name = str(t.alias or t.this).lower()
                tables[name] = t

        if len(tables) < 3:
            return

        # Build join graph
        joins = list(node.find_all(exp.Join))
        edges = set()

        for join in joins:
            on_clause = join.args.get('on')
            if not on_clause:
                continue

            # Get tables referenced in ON clause
            cols = list(on_clause.find_all(exp.Column))
            join_tables = set()
            for col in cols:
                if col.table:
                    join_tables.add(str(col.table).lower())

            if len(join_tables) == 2:
                edge = tuple(sorted(join_tables))
                edges.add(edge)

        # Check for triangles (3 edges forming a cycle)
        table_list = list(tables.keys())
        for i in range(len(table_list)):
            for j in range(i+1, len(table_list)):
                for k in range(j+1, len(table_list)):
                    a, b, c = table_list[i], table_list[j], table_list[k]
                    ab = tuple(sorted([a, b]))
                    bc = tuple(sorted([b, c]))
                    ac = tuple(sorted([a, c]))

                    if ab in edges and bc in edges and ac in edges:
                        yield RuleMatch(
                            node=node,
                            context=context,
                            message=f"Triangle join: {a}-{b}-{c} - verify all joins needed",
                            matched_text=f"Tables {a}, {b}, {c} all joined",
                        )
                        return


class JoinWithSubqueryCouldBeCTERule(ASTRule):
    """SQL-JOIN-010: Join with complex subquery could be CTE.

    STRUCTURAL REWRITE: Moving complex subqueries to CTEs improves
    readability and may help optimizer.

    Problem - Complex inline subquery in JOIN:
        SELECT *
        FROM orders o
        JOIN (
            SELECT customer_id, SUM(amount) as total, COUNT(*) as cnt
            FROM orders
            WHERE date >= '2024-01-01'
            GROUP BY customer_id
            HAVING COUNT(*) > 5
        ) stats ON o.customer_id = stats.customer_id

    Better as CTE:
        WITH customer_stats AS (
            SELECT customer_id, SUM(amount) as total, COUNT(*) as cnt
            FROM orders
            WHERE date >= '2024-01-01'
            GROUP BY customer_id
            HAVING COUNT(*) > 5
        )
        SELECT * FROM orders o
        JOIN customer_stats stats ON o.customer_id = stats.customer_id

    Detection:
    - Find JOIN with subquery that has GROUP BY or multiple conditions
    """

    rule_id = "SQL-JOIN-010"
    name = "Complex Subquery in JOIN"
    severity = "low"
    category = "joins"
    penalty = 5
    description = "Complex subquery in JOIN - extract to CTE for clarity"
    suggestion = "Move complex subqueries to WITH clause for readability"

    target_node_types = (exp.Join,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if join target is a subquery
        join_target = node.this
        if not isinstance(join_target, exp.Subquery):
            return

        inner_select = join_target.find(exp.Select)
        if not inner_select:
            return

        # Check for complexity indicators
        has_group_by = bool(inner_select.args.get('group'))
        has_having = bool(inner_select.find(exp.Having))
        has_where = bool(inner_select.find(exp.Where))
        has_join = bool(inner_select.find(exp.Join))

        complexity = sum([has_group_by, has_having, has_where, has_join])

        if complexity >= 2:
            yield RuleMatch(
                node=node,
                context=context,
                message="Complex subquery in JOIN - extract to CTE",
                matched_text="JOIN (SELECT ... GROUP BY ... WHERE ...)",
            )


class TriangularJoinRule(ASTRule):
    """SQL-JOIN-011: Triangular join pattern (running total anti-pattern).

    STRUCTURAL REWRITE: Optimizer cannot recognize triangular self-joins
    that compute running totals/counts. These have O(n²) complexity.

    Problem - Hidden quadratic complexity:
        SELECT a.id, a.value,
               (SELECT COUNT(*) FROM data b WHERE b.id <= a.id) as running_count,
               (SELECT SUM(value) FROM data b WHERE b.id <= a.id) as running_sum
        FROM data a

    For 10,000 rows: (10000² + 10000)/2 = 50,005,000 comparisons!

    Solution - Window function (linear complexity):
        SELECT id, value,
               COUNT(*) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) as running_count,
               SUM(value) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) as running_sum
        FROM data

    Speedup: 100-10000x at scale

    Detection:
    - Find correlated subquery with <= or < comparison on same table
    - Especially with aggregate (COUNT, SUM) computing running total
    """

    rule_id = "SQL-JOIN-011"
    name = "Triangular Join Pattern"
    severity = "critical"
    category = "joins"
    penalty = 25
    description = "Triangular join has O(n²) complexity - use window function for O(n)"
    suggestion = "Use SUM/COUNT(*) OVER (ORDER BY col ROWS UNBOUNDED PRECEDING)"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Get tables in FROM clause
        from_tables = self._get_from_tables(node)
        if not from_tables:
            return

        # Look for scalar subqueries in SELECT
        for expr in node.expressions:
            subquery = expr.find(exp.Subquery)
            if not subquery:
                continue

            inner_select = subquery.find(exp.Select)
            if not inner_select:
                continue

            # Check for aggregate (running total indicator)
            has_aggregate = bool(inner_select.find((exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)))
            if not has_aggregate:
                continue

            # Check WHERE for triangular pattern (b.id <= a.id)
            where = inner_select.find(exp.Where)
            if not where:
                continue

            # Look for <= or < comparison
            for comp in where.find_all((exp.LTE, exp.LT)):
                # Check if both sides reference columns (potential self-join)
                left_cols = list(comp.this.find_all(exp.Column)) if hasattr(comp, 'this') else []
                right_cols = list(comp.expression.find_all(exp.Column)) if hasattr(comp, 'expression') else []

                if left_cols and right_cols:
                    # Found triangular pattern
                    yield RuleMatch(
                        node=subquery,
                        context=context,
                        message="Triangular join (b.x <= a.x with aggregate) - O(n²) complexity",
                        matched_text="(SELECT aggregate ... WHERE b.col <= a.col)",
                    )
                    return

    def _get_from_tables(self, node: exp.Select) -> set:
        """Get table names/aliases from FROM clause."""
        tables = set()
        from_clause = node.find(exp.From)
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                alias = table.alias or str(table.this)
                tables.add(alias.lower() if alias else "")
        return tables
