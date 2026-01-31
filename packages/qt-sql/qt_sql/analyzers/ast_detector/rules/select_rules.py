"""SELECT clause anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class ScalarSubqueryInSelectRule(ASTRule):
    """SQL-SEL-002: Detect scalar subquery in SELECT list.

    Scalar subqueries in SELECT execute once per row of the outer query:
        SELECT id, (SELECT MAX(total) FROM orders WHERE user_id = u.id)
        FROM users u

    This is often a performance problem and can be rewritten as JOIN or CTE.

    Detection:
    - Find Subquery nodes that are direct children of SELECT expressions
    - Subquery must return single value (scalar)
    """

    rule_id = "SQL-SEL-002"
    name = "Scalar Subquery in SELECT"
    severity = "high"
    category = "select_clause"
    penalty = 15
    description = "Scalar subquery in SELECT list executes once per row"
    suggestion = "Rewrite as JOIN or CTE"

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if this subquery is in the SELECT list
        if not self._is_in_select_expressions(node):
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="Scalar subquery in SELECT - executes per row",
            matched_text=node.sql()[:80],
        )

    def _is_in_select_expressions(self, node: exp.Expression) -> bool:
        """Check if subquery is directly in SELECT's expression list."""
        parent = node.parent

        # Walk up to find if we're in a SELECT's expressions
        while parent:
            if isinstance(parent, exp.Select):
                # Check if node is in the expressions list (not WHERE, etc.)
                return node in parent.expressions or self._is_descendant_of_expressions(node, parent)
            # If we hit WHERE, JOIN, FROM first - not in SELECT list
            if isinstance(parent, (exp.Where, exp.Join, exp.From, exp.Having)):
                return False
            parent = parent.parent

        return False

    def _is_descendant_of_expressions(self, node: exp.Expression, select: exp.Select) -> bool:
        """Check if node is a descendant of SELECT's expressions."""
        for expr in select.expressions:
            if self._is_ancestor(expr, node):
                return True
        return False

    def _is_ancestor(self, potential_ancestor: exp.Expression, node: exp.Expression) -> bool:
        """Check if potential_ancestor is an ancestor of node."""
        current = node.parent
        while current:
            if current is potential_ancestor:
                return True
            current = current.parent
        return False


class SelectStarRule(ASTRule):
    """SQL-SEL-001: Detect SELECT * usage.

    SELECT * retrieves all columns which:
    - Wastes I/O bandwidth reading unneeded columns
    - Breaks code when table schema changes
    - Prevents covering index usage

    False Positive Prevention:
    - SELECT * inside EXISTS is idiomatic and efficient
    - COUNT(*) is not SELECT * - it's a count of rows
    - SELECT * EXCEPT (col) is intentional column exclusion
    """

    rule_id = "SQL-SEL-001"
    name = "SELECT *"
    severity = "medium"
    category = "select_clause"
    penalty = 10
    description = "Selecting all columns without explicit column list"
    suggestion = "List only the columns you need explicitly"

    target_node_types = (exp.Star,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Skip if inside EXISTS - SELECT * is idiomatic there
        if context.in_exists:
            return

        # Skip if this is COUNT(*) - parent will be Count function
        parent = node.parent
        if isinstance(parent, exp.Count):
            return

        # Skip if this is SELECT * EXCEPT(...) - explicit exclusion is intentional
        # sqlglot represents this as Star with except_ attribute
        if hasattr(node, 'except_') and node.except_:
            return

        # Skip qualified star inside aggregation function (rare edge case)
        # e.g., SUM(t.*) - unusual but valid in some dialects
        if self._is_inside_aggregate(node):
            return

        yield RuleMatch(
            node=node,
            context=context,
            matched_text=node.sql(),
        )

    def _is_inside_aggregate(self, node: exp.Expression) -> bool:
        """Check if node is inside an aggregate function."""
        parent = node.parent
        agg_types = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max, exp.ArrayAgg)
        while parent:
            if isinstance(parent, agg_types):
                return True
            if isinstance(parent, exp.Select):
                return False
            parent = parent.parent
        return False


class MultipleScalarSubqueriesRule(ASTRule):
    """SQL-SEL-003: Detect multiple scalar subqueries in SELECT.

    Multiple scalar subqueries multiply the per-row overhead:
        SELECT
            (SELECT COUNT(*) FROM orders WHERE user_id = u.id),
            (SELECT MAX(total) FROM orders WHERE user_id = u.id),
            (SELECT MIN(order_date) FROM orders WHERE user_id = u.id)
        FROM users u

    This is 3 subqueries per row - usually rewritable as single JOIN.

    Detection:
    - Count Subquery nodes in SELECT expressions
    - Flag if >= 2
    """

    rule_id = "SQL-SEL-003"
    name = "Multiple Scalar Subqueries"
    severity = "critical"
    category = "select_clause"
    penalty = 20
    description = "Multiple scalar subqueries multiply per-row overhead"
    suggestion = "Combine into single JOIN or CTE"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check main SELECT, not subqueries
        if context.in_subquery:
            return

        # Count subqueries in SELECT expressions
        subquery_count = 0
        for expr in node.expressions:
            subquery_count += len(list(expr.find_all(exp.Subquery)))

        if subquery_count >= 2:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{subquery_count} scalar subqueries in SELECT - consolidate",
                matched_text=f"SELECT with {subquery_count} subqueries",
            )


class CorrelatedSubqueryInSelectRule(ASTRule):
    """SQL-SEL-004: Detect correlated subquery in SELECT list.

    Correlated subqueries reference outer query, forcing re-execution:
        SELECT
            o.order_id,
            (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id = o.id)
        FROM orders o

    The subquery runs for EVERY row because it references o.id.

    Detection:
    - Find subqueries in SELECT expressions
    - Check if they reference columns from outer query
    """

    rule_id = "SQL-SEL-004"
    name = "Correlated Subquery in SELECT"
    severity = "critical"
    category = "select_clause"
    penalty = 20
    description = "Correlated subquery in SELECT executes per row"
    suggestion = "Rewrite as JOIN with GROUP BY"

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if subquery is in SELECT list
        if not self._is_in_select_expressions(node):
            return

        # Check if it's correlated (references outer columns)
        if self._is_correlated(node):
            yield RuleMatch(
                node=node,
                context=context,
                message="Correlated subquery in SELECT - runs per row",
                matched_text=node.sql()[:80],
            )

    def _is_in_select_expressions(self, node: exp.Expression) -> bool:
        """Check if subquery is in SELECT's expression list."""
        parent = node.parent
        while parent:
            if isinstance(parent, exp.Select):
                return True
            if isinstance(parent, (exp.Where, exp.Join, exp.From, exp.Having)):
                return False
            parent = parent.parent
        return False

    def _is_correlated(self, subquery: exp.Expression) -> bool:
        """Check if subquery references outer query columns."""
        # Get the inner SELECT
        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return False

        # Get tables defined in the subquery
        inner_tables = set()
        for table in inner_select.find_all(exp.Table):
            alias = table.alias or str(table.this)
            inner_tables.add(alias.lower())

        # Check if WHERE/JOIN references columns not from inner tables
        for col in inner_select.find_all(exp.Column):
            table_ref = str(col.table).lower() if col.table else ""
            if table_ref and table_ref not in inner_tables:
                return True

        return False


class DistinctCrutchRule(ASTRule):
    """SQL-SEL-005: Detect DISTINCT that may mask a join problem.

    DISTINCT often hides accidental Cartesian or wrong join:
        SELECT DISTINCT customer_name FROM orders o JOIN customers c ...

    If you need DISTINCT, the query may be producing duplicates
    due to a join issue.

    Detection:
    - Find SELECT DISTINCT
    - Check if query has JOINs (more suspicious)
    """

    rule_id = "SQL-SEL-005"
    name = "DISTINCT as Crutch"
    severity = "medium"
    category = "select_clause"
    penalty = 10
    description = "DISTINCT may mask a join problem"
    suggestion = "Investigate why duplicates exist - may be wrong join"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if DISTINCT is set
        if not node.args.get('distinct'):
            return

        # Only flag if there are JOINs (more likely to be a problem)
        has_joins = bool(node.find(exp.Join))
        if has_joins:
            yield RuleMatch(
                node=node,
                context=context,
                message="SELECT DISTINCT with JOINs - review join conditions",
                matched_text="SELECT DISTINCT ... JOIN",
            )


class ScalarUDFInSelectRule(ASTRule):
    """SQL-SEL-006: Detect scalar UDF in SELECT list.

    Scalar UDFs are black boxes to the optimizer:
        SELECT dbo.fn_calculate_tax(total) FROM orders

    They execute row-by-row and prevent parallelism.

    Detection:
    - Find function calls with schema prefix (dbo., schema.)
    - These are likely user-defined functions
    """

    rule_id = "SQL-SEL-006"
    name = "Scalar UDF in SELECT"
    severity = "high"
    category = "select_clause"
    penalty = 15
    description = "Scalar UDF prevents parallelization - row-by-row"
    suggestion = "Inline the logic or use inline table-valued function"

    target_node_types = (exp.Anonymous,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if function has schema prefix (indicates UDF)
        func_name = str(node.this) if node.this else ""

        # Look for schema.function pattern
        if '.' in func_name:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Scalar UDF {func_name}() - executes row-by-row",
                matched_text=node.sql()[:60],
            )
