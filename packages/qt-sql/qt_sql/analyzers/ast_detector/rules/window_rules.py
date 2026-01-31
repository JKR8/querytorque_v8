"""Window function anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


# Function names that require ORDER BY for deterministic results
ORDERING_REQUIRED_FUNCS = frozenset({'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE'})


class RowNumberWithoutOrderRule(ASTRule):
    """SQL-WIN-001: Detect ROW_NUMBER without ORDER BY.

    ROW_NUMBER() without ORDER BY gives non-deterministic results:
        ROW_NUMBER() OVER ()  -- Random row numbers!
        ROW_NUMBER() OVER (PARTITION BY dept)  -- Still random within partition

    The ORDER BY is required for meaningful row numbering.

    Detection:
    - Find Window nodes (OVER clause)
    - Check if function is ROW_NUMBER, RANK, DENSE_RANK, or NTILE
    - Check if Window lacks ORDER BY
    """

    rule_id = "SQL-WIN-001"
    name = "ROW_NUMBER Without ORDER BY"
    severity = "medium"
    category = "window_functions"
    penalty = 10
    description = "ROW_NUMBER without ORDER BY gives non-deterministic results"
    suggestion = "Add ORDER BY to the OVER clause"

    # Target the Window node (OVER clause) rather than individual functions
    # because sqlglot may parse RANK/DENSE_RANK as Anonymous
    target_node_types = (exp.Window,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get the function inside the Window (e.g., ROW_NUMBER, RANK)
        func = node.args.get('this')
        if not func:
            return

        # Get function name
        func_name = self._get_func_name(func)
        if not func_name:
            return

        # Only check functions that require ORDER BY
        if func_name.upper() not in ORDERING_REQUIRED_FUNCS:
            return

        # Check if window has ORDER BY
        order = node.args.get('order')
        if order:
            return  # Has ORDER BY - OK

        yield RuleMatch(
            node=node,
            context=context,
            message=f"{func_name}() without ORDER BY - non-deterministic",
            matched_text=node.sql()[:80],
        )

    def _get_func_name(self, func: exp.Expression) -> str:
        """Extract function name from window function node."""
        # ROW_NUMBER has its own class
        if isinstance(func, exp.RowNumber):
            return "ROW_NUMBER"

        # Other functions may be Anonymous with 'this' as name
        if isinstance(func, exp.Anonymous):
            return str(func.this).upper() if func.this else ""

        # Fall back to class name
        return type(func).__name__.upper()


class MultipleWindowPartitionsRule(ASTRule):
    """SQL-WIN-002: Detect multiple window functions with different partitions.

    Different PARTITION BY clauses require multiple passes:
        SELECT
            SUM(amount) OVER (PARTITION BY customer_id),
            SUM(amount) OVER (PARTITION BY product_id)
        FROM sales

    Each unique partition/order combination requires a separate sort.

    Detection:
    - Find multiple Window nodes in SELECT
    - Compare partition definitions
    """

    rule_id = "SQL-WIN-002"
    name = "Multiple Window Partitions"
    severity = "medium"
    category = "window_functions"
    penalty = 10
    description = "Different PARTITION BY clauses require multiple sorts"
    suggestion = "Consolidate window specifications where possible"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check main query
        if context.in_subquery:
            return

        # Find all windows in SELECT expressions
        windows = []
        for expr in node.expressions:
            windows.extend(expr.find_all(exp.Window))

        if len(windows) < 2:
            return

        # Get unique partition specs
        partition_specs = set()
        for win in windows:
            partition = win.args.get('partition_by')
            spec = partition.sql() if partition else ""
            partition_specs.add(spec)

        # Flag if more than 2 different partitions
        if len(partition_specs) > 2:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{len(partition_specs)} different PARTITION BY clauses",
                matched_text=f"Query with {len(windows)} window functions",
            )


class WindowWithoutPartitionRule(ASTRule):
    """SQL-WIN-003: Detect OVER() without PARTITION BY.

    Window without PARTITION BY scans entire table:
        SUM(amount) OVER ()  -- Scans all rows
        SUM(amount) OVER (ORDER BY date)  -- Still scans all

    This can be expensive on large tables.

    Detection:
    - Find Window nodes
    - Check if partition_by is missing
    """

    rule_id = "SQL-WIN-003"
    name = "Window Without PARTITION BY"
    severity = "low"
    category = "window_functions"
    penalty = 5
    description = "Window function without PARTITION BY scans all rows"
    suggestion = "Add PARTITION BY if full table scan not intended"

    target_node_types = (exp.Window,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if no partition_by
        if not node.args.get('partition_by'):
            func = node.args.get('this')
            func_name = type(func).__name__ if func else "Window function"

            yield RuleMatch(
                node=node,
                context=context,
                message=f"{func_name} without PARTITION BY - full table scan",
                matched_text=node.sql()[:60],
            )


class NestedWindowFunctionRule(ASTRule):
    """SQL-WIN-004: Detect nested window functions.

    Window functions cannot be nested in standard SQL:
        SELECT SUM(ROW_NUMBER() OVER ()) OVER ()  -- Error!
        SELECT AVG(RANK() OVER (ORDER BY id)) OVER ()  -- Error!

    However, some databases allow it with subqueries, but it's often
    unintentional and causes errors.

    Detection:
    - Find Window nodes
    - Check if the window function argument contains another window
    """

    rule_id = "SQL-WIN-004"
    name = "Nested Window Function"
    severity = "high"
    category = "window_functions"
    penalty = 15
    description = "Window function contains another window function"
    suggestion = "Use subquery or CTE to compute inner window first"

    target_node_types = (exp.Window,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get the function inside the window
        func = node.args.get('this')
        if not func:
            return

        # Check if the function arguments contain another window
        for arg in func.walk():
            if isinstance(arg, exp.Window) and arg is not node:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="Nested window function - not allowed in standard SQL",
                    matched_text=node.sql()[:80],
                )
                return
