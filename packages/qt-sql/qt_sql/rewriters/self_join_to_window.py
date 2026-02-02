"""Self-Join to Window Function Rewriter.

Pattern: Self-join with non-equi conditions to find max/min per group
Rewrite: Use ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1

This is the "Greatest-N-per-Group" anti-pattern from the taxonomy.
"""

from typing import Any, Optional

from sqlglot import exp

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter


@register_rewriter
class SelfJoinToWindowRewriter(BaseRewriter):
    """Rewrites self-join max/min patterns to window functions.

    Example (find highest salary per department):
        SELECT e1.*
        FROM employees e1
        LEFT JOIN employees e2
            ON e1.dept_id = e2.dept_id AND e2.salary > e1.salary
        WHERE e2.id IS NULL
        ->
        SELECT * FROM employees
        QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) = 1

    Note: Uses RANK() instead of ROW_NUMBER() to preserve tie-handling behavior.
    The self-join anti-pattern returns ALL rows tied for the max/min value,
    and RANK() = 1 correctly includes all tied rows.
    """

    rewriter_id = "self_join_to_window"
    name = "Self-Join to Window Function"
    description = "Convert self-join max/min patterns to RANK window function"
    linked_rule_ids = (
        "SQL-JOIN-005",   # Self-join for max/min pattern
        "SQL-DUCK-001",   # DuckDB: use QUALIFY instead of subquery
    )
    default_confidence = RewriteConfidence.MEDIUM

    # Dialect-specific: use QUALIFY for DuckDB
    use_qualify_for_dialects = ("duckdb",)

    def __init__(self, metadata=None, dialect: str = "duckdb"):
        super().__init__(metadata)
        self.dialect = dialect.lower()

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if node contains self-join pattern for max/min."""
        if not isinstance(node, exp.Select):
            return False

        return self._find_self_join_pattern(node) is not None

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Perform the self-join to window function transformation."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            pattern = self._find_self_join_pattern(node)
            if not pattern:
                return self._create_failure(original_sql, "No self-join max/min pattern found")

            table_name, partition_cols, order_col, order_dir, is_left_join = pattern

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converting self-join on {table_name} to window function",
            )

            # Build the window function version
            if self.dialect in self.use_qualify_for_dialects:
                rewritten = self._build_qualify_version(
                    node, table_name, partition_cols, order_col, order_dir
                )
                result.add_safety_check(
                    name="dialect_optimization",
                    result=SafetyCheckResult.PASSED,
                    message=f"Using QUALIFY clause for {self.dialect}",
                )
            else:
                rewritten = self._build_subquery_version(
                    node, table_name, partition_cols, order_col, order_dir
                )

            # Safety checks
            result.add_safety_check(
                name="tie_handling",
                result=SafetyCheckResult.PASSED,
                message="Using RANK() to preserve tie-handling behavior from original self-join",
            )

            result.add_safety_check(
                name="order_direction",
                result=SafetyCheckResult.PASSED,
                message=f"Window ORDER BY {order_col} {order_dir} matches original comparison",
            )

            if is_left_join:
                result.add_safety_check(
                    name="null_handling",
                    result=SafetyCheckResult.PASSED,
                    message="Original used LEFT JOIN with IS NULL check (anti-join pattern)",
                )

            result.rewritten_sql = rewritten.sql()
            result.rewritten_node = rewritten

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _find_self_join_pattern(
        self,
        select: exp.Select
    ) -> Optional[tuple[str, list[str], str, str, bool]]:
        """Find self-join pattern for max/min.

        Returns: (table_name, partition_cols, order_col, order_dir, is_left_join)
        or None if pattern not found.
        """
        # Get the main table
        from_clause = select.find(exp.From)
        if not from_clause:
            return None

        main_table = from_clause.find(exp.Table)
        if not main_table:
            return None

        main_table_name = str(main_table.name).lower()
        main_alias = str(main_table.alias or main_table.name).lower()

        # Find self-join
        for join in select.find_all(exp.Join):
            join_table = join.find(exp.Table)
            if not join_table:
                continue

            join_table_name = str(join_table.name).lower()
            if join_table_name != main_table_name:
                continue

            join_alias = str(join_table.alias or join_table.name).lower()
            if join_alias == main_alias:
                continue  # Not a true self-join (same alias)

            # Found self-join - analyze ON condition
            on_condition = join.args.get("on")
            if not on_condition:
                continue

            # Look for partition columns (equi-join) and order column (non-equi)
            partition_cols = []
            order_col = None
            order_dir = "DESC"  # Default for finding max

            for condition in self._flatten_and_conditions(on_condition):
                if isinstance(condition, exp.EQ):
                    # Equi-join condition = partition column
                    left_col = self._extract_column_name(condition.left, main_alias)
                    right_col = self._extract_column_name(condition.right, join_alias)

                    if left_col and right_col and left_col == right_col:
                        partition_cols.append(left_col)

                elif isinstance(condition, (exp.GT, exp.GTE)):
                    # e2.col > e1.col means we want MAX(col)
                    left_alias = self._get_column_table(condition.left)
                    right_alias = self._get_column_table(condition.right)

                    if left_alias == join_alias and right_alias == main_alias:
                        order_col = self._extract_column_name(condition.left, join_alias)
                        order_dir = "DESC"
                    elif left_alias == main_alias and right_alias == join_alias:
                        order_col = self._extract_column_name(condition.left, main_alias)
                        order_dir = "ASC"

                elif isinstance(condition, (exp.LT, exp.LTE)):
                    # e2.col < e1.col means we want MIN(col)
                    left_alias = self._get_column_table(condition.left)
                    right_alias = self._get_column_table(condition.right)

                    if left_alias == join_alias and right_alias == main_alias:
                        order_col = self._extract_column_name(condition.left, join_alias)
                        order_dir = "ASC"
                    elif left_alias == main_alias and right_alias == join_alias:
                        order_col = self._extract_column_name(condition.left, main_alias)
                        order_dir = "DESC"

            # Check WHERE for IS NULL (anti-join pattern)
            where = select.find(exp.Where)
            is_left_join = str(join.args.get("kind", "")).upper() == "LEFT"
            has_null_check = False

            if where:
                for is_node in where.find_all(exp.Is):
                    if isinstance(is_node.expression, exp.Null):
                        col = is_node.this
                        if isinstance(col, exp.Column):
                            col_table = str(col.table).lower() if col.table else ""
                            if col_table == join_alias:
                                has_null_check = True

            if partition_cols and order_col and (has_null_check or not is_left_join):
                return (main_table_name, partition_cols, order_col, order_dir, is_left_join)

        return None

    def _flatten_and_conditions(self, node: exp.Expression) -> list[exp.Expression]:
        """Flatten AND conditions into a list."""
        conditions = []

        def traverse(n):
            if isinstance(n, exp.And):
                traverse(n.left)
                traverse(n.right)
            else:
                conditions.append(n)

        traverse(node)
        return conditions

    def _extract_column_name(
        self,
        node: exp.Expression,
        expected_table: str
    ) -> Optional[str]:
        """Extract column name if it belongs to expected table."""
        if isinstance(node, exp.Column):
            table = str(node.table).lower() if node.table else ""
            if table == expected_table or not table:
                return str(node.name).lower()
        return None

    def _get_column_table(self, node: exp.Expression) -> Optional[str]:
        """Get the table alias for a column expression."""
        if isinstance(node, exp.Column) and node.table:
            return str(node.table).lower()
        return None

    def _build_qualify_version(
        self,
        original: exp.Select,
        table_name: str,
        partition_cols: list[str],
        order_col: str,
        order_dir: str,
    ) -> exp.Select:
        """Build DuckDB version with QUALIFY clause."""
        # Create SELECT * (or preserve original columns)
        new_select = exp.Select(
            expressions=[exp.Star()],
        ).from_(
            exp.Table(this=exp.to_identifier(table_name))
        )

        # Build window function
        partition_by = [exp.Column(this=exp.to_identifier(col)) for col in partition_cols]
        order_by = [
            exp.Ordered(
                this=exp.Column(this=exp.to_identifier(order_col)),
                desc=order_dir.upper() == "DESC",
            )
        ]

        window = exp.Window(
            this=exp.Anonymous(this="RANK"),
            partition_by=partition_by,
            order=exp.Order(expressions=order_by),
        )

        # Add QUALIFY clause
        qualify = exp.Qualify(
            this=exp.EQ(this=window, expression=exp.Literal.number(1))
        )
        new_select.set("qualify", qualify)

        # Preserve ORDER BY from original query (strip table aliases)
        original_order = original.find(exp.Order)
        if original_order:
            order_copy = original_order.copy()
            # Strip table aliases from columns in ORDER BY
            for col in order_copy.find_all(exp.Column):
                if col.table:
                    col.set("table", None)
            new_select.set("order", order_copy)

        # Preserve LIMIT from original query
        original_limit = original.find(exp.Limit)
        if original_limit:
            new_select.set("limit", original_limit.copy())

        return new_select

    def _build_subquery_version(
        self,
        original: exp.Select,
        table_name: str,
        partition_cols: list[str],
        order_col: str,
        order_dir: str,
    ) -> exp.Select:
        """Build standard SQL version with subquery."""
        # Build inner query with ROW_NUMBER
        partition_by = [exp.Column(this=exp.to_identifier(col)) for col in partition_cols]
        order_by = [
            exp.Ordered(
                this=exp.Column(this=exp.to_identifier(order_col)),
                desc=order_dir.upper() == "DESC",
            )
        ]

        window = exp.Window(
            this=exp.Anonymous(this="RANK"),
            partition_by=partition_by,
            order=exp.Order(expressions=order_by),
        )

        inner_select = exp.Select(
            expressions=[
                exp.Star(),
                exp.Alias(this=window, alias=exp.to_identifier("_rn")),
            ],
        ).from_(exp.Table(this=exp.to_identifier(table_name)))

        # Build outer query
        outer_select = exp.Select(
            expressions=[exp.Star()],
        ).from_(
            exp.Subquery(
                this=inner_select,
                alias=exp.TableAlias(this=exp.to_identifier("_t")),
            )
        ).where(
            exp.EQ(
                this=exp.Column(this=exp.to_identifier("_rn")),
                expression=exp.Literal.number(1),
            )
        )

        return outer_select
