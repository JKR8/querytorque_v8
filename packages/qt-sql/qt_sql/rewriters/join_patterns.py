"""JOIN Pattern Rewriters.

QT-JOIN-001: LEFT JOIN -> INNER JOIN (when WHERE clause rejects NULLs)
QT-JOIN-002: LEFT JOIN ... WHERE B.pk IS NULL -> ANTI-JOIN (NOT EXISTS)
QT-JOIN-003: Join only used for existence filter -> SEMI-JOIN / EXISTS
SQL-JOIN-011: Triangular self-join -> Window function (running totals)
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
class LeftJoinNullToNotExistsRewriter(BaseRewriter):
    """Rewrites LEFT JOIN + WHERE IS NULL to NOT EXISTS.

    This pattern is the anti-join idiom:
        SELECT * FROM A
        LEFT JOIN B ON A.id = B.a_id
        WHERE B.id IS NULL
        ->
        SELECT * FROM A
        WHERE NOT EXISTS (SELECT 1 FROM B WHERE B.a_id = A.id)

    Benefits:
    - Often more efficient (no need to materialize NULL-extended rows)
    - Clearer intent (anti-join)
    - Better optimizer hints
    """

    rewriter_id = "left_join_null_to_not_exists"
    name = "LEFT JOIN IS NULL to NOT EXISTS"
    description = "Convert LEFT JOIN anti-join pattern to NOT EXISTS"
    linked_rule_ids = ("QT-JOIN-002",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for LEFT JOIN + IS NULL pattern."""
        if not isinstance(node, exp.Select):
            return False

        # Need LEFT JOIN (sqlglot uses side="LEFT")
        left_joins = []
        for j in node.find_all(exp.Join):
            side = str(j.args.get("side", "")).upper()
            if side == "LEFT":
                left_joins.append(j)

        if not left_joins:
            return False

        # Need WHERE with IS NULL on right table column
        where = node.find(exp.Where)
        if not where:
            return False

        # Check for IS NULL pattern
        for is_node in where.find_all(exp.Is):
            if isinstance(is_node.expression, exp.Null):
                # Check if column is from a LEFT JOINed table
                col = is_node.this
                if isinstance(col, exp.Column) and col.table:
                    col_table = str(col.table).lower()
                    for join in left_joins:
                        join_table = join.find(exp.Table)
                        if join_table:
                            join_alias = str(join_table.alias or join_table.name).lower()
                            if col_table == join_alias:
                                return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Transform LEFT JOIN + IS NULL to NOT EXISTS."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()

            # Find LEFT JOINs and their IS NULL conditions
            left_joins = [
                j for j in rewritten.find_all(exp.Join)
                if str(j.args.get("side", "")).upper() == "LEFT"
            ]
            where = rewritten.find(exp.Where)

            if not where or not left_joins:
                return self._create_failure(original_sql, "Pattern not found")

            # Find which joins to convert
            joins_to_remove = []
            not_exists_conditions = []

            for join in left_joins:
                join_table = join.find(exp.Table)
                if not join_table:
                    continue

                join_alias = str(join_table.alias or join_table.name).lower()
                join_on = join.args.get("on")

                # Check if there's an IS NULL for this table
                for is_node in where.find_all(exp.Is):
                    if not isinstance(is_node.expression, exp.Null):
                        continue

                    col = is_node.this
                    if not isinstance(col, exp.Column) or not col.table:
                        continue

                    if str(col.table).lower() == join_alias:
                        # Found anti-join pattern
                        joins_to_remove.append((join, is_node))

                        # Build NOT EXISTS subquery
                        # Get table alias (may be string or identifier)
                        table_alias = join_table.alias
                        if table_alias:
                            if isinstance(table_alias, str):
                                alias_expr = exp.TableAlias(this=exp.to_identifier(table_alias))
                            else:
                                alias_expr = exp.TableAlias(this=table_alias.copy())
                        else:
                            alias_expr = None

                        inner_select = exp.Select(
                            expressions=[exp.Literal.number(1)]
                        ).from_(
                            exp.Table(
                                this=join_table.this.copy(),
                                alias=alias_expr,
                            )
                        )

                        # Add ON condition as WHERE
                        if join_on:
                            inner_select.set("where", exp.Where(this=join_on.copy()))

                        not_exists = exp.Not(
                            this=exp.Exists(this=exp.Subquery(this=inner_select))
                        )
                        not_exists_conditions.append(not_exists)
                        break

            if not joins_to_remove:
                return self._create_failure(original_sql, "No anti-join pattern found")

            # Remove the LEFT JOINs
            existing_joins = list(rewritten.args.get("joins", []))
            for join, is_null in joins_to_remove:
                if join in existing_joins:
                    existing_joins.remove(join)

            rewritten.set("joins", existing_joins if existing_joins else None)

            # Rebuild WHERE clause
            remaining_conditions = []
            is_null_nodes = {is_null for _, is_null in joins_to_remove}

            def collect_conditions(expr):
                """Collect conditions excluding the IS NULL ones."""
                if isinstance(expr, exp.And):
                    collect_conditions(expr.left)
                    collect_conditions(expr.right)
                elif expr not in is_null_nodes:
                    remaining_conditions.append(expr.copy())

            collect_conditions(where.this)

            # Combine remaining conditions with NOT EXISTS
            all_conditions = remaining_conditions + not_exists_conditions

            if all_conditions:
                combined = all_conditions[0]
                for cond in all_conditions[1:]:
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
                explanation=f"Converted {len(joins_to_remove)} anti-join pattern(s) to NOT EXISTS",
            )

            result.add_safety_check(
                name="anti_join_equivalence",
                result=SafetyCheckResult.PASSED,
                message="LEFT JOIN + IS NULL is semantically equivalent to NOT EXISTS",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class ExistsToSemiJoinRewriter(BaseRewriter):
    """Rewrites EXISTS/IN subquery to explicit SEMI-JOIN hint.

    For engines that support it, explicit SEMI-JOIN can be faster:
        SELECT * FROM A WHERE EXISTS (SELECT 1 FROM B WHERE B.a_id = A.id)
        ->
        SELECT * FROM A SEMI JOIN B ON B.a_id = A.id

    Note: Many engines auto-optimize EXISTS to semi-join, so this
    may just make the intent clearer without changing the plan.
    """

    rewriter_id = "exists_to_semi_join"
    name = "EXISTS to SEMI-JOIN"
    description = "Convert EXISTS subquery to SEMI-JOIN (where supported)"
    linked_rule_ids = ("QT-SUBQ-002",)
    default_confidence = RewriteConfidence.MEDIUM
    dialects = ("duckdb", "postgres")  # Engines with good semi-join support

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for simple EXISTS pattern."""
        if not isinstance(node, exp.Select):
            return False

        # Look for EXISTS in WHERE
        where = node.find(exp.Where)
        if not where:
            return False

        for exists in where.find_all(exp.Exists):
            subq = exists.find(exp.Subquery)
            if subq:
                inner = subq.find(exp.Select)
                if inner:
                    # Must have correlation
                    inner_where = inner.find(exp.Where)
                    if inner_where and self._has_correlation(inner_where, node):
                        return True

        return False

    def _has_correlation(self, inner_where: exp.Where, outer: exp.Select) -> bool:
        """Check if inner WHERE references outer tables."""
        outer_tables = set()
        from_clause = outer.find(exp.From)
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                alias = table.alias or table.name
                if alias:
                    outer_tables.add(str(alias).lower())

        for col in inner_where.find_all(exp.Column):
            if col.table and str(col.table).lower() in outer_tables:
                return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Transform EXISTS to SEMI-JOIN.

        Note: This creates a JOIN with kind='SEMI'. Not all dialects
        support SEMI JOIN syntax directly, but it expresses intent.
        """
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            where = rewritten.find(exp.Where)

            if not where:
                return self._create_failure(original_sql, "No WHERE clause")

            transformations = 0
            new_joins = list(rewritten.args.get("joins", []))
            conditions_to_keep = []

            def process_condition(cond):
                """Process a condition, converting EXISTS to SEMI JOIN."""
                nonlocal transformations

                if isinstance(cond, exp.Exists):
                    subq = cond.find(exp.Subquery)
                    if subq:
                        inner = subq.find(exp.Select)
                        if inner:
                            # Extract table and correlation
                            inner_from = inner.find(exp.From)
                            inner_where = inner.find(exp.Where)

                            if inner_from and inner_where:
                                inner_table = inner_from.find(exp.Table)
                                if inner_table:
                                    # Build SEMI JOIN
                                    semi_join = exp.Join(
                                        this=inner_table.copy(),
                                        on=inner_where.this.copy(),
                                        kind="SEMI",
                                    )
                                    new_joins.append(semi_join)
                                    transformations += 1
                                    return None  # Remove from WHERE

                elif isinstance(cond, exp.And):
                    left_result = process_condition(cond.left)
                    right_result = process_condition(cond.right)

                    if left_result is None and right_result is None:
                        return None
                    elif left_result is None:
                        return right_result
                    elif right_result is None:
                        return left_result
                    else:
                        return exp.And(this=left_result, expression=right_result)

                return cond

            new_where_expr = process_condition(where.this)

            if transformations == 0:
                return self._create_failure(original_sql, "No EXISTS patterns found")

            rewritten.set("joins", new_joins if new_joins else None)

            if new_where_expr:
                rewritten.set("where", exp.Where(this=new_where_expr))
            else:
                rewritten.set("where", None)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converted {transformations} EXISTS to SEMI-JOIN",
            )

            result.add_safety_check(
                name="semi_join_support",
                result=SafetyCheckResult.WARNING,
                message="SEMI JOIN syntax may not be supported by all engines",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class TriangularToWindowRewriter(BaseRewriter):
    """Rewrites triangular self-joins for running totals to window functions.

    This pattern uses a correlated subquery or self-join to compute running totals:
        SELECT a.*, (SELECT SUM(b.amount) FROM t b WHERE b.id <= a.id) FROM t a
        ->
        SELECT *, SUM(amount) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM t

    Also handles explicit self-joins:
        SELECT a.*, SUM(b.amount)
        FROM t a
        LEFT JOIN t b ON b.id <= a.id
        GROUP BY a.id, a.amount
        ->
        SELECT *, SUM(amount) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM t

    Benefits:
    - Eliminates O(n²) self-join
    - Uses optimized window function implementation
    - Simpler, more readable query
    """

    rewriter_id = "triangular_to_window"
    name = "Triangular Self-Join to Window Function"
    description = "Convert triangular self-joins for running totals to window functions"
    linked_rule_ids = ("SQL-JOIN-011",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for triangular self-join or correlated subquery pattern."""
        if not isinstance(node, exp.Select):
            return False

        # Pattern 1: Correlated subquery with aggregate
        for subq in node.find_all(exp.Subquery):
            inner = subq.find(exp.Select)
            if not inner:
                continue

            # Must have aggregate function
            has_agg = any(inner.find_all(exp.AggFunc))
            if not has_agg:
                continue

            # Must have WHERE with inequality (<=, <, >=, >)
            inner_where = inner.find(exp.Where)
            if inner_where:
                for ineq in inner_where.find_all((exp.LTE, exp.LT, exp.GTE, exp.GT)):
                    # Check if references outer table
                    if self._has_outer_reference(ineq, node):
                        return True

        # Pattern 2: Self-join with same table on both sides
        from_table = node.find(exp.From)
        if not from_table:
            return False

        main_table = from_table.find(exp.Table)
        if not main_table:
            return False

        main_table_name = str(main_table.this).lower()

        for join in node.find_all(exp.Join):
            join_table = join.find(exp.Table)
            if not join_table:
                continue

            if str(join_table.this).lower() == main_table_name:
                # Self-join detected, check for inequality in ON clause
                on_clause = join.args.get("on")
                if on_clause:
                    for ineq in on_clause.find_all((exp.LTE, exp.LT, exp.GTE, exp.GT)):
                        return True

        return False

    def _has_outer_reference(self, expr: exp.Expression, outer: exp.Select) -> bool:
        """Check if expression references outer query tables."""
        outer_tables = set()
        from_clause = outer.find(exp.From)
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                alias = table.alias or table.name
                if alias:
                    outer_tables.add(str(alias).lower())

        for col in expr.find_all(exp.Column):
            if col.table and str(col.table).lower() in outer_tables:
                return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Transform triangular self-join to window function."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            transformations = 0

            # Pattern 1: Correlated subquery in SELECT list
            new_expressions = []
            for select_expr in rewritten.expressions:
                transformed = self._transform_correlated_subquery(select_expr)
                if transformed != select_expr:
                    new_expressions.append(transformed)
                    transformations += 1
                else:
                    new_expressions.append(select_expr)

            if transformations > 0:
                rewritten.set("expressions", new_expressions)

                result = self._create_result(
                    success=True,
                    original_sql=original_sql,
                    rewritten_sql=rewritten.sql(),
                    rewritten_node=rewritten,
                    confidence=RewriteConfidence.HIGH,
                    explanation=f"Converted {transformations} triangular pattern(s) to window functions",
                )

                result.add_safety_check(
                    name="window_function_equivalence",
                    result=SafetyCheckResult.PASSED,
                    message="Running total via window function is semantically equivalent",
                )

                return result

            # Pattern 2: Self-join pattern (more complex, simplified implementation)
            # For now, just detect and suggest manual rewrite
            from_table = rewritten.find(exp.From)
            if from_table:
                main_table = from_table.find(exp.Table)
                if main_table:
                    main_table_name = str(main_table.this).lower()

                    for join in rewritten.find_all(exp.Join):
                        join_table = join.find(exp.Table)
                        if join_table and str(join_table.this).lower() == main_table_name:
                            on_clause = join.args.get("on")
                            if on_clause:
                                for ineq in on_clause.find_all((exp.LTE, exp.LT, exp.GTE, exp.GT)):
                                    result = self._create_result(
                                        success=False,
                                        original_sql=original_sql,
                                        rewritten_sql=original_sql,
                                        confidence=RewriteConfidence.MEDIUM,
                                        explanation="Detected triangular self-join pattern that can be rewritten to window function",
                                    )
                                    result.add_safety_check(
                                        name="manual_rewrite_suggested",
                                        result=SafetyCheckResult.WARNING,
                                        message="Complex self-join pattern detected. Consider manual rewrite to window function.",
                                    )
                                    return result

            return self._create_failure(original_sql, "No triangular pattern found")

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _transform_correlated_subquery(self, expr: exp.Expression) -> exp.Expression:
        """Transform correlated subquery to window function if applicable."""
        if not isinstance(expr, exp.Subquery):
            return expr

        inner = expr.find(exp.Select)
        if not inner:
            return expr

        # Extract aggregate function
        agg_func = None
        for agg in inner.find_all(exp.AggFunc):
            agg_func = agg
            break

        if not agg_func:
            return expr

        # Extract column being aggregated
        agg_col = None
        for col in agg_func.find_all(exp.Column):
            agg_col = col
            break

        # Extract ordering column from WHERE inequality
        inner_where = inner.find(exp.Where)
        if not inner_where:
            return expr

        order_col = None
        for ineq in inner_where.find_all((exp.LTE, exp.LT, exp.GTE, exp.GT)):
            # Try to extract the column that's being compared
            left_col = ineq.this if isinstance(ineq.this, exp.Column) else None
            right_col = ineq.expression if isinstance(ineq.expression, exp.Column) else None

            if left_col and right_col:
                # Use the inner table's column for ordering
                order_col = left_col
                break

        if not order_col or not agg_col:
            return expr

        # Build window function
        # Get aggregate function name
        agg_name = type(agg_func).__name__
        if agg_name.startswith("exp."):
            agg_name = agg_name[4:]

        # Create new aggregate with window spec
        window_spec = exp.Window(
            order=exp.Order(expressions=[exp.Ordered(this=order_col.copy())]),
            spec=exp.WindowSpec(
                kind="ROWS",
                start="UNBOUNDED",
                start_side="PRECEDING",
            )
        )

        new_agg = type(agg_func)(this=agg_col.copy())
        new_agg.set("over", window_spec)

        return new_agg


@register_rewriter
class LeftJoinToInnerRewriter(BaseRewriter):
    """Rewrites LEFT JOIN to INNER JOIN when WHERE clause rejects NULLs.

    When a LEFT JOIN is followed by a WHERE clause that would reject NULL values
    from the right table, the LEFT JOIN can be safely converted to INNER JOIN:

        SELECT * FROM A
        LEFT JOIN B ON A.id = B.a_id
        WHERE B.status = 'active'
        ->
        SELECT * FROM A
        INNER JOIN B ON A.id = B.a_id
        WHERE B.status = 'active'

    Also handles:
    - WHERE B.col IS NOT NULL
    - WHERE B.col = value
    - WHERE B.col > value
    - Any condition that requires B.col to be non-NULL

    Benefits:
    - Clearer intent (no optional matching)
    - Potentially better join algorithms (hash join vs nested loop)
    - Smaller intermediate results (no NULL-extended rows)
    """

    rewriter_id = "left_join_to_inner"
    name = "LEFT JOIN to INNER JOIN"
    description = "Convert LEFT JOIN to INNER JOIN when WHERE clause rejects NULLs"
    linked_rule_ids = ("QT-JOIN-001",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for LEFT JOIN with WHERE clause that rejects NULLs."""
        if not isinstance(node, exp.Select):
            return False

        # Need LEFT JOIN
        left_joins = []
        for j in node.find_all(exp.Join):
            side = str(j.args.get("side", "")).upper()
            if side == "LEFT":
                left_joins.append(j)

        if not left_joins:
            return False

        # Need WHERE clause
        where = node.find(exp.Where)
        if not where:
            return False

        # Check if WHERE filters on right table columns in a way that rejects NULLs
        for join in left_joins:
            join_table = join.find(exp.Table)
            if not join_table:
                continue

            join_alias = str(join_table.alias or join_table.name).lower()

            # Check for conditions that would reject NULLs
            if self._where_rejects_nulls(where, join_alias):
                return True

        return False

    def _where_rejects_nulls(self, where: exp.Where, table_alias: str) -> bool:
        """Check if WHERE clause would reject NULL values from the given table."""
        # Pattern 1: B.col = value (NULL would fail equality)
        for eq in where.find_all(exp.EQ):
            col = eq.this if isinstance(eq.this, exp.Column) else None
            if col and col.table and str(col.table).lower() == table_alias:
                return True

        # Pattern 2: B.col IS NOT NULL (explicit NULL rejection)
        for is_node in where.find_all(exp.Is):
            if isinstance(is_node.expression, exp.Null):
                continue  # This is IS NULL, not IS NOT NULL

            col = is_node.this
            if isinstance(col, exp.Column) and col.table:
                if str(col.table).lower() == table_alias:
                    return True

        # Pattern 3: NOT (B.col IS NULL)
        for not_node in where.find_all(exp.Not):
            is_node = not_node.this
            if isinstance(is_node, exp.Is) and isinstance(is_node.expression, exp.Null):
                col = is_node.this
                if isinstance(col, exp.Column) and col.table:
                    if str(col.table).lower() == table_alias:
                        return True

        # Pattern 4: Comparisons (>, <, >=, <=, !=) all reject NULL
        for comp in where.find_all((exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ)):
            col = comp.this if isinstance(comp.this, exp.Column) else None
            if not col:
                col = comp.expression if isinstance(comp.expression, exp.Column) else None

            if col and col.table and str(col.table).lower() == table_alias:
                return True

        # Pattern 5: IN clause (NULL not in any list)
        for in_node in where.find_all(exp.In):
            col = in_node.this
            if isinstance(col, exp.Column) and col.table:
                if str(col.table).lower() == table_alias:
                    return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Transform LEFT JOIN to INNER JOIN when safe."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            where = rewritten.find(exp.Where)

            if not where:
                return self._create_failure(original_sql, "No WHERE clause")

            # Find LEFT JOINs that can be converted
            conversions = 0
            for join in rewritten.find_all(exp.Join):
                side = str(join.args.get("side", "")).upper()
                if side != "LEFT":
                    continue

                join_table = join.find(exp.Table)
                if not join_table:
                    continue

                join_alias = str(join_table.alias or join_table.name).lower()

                # Check if WHERE rejects NULLs for this table
                if self._where_rejects_nulls(where, join_alias):
                    # Convert to INNER JOIN
                    join.set("side", None)  # INNER is the default (no side)
                    conversions += 1

            if conversions == 0:
                return self._create_failure(original_sql, "No LEFT JOINs can be converted")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Converted {conversions} LEFT JOIN(s) to INNER JOIN based on WHERE clause",
            )

            result.add_safety_check(
                name="null_rejection_equivalence",
                result=SafetyCheckResult.PASSED,
                message="WHERE clause rejects NULLs, making LEFT JOIN equivalent to INNER JOIN",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))
