"""PostgreSQL-specific anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class CountStarInsteadOfExistsRule(ASTRule):
    """SQL-PG-001: COUNT(*) when EXISTS would suffice.

    Checking if rows exist using COUNT(*) scans entire result:
        SELECT CASE WHEN (SELECT COUNT(*) FROM orders WHERE user_id = 1) > 0
               THEN 'yes' ELSE 'no' END

    EXISTS stops at first match:
        SELECT CASE WHEN EXISTS(SELECT 1 FROM orders WHERE user_id = 1)
               THEN 'yes' ELSE 'no' END

    Detection:
    - Find COUNT(*) in subquery
    - Check if result is compared to 0
    """

    rule_id = "SQL-PG-001"
    name = "COUNT(*) Instead of EXISTS"
    severity = "high"
    category = "postgres"
    penalty = 15
    description = "COUNT(*) scans all rows - use EXISTS to stop at first match"
    suggestion = "Replace COUNT(*) > 0 with EXISTS subquery"
    dialects = ("postgres", "postgresql", "redshift")

    target_node_types = (exp.Count,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if COUNT(*) is in a subquery
        if not context.in_subquery:
            return

        # Check if this is COUNT(*) - has Star as argument
        if not any(isinstance(arg, exp.Star) for arg in node.expressions):
            return

        # Check if parent is comparison with 0
        parent = node.parent
        while parent and not isinstance(parent, exp.Select):
            if isinstance(parent, (exp.GT, exp.GTE, exp.EQ)):
                # Check if compared to 0 or literal
                for child in parent.iter_expressions():
                    if isinstance(child, exp.Literal) and str(child.this) == "0":
                        yield RuleMatch(
                            node=node,
                            context=context,
                            message="COUNT(*) > 0 in subquery - use EXISTS instead",
                            matched_text=node.sql()[:60],
                        )
                        return
            parent = parent.parent


class LargeInListRule(ASTRule):
    """SQL-PG-002: Large IN list instead of ANY(ARRAY[...]).

    Large IN lists are less efficient in PostgreSQL:
        WHERE id IN (1, 2, 3, ... 100 values)

    Use ANY with array for better performance:
        WHERE id = ANY(ARRAY[1, 2, 3, ...])

    Detection:
    - Find IN expressions with many literal values
    - Flag if > 10 values
    """

    rule_id = "SQL-PG-002"
    name = "Large IN List"
    severity = "medium"
    category = "postgres"
    penalty = 10
    description = "Large IN list - use = ANY(ARRAY[...]) for better plan caching"
    suggestion = "Replace IN (v1, v2, ...) with = ANY(ARRAY[v1, v2, ...])"
    dialects = ("postgres", "postgresql", "redshift")

    target_node_types = (exp.In,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Skip if IN has a subquery (that's fine)
        if node.find(exp.Subquery):
            return

        # Count literal values in the IN list
        expressions = node.expressions if hasattr(node, 'expressions') else []
        literal_count = sum(1 for e in expressions if isinstance(e, exp.Literal))

        if literal_count > 10:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"IN list with {literal_count} values - use ANY(ARRAY[...])",
                matched_text=f"IN ({literal_count} values)",
            )


class CurrentTimestampInWhereRule(ASTRule):
    """SQL-PG-003: current_timestamp in WHERE prevents index usage.

    Using current_timestamp in WHERE:
        WHERE created_at > current_timestamp - interval '1 day'

    The function is re-evaluated and may prevent index usage.
    Consider using a parameter or subquery.

    Detection:
    - Find current_timestamp/now() in WHERE clause
    """

    rule_id = "SQL-PG-003"
    name = "current_timestamp in WHERE"
    severity = "medium"
    category = "postgres"
    penalty = 10
    description = "current_timestamp in WHERE may prevent index usage"
    suggestion = "Consider using a parameterized value for better plan caching"
    dialects = ("postgres", "postgresql")

    target_node_types = (exp.CurrentTimestamp, exp.Anonymous)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check in WHERE clause
        if not context.in_where:
            return

        # Check for current_timestamp or now()
        is_now = False
        if isinstance(node, exp.CurrentTimestamp):
            is_now = True
        elif isinstance(node, exp.Anonymous):
            func_name = str(node.this).lower() if node.this else ""
            if func_name in ("now", "current_timestamp", "current_date"):
                is_now = True

        if is_now:
            yield RuleMatch(
                node=node,
                context=context,
                message="current_timestamp in WHERE - may affect plan caching",
                matched_text=node.sql()[:40],
            )


class ArrayAggWithoutOrderRule(ASTRule):
    """SQL-PG-005: ARRAY_AGG without ORDER BY.

    ARRAY_AGG without ORDER BY gives non-deterministic results:
        SELECT ARRAY_AGG(name) FROM users GROUP BY department

    Order depends on physical storage, which can change.

    Detection:
    - Find ARRAY_AGG calls
    - Check if ORDER BY is specified inside
    """

    rule_id = "SQL-PG-005"
    name = "ARRAY_AGG Without ORDER BY"
    severity = "medium"
    category = "postgres"
    penalty = 10
    description = "ARRAY_AGG without ORDER BY gives non-deterministic results"
    suggestion = "Add ORDER BY inside ARRAY_AGG for deterministic output"
    dialects = ("postgres", "postgresql", "redshift")

    target_node_types = (exp.ArrayAgg,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if ARRAY_AGG has ORDER BY clause
        has_order = node.find(exp.Order) is not None

        if not has_order:
            yield RuleMatch(
                node=node,
                context=context,
                message="ARRAY_AGG without ORDER BY - non-deterministic",
                matched_text=node.sql()[:60],
            )


class SerialColumnRule(ASTRule):
    """SQL-PG-006: Using SERIAL instead of IDENTITY.

    SERIAL is legacy PostgreSQL syntax:
        CREATE TABLE t (id SERIAL PRIMARY KEY)

    IDENTITY is SQL standard and preferred:
        CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY)

    Detection:
    - Find SERIAL, BIGSERIAL, SMALLSERIAL type definitions
    """

    rule_id = "SQL-PG-006"
    name = "SERIAL Column Type"
    severity = "low"
    category = "postgres"
    penalty = 5
    description = "SERIAL is legacy - use GENERATED AS IDENTITY"
    suggestion = "Use INT GENERATED ALWAYS AS IDENTITY instead of SERIAL"
    dialects = ("postgres", "postgresql")

    target_node_types = (exp.DataType,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        type_name = str(node.this).upper() if node.this else ""

        if type_name in ("SERIAL", "BIGSERIAL", "SMALLSERIAL"):
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{type_name} is legacy - use GENERATED AS IDENTITY",
                matched_text=type_name,
            )


class JsonbWithoutGinIndexHintRule(ASTRule):
    """SQL-PG-007: JSONB containment without index consideration.

    JSONB containment operators (@>, ?) need GIN index:
        WHERE data @> '{"status": "active"}'

    Without GIN index, this scans entire table.

    Detection:
    - Find @> or ? operators on JSONB columns
    - Flag as reminder to verify index exists
    """

    rule_id = "SQL-PG-007"
    name = "JSONB Containment Query"
    severity = "medium"
    category = "postgres"
    penalty = 10
    description = "JSONB containment query - ensure GIN index exists"
    suggestion = "CREATE INDEX ON table USING GIN (column) for @> and ? operators"
    dialects = ("postgres", "postgresql")

    target_node_types = (exp.JSONBContains, exp.Anonymous)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check for @> operator (JSONBContains) or ? operator
        is_jsonb_op = isinstance(node, exp.JSONBContains)

        if not is_jsonb_op and isinstance(node, exp.Anonymous):
            func_name = str(node.this).lower() if node.this else ""
            # PostgreSQL JSONB operators sometimes parsed as functions
            if func_name in ("jsonb_exists", "jsonb_exists_any", "jsonb_exists_all"):
                is_jsonb_op = True

        if is_jsonb_op and context.in_where:
            yield RuleMatch(
                node=node,
                context=context,
                message="JSONB containment in WHERE - ensure GIN index exists",
                matched_text=node.sql()[:60],
            )


class RandomOrderByRule(ASTRule):
    """SQL-PG-010: ORDER BY random() causes full table scan.

    Ordering by random():
        SELECT * FROM users ORDER BY random() LIMIT 10

    This requires scanning and sorting entire table.
    Use TABLESAMPLE for large tables.

    Detection:
    - Find ORDER BY with random() function
    """

    rule_id = "SQL-PG-010"
    name = "ORDER BY random()"
    severity = "high"
    category = "postgres"
    penalty = 15
    description = "ORDER BY random() scans and sorts entire table"
    suggestion = "Use TABLESAMPLE for random selection from large tables"
    dialects = ("postgres", "postgresql")

    target_node_types = (exp.Order,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if ORDER BY contains random() function
        for expr in node.expressions:
            anon_funcs = expr.find_all(exp.Anonymous)
            for func in anon_funcs:
                func_name = str(func.this).lower() if func.this else ""
                if func_name == "random":
                    yield RuleMatch(
                        node=node,
                        context=context,
                        message="ORDER BY random() - full table scan required",
                        matched_text="ORDER BY random()",
                    )
                    return


class MissingNullsOrderRule(ASTRule):
    """SQL-PG-004: ORDER BY without explicit NULLS FIRST/LAST consideration.

    PostgreSQL sorts NULLs last for ASC, first for DESC by default.
    This differs from other databases and can cause issues:
        ORDER BY created_at DESC  -- NULLs come first!

    Explicit NULLS ordering makes intent clear.

    Note: This rule checks the original SQL text since sqlglot normalizes
    the expression and adds default NULLS ordering.

    Detection:
    - Find ORDER BY clauses
    - Check original SQL for explicit NULLS keyword
    """

    rule_id = "SQL-PG-004"
    name = "Consider NULLS Ordering"
    severity = "low"
    category = "postgres"
    penalty = 5
    description = "ORDER BY may benefit from explicit NULLS FIRST/LAST"
    suggestion = "Consider adding NULLS FIRST or NULLS LAST for explicit NULL ordering"
    dialects = ("postgres", "postgresql")

    target_node_types = (exp.Order,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if the ORDER BY has any DESC orderings
        has_desc = False
        for ordered in node.find_all(exp.Ordered):
            if ordered.args.get('desc', False):
                has_desc = True
                break

        if not has_desc:
            return

        # Check original SQL text for explicit NULLS keyword
        sql_upper = context.sql_text.upper()
        if "NULLS FIRST" in sql_upper or "NULLS LAST" in sql_upper:
            return  # User explicitly specified NULLS ordering

        # Flag as a reminder for DESC ordering
        yield RuleMatch(
            node=node,
            context=context,
            message="ORDER BY DESC - consider explicit NULLS FIRST/LAST",
            matched_text="ORDER BY ... DESC",
        )


class NotUsingLateralRule(ASTRule):
    """SQL-PG-009: Correlated subquery instead of LATERAL.

    Correlated subqueries in FROM are inefficient:
        SELECT u.*, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id)
        FROM users u

    LATERAL joins are more efficient and clearer:
        SELECT u.*, o.order_count
        FROM users u
        LEFT JOIN LATERAL (
            SELECT COUNT(*) as order_count FROM orders WHERE user_id = u.id
        ) o ON true

    Detection:
    - Find correlated subqueries that could use LATERAL
    """

    rule_id = "SQL-PG-009"
    name = "Consider LATERAL Join"
    severity = "medium"
    category = "postgres"
    penalty = 10
    description = "Correlated subquery may be clearer as LATERAL join"
    suggestion = "Consider rewriting as LATERAL join for clarity and performance"
    dialects = ("postgres", "postgresql")

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check subqueries in SELECT list that are correlated
        if not self._is_in_select_list(node):
            return

        if self._is_correlated(node):
            yield RuleMatch(
                node=node,
                context=context,
                message="Correlated subquery - consider LATERAL join",
                matched_text=node.sql()[:60],
            )

    def _is_in_select_list(self, node: exp.Expression) -> bool:
        parent = node.parent
        while parent:
            if isinstance(parent, exp.Select):
                return True
            if isinstance(parent, (exp.Where, exp.Join, exp.From)):
                return False
            parent = parent.parent
        return False

    def _is_correlated(self, subquery: exp.Expression) -> bool:
        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return False

        inner_tables = set()
        for table in inner_select.find_all(exp.Table):
            alias = table.alias or str(table.this)
            inner_tables.add(alias.lower())

        for col in inner_select.find_all(exp.Column):
            table_ref = str(col.table).lower() if col.table else ""
            if table_ref and table_ref not in inner_tables:
                return True

        return False


class TextWithoutCollationRule(ASTRule):
    """SQL-PG-008: Text comparison without collation awareness.

    PostgreSQL text comparisons are collation-dependent:
        WHERE name LIKE 'A%'  -- May not use index with non-C collation

    For pattern matching, consider:
        CREATE INDEX ON t (name text_pattern_ops)

    Detection:
    - Find LIKE patterns on text columns
    """

    rule_id = "SQL-PG-008"
    name = "LIKE Without Pattern Ops"
    severity = "low"
    category = "postgres"
    penalty = 5
    description = "LIKE may not use index without text_pattern_ops"
    suggestion = "Create index with text_pattern_ops for LIKE patterns"
    dialects = ("postgres", "postgresql")

    target_node_types = (exp.Like,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only flag LIKE patterns that start with a non-wildcard
        # (leading % already caught by generic rule)
        pattern = node.expression
        if isinstance(pattern, exp.Literal):
            pattern_str = str(pattern.this)
            # If pattern doesn't start with %, might expect index usage
            if pattern_str and not pattern_str.startswith('%'):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="LIKE pattern - ensure text_pattern_ops index exists",
                    matched_text=node.sql()[:50],
                )
