"""Optimization opportunity detection rules.

These rules detect patterns that can be optimized via rewriting.
Based on the POC rewriter benchmark rulebook.
"""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class SingleUseCTEInlineRule(ASTRule):
    """QT-CTE-002: Single-use CTE that could be inlined.

    CTEs used only once may prevent predicate pushdown:
        WITH filtered AS (SELECT * FROM orders WHERE region = 'US')
        SELECT * FROM filtered WHERE amount > 100

    Inlining allows optimizer to combine predicates:
        SELECT * FROM orders WHERE region = 'US' AND amount > 100
    """

    rule_id = "QT-CTE-002"
    name = "Single-Use CTE"
    severity = "low"
    category = "optimization"
    penalty = 5
    description = "Single-use CTE may block predicate pushdown - consider inlining"
    suggestion = "Inline single-use CTE to enable predicate pushdown"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        ctes = list(node.find_all(exp.CTE))
        parent = node.parent
        if not parent:
            return

        for cte in ctes:
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if not cte_name:
                continue

            # Count references
            ref_count = sum(1 for t in parent.find_all(exp.Table)
                          if str(t.this).lower() == cte_name)

            if ref_count == 1:
                yield RuleMatch(
                    node=cte,
                    context=context,
                    message=f"CTE '{cte_name}' used once - inline to enable pushdown",
                    matched_text=f"WITH {cte_name} AS (...) -- 1 reference",
                )


class UnusedCTERule(ASTRule):
    """QT-CTE-001: CTE defined but never referenced.

    Unreferenced CTEs waste parsing/planning time:
        WITH unused AS (SELECT * FROM big_table)
        SELECT * FROM other_table  -- 'unused' never referenced
    """

    rule_id = "QT-CTE-001"
    name = "Unused CTE"
    severity = "medium"
    category = "optimization"
    penalty = 10
    description = "CTE defined but never referenced - remove it"
    suggestion = "Remove unreferenced CTE"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        ctes = list(node.find_all(exp.CTE))
        parent = node.parent
        if not parent:
            return

        for cte in ctes:
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if not cte_name:
                continue

            # Count references (excluding the definition itself)
            ref_count = sum(1 for t in parent.find_all(exp.Table)
                          if str(t.this).lower() == cte_name)

            if ref_count == 0:
                yield RuleMatch(
                    node=cte,
                    context=context,
                    message=f"CTE '{cte_name}' is never referenced - remove it",
                    matched_text=f"WITH {cte_name} AS (...) -- 0 references",
                )


class ExistsToSemiJoinRule(ASTRule):
    """QT-SUBQ-002: EXISTS subquery that could be SEMI JOIN.

    EXISTS with correlated subquery can often be a SEMI JOIN:
        SELECT * FROM orders o
        WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)

    Could be:
        SELECT o.* FROM orders o SEMI JOIN customers c ON c.id = o.customer_id
    """

    rule_id = "QT-SUBQ-002"
    name = "EXISTS to SEMI JOIN"
    severity = "low"
    category = "optimization"
    penalty = 5
    description = "EXISTS subquery could be SEMI JOIN for clarity"
    suggestion = "Consider SEMI JOIN syntax (where supported)"

    target_node_types = (exp.Exists,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if EXISTS has a correlated subquery
        subquery = node.find(exp.Subquery)
        if not subquery:
            return

        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return

        inner_where = inner_select.find(exp.Where)
        if inner_where:
            # Has correlation - could be semi-join
            yield RuleMatch(
                node=node,
                context=context,
                message="EXISTS with correlated subquery - consider SEMI JOIN",
                matched_text="EXISTS (SELECT ... WHERE correlated)",
            )


class LeftJoinAntiPatternRule(ASTRule):
    """QT-JOIN-002: LEFT JOIN + IS NULL anti-join pattern.

    This anti-join idiom can be clearer as NOT EXISTS:
        SELECT * FROM orders o
        LEFT JOIN returns r ON o.id = r.order_id
        WHERE r.id IS NULL

    Clearer as:
        SELECT * FROM orders o
        WHERE NOT EXISTS (SELECT 1 FROM returns r WHERE r.order_id = o.id)
    """

    rule_id = "QT-JOIN-002"
    name = "LEFT JOIN Anti-Pattern"
    severity = "medium"
    category = "optimization"
    penalty = 10
    description = "LEFT JOIN + IS NULL is anti-join pattern - use NOT EXISTS"
    suggestion = "Convert to NOT EXISTS for clearer intent"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Find LEFT JOINs
        left_joins = []
        for join in node.find_all(exp.Join):
            side = str(join.args.get("side", "")).upper()
            if side == "LEFT":
                left_joins.append(join)

        if not left_joins:
            return

        # Check WHERE for IS NULL on right table
        where = node.find(exp.Where)
        if not where:
            return

        for is_node in where.find_all(exp.Is):
            if not isinstance(is_node.expression, exp.Null):
                continue

            col = is_node.this
            if not isinstance(col, exp.Column) or not col.table:
                continue

            col_table = str(col.table).lower()
            for join in left_joins:
                join_table = join.find(exp.Table)
                if join_table:
                    join_alias = str(join_table.alias or join_table.name).lower()
                    if col_table == join_alias:
                        yield RuleMatch(
                            node=join,
                            context=context,
                            message=f"LEFT JOIN + IS NULL on '{join_alias}' - use NOT EXISTS",
                            matched_text=f"LEFT JOIN {join_alias} ... WHERE {col_table}.col IS NULL",
                        )
                        return


class LeftJoinFilterRule(ASTRule):
    """QT-JOIN-001: LEFT JOIN with filter on right table.

    Filtering on right table of LEFT JOIN often means INNER JOIN intended:
        SELECT * FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.id
        WHERE c.status = 'active'  -- Converts to INNER JOIN anyway

    Either use INNER JOIN or move filter to ON clause.
    """

    rule_id = "QT-JOIN-001"
    name = "LEFT JOIN with Right Filter"
    severity = "medium"
    category = "optimization"
    penalty = 10
    description = "LEFT JOIN with filter on right table - should be INNER JOIN?"
    suggestion = "Use INNER JOIN or move filter to ON clause"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Find LEFT JOINs and their table aliases
        left_join_tables = set()
        for join in node.find_all(exp.Join):
            side = str(join.args.get("side", "")).upper()
            if side == "LEFT":
                join_table = join.find(exp.Table)
                if join_table:
                    alias = str(join_table.alias or join_table.name).lower()
                    left_join_tables.add(alias)

        if not left_join_tables:
            return

        # Check WHERE for non-NULL filters on right tables
        where = node.find(exp.Where)
        if not where:
            return

        for col in where.find_all(exp.Column):
            if col.table and str(col.table).lower() in left_join_tables:
                # Skip IS NULL checks (those are anti-join pattern)
                parent = col.parent
                if isinstance(parent, exp.Is) and isinstance(parent.expression, exp.Null):
                    continue

                yield RuleMatch(
                    node=where,
                    context=context,
                    message=f"Filter on LEFT JOIN table '{col.table}' - use INNER JOIN?",
                    matched_text=f"LEFT JOIN ... WHERE {col.table}.{col.name} = ...",
                )
                return


class NonSargablePredicateRule(ASTRule):
    """QT-FILT-001: Non-sargable predicate prevents index usage.

    Functions on columns prevent index seeks:
        WHERE DATE(created_at) = '2024-01-01'
        WHERE YEAR(order_date) = 2024
        WHERE UPPER(name) = 'JOHN'

    Convert to range predicates:
        WHERE created_at >= '2024-01-01' AND created_at < '2024-01-02'
    """

    rule_id = "QT-FILT-001"
    name = "Non-Sargable Predicate"
    severity = "high"
    category = "optimization"
    penalty = 15
    description = "Function on column prevents index usage"
    suggestion = "Rewrite as range predicate to enable index seek"

    target_node_types = (exp.EQ,)

    # Functions that wrap columns and prevent sargability
    NON_SARGABLE_FUNCS = (
        exp.Date, exp.DateTrunc, exp.Year, exp.Month, exp.Day,
        exp.Upper, exp.Lower, exp.Trim, exp.Substring,
        exp.Cast, exp.TryCast,
    )

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Check left side for function wrapping column
        left = node.left
        if isinstance(left, self.NON_SARGABLE_FUNCS):
            inner_col = left.find(exp.Column)
            if inner_col:
                func_name = type(left).__name__
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"{func_name}(column) prevents index usage",
                    matched_text=f"{func_name}({inner_col.name}) = ...",
                )


class NotInNullTrapRule(ASTRule):
    """QT-NULL-001: NOT IN with subquery - NULL trap.

    NOT IN returns no rows if subquery contains NULL:
        SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM inactive)

    If inactive.id has any NULL, query returns nothing!

    Safer as NOT EXISTS:
        SELECT * FROM orders o
        WHERE NOT EXISTS (SELECT 1 FROM inactive i WHERE i.id = o.customer_id)
    """

    rule_id = "QT-NULL-001"
    name = "NOT IN NULL Trap"
    severity = "high"
    category = "correctness"
    penalty = 15
    description = "NOT IN with subquery - NULL causes unexpected empty results"
    suggestion = "Use NOT EXISTS to avoid NULL trap"

    target_node_types = (exp.In,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if it's NOT IN
        parent = node.parent
        if not isinstance(parent, exp.Not):
            return

        # Check if it has a subquery
        if not node.find(exp.Subquery):
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="NOT IN with subquery - use NOT EXISTS to avoid NULL trap",
            matched_text="NOT IN (SELECT ...)",
        )


class HavingNonAggregateRule(ASTRule):
    """QT-AGG-005: HAVING with non-aggregate predicate.

    Non-aggregate HAVING filters should be in WHERE for early filtering:
        SELECT dept, COUNT(*) FROM employees
        GROUP BY dept
        HAVING dept != 'IT'  -- Should be WHERE

    WHERE filters before grouping (fewer rows to aggregate).
    """

    rule_id = "QT-AGG-005"
    name = "HAVING Non-Aggregate"
    severity = "medium"
    category = "optimization"
    penalty = 10
    description = "HAVING with non-aggregate filter - move to WHERE"
    suggestion = "Move non-aggregate predicates to WHERE for early filtering"

    target_node_types = (exp.Having,)

    AGGREGATE_FUNCS = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max, exp.ArrayAgg)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if HAVING condition uses aggregates
        has_aggregate = bool(node.find(self.AGGREGATE_FUNCS))

        if not has_aggregate:
            yield RuleMatch(
                node=node,
                context=context,
                message="HAVING without aggregate - move to WHERE",
                matched_text=f"HAVING {node.this.sql()[:50]}",
            )


class DistinctGroupByRedundancyRule(ASTRule):
    """QT-AGG-006: Redundant DISTINCT with GROUP BY.

    DISTINCT is redundant when GROUP BY covers all selected columns:
        SELECT DISTINCT dept, COUNT(*) FROM employees GROUP BY dept

    GROUP BY already ensures uniqueness.
    """

    rule_id = "QT-AGG-006"
    name = "Redundant DISTINCT"
    severity = "low"
    category = "optimization"
    penalty = 5
    description = "DISTINCT redundant with GROUP BY - remove DISTINCT"
    suggestion = "Remove DISTINCT when GROUP BY ensures uniqueness"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Check for DISTINCT
        if not node.args.get("distinct"):
            return

        # Check for GROUP BY
        group = node.find(exp.Group)
        if not group:
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="DISTINCT with GROUP BY - likely redundant",
            matched_text="SELECT DISTINCT ... GROUP BY ...",
        )


class OffsetPaginationRule(ASTRule):
    """QT-INT-001: OFFSET pagination anti-pattern.

    OFFSET pagination scans and discards rows:
        SELECT * FROM orders ORDER BY id LIMIT 10 OFFSET 10000
        -- Scans 10010 rows, returns 10

    Keyset pagination is more efficient:
        SELECT * FROM orders WHERE id > :last_id ORDER BY id LIMIT 10
    """

    rule_id = "QT-INT-001"
    name = "OFFSET Pagination"
    severity = "medium"
    category = "optimization"
    penalty = 10
    description = "OFFSET pagination scales poorly - use keyset pagination"
    suggestion = "Use WHERE id > :last_id instead of OFFSET"

    target_node_types = (exp.Offset,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if offset value is significant (not just OFFSET 0)
        offset_val = node.this
        if isinstance(offset_val, exp.Literal):
            try:
                val = int(offset_val.this)
                if val == 0:
                    return
            except (ValueError, TypeError):
                pass

        yield RuleMatch(
            node=node,
            context=context,
            message="OFFSET pagination scans discarded rows - use keyset",
            matched_text=f"OFFSET {offset_val}",
        )


class TopNPerGroupRule(ASTRule):
    """QT-TOPK-003: Top-N per group via correlated subquery.

    Correlated subquery for top-N is slow:
        SELECT * FROM orders o
        WHERE o.amount = (SELECT MAX(amount) FROM orders WHERE customer_id = o.customer_id)

    Window function is more efficient:
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) rn
            FROM orders
        ) t WHERE rn = 1
    """

    rule_id = "QT-TOPK-003"
    name = "Top-N Per Group Subquery"
    severity = "high"
    category = "optimization"
    penalty = 15
    description = "Correlated subquery for top-N - use window function"
    suggestion = "Use ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) instead"

    target_node_types = (exp.EQ,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Check for pattern: col = (SELECT MAX/MIN(...) ... WHERE correlated)
        right = node.expression
        if not isinstance(right, exp.Subquery):
            return

        inner_select = right.find(exp.Select)
        if not inner_select:
            return

        # Check for MAX/MIN aggregate
        has_minmax = bool(inner_select.find((exp.Max, exp.Min)))
        if not has_minmax:
            return

        # Check for correlation in WHERE
        inner_where = inner_select.find(exp.Where)
        if inner_where:
            yield RuleMatch(
                node=node,
                context=context,
                message="Correlated MAX/MIN subquery - use window function",
                matched_text="col = (SELECT MAX(...) WHERE correlated)",
            )


class UnnecessaryDistinctRule(ASTRule):
    """QT-DIST-001: Potentially unnecessary DISTINCT.

    DISTINCT may be unnecessary when:
    - Selecting from primary key
    - After GROUP BY
    - On already unique result

    DISTINCT forces sort/hash which is expensive.
    """

    rule_id = "QT-DIST-001"
    name = "Potentially Unnecessary DISTINCT"
    severity = "low"
    category = "optimization"
    penalty = 5
    description = "DISTINCT may be unnecessary - verify uniqueness"
    suggestion = "Remove DISTINCT if query already returns unique rows"

    target_node_types = (exp.Distinct,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield RuleMatch(
            node=node,
            context=context,
            message="DISTINCT adds overhead - verify it's needed",
            matched_text="SELECT DISTINCT ...",
        )


class OrToUnionRule(ASTRule):
    """QT-BOOL-001: OR across different columns.

    OR on different columns prevents index usage:
        SELECT * FROM users WHERE email = 'x' OR phone = 'y'

    May perform better as UNION:
        SELECT * FROM users WHERE email = 'x'
        UNION
        SELECT * FROM users WHERE phone = 'y'
    """

    rule_id = "QT-BOOL-001"
    name = "OR Across Columns"
    severity = "low"
    category = "optimization"
    penalty = 5
    description = "OR on different columns may prevent index usage"
    suggestion = "Consider UNION for better index utilization"

    target_node_types = (exp.Or,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Check if OR is between conditions on different columns
        left_cols = {str(c.name).lower() for c in node.left.find_all(exp.Column)}
        right_cols = {str(c.name).lower() for c in node.right.find_all(exp.Column)}

        if left_cols and right_cols and not left_cols.intersection(right_cols):
            yield RuleMatch(
                node=node,
                context=context,
                message="OR across different columns - consider UNION",
                matched_text=f"... OR ... (different columns)",
            )


class WindowPushdownRule(ASTRule):
    """QT-PLAN-001: Window function blocking predicate pushdown.

    Filters after window functions process all rows first:
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (...) rn FROM large_table
        ) t WHERE rn = 1 AND status = 'active'

    Moving the filter inside can help:
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (...) rn
            FROM large_table WHERE status = 'active'
        ) t WHERE rn = 1
    """

    rule_id = "QT-PLAN-001"
    name = "Window Blocks Pushdown"
    severity = "medium"
    category = "optimization"
    penalty = 10
    description = "Filter after window function - consider pushing filter inside"
    suggestion = "Move non-window filters inside the subquery"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Check if FROM is a subquery with window function
        from_clause = node.find(exp.From)
        if not from_clause:
            return

        subquery = from_clause.find(exp.Subquery)
        if not subquery:
            return

        inner_select = subquery.find(exp.Select)
        if not inner_select or not inner_select.find(exp.Window):
            return

        # Check if outer has WHERE that could be pushed
        outer_where = node.args.get("where")
        if not outer_where:
            return

        # Check if WHERE has non-window column references
        for col in outer_where.find_all(exp.Column):
            col_name = str(col.name).lower()
            # Skip if it's the window alias (like 'rn')
            for expr in inner_select.expressions:
                if isinstance(expr, exp.Alias) and expr.find(exp.Window):
                    if str(expr.alias).lower() == col_name:
                        continue

            yield RuleMatch(
                node=outer_where,
                context=context,
                message="Filter after window - some predicates could be pushed inside",
                matched_text="SELECT FROM (window subquery) WHERE ...",
            )
            return


class PreAggregateRule(ASTRule):
    """QT-AGG-002: Aggregation after large join.

    Aggregating after joining large tables is expensive:
        SELECT c.name, SUM(o.amount)
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.name

    Pre-aggregating can reduce join size:
        SELECT c.name, agg.total
        FROM customers c
        JOIN (SELECT customer_id, SUM(amount) total FROM orders GROUP BY customer_id) agg
        ON c.id = agg.customer_id
    """

    rule_id = "QT-AGG-002"
    name = "Aggregate After Join"
    severity = "info"  # Demoted: too generic, low hit rate (4%) with optimizer
    category = "optimization"
    penalty = 0  # Info only - too broad to be actionable
    description = "Aggregation after join - consider pre-aggregating"
    suggestion = "Pre-aggregate in subquery before joining"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Check for JOIN + GROUP BY + aggregate
        has_join = bool(node.find(exp.Join))
        has_group = bool(node.find(exp.Group))
        has_agg = bool(node.find((exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)))

        if has_join and has_group and has_agg:
            yield RuleMatch(
                node=node,
                context=context,
                message="Aggregate after join - consider pre-aggregating",
                matched_text="SELECT ... FROM ... JOIN ... GROUP BY ...",
            )


class GroupByFunctionalDependencyRule(ASTRule):
    """QT-AGG-003: Redundant GROUP BY columns.

    If grouping by primary key, other columns from same table are redundant:
        SELECT id, name, email, COUNT(*) FROM users GROUP BY id, name, email

    If id is PK, only need:
        SELECT id, name, email, COUNT(*) FROM users GROUP BY id
    """

    rule_id = "QT-AGG-003"
    name = "Redundant GROUP BY"
    severity = "low"
    category = "optimization"
    penalty = 5
    description = "GROUP BY may have redundant columns via functional dependency"
    suggestion = "If grouping by PK, other columns from same table are redundant"

    target_node_types = (exp.Group,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Count GROUP BY columns from same table
        table_cols: dict[str, list[str]] = {}
        for col in node.find_all(exp.Column):
            table = str(col.table).lower() if col.table else "__default__"
            if table not in table_cols:
                table_cols[table] = []
            table_cols[table].append(str(col.name).lower())

        # Flag if any table has multiple columns in GROUP BY
        for table, cols in table_cols.items():
            if len(cols) >= 3 and table != "__default__":
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"GROUP BY has {len(cols)} columns from '{table}' - check for FD",
                    matched_text=f"GROUP BY {table}.* ({len(cols)} cols)",
                )
                return
