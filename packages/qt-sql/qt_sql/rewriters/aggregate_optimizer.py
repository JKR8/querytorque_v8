"""Aggregate Optimization Rewriters.

QT-AGG-005: HAVING filter that can be pushed to WHERE -> push down
QT-AGG-006: DISTINCT + GROUP BY redundancy -> remove redundant layer
SQL-PG-001: COUNT(*) > 0 -> EXISTS
SQL-UNION-001: UNION -> UNION ALL (with warning)
SQL-AGG-007: Nested aggregate detection
QT-AGG-002: Pre-aggregate before join (rule-based fallback)
"""

from typing import Any, Optional
from collections import defaultdict

from sqlglot import exp

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter


@register_rewriter
class HavingToWhereRewriter(BaseRewriter):
    """Pushes non-aggregate HAVING predicates to WHERE.

    HAVING is evaluated after GROUP BY, but predicates on raw columns
    can be evaluated earlier in WHERE for better performance.

    Example:
        SELECT dept, COUNT(*) FROM employees
        GROUP BY dept
        HAVING dept = 'Engineering' AND COUNT(*) > 5
        ->
        SELECT dept, COUNT(*) FROM employees
        WHERE dept = 'Engineering'
        GROUP BY dept
        HAVING COUNT(*) > 5

    Benefits:
    - Reduces rows before grouping
    - Better index usage
    - Earlier predicate application
    """

    rewriter_id = "having_to_where"
    name = "HAVING to WHERE"
    description = "Push non-aggregate HAVING predicates to WHERE"
    linked_rule_ids = ("QT-AGG-005",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for non-aggregate predicates in HAVING."""
        if not isinstance(node, exp.Select):
            return False

        having = node.find(exp.Having)
        if not having:
            return False

        # Check if any predicate in HAVING doesn't contain aggregates
        for cond in self._extract_conditions(having.this):
            if not self._has_aggregate(cond):
                return True

        return False

    def _extract_conditions(self, expr: exp.Expression) -> list[exp.Expression]:
        """Extract individual conditions from AND chain."""
        conditions = []

        def traverse(e):
            if isinstance(e, exp.And):
                traverse(e.left)
                traverse(e.right)
            else:
                conditions.append(e)

        traverse(expr)
        return conditions

    def _has_aggregate(self, expr: exp.Expression) -> bool:
        """Check if expression contains an aggregate function."""
        agg_types = (
            exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
            exp.ArrayAgg, exp.GroupConcat, exp.Stddev, exp.Variance,
        )

        if isinstance(expr, agg_types):
            return True

        for child in expr.iter_expressions():
            if self._has_aggregate(child):
                return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Push non-aggregate HAVING predicates to WHERE."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            having = rewritten.find(exp.Having)

            if not having:
                return self._create_failure(original_sql, "No HAVING clause")

            # Separate conditions
            conditions = self._extract_conditions(having.this)
            where_conditions = []
            having_conditions = []

            for cond in conditions:
                if self._has_aggregate(cond):
                    having_conditions.append(cond.copy())
                else:
                    where_conditions.append(cond.copy())

            if not where_conditions:
                return self._create_failure(original_sql, "No pushable HAVING conditions")

            # Update WHERE
            existing_where = rewritten.find(exp.Where)
            if existing_where:
                # AND with existing WHERE
                combined = existing_where.this.copy()
                for cond in where_conditions:
                    combined = exp.And(this=combined, expression=cond)
                rewritten.set("where", exp.Where(this=combined))
            else:
                # Create new WHERE
                if len(where_conditions) == 1:
                    rewritten.set("where", exp.Where(this=where_conditions[0]))
                else:
                    combined = where_conditions[0]
                    for cond in where_conditions[1:]:
                        combined = exp.And(this=combined, expression=cond)
                    rewritten.set("where", exp.Where(this=combined))

            # Update HAVING
            if having_conditions:
                if len(having_conditions) == 1:
                    rewritten.set("having", exp.Having(this=having_conditions[0]))
                else:
                    combined = having_conditions[0]
                    for cond in having_conditions[1:]:
                        combined = exp.And(this=combined, expression=cond)
                    rewritten.set("having", exp.Having(this=combined))
            else:
                rewritten.set("having", None)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Pushed {len(where_conditions)} predicate(s) from HAVING to WHERE",
            )

            result.add_safety_check(
                name="predicate_pushdown",
                result=SafetyCheckResult.PASSED,
                message="Non-aggregate predicates can safely move to WHERE",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class DistinctGroupByRedundancyRewriter(BaseRewriter):
    """Removes redundant DISTINCT when GROUP BY covers all columns.

    Example:
        SELECT DISTINCT dept, COUNT(*) as cnt
        FROM employees
        GROUP BY dept
        ->
        SELECT dept, COUNT(*) as cnt
        FROM employees
        GROUP BY dept

    GROUP BY already guarantees uniqueness of (dept, aggregate) combinations.
    """

    rewriter_id = "distinct_group_by_redundancy"
    name = "Remove Redundant DISTINCT"
    description = "Remove DISTINCT when GROUP BY makes it redundant"
    linked_rule_ids = ("QT-AGG-006",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for DISTINCT with GROUP BY."""
        if not isinstance(node, exp.Select):
            return False

        # Must have DISTINCT
        if not node.args.get("distinct"):
            return False

        # Must have GROUP BY
        group = node.find(exp.Group)
        if not group:
            return False

        # All non-aggregate SELECT expressions must be in GROUP BY
        group_cols = self._get_group_by_columns(group)
        select_cols = self._get_non_agg_select_columns(node)

        # If all select columns are in group by, DISTINCT is redundant
        return select_cols.issubset(group_cols)

    def _get_group_by_columns(self, group: exp.Group) -> set[str]:
        """Get normalized column names from GROUP BY."""
        cols = set()
        for expr in group.expressions:
            if isinstance(expr, exp.Column):
                # Normalize to just column name (ignore table qualifier)
                cols.add(str(expr.name).lower())
            elif isinstance(expr, exp.Literal):
                # GROUP BY 1, 2, 3 style - harder to match
                pass
        return cols

    def _get_non_agg_select_columns(self, node: exp.Select) -> set[str]:
        """Get non-aggregate column names from SELECT."""
        cols = set()
        agg_types = (
            exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
            exp.ArrayAgg, exp.GroupConcat,
        )

        for expr in node.expressions:
            # Unwrap alias
            if isinstance(expr, exp.Alias):
                inner = expr.this
            else:
                inner = expr

            # Skip aggregates
            if isinstance(inner, agg_types):
                continue

            # Check for aggregates in expression
            has_agg = False
            for child in inner.walk():
                if isinstance(child, agg_types):
                    has_agg = True
                    break

            if has_agg:
                continue

            # Collect column names
            if isinstance(inner, exp.Column):
                cols.add(str(inner.name).lower())
            else:
                for col in inner.find_all(exp.Column):
                    cols.add(str(col.name).lower())

        return cols

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Remove redundant DISTINCT."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()

            # Remove DISTINCT
            rewritten.set("distinct", None)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation="Removed DISTINCT (redundant with GROUP BY)",
            )

            result.add_safety_check(
                name="group_by_uniqueness",
                result=SafetyCheckResult.PASSED,
                message="GROUP BY guarantees uniqueness of result rows",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class CountToExistsRewriter(BaseRewriter):
    """Convert COUNT(*) > 0 or COUNT(*) >= 1 to EXISTS.

    EXISTS is more efficient because it can short-circuit on the first match,
    while COUNT(*) must scan all matching rows.

    Examples:
        SELECT * FROM orders o
        WHERE (SELECT COUNT(*) FROM items i WHERE i.order_id = o.id) > 0
        ->
        SELECT * FROM orders o
        WHERE EXISTS (SELECT 1 FROM items i WHERE i.order_id = o.id)

        SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
        FROM products WHERE status = 'active'
        ->
        SELECT CASE WHEN EXISTS (SELECT 1 FROM products WHERE status = 'active') THEN 1 ELSE 0 END

    Benefits:
    - Short-circuit evaluation (stops at first match)
    - No need to count all rows
    - More semantic clarity
    """

    rewriter_id = "count_to_exists"
    name = "COUNT(*) > 0 to EXISTS"
    description = "Replace COUNT(*) existence checks with EXISTS"
    linked_rule_ids = ("SQL-PG-001",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for COUNT(*) > 0 or COUNT(*) >= 1 patterns."""
        # Look for comparison expressions
        for comparison in node.find_all((exp.GT, exp.GTE, exp.EQ)):
            left = comparison.left
            right = comparison.right

            # Check for COUNT(*) on either side
            count_node = None
            literal_node = None

            if self._is_count_star(left):
                count_node = left
                literal_node = right
            elif self._is_count_star(right):
                count_node = right
                literal_node = left

            if not count_node or not literal_node:
                continue

            # Check if comparison is > 0, >= 1, or = 1 style pattern
            if isinstance(literal_node, exp.Literal):
                value = literal_node.this
                # COUNT(*) > 0, COUNT(*) >= 1, 0 < COUNT(*), 1 <= COUNT(*)
                if isinstance(comparison, exp.GT) and value == "0":
                    return True
                if isinstance(comparison, exp.GTE) and value == "1":
                    return True

        return False

    def _is_count_star(self, expr: exp.Expression) -> bool:
        """Check if expression is COUNT(*) or in a subquery."""
        # Direct COUNT(*)
        if isinstance(expr, exp.Count):
            if expr.args.get("expressions"):
                # COUNT(*) has Star as expression
                return any(isinstance(e, exp.Star) for e in expr.expressions)
            return True

        # COUNT(*) inside a subquery
        if isinstance(expr, exp.Subquery):
            subquery_select = expr.this
            if isinstance(subquery_select, exp.Select):
                for select_expr in subquery_select.expressions:
                    if self._is_count_star(select_expr):
                        return True

        return False

    def _get_count_subquery(self, node: exp.Expression) -> Optional[exp.Subquery]:
        """Extract the subquery containing COUNT(*)."""
        for comparison in node.find_all((exp.GT, exp.GTE)):
            left = comparison.left
            right = comparison.right

            # Check left side
            if isinstance(left, exp.Subquery):
                select = left.this
                if isinstance(select, exp.Select):
                    for expr in select.expressions:
                        if self._is_count_star(expr):
                            return left

            # Check right side
            if isinstance(right, exp.Subquery):
                select = right.this
                if isinstance(select, exp.Select):
                    for expr in select.expressions:
                        if self._is_count_star(expr):
                            return right

        return None

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert COUNT(*) > 0 patterns to EXISTS."""
        original_sql = node.sql()

        try:
            rewritten = node.copy()
            rewrites_made = 0

            # Find all comparisons
            for comparison in list(rewritten.find_all((exp.GT, exp.GTE))):
                left = comparison.left
                right = comparison.right

                count_node = None
                literal_node = None
                is_count_on_left = False

                if self._is_count_star(left):
                    count_node = left
                    literal_node = right
                    is_count_on_left = True
                elif self._is_count_star(right):
                    count_node = right
                    literal_node = left

                if not count_node or not literal_node:
                    continue

                # Verify it's a valid pattern
                if not isinstance(literal_node, exp.Literal):
                    continue

                value = literal_node.this
                is_valid = False

                if isinstance(comparison, exp.GT) and value == "0":
                    is_valid = True
                elif isinstance(comparison, exp.GTE) and value == "1":
                    is_valid = True

                if not is_valid:
                    continue

                # Get the subquery
                if isinstance(count_node, exp.Subquery):
                    subquery = count_node
                elif isinstance(left, exp.Subquery) and self._is_count_star(left):
                    subquery = left
                elif isinstance(right, exp.Subquery) and self._is_count_star(right):
                    subquery = right
                else:
                    continue

                # Create EXISTS expression
                select = subquery.this.copy()
                # Replace SELECT COUNT(*) with SELECT 1
                select.set("expressions", [exp.Literal.number(1)])
                # Remove any GROUP BY, HAVING, ORDER BY (not needed for EXISTS)
                select.set("group", None)
                select.set("having", None)
                select.set("order", None)

                exists_expr = exp.Exists(this=select)

                # Replace the comparison with EXISTS
                comparison.replace(exists_expr)
                rewrites_made += 1

            if rewrites_made == 0:
                return self._create_failure(original_sql, "No COUNT(*) > 0 patterns found to rewrite")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Converted {rewrites_made} COUNT(*) existence check(s) to EXISTS",
            )

            result.add_safety_check(
                name="exists_equivalence",
                result=SafetyCheckResult.PASSED,
                message="EXISTS is semantically equivalent to COUNT(*) > 0 and more efficient",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class UnionAddAllRewriter(BaseRewriter):
    """Suggest UNION ALL instead of UNION when duplicates don't matter.

    UNION performs implicit DISTINCT, which requires sorting/hashing.
    UNION ALL just concatenates results without deduplication.

    Example:
        SELECT dept FROM employees_2023
        UNION
        SELECT dept FROM employees_2024
        ->
        SELECT dept FROM employees_2023
        UNION ALL
        SELECT dept FROM employees_2024

    Benefits:
    - No deduplication overhead
    - Can stream results
    - Much faster for large datasets

    WARNING: Changes semantics! Only use if duplicates are acceptable.
    """

    rewriter_id = "union_add_all"
    name = "UNION to UNION ALL"
    description = "Replace UNION with UNION ALL to avoid deduplication"
    linked_rule_ids = ("SQL-UNION-001",)
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for UNION (not UNION ALL)."""
        for union in node.find_all(exp.Union):
            # UNION ALL has distinct=False, UNION has distinct=True (or not set)
            if union.args.get("distinct") is not False:
                return True
        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert UNION to UNION ALL."""
        original_sql = node.sql()

        try:
            rewritten = node.copy()
            rewrites_made = 0

            # Find all UNION (not UNION ALL)
            for union in rewritten.find_all(exp.Union):
                if union.args.get("distinct") is not False:
                    # Change to UNION ALL
                    union.set("distinct", False)
                    rewrites_made += 1

            if rewrites_made == 0:
                return self._create_failure(original_sql, "No UNION found to convert")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converted {rewrites_made} UNION to UNION ALL",
            )

            result.add_safety_check(
                name="semantic_change_warning",
                result=SafetyCheckResult.WARNING,
                message=(
                    "UNION ALL changes semantics! It preserves duplicates. "
                    "Only use if: (1) inputs are guaranteed disjoint, or "
                    "(2) duplicates are acceptable for your use case. "
                    "Verify correctness before deploying."
                ),
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class NestedAggregateRewriter(BaseRewriter):
    """Detect and suggest fixes for invalid nested aggregates.

    Nested aggregates like MAX(SUM(x)) are invalid in most SQL dialects.
    They need to be restructured with a subquery.

    Example:
        SELECT MAX(SUM(sales)) FROM orders GROUP BY region
        ->
        SELECT MAX(region_total) FROM (
            SELECT SUM(sales) as region_total
            FROM orders
            GROUP BY region
        ) subquery

    This rewriter detects the pattern and suggests restructuring.
    """

    rewriter_id = "nested_aggregate"
    name = "Nested Aggregate Restructuring"
    description = "Detect nested aggregates and suggest subquery restructuring"
    linked_rule_ids = ("SQL-AGG-007",)
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for nested aggregate functions."""
        agg_types = (
            exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
            exp.ArrayAgg, exp.GroupConcat, exp.Stddev, exp.Variance,
        )

        for agg in node.find_all(agg_types):
            # Check if this aggregate contains another aggregate
            for inner in agg.find_all(agg_types):
                if inner is not agg:
                    return True

        return False

    def _find_nested_aggregates(self, node: exp.Expression) -> list[tuple[exp.Expression, exp.Expression]]:
        """Find pairs of (outer_agg, inner_agg)."""
        agg_types = (
            exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
            exp.ArrayAgg, exp.GroupConcat, exp.Stddev, exp.Variance,
        )

        nested = []
        for agg in node.find_all(agg_types):
            for inner in agg.find_all(agg_types):
                if inner is not agg:
                    nested.append((agg, inner))
                    break  # Only first nested level

        return nested

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Suggest restructuring for nested aggregates."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            nested_aggs = self._find_nested_aggregates(node)
            if not nested_aggs:
                return self._create_failure(original_sql, "No nested aggregates found")

            # Build a restructured query
            rewritten = node.copy()
            group_by = rewritten.find(exp.Group)

            # Create subquery with inner aggregates
            subquery_select = exp.Select()

            # Add the inner aggregate(s) to subquery
            inner_expressions = []
            for i, (outer_agg, inner_agg) in enumerate(nested_aggs):
                alias = f"agg_{i}"
                inner_expressions.append(
                    exp.Alias(this=inner_agg.copy(), alias=alias)
                )

            # Copy GROUP BY to subquery
            if group_by:
                subquery_select.set("group", group_by.copy())

            # Copy FROM to subquery
            from_clause = rewritten.args.get("from")
            if from_clause:
                subquery_select.set("from", from_clause.copy())

            # Copy WHERE to subquery if exists
            where_clause = rewritten.args.get("where")
            if where_clause:
                subquery_select.set("where", where_clause.copy())

            subquery_select.set("expressions", inner_expressions)

            # Create outer query
            outer_select = exp.Select()
            outer_expressions = []

            # Replace nested aggregates with references to subquery columns
            for i, (outer_agg, inner_agg) in enumerate(nested_aggs):
                alias = f"agg_{i}"
                # Create new aggregate that references the subquery column
                new_outer_agg = outer_agg.copy()
                # Replace the inner aggregate with a column reference
                for inner in new_outer_agg.find_all(type(inner_agg)):
                    if inner is not new_outer_agg:
                        inner.replace(exp.Column(this=alias))
                        break

                outer_expressions.append(new_outer_agg)

            outer_select.set("expressions", outer_expressions)
            outer_select.set(
                "from",
                exp.From(
                    expressions=[
                        exp.Subquery(
                            this=subquery_select,
                            alias=exp.TableAlias(this=exp.Identifier(this="subquery"))
                        )
                    ]
                )
            )

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=outer_select.sql(),
                rewritten_node=outer_select,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Restructured {len(nested_aggs)} nested aggregate(s) into subquery",
            )

            result.add_safety_check(
                name="nested_aggregate_fix",
                result=SafetyCheckResult.WARNING,
                message=(
                    "Nested aggregates are invalid SQL. This rewrite restructures the query "
                    "with a subquery. Verify the logic matches your intent, especially with "
                    "GROUP BY clauses."
                ),
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class PreAggregateBeforeJoinRewriter(BaseRewriter):
    """Pre-aggregate fact tables before joining to dimensions.

    When aggregating after a join, pre-aggregating the fact table can
    significantly reduce the number of rows being joined.

    Example:
        SELECT d.region, SUM(s.amount)
        FROM sales s
        JOIN stores d ON d.store_id = s.store_id
        GROUP BY d.region
        ->
        WITH store_totals AS (
            SELECT store_id, SUM(amount) as amount
            FROM sales
            GROUP BY store_id
        )
        SELECT d.region, SUM(st.amount)
        FROM store_totals st
        JOIN stores d ON d.store_id = st.store_id
        GROUP BY d.region

    This is a rule-based implementation that handles common patterns.
    For complex cases, the LLM-based rewriter provides better results.
    """

    rewriter_id = "pre_aggregate_rule_based"
    name = "Pre-Aggregate Before Join (Rule-Based)"
    description = "Pre-aggregate fact table by join keys before joining"
    linked_rule_ids = ("QT-AGG-002",)
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for aggregate + join + GROUP BY pattern."""
        if not isinstance(node, exp.Select):
            return False

        # Must have aggregates
        agg_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
        if not any(node.find_all(agg_types)):
            return False

        # Must have joins
        if not list(node.find_all(exp.Join)):
            return False

        # Must have GROUP BY
        if not node.find(exp.Group):
            return False

        return True

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Apply pre-aggregation transformation."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            # Analyze the query structure
            from_clause = node.find(exp.From)
            if not from_clause:
                return self._create_failure(original_sql, "No FROM clause found")

            joins = list(node.find_all(exp.Join))
            if not joins:
                return self._create_failure(original_sql, "No JOINs found")

            group_by = node.find(exp.Group)
            if not group_by:
                return self._create_failure(original_sql, "No GROUP BY found")

            # Identify tables and their roles
            main_table = from_clause.find(exp.Table)
            if not main_table:
                return self._create_failure(original_sql, "No main table found")

            main_alias = str(main_table.alias or main_table.name).lower()
            main_name = str(main_table.name).lower()

            # Find aggregates and their source columns
            agg_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
            aggregates = list(node.find_all(agg_types))

            # Check if aggregates reference the main table
            main_table_aggs = []
            for agg in aggregates:
                for col in agg.find_all(exp.Column):
                    col_table = str(col.table).lower() if col.table else main_alias
                    if col_table == main_alias:
                        main_table_aggs.append(agg)
                        break

            if not main_table_aggs:
                return self._create_failure(
                    original_sql,
                    "No aggregates on main table - pre-aggregation not beneficial"
                )

            # Find join keys from main table
            # Check both ON clauses and WHERE clause (for implicit joins)
            join_keys = set()

            # Check explicit ON clauses
            for join in joins:
                on_clause = join.args.get("on")
                if on_clause:
                    for eq in on_clause.find_all(exp.EQ):
                        for col in eq.find_all(exp.Column):
                            col_table = str(col.table).lower() if col.table else ''
                            if col_table == main_alias or not col_table:
                                col_name = str(col.name).lower()
                                # Check if column name suggests it's from main table
                                if col_table == main_alias or col_name.startswith(main_name[:2]):
                                    join_keys.add(str(col.name))

            # Check WHERE clause for implicit join conditions
            where_clause = node.find(exp.Where)
            if where_clause and not join_keys:
                for eq in where_clause.find_all(exp.EQ):
                    left = eq.left
                    right = eq.right
                    if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                        # Check for foreign key patterns (xxx_sk, xxx_id)
                        left_name = str(left.name).lower()
                        right_name = str(right.name).lower()
                        left_table = str(left.table).lower() if left.table else ''
                        right_table = str(right.table).lower() if right.table else ''

                        # Identify which column belongs to main table
                        left_is_main = (left_table == main_alias or
                                       (not left_table and left_name.startswith(main_name[:2])))
                        right_is_main = (right_table == main_alias or
                                        (not right_table and right_name.startswith(main_name[:2])))

                        if left_is_main and not right_is_main:
                            join_keys.add(str(left.name))
                        elif right_is_main and not left_is_main:
                            join_keys.add(str(right.name))

            if not join_keys:
                return self._create_failure(original_sql, "Could not identify join keys")

            # Build the pre-aggregation CTE
            cte_name = f"{main_name}_agg"

            # Create CTE select: SELECT join_keys, AGG(cols) FROM main GROUP BY join_keys
            cte_expressions = []

            # Add join key columns
            for key in join_keys:
                cte_expressions.append(exp.Column(this=exp.to_identifier(key)))

            # Add aggregate expressions with aliases
            agg_aliases = {}
            for i, agg in enumerate(main_table_aggs):
                alias = f"agg_{i}"
                agg_aliases[id(agg)] = alias
                cte_expressions.append(exp.Alias(this=agg.copy(), alias=alias))

            cte_select = exp.Select(expressions=cte_expressions)
            cte_select = cte_select.from_(exp.Table(this=exp.to_identifier(main_name)))

            # Add GROUP BY for join keys
            group_expressions = [exp.Column(this=exp.to_identifier(key)) for key in join_keys]
            cte_select.set("group", exp.Group(expressions=group_expressions))

            # Copy WHERE conditions that apply to main table
            where_clause = node.find(exp.Where)
            if where_clause:
                main_conditions = []
                for cond in self._extract_pre_agg_conditions(where_clause.this, main_alias):
                    main_conditions.append(cond.copy())

                if main_conditions:
                    combined = main_conditions[0]
                    for c in main_conditions[1:]:
                        combined = exp.And(this=combined, expression=c)
                    cte_select.set("where", exp.Where(this=combined))

            # Create suggestion output
            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=f"-- Pre-aggregation suggestion:\nWITH {cte_name} AS (\n{cte_select.sql()}\n)\n-- Then join {cte_name} instead of {main_name}\n{original_sql}",
                confidence=RewriteConfidence.LOW,
                explanation=f"Suggested pre-aggregating {main_name} by {', '.join(join_keys)} before joining",
            )

            result.add_safety_check(
                name="pre_aggregate_suggestion",
                result=SafetyCheckResult.WARNING,
                message=(
                    "This is a suggestion for pre-aggregation. Manual review needed to ensure "
                    "correct join key selection and aggregate handling. For SUM, use SUM of sums. "
                    "For AVG, need weighted average. For COUNT, sum the counts."
                ),
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _extract_pre_agg_conditions(self, expr: exp.Expression, main_alias: str) -> list[exp.Expression]:
        """Extract conditions that only reference main table."""
        conditions = []

        def traverse(e):
            if isinstance(e, exp.And):
                traverse(e.left)
                traverse(e.right)
            else:
                # Check if condition only references main table
                cols = list(e.find_all(exp.Column))
                if all(
                    (str(c.table).lower() if c.table else main_alias) == main_alias
                    for c in cols
                ):
                    conditions.append(e)

        traverse(expr)
        return conditions
