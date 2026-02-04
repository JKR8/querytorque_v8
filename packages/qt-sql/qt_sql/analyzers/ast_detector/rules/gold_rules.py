"""Gold Standard Rules - Verified optimization patterns with proven speedups.

These rules detect patterns that have produced significant speedups in TPC-DS benchmarks.
All GLD-* rules have empirical evidence from real-world optimization results.

Evidence:
- GLD-001 (Decorrelate): 2.81x (Q1), avg 1.32x
- GLD-002 (OR to UNION): 2.67x (Q15), avg 1.18x
- GLD-003 (Early Filter): 2.71x (Q93), 1.84x (Q90), 1.24x (Q80), 1.23x (Q27)
- GLD-004 (Projection Prune): 1.21x (Q78)
- GLD-005 (Correlated WHERE): 1.80x avg, 67% win rate
"""

from typing import Iterator, Set, List
from sqlglot import exp
from ..base import ASTRule, ASTContext, RuleMatch


# ============================================================================
# MISSING DETECTORS (High Priority - Proven Winners)
# ============================================================================

# Dimension tables (small, heavily filtered)
DIMENSION_TABLES = {
    'store', 'customer', 'item', 'customer_address', 'customer_demographics',
    'household_demographics', 'promotion', 'reason', 'ship_mode', 'warehouse',
    'web_site', 'web_page', 'catalog_page', 'call_center', 'income_band',
    'time_dim', 'date_dim'
}

# Fact tables (large, rarely filtered directly)
FACT_TABLES = {
    'store_sales', 'catalog_sales', 'web_sales',
    'store_returns', 'catalog_returns', 'web_returns',
    'inventory', 'orders', 'lineitem', 'sales', 'transactions'
}


class EarlyFilterPushdownGold(ASTRule):
    """GLD-003: Early Filter Pushdown (Dimension Before Fact Join).

    **GOLD STANDARD - Proven speedups: 2.71x (Q93), 1.84x (Q90), 1.24x (Q80), 1.23x (Q27)**

    Pattern detected:
        SELECT ...
        FROM large_fact_table
        JOIN dimension_table ON fact.fk = dim.pk
        WHERE dim.selective_filter = 'value'

    Optimization:
        WITH filtered_dim AS (
            SELECT pk FROM dimension_table WHERE selective_filter = 'value'
        )
        SELECT ...
        FROM large_fact_table
        JOIN filtered_dim ON fact.fk = filtered_dim.pk

    Detection:
    1. Query has both fact table and dimension table
    2. Dimension table has selective filter in WHERE
    3. Dimension is joined to fact table
    4. Filter is NOT already in a CTE (not pre-filtered)

    This is different from QT-OPT-004 (pushdown into existing CTE).
    This detects when dimension filter should be extracted into NEW CTE.
    """

    rule_id = "GLD-003"
    name = "Early Filter Pushdown (Dimension Before Fact)"
    severity = "gold"
    category = "verified_optimization"
    penalty = 0
    description = "Dimension filter after fact join - push filter BEFORE join for 1.5-3x speedup"
    suggestion = "Extract dimension filter into CTE, join fact table to pre-filtered dimension"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check both top-level and subqueries (early_filter can be in either)
        # Only skip deeply nested (> 1 level)
        if context.subquery_depth > 1:
            return

        # Must have WHERE clause
        where = node.find(exp.Where)
        if not where:
            return

        # Build table name -> alias mapping
        table_info = self._get_table_info(node)

        # Identify fact and dimension tables
        fact_tables = [(name, alias) for name, alias in table_info if self._is_fact_table(name)]
        dim_tables = [(name, alias) for name, alias in table_info if self._is_dimension_table(name)]

        if not fact_tables or not dim_tables:
            return

        # Check if dimension tables have filters in WHERE
        dim_filters = self._find_dimension_filters(where, dim_tables)

        if not dim_filters:
            return

        # Check if dimension is already pre-filtered in CTE
        dim_names = [name for name, _ in dim_tables]
        if self._has_filtered_dim_cte(node, dim_names):
            return

        # Report opportunity for both:
        # 1. Flat queries (no CTE) - should extract dimension filter
        # 2. Queries with CTEs but dimension filter in main WHERE
        dim_info = ", ".join(f"{t}({','.join(f[:2])})" for t, f in dim_filters.items())
        fact_info = ", ".join([name for name, _ in fact_tables[:2]])

        has_cte = bool(node.find(exp.With))
        suggestion = "Extract into CTE" if not has_cte else "Move filter into CTE"

        yield RuleMatch(
            node=node,
            context=context,
            message=f"💰 GOLD: Dimension filters [{dim_info}] after join to facts [{fact_info}] - "
                   f"{suggestion} for 1.5-3x speedup",
            matched_text=f"Fact-dimension join with late filter",
        )

    def _get_table_info(self, node: exp.Expression) -> list:
        """Get table info (name, alias) from FROM and JOIN clauses."""
        table_info = []
        for table in node.find_all(exp.Table):
            if table.this:
                name = str(table.this).lower()
                # Get alias if present, otherwise use first letter of table name
                alias = str(table.alias).lower() if table.alias else name[0] if name else None
                table_info.append((name, alias))
        return table_info

    def _is_fact_table(self, table_name: str) -> bool:
        """Check if table is a fact table."""
        return table_name in FACT_TABLES or any(t in table_name for t in ['_sales', '_returns'])

    def _is_dimension_table(self, table_name: str) -> bool:
        """Check if table is a dimension table."""
        return table_name in DIMENSION_TABLES or table_name.endswith('_dim')

    def _find_dimension_filters(self, where: exp.Expression, dim_tables: list) -> dict:
        """Find filters on dimension tables in WHERE clause.

        Args:
            dim_tables: List of (table_name, alias) tuples
        """
        filters = {}

        for eq in where.find_all(exp.EQ):
            # Check both sides for column references
            for col in eq.find_all(exp.Column):
                col_table = str(col.table).lower() if col.table else ""
                col_name = str(col.this).lower() if col.this else ""

                if not col_name:
                    continue

                # Match column to dimension table by:
                # 1. Explicit table qualifier (col_table matches)
                # 2. Column name prefix (e.g., r_reason_desc → reason table)
                for dim_name, dim_alias in dim_tables:
                    matched = False

                    # Explicit table qualifier
                    if col_table and (col_table == dim_name or col_table == dim_alias):
                        matched = True

                    # Infer from column name prefix when unqualified
                    # E.g., 'r_reason_desc' likely belongs to 'reason' table (r_ prefix)
                    elif not col_table and col_name.startswith(dim_alias + '_'):
                        matched = True

                    if matched and not col_name.endswith('_sk'):  # Skip join keys
                        if dim_name not in filters:
                            filters[dim_name] = []
                        filters[dim_name].append(col_name)

        return filters

    def _has_filtered_dim_cte(self, node: exp.Expression, dim_tables: list) -> bool:
        """Check if query already has CTE for filtered dimensions."""
        with_clause = node.find(exp.With)
        if not with_clause:
            return False

        # Check if any CTE contains dimension filtering
        for cte in with_clause.find_all(exp.CTE):
            cte_sql = cte.sql().lower()
            for dim in dim_tables:
                if dim in cte_sql:
                    # Has dimension in CTE with WHERE
                    inner_select = cte.find(exp.Select)
                    if inner_select and inner_select.find(exp.Where):
                        return True

        return False


class ProjectionPruningGold(ASTRule):
    """GLD-004: Projection Pruning (Unused Columns in CTEs).

    **GOLD STANDARD - Proven speedup: 1.21x (Q78)**

    Pattern detected:
        WITH cte AS (
            SELECT col1, col2, col3, col4, col5  -- Many columns
            FROM large_table
        )
        SELECT col1, col3  -- Only uses 2 of 5 columns
        FROM cte

    Optimization:
        WITH cte AS (
            SELECT col1, col3  -- Only select what's needed
            FROM large_table
        )
        SELECT col1, col3
        FROM cte

    Detection:
    1. Query has CTE with multiple columns
    2. Main query references CTE but uses subset of columns
    3. Unused columns are not join keys or needed for filters

    Benefits:
    - Reduces memory footprint
    - Less I/O from storage
    - Better cache utilization
    """

    rule_id = "GLD-004"
    name = "Projection Pruning"
    severity = "gold"
    category = "verified_optimization"
    penalty = 0
    description = "CTE selects unused columns - prune for 1.2x speedup"
    suggestion = "Remove unused columns from CTE SELECT list"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get parent SELECT
        parent = node.parent
        if not isinstance(parent, exp.Select):
            return

        # Check each CTE
        for cte in node.find_all(exp.CTE):
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if not cte_name:
                continue

            # Get columns selected in CTE
            cte_select = cte.find(exp.Select)
            if not cte_select:
                continue

            cte_columns = self._get_selected_columns(cte_select)
            if len(cte_columns) < 3:  # Need at least 3 to be worth pruning
                continue

            # Find where CTE is referenced in main query
            used_columns = self._find_used_columns(parent, cte_name)

            # Check for unused columns
            unused = cte_columns - used_columns

            if len(unused) >= 2:  # At least 2 unused columns
                unused_list = ", ".join(sorted(unused)[:3])
                if len(unused) > 3:
                    unused_list += f" +{len(unused) - 3} more"

                yield RuleMatch(
                    node=cte,
                    context=context,
                    message=f"💰 GOLD: CTE '{cte_name}' selects {len(cte_columns)} columns, "
                           f"but {len(unused)} unused [{unused_list}] - prune for 1.2x speedup",
                    matched_text=f"WITH {cte_name} AS (SELECT {len(cte_columns)} columns)",
                )
                return  # Only report first

    def _get_selected_columns(self, select: exp.Select) -> Set[str]:
        """Get column names from SELECT clause."""
        columns = set()

        # Check if SELECT *
        if select.args.get('expressions'):
            for expr in select.args['expressions']:
                if isinstance(expr, exp.Star):
                    return set()  # Can't analyze SELECT *

                # Get column name or alias
                if isinstance(expr, exp.Column):
                    col_name = str(expr.this).lower() if expr.this else ""
                    if col_name:
                        columns.add(col_name)
                elif isinstance(expr, exp.Alias):
                    alias_name = str(expr.alias).lower() if expr.alias else ""
                    if alias_name:
                        columns.add(alias_name)

        return columns

    def _find_used_columns(self, query: exp.Expression, cte_name: str) -> Set[str]:
        """Find which CTE columns are actually used in main query."""
        used = set()

        # Find all column references
        for col in query.find_all(exp.Column):
            col_table = str(col.table).lower() if col.table else ""
            col_name = str(col.this).lower() if col.this else ""

            # Check if column references this CTE
            if col_table == cte_name and col_name:
                used.add(col_name)

        return used


class UnionCTESpecializationGold(ASTRule):
    """GLD-006: CTE with UNION ALL filtered by discriminator - specialization opportunity.

    Evidence: Q74 (1.42x speedup)

    Pattern detected:
        WITH combined AS (
            SELECT ..., 's' AS sale_type FROM store_sales ...
            UNION ALL
            SELECT ..., 'w' AS sale_type FROM web_sales ...
        )
        SELECT * FROM combined c1, combined c2
        WHERE c1.sale_type = 's' AND c2.sale_type = 'w'

    Suggested rewrite:
        WITH store_cte AS (SELECT ... FROM store_sales ...),
             web_cte AS (SELECT ... FROM web_sales ...)
        SELECT * FROM store_cte c1, web_cte c2

    Transform: Split generic UNION ALL CTE into specialized CTEs based on discriminator column.
    """

    rule_id = "GLD-006"
    name = "Union CTE Specialization"
    severity = "gold"
    category = "verified_optimization"
    penalty = 20
    description = "CTE with UNION ALL and discriminator column - split into specialized CTEs for 1.4x speedup"
    suggestion = "Split UNION ALL CTE into separate CTEs for each branch, eliminate discriminator filtering"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        """Detect CTEs with UNION ALL that are filtered by discriminator in main query."""

        # Find CTEs with UNION ALL
        for cte in node.find_all(exp.CTE):
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if not cte_name:
                continue

            # Check if CTE contains UNION ALL
            union_all = cte.find(exp.Union)
            if not union_all or not union_all.args.get('distinct') is False:
                continue  # Need UNION ALL (distinct=False)

            # Find discriminator columns (literal values that differ between branches)
            discriminators = self._find_discriminators(union_all)
            if not discriminators:
                continue

            # Check if main query filters on discriminator columns
            parent = node.parent
            if not isinstance(parent, exp.Select):
                continue

            where = parent.find(exp.Where)
            if not where:
                continue

            # Check if WHERE filters on discriminator columns of this CTE
            filtered_discriminators = self._find_discriminator_filters(where, cte_name, discriminators)

            if filtered_discriminators:
                disc_list = ", ".join(f"{col}={val}" for col, val in filtered_discriminators[:2])
                yield RuleMatch(
                    node=cte,
                    context=context,
                    message=f"💰 GOLD: CTE '{cte_name}' with UNION ALL filtered by [{disc_list}] - "
                           f"split into specialized CTEs for 1.4x speedup (Q74 pattern)",
                    matched_text=f"WITH {cte_name} AS (...UNION ALL...) WHERE {disc_list}",
                )
                return  # Only report once

    def _find_discriminators(self, union_all: exp.Union) -> List[str]:
        """Find column names that have literal discriminator values (e.g., sale_type = 's' vs 'w')."""
        discriminators = []

        # Get both sides of UNION
        left_select = union_all.this if isinstance(union_all.this, exp.Select) else union_all.this.find(exp.Select)
        right_select = union_all.expression if isinstance(union_all.expression, exp.Select) else union_all.expression.find(exp.Select)

        if not left_select or not right_select:
            return []

        # Look for columns with literal values in SELECT
        left_exprs = left_select.args.get('expressions', [])
        right_exprs = right_select.args.get('expressions', [])

        for i, (left_expr, right_expr) in enumerate(zip(left_exprs, right_exprs)):
            # Check if both sides have literals (discriminator pattern)
            left_literal = left_expr.find(exp.Literal)
            right_literal = right_expr.find(exp.Literal)

            if left_literal and right_literal:
                # Get alias name if present
                col_name = ""
                if isinstance(left_expr, exp.Alias) and left_expr.alias:
                    col_name = str(left_expr.alias).lower()
                elif isinstance(left_expr, exp.Literal):
                    col_name = f"col_{i}"

                if col_name and str(left_literal.this) != str(right_literal.this):
                    discriminators.append(col_name)

        return discriminators

    def _find_discriminator_filters(self, where: exp.Where, cte_name: str,
                                   discriminators: List[str]) -> List[tuple]:
        """Find filters on discriminator columns."""
        filters = []

        for eq in where.find_all(exp.EQ):
            for col in eq.find_all(exp.Column):
                col_table = str(col.table).lower() if col.table else ""
                col_name = str(col.this).lower() if col.this else ""

                # Check if this column references the CTE and is a discriminator
                if col_table and col_name in discriminators:
                    # Find the literal value being compared
                    literal = eq.find(exp.Literal)
                    if literal:
                        filters.append((col_name, str(literal.this)))

        return filters


class SubqueryMaterializationGold(ASTRule):
    """GLD-007: Complex subquery in FROM - materialized CTE opportunity.

    Evidence: Q73 (1.24x speedup)

    Pattern detected:
        SELECT ... FROM
            (SELECT ... FROM fact_table, date_dim, dimension_table
             WHERE ... AND date_dim.d_year IN (...)
             GROUP BY ...) subq,
            another_table
        WHERE ...

    Suggested rewrite:
        WITH materialized AS (
            SELECT ... FROM fact_table, date_dim, dimension_table
            WHERE ... AND date_dim.d_year IN (...)
            GROUP BY ...
        )
        SELECT ... FROM materialized, another_table WHERE ...

    Transform: Convert inline subquery to materialized CTE for better optimization.
    """

    rule_id = "GLD-007"
    name = "Subquery Materialization"
    severity = "gold"
    category = "verified_optimization"
    penalty = 15
    description = "Complex subquery in FROM with joins/aggregation - materialize as CTE for 1.2x speedup"
    suggestion = "Convert inline subquery to WITH clause for better query planning and optimization"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        """Detect complex subqueries in FROM that should be CTEs."""

        # Only check top-level queries (not already inside subqueries)
        if context.subquery_depth > 0:
            return

        # Skip if query already uses CTEs (already optimized)
        if node.find(exp.With):
            return

        # Find subqueries in FROM clause
        from_clause = node.args.get('from')
        if not from_clause:
            return

        for subquery in from_clause.find_all(exp.Subquery):
            inner_select = subquery.find(exp.Select)
            if not inner_select:
                continue

            # Check complexity indicators
            complexity_score = 0
            indicators = []

            # Multiple tables (joins)
            tables = list(inner_select.find_all(exp.Table))
            if len(tables) >= 3:
                complexity_score += 2
                indicators.append(f"{len(tables)} tables")

            # Has aggregation
            if inner_select.find(exp.AggFunc):
                complexity_score += 2
                indicators.append("aggregation")

            # Has GROUP BY
            if inner_select.find(exp.Group):
                complexity_score += 1
                indicators.append("GROUP BY")

            # Has date dimension with filter
            has_date_dim = any('date' in str(t.this).lower() for t in tables)
            has_date_filter = False
            where = inner_select.find(exp.Where)
            if where and has_date_dim:
                for col in where.find_all(exp.Column):
                    col_name = str(col.this).lower() if col.this else ""
                    if 'd_year' in col_name or 'd_date' in col_name or 'd_qoy' in col_name:
                        has_date_filter = True
                        break

            if has_date_dim and has_date_filter:
                complexity_score += 2
                indicators.append("date filter")

            # If complex enough, suggest materialization
            if complexity_score >= 4:
                indicator_list = ", ".join(indicators)
                yield RuleMatch(
                    node=subquery,
                    context=context,
                    message=f"💰 GOLD: Complex subquery in FROM [{indicator_list}] - "
                           f"materialize as CTE for 1.2x speedup (Q73 pattern)",
                    matched_text=f"FROM (SELECT ... {len(tables)} tables, {indicator_list})",
                )
                return  # Only report first


# ============================================================================
# EXISTING GOLD RULES (Already Implemented in opportunity_rules.py)
# These should be MOVED here and renamed to GLD-*
# ============================================================================

class CorrelatedSubqueryGold(ASTRule):
    """GLD-005: Correlated Subquery in WHERE (High Value Pattern).

    **GOLD STANDARD - Proven speedup: 1.80x avg, 67% win rate**

    This is the SAME as SQL-SUB-001 but elevated to gold status.

    Pattern detected:
        SELECT *
        FROM table1 t1
        WHERE t1.value > (
            SELECT AVG(value)
            FROM table2 t2
            WHERE t2.category = t1.category  -- Correlated!
        )

    Optimization:
        WITH category_avg AS (
            SELECT category, AVG(value) as avg_val
            FROM table2
            GROUP BY category
        )
        SELECT *
        FROM table1 t1
        JOIN category_avg ca ON t1.category = ca.category
        WHERE t1.value > ca.avg_val
    """

    rule_id = "GLD-005"
    name = "Correlated Subquery in WHERE"
    severity = "gold"
    category = "verified_optimization"
    penalty = 0
    description = "Correlated subquery runs per row - decorrelate for 1.5-2x speedup"
    suggestion = "Rewrite as JOIN with CTE or derived table"

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Must be in WHERE clause
        if not context.in_where:
            return

        # Check for correlation: subquery references outer query
        inner_select = node.find(exp.Select)
        if not inner_select:
            return

        inner_where = inner_select.find(exp.Where)
        if not inner_where:
            return

        # Look for correlated predicate (equality between columns from different scopes)
        has_correlation = False
        for eq in inner_where.find_all(exp.EQ):
            cols = list(eq.find_all(exp.Column))
            if len(cols) >= 2:
                # Two columns in equality - likely correlation
                has_correlation = True
                break

        if has_correlation:
            yield RuleMatch(
                node=node,
                context=context,
                message="💰 GOLD: Correlated subquery in WHERE - decorrelate for 1.5-2x speedup",
                matched_text="WHERE ... (SELECT ... WHERE correlated)",
            )


class DecorrelateSubqueryGold(ASTRule):
    """GLD-001: Decorrelate Subquery to CTE.

    **GOLD STANDARD - Proven speedup: 2.81x (Q1), avg 1.32x**

    This is the SAME as QT-OPT-002.
    Pattern is detected in opportunity_rules.py - should be moved here.
    """

    rule_id = "GLD-001"
    name = "Decorrelate Subquery to Pre-computed CTE"
    severity = "gold"
    category = "verified_optimization"
    penalty = 25
    description = "Correlated subquery with aggregate - pre-compute for 2-3x speedup"
    suggestion = "Extract correlated aggregate into separate CTE with GROUP BY, then JOIN"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.subquery_depth > 0:
            return

        where = node.find(exp.Where)
        if not where:
            return

        # Look for comparison with correlated subquery containing aggregate
        for compare in where.find_all((exp.GT, exp.GTE, exp.LT, exp.LTE)):
            subq = compare.find(exp.Subquery)
            if not subq:
                continue

            inner_select = subq.find(exp.Select)
            if not inner_select:
                continue

            # Check for aggregate function
            has_agg = bool(inner_select.find(exp.AggFunc))
            if not has_agg:
                continue

            # Check for correlation
            inner_where = inner_select.find(exp.Where)
            if not inner_where:
                continue

            has_correlation = False
            corr_col = None
            for eq in inner_where.find_all(exp.EQ):
                cols = list(eq.find_all(exp.Column))
                if len(cols) >= 2:
                    has_correlation = True
                    corr_col = str(cols[0].this) if cols[0].this else "column"
                    break

            if has_correlation:
                agg_func = inner_select.find(exp.AggFunc)
                agg_name = type(agg_func).__name__ if agg_func else "aggregate"

                yield RuleMatch(
                    node=compare,
                    context=context,
                    message=f"💰 GOLD: Correlated {agg_name} subquery on {corr_col} - "
                           f"pre-compute CTE for 2-3x speedup",
                    matched_text=f"WHERE ... > (SELECT {agg_name}(...) ... WHERE correlated)",
                )
                return


class OrToUnionGold(ASTRule):
    """GLD-002: OR to UNION ALL.

    **GOLD STANDARD - Proven speedup: 2.67x (Q15), avg 1.18x**

    This is the SAME as QT-OPT-001.
    Pattern is detected in opportunity_rules.py - should be moved here.
    """

    rule_id = "GLD-002"
    name = "OR to UNION ALL Decomposition"
    severity = "gold"
    category = "verified_optimization"
    penalty = 0
    description = "OR across different columns - split into UNION ALL for 2-3x speedup"
    suggestion = "Split OR conditions into separate SELECT statements joined with UNION ALL"

    target_node_types = (exp.Or,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Only check top-level OR
        if isinstance(node.parent, exp.Or):
            return

        # Collect all OR branches
        branches = self._collect_or_branches(node)
        if len(branches) < 2:
            return

        # Get columns from each branch
        branch_columns = [self._get_filter_columns(b) for b in branches]

        # Check if branches filter on DIFFERENT columns
        all_different = True
        for i, cols_i in enumerate(branch_columns):
            for j, cols_j in enumerate(branch_columns):
                if i < j and cols_i and cols_j:
                    if cols_i.intersection(cols_j):
                        all_different = False
                        break

        if all_different and len(branches) >= 2:
            col_summary = " | ".join(
                ",".join(sorted(cols)[:2]) or "expr"
                for cols in branch_columns[:3]
            )
            yield RuleMatch(
                node=node,
                context=context,
                message=f"💰 GOLD: OR across [{col_summary}] - UNION ALL gives 2-3x speedup",
                matched_text=f"{len(branches)} OR branches on different columns",
            )

    def _collect_or_branches(self, node: exp.Expression) -> list:
        branches = []
        if isinstance(node, exp.Or):
            branches.extend(self._collect_or_branches(node.this))
            branches.extend(self._collect_or_branches(node.expression))
        else:
            branches.append(node)
        return branches

    def _get_filter_columns(self, node: exp.Expression) -> Set[str]:
        cols = set()
        if node:
            for col in node.find_all(exp.Column):
                col_name = str(col.this).lower() if col.this else ""
                if col_name:
                    cols.add(col_name)
        return cols
