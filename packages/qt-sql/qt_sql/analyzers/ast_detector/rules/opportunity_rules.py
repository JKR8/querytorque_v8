"""Optimization opportunity detection rules.

These rules detect patterns that are likely to benefit from specific rewrites,
based on empirical evidence from TPC-DS optimization runs.

Pattern Evidence (TPC-DS SF100, DuckDB):
- UNION decomposition: 2-3x speedup (q15: 2.98x, q23: 2.33x, q45: 2.26x)
- Early date filtering: 1.5-2.5x speedup (q39: 2.44x, q92: 2.06x)
- Materialized CTE: 1.2-2x speedup (q95: 2.25x)
"""

from typing import Iterator, Set

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


# Date dimension columns that indicate filtering opportunity
DATE_FILTER_COLUMNS = {'d_year', 'd_qoy', 'd_moy', 'd_date', 'd_month', 'd_quarter'}
DATE_SK_COLUMNS = {'d_date_sk', 'date_sk', 'sold_date_sk', 'ship_date_sk'}

# Large fact tables that benefit from early filtering
FACT_TABLES = {
    'store_sales', 'catalog_sales', 'web_sales',
    'store_returns', 'catalog_returns', 'web_returns',
    'inventory', 'orders', 'lineitem', 'sales', 'transactions'
}


class OrToUnionOpportunity(ASTRule):
    """QT-OPT-001: OR conditions across different columns - UNION ALL opportunity.

    Empirical speedup: 2-3x (q15: 2.98x, q23: 2.33x, q45: 2.26x, q24: 2.16x)

    Pattern detected:
        WHERE (col_a = X OR col_b = Y OR col_c > Z)

    Suggested rewrite:
        SELECT ... WHERE col_a = X
        UNION ALL
        SELECT ... WHERE col_b = Y
        UNION ALL
        SELECT ... WHERE col_c > Z

    Why it helps:
    - Each branch can use its own index
    - Parallel execution of branches in modern DBs
    - Reduces rows processed per branch
    """

    rule_id = "QT-OPT-001"
    name = "OR to UNION ALL Opportunity"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0  # Not a penalty - it's an opportunity
    description = "OR across different columns can be rewritten as UNION ALL for 2-3x speedup"
    suggestion = "Split OR conditions into separate SELECT statements joined with UNION ALL"

    target_node_types = (exp.Or,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Only check top-level OR (not nested)
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
                message=f"OR across [{col_summary}] - UNION ALL can give 2-3x speedup",
                matched_text=f"{len(branches)} OR branches on different columns",
            )

    def _collect_or_branches(self, node: exp.Expression) -> list:
        """Collect all branches of an OR chain."""
        branches = []
        if isinstance(node, exp.Or):
            branches.extend(self._collect_or_branches(node.this))
            branches.extend(self._collect_or_branches(node.expression))
        else:
            branches.append(node)
        return branches

    def _get_filter_columns(self, node: exp.Expression) -> Set[str]:
        """Extract column names used in filter expression."""
        cols = set()
        if node:
            for col in node.find_all(exp.Column):
                col_name = str(col.this).lower() if col.this else ""
                if col_name:
                    cols.add(col_name)
        return cols


class LateDateFilterOpportunity(ASTRule):
    """QT-OPT-002: Date filtering happens late - early CTE opportunity.

    Empirical speedup: 1.5-2.5x (q39: 2.44x, q92: 2.06x, q47: 1.24x)

    Pattern detected:
        FROM fact_table, date_dim, other_tables
        WHERE fact.date_sk = date_dim.d_date_sk
        AND date_dim.d_year = 2001
        AND other_conditions...

    Suggested rewrite:
        WITH filtered_dates AS (
            SELECT d_date_sk FROM date_dim WHERE d_year = 2001
        )
        SELECT ...
        FROM fact_table
        INNER JOIN filtered_dates ON fact.date_sk = d_date_sk
        ...

    Why it helps:
    - Filters date_dim first (small table, ~73k rows for 20 years)
    - Join to fact table early, reducing rows before other joins
    - Enables better join ordering by optimizer
    """

    rule_id = "QT-OPT-002"
    name = "Late Date Filter - Early CTE Opportunity"
    severity = "high"  # Promoted: 43% optimizer hit rate, common win on TPC-DS
    category = "optimization_opportunity"
    penalty = 15  # Directly actionable - filter pushdown optimization
    description = "Date filtering late in query - early date CTE can give 1.5-2.5x speedup"
    suggestion = "Extract date filtering into a CTE and join early to fact tables"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check top-level SELECT (not subqueries for now)
        if context.subquery_depth > 0:
            return

        # Check if query already uses CTEs for dates
        if self._has_date_cte(node):
            return

        # Find date_dim in FROM clause
        tables = self._get_tables(node)
        has_date_dim = any('date' in t.lower() for t in tables)
        has_fact_table = any(t.lower() in FACT_TABLES for t in tables)

        if not (has_date_dim and has_fact_table):
            return

        # Check for date filter columns in WHERE
        where = node.find(exp.Where)
        if not where:
            return

        date_filters = self._find_date_filters(where)
        if date_filters:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Date filter ({', '.join(date_filters)}) with fact table - early CTE can give 1.5-2.5x speedup",
                matched_text=f"date_dim filtered by {', '.join(date_filters)}",
            )

    def _has_date_cte(self, node: exp.Expression) -> bool:
        """Check if query already has a CTE for date filtering."""
        with_clause = node.find(exp.With)
        if with_clause:
            cte_sql = with_clause.sql().lower()
            if 'date' in cte_sql and any(c in cte_sql for c in DATE_FILTER_COLUMNS):
                return True
        return False

    def _get_tables(self, node: exp.Expression) -> list:
        """Get table names from FROM clause."""
        tables = []
        for table in node.find_all(exp.Table):
            if table.this:
                tables.append(str(table.this))
        return tables

    def _find_date_filters(self, where: exp.Expression) -> list:
        """Find date dimension filter columns in WHERE."""
        filters = []
        for col in where.find_all(exp.Column):
            col_name = str(col.this).lower() if col.this else ""
            if col_name in DATE_FILTER_COLUMNS:
                filters.append(col_name)
        return list(set(filters))


class RepeatedSubqueryOpportunity(ASTRule):
    """QT-OPT-003: Repeated subquery - materialized CTE opportunity.

    Empirical speedup: 1.2-2x (q95: 2.25x)

    Pattern detected:
        SELECT ...
        FROM (SELECT ... FROM big_table WHERE ...) sq1
        JOIN (SELECT ... FROM big_table WHERE ...) sq2  -- Same pattern!

    Suggested rewrite:
        WITH materialized_data AS MATERIALIZED (
            SELECT ... FROM big_table WHERE ...
        )
        SELECT ...
        FROM materialized_data sq1
        JOIN materialized_data sq2

    Why it helps:
    - Computes expensive subquery once
    - MATERIALIZED hint forces early computation
    - Avoids redundant scans of large tables
    """

    rule_id = "QT-OPT-003"
    name = "Repeated Subquery - Materialized CTE Opportunity"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "Repeated subquery pattern - materialized CTE can give 1.2-2x speedup"
    suggestion = "Extract repeated subquery into a materialized CTE"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.subquery_depth > 0:
            return

        # Find all subqueries
        subqueries = list(node.find_all(exp.Subquery))
        if len(subqueries) < 2:
            return

        # Compare subquery structures (simplified - check table names)
        subquery_tables = []
        for sq in subqueries:
            tables = set()
            for table in sq.find_all(exp.Table):
                if table.this:
                    tables.add(str(table.this).lower())
            if tables:
                subquery_tables.append((sq, frozenset(tables)))

        # Find repeated table patterns
        seen = {}
        for sq, tables in subquery_tables:
            if tables in seen:
                # Found repeated pattern
                table_list = ", ".join(sorted(tables)[:2])
                yield RuleMatch(
                    node=sq,
                    context=context,
                    message=f"Repeated subquery on [{table_list}] - materialized CTE can give 1.2-2x speedup",
                    matched_text=f"Subquery on {table_list} appears multiple times",
                )
                return  # Only report once
            seen[tables] = sq


class CorrelatedSubqueryOpportunity(ASTRule):
    """QT-OPT-004: Correlated subquery - window function opportunity.

    Empirical speedup: 1.2-1.5x

    Pattern detected:
        SELECT *, (SELECT MAX(x) FROM t2 WHERE t2.id = t1.id) as max_x
        FROM t1

    Suggested rewrite:
        SELECT *, MAX(x) OVER (PARTITION BY id) as max_x
        FROM t1 JOIN t2 ON t1.id = t2.id

    Why it helps:
    - Single pass through data instead of N subquery executions
    - Better parallelization
    """

    rule_id = "QT-OPT-004"
    name = "Correlated Subquery - Window Function Opportunity"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "Correlated subquery may be convertible to window function"
    suggestion = "Consider rewriting as window function with OVER (PARTITION BY ...)"

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if subquery is in SELECT clause (scalar subquery)
        if not context.in_select:
            return

        # Check for correlation (reference to outer table)
        inner_select = node.find(exp.Select)
        if not inner_select:
            return

        inner_where = inner_select.find(exp.Where)
        if not inner_where:
            return

        # Look for aggregation with correlation
        has_agg = bool(inner_select.find(exp.AggFunc))

        # Check for equality join to outer (simplified check)
        has_eq = bool(inner_where.find(exp.EQ))

        if has_agg and has_eq:
            yield RuleMatch(
                node=node,
                context=context,
                message="Correlated scalar subquery with aggregation - window function may be faster",
                matched_text="Correlated subquery in SELECT",
            )


class CountToExistsOpportunity(ASTRule):
    """QT-OPT-006: COUNT(*) > 0 in subquery - EXISTS opportunity.

    Empirical speedup: 1.5-1.7x (q41: 1.69x)

    Pattern detected:
        WHERE (SELECT COUNT(*) FROM t WHERE ...) > 0

    Suggested rewrite:
        WHERE EXISTS (SELECT 1 FROM t WHERE ...)

    Why it helps:
    - EXISTS stops at first match
    - COUNT(*) scans all matching rows just to check > 0
    - Significant savings for large result sets
    """

    rule_id = "QT-OPT-006"
    name = "COUNT(*) to EXISTS Opportunity"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "COUNT(*) > 0 in subquery - EXISTS stops at first match for 1.5-1.7x speedup"
    suggestion = "Replace (SELECT COUNT(*) ...) > 0 with EXISTS (SELECT 1 ...)"

    target_node_types = (exp.GT, exp.GTE)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        # Check for COUNT(*) > 0 or COUNT(*) >= 1 pattern
        left = node.this
        right = node.expression

        # Left side should be a subquery with COUNT
        if isinstance(left, exp.Subquery) or isinstance(left, exp.Select):
            subq = left.this if isinstance(left, exp.Subquery) else left
            if self._has_count_star(subq):
                # Right side should be 0 (for >) or 1 (for >=)
                if isinstance(right, exp.Literal):
                    val = right.this
                    if (isinstance(node, exp.GT) and str(val) == '0') or \
                       (isinstance(node, exp.GTE) and str(val) == '1'):
                        yield RuleMatch(
                            node=node,
                            context=context,
                            message="COUNT(*) > 0 pattern - EXISTS stops at first match",
                            matched_text="(SELECT COUNT(*) ...) > 0",
                        )

    def _has_count_star(self, node: exp.Expression) -> bool:
        """Check if node contains COUNT(*)."""
        for count in node.find_all(exp.Count):
            # COUNT(*) or COUNT(1)
            if count.this is None or isinstance(count.this, exp.Star):
                return True
            if isinstance(count.this, exp.Literal) and str(count.this.this) == '1':
                return True
        return False


class ImplicitCrossJoinOpportunity(ASTRule):
    """QT-OPT-005: Implicit join syntax - explicit JOIN opportunity.

    Pattern detected:
        FROM a, b, c WHERE a.id = b.id AND b.id = c.id

    Suggested rewrite:
        FROM a
        INNER JOIN b ON a.id = b.id
        INNER JOIN c ON b.id = c.id

    Why it helps:
    - Clearer join order for optimizer
    - Explicit join conditions prevent accidental cross joins
    - Often enables better query plans
    """

    rule_id = "QT-OPT-005"
    name = "Implicit Cross Join Syntax"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "Implicit comma-join syntax - explicit JOIN may improve optimizer decisions"
    suggestion = "Rewrite using explicit INNER JOIN syntax for clarity and potentially better plans"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.subquery_depth > 0:
            return

        # Count tables in FROM that use comma syntax (not explicit JOINs)
        from_clause = node.args.get('from')
        if not from_clause:
            return

        # Check for multiple tables without explicit JOIN
        tables = list(from_clause.find_all(exp.Table))
        joins = list(node.find_all(exp.Join))

        # If we have multiple tables but few/no explicit JOINs, flag it
        if len(tables) >= 3 and len(joins) < len(tables) - 1:
            table_names = [str(t.this) for t in tables[:4] if t.this]
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Implicit join of {len(tables)} tables - explicit JOIN syntax recommended",
                matched_text=f"FROM {', '.join(table_names)}...",
            )


# Dimension tables commonly filtered
DIMENSION_TABLES = {
    'store', 'customer', 'item', 'customer_address', 'customer_demographics',
    'household_demographics', 'promotion', 'reason', 'ship_mode', 'warehouse',
    'web_site', 'web_page', 'catalog_page', 'call_center', 'income_band',
    'time_dim', 'date_dim'
}


class PredicatePushdownOpportunity(ASTRule):
    """QT-OPT-007: Dimension filter in main query - pushdown into CTE opportunity.

    Empirical speedup: 2.71x (Q93), 1.23x (Q27)

    Pattern detected:
        WITH agg AS (SELECT fk_col, SUM(val) FROM fact GROUP BY fk_col)
        SELECT * FROM agg, dim
        WHERE agg.fk_col = dim.pk AND dim.filter = 'X'

    Suggested rewrite:
        WITH agg AS (
            SELECT fk_col, SUM(val) FROM fact, dim
            WHERE fact.fk_col = dim.pk AND dim.filter = 'X'
            GROUP BY fk_col
        )
        SELECT * FROM agg

    Why it helps:
    - Filters fact rows BEFORE aggregation
    - Reduces rows entering GROUP BY
    - Q93: 2.71x speedup filtering reason='duplicate purchase' early
    """

    rule_id = "QT-OPT-007"
    name = "Predicate Pushdown into CTE Opportunity"
    severity = "high"
    category = "optimization_opportunity"
    penalty = 20  # High impact - 2.71x on Q93
    description = "Dimension filter in main query could be pushed into CTE for 2-3x speedup"
    suggestion = "Move dimension join and filter INTO the CTE before GROUP BY"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.subquery_depth > 0:
            return

        # Must have WITH clause (CTEs)
        with_clause = node.find(exp.With)
        if not with_clause:
            return

        # Get CTE names
        cte_names = set()
        for cte in with_clause.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(str(cte.alias).lower())

        if not cte_names:
            return

        # Check main query for dimension table with filter
        where = node.find(exp.Where)
        if not where:
            return

        # Get tables in main FROM (not in CTEs)
        main_tables = self._get_main_query_tables(node, cte_names)

        # Find dimension tables with filters in main query
        dim_filters = []
        for table in main_tables:
            table_lower = table.lower()
            if table_lower in DIMENSION_TABLES or any(d in table_lower for d in ['dim', 'store', 'customer', 'item', 'reason']):
                # Check if this table has a filter in WHERE
                filters = self._find_table_filters(where, table)
                if filters:
                    dim_filters.append((table, filters))

        # Check if CTEs reference fact tables (aggregation target)
        cte_has_fact = False
        for cte in with_clause.find_all(exp.CTE):
            cte_sql = cte.sql().lower()
            if any(f in cte_sql for f in FACT_TABLES) and ('group by' in cte_sql or 'sum(' in cte_sql or 'count(' in cte_sql):
                cte_has_fact = True
                break

        if dim_filters and cte_has_fact:
            dim_info = ", ".join(f"{t}({','.join(f[:2])})" for t, f in dim_filters[:2])
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Dimension filter [{dim_info}] in main query - push into CTE for 2-3x speedup",
                matched_text=f"Filter on {dim_info} outside CTE aggregation",
            )

    def _get_main_query_tables(self, node: exp.Expression, cte_names: set) -> list:
        """Get tables from main query FROM clause (excluding CTE references)."""
        tables = []
        from_clause = node.args.get('from')
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                if table.this:
                    name = str(table.this)
                    if name.lower() not in cte_names:
                        tables.append(name)
        # Also check JOINs
        for join in node.find_all(exp.Join):
            for table in join.find_all(exp.Table):
                if table.this:
                    name = str(table.this)
                    if name.lower() not in cte_names:
                        tables.append(name)
        return tables

    def _find_table_filters(self, where: exp.Expression, table_name: str) -> list:
        """Find filter columns for a specific table."""
        filters = []
        table_lower = table_name.lower()
        for eq in where.find_all(exp.EQ):
            for col in eq.find_all(exp.Column):
                col_table = str(col.table).lower() if col.table else ""
                col_name = str(col.this).lower() if col.this else ""
                # Check if column belongs to this table (by alias or table name prefix)
                if col_table and (col_table == table_lower or col_table.startswith(table_lower[0])):
                    if col_name and not col_name.endswith('_sk'):  # Skip join keys
                        filters.append(col_name)
        return filters


class CorrelatedToPrecomputedCTEOpportunity(ASTRule):
    """QT-OPT-008: Correlated subquery comparing to aggregate - pre-computed CTE opportunity.

    Empirical speedup: 2.81x (Q1)

    Pattern detected:
        WITH ctr AS (SELECT store_sk, SUM(fee) AS total FROM returns GROUP BY store_sk, customer_sk)
        SELECT * FROM ctr c1
        WHERE c1.total > (SELECT AVG(total) * 1.2 FROM ctr c2 WHERE c1.store_sk = c2.store_sk)

    Suggested rewrite:
        WITH ctr AS (...),
             store_avg AS (SELECT store_sk, AVG(total) * 1.2 AS threshold FROM ctr GROUP BY store_sk)
        SELECT * FROM ctr c1
        JOIN store_avg sa ON c1.store_sk = sa.store_sk
        WHERE c1.total > sa.threshold

    Why it helps:
    - Eliminates O(n²) correlated execution
    - Pre-computes thresholds once per group
    - Q1: 2.81x speedup (241ms → 86ms)
    """

    rule_id = "QT-OPT-008"
    name = "Correlated Subquery to Pre-computed CTE Opportunity"
    severity = "high"
    category = "optimization_opportunity"
    penalty = 25  # Highest impact - 2.81x on Q1
    description = "Correlated subquery with aggregate comparison - pre-computed CTE can give 2-3x speedup"
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

            # Check for aggregate function (AVG, SUM, COUNT, MAX, MIN)
            has_agg = bool(inner_select.find(exp.AggFunc))
            if not has_agg:
                continue

            # Check for correlation (WHERE with equality referencing outer)
            inner_where = inner_select.find(exp.Where)
            if not inner_where:
                continue

            # Look for correlated equality (col1 = col2 pattern)
            has_correlation = False
            corr_col = None
            for eq in inner_where.find_all(exp.EQ):
                cols = list(eq.find_all(exp.Column))
                if len(cols) >= 2:
                    # Two columns in equality suggests correlation
                    has_correlation = True
                    corr_col = str(cols[0].this) if cols[0].this else "column"
                    break

            if has_correlation:
                # Find the aggregate function name
                agg_func = inner_select.find(exp.AggFunc)
                agg_name = type(agg_func).__name__ if agg_func else "aggregate"

                yield RuleMatch(
                    node=compare,
                    context=context,
                    message=f"Correlated {agg_name} subquery on {corr_col} - pre-computed CTE can give 2-3x speedup",
                    matched_text=f"WHERE ... > (SELECT {agg_name}(...) ... WHERE correlated)",
                )
                return  # Only report once per query


class JoinEliminationOpportunity(ASTRule):
    """QT-OPT-009: Table joined but no columns used - join elimination opportunity.

    Empirical speedup: 2.18x (Q23)

    Pattern detected:
        SELECT a.col1, a.col2, SUM(a.value)
        FROM fact a
        JOIN dim d ON a.dim_sk = d.dim_sk  -- d columns never used!
        GROUP BY a.col1, a.col2

    Suggested rewrite:
        SELECT col1, col2, SUM(value)
        FROM fact
        WHERE dim_sk IS NOT NULL  -- Preserve NULL filtering from join
        GROUP BY col1, col2

    Why it helps:
    - Removes unnecessary table scan
    - Eliminates join operation
    - Q23: 2.18x speedup removing item/customer joins

    CRITICAL: Must add IS NOT NULL to preserve join's implicit NULL filtering!
    """

    rule_id = "QT-OPT-009"
    name = "Join Elimination Opportunity"
    severity = "high"
    category = "optimization_opportunity"
    penalty = 20
    description = "Table joined but no columns selected - can remove join for 2x+ speedup (add IS NOT NULL!)"
    suggestion = "Remove unused join, add WHERE fk_column IS NOT NULL to preserve semantics"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.subquery_depth > 0:
            return

        # Get all columns actually used in SELECT, WHERE, GROUP BY, ORDER BY
        used_columns = self._get_used_columns(node)

        # Get tables and their aliases
        table_aliases = self._get_table_aliases(node)

        # Check each JOIN
        for join in node.find_all(exp.Join):
            joined_table = join.find(exp.Table)
            if not joined_table or not joined_table.this:
                continue

            table_name = str(joined_table.this).lower()
            alias = str(joined_table.alias).lower() if joined_table.alias else table_name

            # Check if any columns from this table are used
            table_used = False
            for col_table, col_name in used_columns:
                if col_table == alias or col_table == table_name:
                    table_used = True
                    break
                # Also check if column could belong to this table (no qualifier)
                if not col_table and table_name in DIMENSION_TABLES:
                    # Ambiguous - assume it might be used
                    table_used = True
                    break

            if not table_used and table_name in DIMENSION_TABLES:
                # Find the join key
                join_on = join.find(exp.EQ)
                join_col = ""
                if join_on:
                    for col in join_on.find_all(exp.Column):
                        col_t = str(col.table).lower() if col.table else ""
                        if col_t != alias and col_t != table_name:
                            join_col = str(col.this) if col.this else ""
                            break

                yield RuleMatch(
                    node=join,
                    context=context,
                    message=f"Table '{table_name}' joined but no columns used - remove join, add {join_col} IS NOT NULL",
                    matched_text=f"JOIN {table_name} (no columns selected)",
                )
                return  # Only report first occurrence

    def _get_used_columns(self, node: exp.Expression) -> set:
        """Get all (table, column) pairs used in query."""
        used = set()

        # SELECT columns
        for sel in node.find_all(exp.Select):
            if sel == node or sel.parent == node:  # Only main select
                for col in sel.find_all(exp.Column):
                    table = str(col.table).lower() if col.table else ""
                    name = str(col.this).lower() if col.this else ""
                    if name:
                        used.add((table, name))

        # WHERE columns
        where = node.find(exp.Where)
        if where:
            for col in where.find_all(exp.Column):
                table = str(col.table).lower() if col.table else ""
                name = str(col.this).lower() if col.this else ""
                if name:
                    used.add((table, name))

        # GROUP BY columns
        for gb in node.find_all(exp.Group):
            for col in gb.find_all(exp.Column):
                table = str(col.table).lower() if col.table else ""
                name = str(col.this).lower() if col.this else ""
                if name:
                    used.add((table, name))

        # ORDER BY columns
        for ob in node.find_all(exp.Order):
            for col in ob.find_all(exp.Column):
                table = str(col.table).lower() if col.table else ""
                name = str(col.this).lower() if col.this else ""
                if name:
                    used.add((table, name))

        return used

    def _get_table_aliases(self, node: exp.Expression) -> dict:
        """Get mapping of alias -> table name."""
        aliases = {}
        for table in node.find_all(exp.Table):
            if table.this:
                name = str(table.this).lower()
                alias = str(table.alias).lower() if table.alias else name
                aliases[alias] = name
        return aliases


class ScanConsolidationOpportunity(ASTRule):
    """QT-OPT-010: Same table scanned multiple times - consolidation opportunity.

    Empirical speedup: 1.84x (Q90)

    Pattern detected:
        WITH morning AS (SELECT ... FROM sales WHERE hour BETWEEN 8 AND 12),
             afternoon AS (SELECT ... FROM sales WHERE hour BETWEEN 13 AND 17)
        SELECT morning.total, afternoon.total FROM morning, afternoon

    Suggested rewrite:
        WITH combined AS (
            SELECT
                SUM(CASE WHEN hour BETWEEN 8 AND 12 THEN amount END) AS morning_total,
                SUM(CASE WHEN hour BETWEEN 13 AND 17 THEN amount END) AS afternoon_total
            FROM sales
            WHERE hour BETWEEN 8 AND 17
        )
        SELECT morning_total, afternoon_total FROM combined

    Why it helps:
    - Single table scan instead of multiple
    - Reduces I/O significantly
    - Q90: 1.84x speedup combining AM/PM counting
    """

    rule_id = "QT-OPT-010"
    name = "Scan Consolidation Opportunity"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 15
    description = "Same table scanned multiple times - consolidate with CASE WHEN for 1.5-2x speedup"
    suggestion = "Combine multiple scans into single scan with CASE WHEN conditional aggregates"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Count table occurrences across CTEs
        table_counts = {}
        cte_tables = {}  # CTE name -> tables used

        for cte in node.find_all(exp.CTE):
            cte_name = str(cte.alias).lower() if cte.alias else ""
            tables_in_cte = set()

            for table in cte.find_all(exp.Table):
                if table.this:
                    table_name = str(table.this).lower()
                    tables_in_cte.add(table_name)
                    table_counts[table_name] = table_counts.get(table_name, 0) + 1

            if cte_name:
                cte_tables[cte_name] = tables_in_cte

        # Find tables scanned 2+ times
        repeated = [(t, c) for t, c in table_counts.items() if c >= 2 and t in FACT_TABLES]

        if repeated:
            table_info = ", ".join(f"{t}({c}x)" for t, c in repeated[:2])
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Table scanned multiple times [{table_info}] - consolidate with CASE WHEN for 1.5-2x speedup",
                matched_text=f"Multiple CTEs scan {table_info}",
            )
