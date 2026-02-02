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
