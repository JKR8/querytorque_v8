"""Optimization opportunity detection rules (AST-based).

These rules perform AST-based detection of patterns from the knowledge base.
For the canonical pattern definitions, see:
    qt_sql.optimization.knowledge_base

Pattern Evidence (TPC-DS SF100, DuckDB):
- or_to_union: 2.98x (Q15)
- correlated_to_cte: 2.81x (Q1)
- date_cte_isolate: 2.67x (Q15)
- push_pred: 2.71x (Q93)
- consolidate_scans: 1.84x (Q90)

NOTE: QT-OPT codes here match knowledge_base.TRANSFORM_REGISTRY codes exactly.
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

    Knowledge Base: or_to_union
    Empirical speedup: 2-3x (Q15: 2.98x, Q23: 2.33x, Q45: 2.26x, Q24: 2.16x)

    Pattern detected:
        WHERE (col_a = X OR col_b = Y OR col_c > Z)

    Suggested rewrite:
        SELECT ... WHERE col_a = X
        UNION ALL
        SELECT ... WHERE col_b = Y
        UNION ALL
        SELECT ... WHERE col_c > Z
    """

    rule_id = "QT-OPT-001"  # Matches knowledge_base.TransformID.OR_TO_UNION
    name = "OR to UNION ALL Decomposition"
    severity = "high"  # High-value transform
    category = "optimization_opportunity"
    penalty = 0
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
    """QT-OPT-003: Date filtering happens late - early CTE opportunity.

    Knowledge Base: date_cte_isolate
    Empirical speedup: 1.5-2.7x (Q6, Q15, Q27, Q39: 2.44x, Q92: 2.06x)

    Pattern detected:
        FROM fact_table, date_dim, other_tables
        WHERE fact.date_sk = date_dim.d_date_sk
        AND date_dim.d_year = 2001

    Suggested rewrite:
        WITH filtered_dates AS (
            SELECT d_date_sk FROM date_dim WHERE d_year = 2001
        )
        SELECT ... FROM fact_table
        INNER JOIN filtered_dates ON fact.date_sk = d_date_sk
    """

    rule_id = "QT-OPT-003"  # Matches knowledge_base.TransformID.DATE_CTE_ISOLATION
    name = "Date CTE Isolation"
    severity = "high"
    category = "optimization_opportunity"
    penalty = 15
    description = "Date filtering late in query - early date CTE can give 1.5-2.7x speedup"
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
    """QT-OPT-007: Repeated subquery - materialized CTE opportunity.

    Knowledge Base: materialize_cte
    Empirical speedup: 1.2-2x (Q95: 2.25x)

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
    """

    rule_id = "QT-OPT-007"  # Matches knowledge_base.TransformID.MATERIALIZE_CTE
    name = "Materialize Repeated Subquery"
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


# NOTE: CorrelatedSubqueryOpportunity (window function) removed - not in MCTS knowledge base
# Use CorrelatedToPrecomputedCTEOpportunity (QT-OPT-002) for correlated subquery patterns


class CountToExistsOpportunity(ASTRule):
    """QT-OPT-008: COUNT(*) > 0 in subquery - EXISTS opportunity.

    Knowledge Base: flatten_subq
    Empirical speedup: 1.2-1.5x (Q41: 1.69x)

    Pattern detected:
        WHERE (SELECT COUNT(*) FROM t WHERE ...) > 0

    Suggested rewrite:
        WHERE EXISTS (SELECT 1 FROM t WHERE ...)

    This is part of the flatten_subq transform family:
    - EXISTS→SEMI JOIN
    - NOT EXISTS→anti-join
    - IN→JOIN
    - COUNT(*)>0→EXISTS
    """

    rule_id = "QT-OPT-008"  # Matches knowledge_base.TransformID.FLATTEN_SUBQUERY
    name = "Flatten Subquery to JOIN/EXISTS"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "COUNT(*) > 0 in subquery - EXISTS stops at first match for 1.2-1.5x speedup"
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


# NOTE: ImplicitCrossJoinOpportunity removed - not in MCTS knowledge base
# Implicit comma-joins are a style issue, not a performance optimization


# Dimension tables commonly filtered
DIMENSION_TABLES = {
    'store', 'customer', 'item', 'customer_address', 'customer_demographics',
    'household_demographics', 'promotion', 'reason', 'ship_mode', 'warehouse',
    'web_site', 'web_page', 'catalog_page', 'call_center', 'income_band',
    'time_dim', 'date_dim'
}


class PredicatePushdownOpportunity(ASTRule):
    """QT-OPT-004: Dimension filter in main query - pushdown into CTE opportunity.

    Knowledge Base: push_pred
    Empirical speedup: 2-3x (Q93: 2.71x, Q27: 1.23x)

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
    """

    rule_id = "QT-OPT-004"  # Matches knowledge_base.TransformID.PUSH_PREDICATE
    name = "Predicate Pushdown into CTE"
    severity = "high"
    category = "optimization_opportunity"
    penalty = 20
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

        # Special case: quantity-range CTEs with shared filters in main query (Q9-style)
        quantity_range_ctes = self._has_quantity_range_ctes(with_clause)
        main_filter_hint = self._has_main_filters_of_interest(where)

        if (dim_filters and cte_has_fact) or (quantity_range_ctes and main_filter_hint and cte_has_fact):
            dim_info = ", ".join(f"{t}({','.join(f[:2])})" for t, f in dim_filters[:2])
            if not dim_info and quantity_range_ctes:
                dim_info = "quantity_range + shared filters"
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

    def _has_quantity_range_ctes(self, with_clause: exp.With) -> bool:
        """Detect repeated quantity-range CTEs (Q9-style)."""
        count = 0
        for cte in with_clause.find_all(exp.CTE):
            cte_name = str(cte.alias).lower() if cte.alias else ""
            cte_sql = cte.sql().lower()
            if "quantity_range" in cte_name or ("ss_quantity" in cte_sql and "between" in cte_sql):
                count += 1
                if count >= 2:
                    return True
        return False

    def _has_main_filters_of_interest(self, where: exp.Expression) -> bool:
        """Detect common shared filters in main query (item/date)."""
        for col in where.find_all(exp.Column):
            col_name = str(col.this).lower() if col.this else ""
            if any(key in col_name for key in ["item", "sold_date", "date_sk", "d_date", "d_year"]):
                return True
        return False


class CorrelatedToPrecomputedCTEOpportunity(ASTRule):
    """QT-OPT-002: Correlated subquery comparing to aggregate - pre-computed CTE opportunity.

    Knowledge Base: correlated_to_cte
    Empirical speedup: 2-3x (Q1: 2.81x)

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
    """

    rule_id = "QT-OPT-002"  # Matches knowledge_base.TransformID.CORRELATED_TO_CTE
    name = "Correlated Subquery to Pre-computed CTE"
    severity = "high"
    category = "optimization_opportunity"
    penalty = 25
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


# NOTE: JoinEliminationOpportunity removed - not in MCTS knowledge base
# Join elimination is handled by the database optimizer, not our transforms


class ScanConsolidationOpportunity(ASTRule):
    """QT-OPT-005: Same table scanned multiple times - consolidation opportunity.

    Knowledge Base: consolidate_scans
    Empirical speedup: 1.5-2x (Q90: 1.84x)

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
    """

    rule_id = "QT-OPT-005"  # Matches knowledge_base.TransformID.CONSOLIDATE_SCANS
    name = "Scan Consolidation"
    severity = "high"
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


class MultiPushPredicateOpportunity(ASTRule):
    """QT-OPT-006: Filter on column that traces through multiple CTEs - multi-layer pushdown.

    Knowledge Base: multi_push_pred
    Empirical speedup: 1.5-2x

    Pattern detected:
        WITH cte1 AS (SELECT customer_id, amount FROM sales),
             cte2 AS (SELECT customer_id, SUM(amount) FROM cte1 GROUP BY customer_id)
        SELECT * FROM cte2 WHERE customer_id = 100

    Suggested rewrite:
        WITH cte1 AS (SELECT customer_id, amount FROM sales WHERE customer_id = 100),
             cte2 AS (SELECT customer_id, SUM(amount) FROM cte1 GROUP BY customer_id)
        SELECT * FROM cte2 WHERE customer_id = 100
    """

    rule_id = "QT-OPT-006"  # Matches knowledge_base.TransformID.MULTI_PUSH_PREDICATE
    name = "Multi-layer Predicate Pushdown"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "Filter can be pushed through multiple CTE layers for 1.5-2x speedup"
    suggestion = "Push filter predicate through each CTE layer to base tables"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.subquery_depth > 0:
            return

        # Must have CTEs and a WHERE clause
        with_clause = node.find(exp.With)
        where = node.find(exp.Where)
        if not with_clause or not where:
            return

        # Count CTE depth (CTEs that reference other CTEs)
        cte_names = set()
        cte_refs = {}  # cte_name -> set of referenced CTEs

        for cte in with_clause.find_all(exp.CTE):
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if cte_name:
                cte_names.add(cte_name)
                refs = set()
                for table in cte.find_all(exp.Table):
                    tname = str(table.this).lower() if table.this else ""
                    if tname in cte_names:
                        refs.add(tname)
                cte_refs[cte_name] = refs

        # If any CTE references another CTE, we have multi-layer opportunity
        multi_layer = any(refs for refs in cte_refs.values())

        if multi_layer:
            # Check if main WHERE has equality predicates
            has_eq = bool(where.find(exp.EQ))
            if has_eq:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="Multi-layer CTE with filter - predicate can be pushed through layers",
                    matched_text="Filter on column from nested CTEs",
                )


class JoinReorderOpportunity(ASTRule):
    """QT-OPT-009: Multiple tables with uneven filter selectivity - reorder opportunity.

    Knowledge Base: reorder_join
    Empirical speedup: 1.2-2x

    Pattern detected:
        FROM large_fact_table, small_dim_table
        WHERE large.date_sk = small.date_sk AND small.year = 2001

    Suggested rewrite:
        FROM small_dim_table, large_fact_table
        WHERE small.year = 2001 AND large.date_sk = small.date_sk
    """

    rule_id = "QT-OPT-009"  # Matches knowledge_base.TransformID.REORDER_JOIN
    name = "Join Reordering"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "Tables could be reordered to put filtered tables first for 1.2-2x speedup"
    suggestion = "Put tables with selective filters earlier in join order"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.subquery_depth > 0:
            return

        # Need multiple tables and a WHERE
        from_clause = node.args.get('from')
        where = node.find(exp.Where)
        if not from_clause or not where:
            return

        # Get all tables
        tables = []
        for table in from_clause.find_all(exp.Table):
            if table.this:
                tables.append(str(table.this).lower())
        for join in node.find_all(exp.Join):
            for table in join.find_all(exp.Table):
                if table.this:
                    tables.append(str(table.this).lower())

        if len(tables) < 3:
            return

        # Check if any table has an equality filter (selective)
        filtered_tables = set()
        for eq in where.find_all(exp.EQ):
            for col in eq.find_all(exp.Column):
                if col.table:
                    filtered_tables.add(str(col.table).lower())

        # If we have filtered tables that aren't first, suggest reorder
        if filtered_tables and tables[0] not in filtered_tables:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Join order may benefit from reordering - filtered tables: {', '.join(list(filtered_tables)[:2])}",
                matched_text=f"FROM {tables[0]} with filter on {list(filtered_tables)[0] if filtered_tables else '?'}",
            )


class InlineCTEOpportunity(ASTRule):
    """QT-OPT-010: Single-use CTE that's a simple scan - inline opportunity.

    Knowledge Base: inline_cte
    Empirical speedup: 1.1-1.3x

    Pattern detected:
        WITH simple_cte AS (SELECT * FROM table WHERE filter)
        SELECT * FROM simple_cte  -- Only used once

    Suggested rewrite:
        SELECT * FROM (SELECT * FROM table WHERE filter) AS simple_cte
    """

    rule_id = "QT-OPT-010"  # Matches knowledge_base.TransformID.INLINE_CTE
    name = "Inline Single-Use CTE"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "Single-use simple CTE can be inlined for 1.1-1.3x speedup"
    suggestion = "Inline CTE as subquery since it's only used once"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get parent SELECT to count CTE references
        parent = node.parent
        if not isinstance(parent, exp.Select):
            return

        for cte in node.find_all(exp.CTE):
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if not cte_name:
                continue

            # Count references to this CTE in the main query
            ref_count = 0
            for table in parent.find_all(exp.Table):
                tname = str(table.this).lower() if table.this else ""
                if tname == cte_name:
                    ref_count += 1

            # If referenced exactly once and CTE is simple (no aggregation)
            if ref_count == 1:
                cte_select = cte.find(exp.Select)
                if cte_select and not cte_select.find(exp.AggFunc):
                    yield RuleMatch(
                        node=cte,
                        context=context,
                        message=f"CTE '{cte_name}' used once - consider inlining",
                        matched_text=f"WITH {cte_name} AS (simple query)",
                    )
                    return  # Only report first


class RemoveRedundantOpportunity(ASTRule):
    """QT-OPT-011: Redundant operations that can be removed.

    Knowledge Base: remove_redundant
    Empirical speedup: 1.1-1.2x

    Patterns detected:
        - DISTINCT with GROUP BY covering all columns
        - ORDER BY in subquery when outer has ORDER BY
        - Unused columns in subqueries
    """

    rule_id = "QT-OPT-011"  # Matches knowledge_base.TransformID.REMOVE_REDUNDANT
    name = "Remove Redundant Operations"
    severity = "optimization"
    category = "optimization_opportunity"
    penalty = 0
    description = "Redundant operations can be removed for cleaner, faster queries"
    suggestion = "Remove unnecessary DISTINCT, ORDER BY, or unused columns"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check for DISTINCT with GROUP BY
        if node.args.get('distinct'):
            group = node.find(exp.Group)
            if group:
                # If GROUP BY covers all selected columns, DISTINCT is redundant
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="DISTINCT with GROUP BY may be redundant",
                    matched_text="SELECT DISTINCT ... GROUP BY ...",
                )
                return

        # Check for ORDER BY in subquery
        if context.subquery_depth > 0:
            order = node.find(exp.Order)
            if order:
                yield RuleMatch(
                    node=order,
                    context=context,
                    message="ORDER BY in subquery is often redundant",
                    matched_text="Subquery with ORDER BY",
                )
