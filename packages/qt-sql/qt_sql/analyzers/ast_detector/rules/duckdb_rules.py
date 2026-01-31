"""DuckDB-specific anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class NotUsingQualifyRule(ASTRule):
    """SQL-DUCK-001: Subquery for window filter instead of QUALIFY.

    DuckDB supports QUALIFY for filtering window results:
        -- Instead of:
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
            FROM employees
        ) t WHERE rn = 1

        -- Use:
        SELECT * FROM employees
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1

    Detection:
    - Find subquery with ROW_NUMBER and outer WHERE on that column
    """

    rule_id = "SQL-DUCK-001"
    name = "Subquery Instead of QUALIFY"
    severity = "medium"
    category = "duckdb"
    penalty = 10
    description = "Use QUALIFY clause instead of subquery for window filtering"
    suggestion = "DuckDB supports QUALIFY - filter window functions directly"
    dialects = ("duckdb",)

    target_node_types = (exp.Subquery,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if subquery contains window function
        inner_select = node.find(exp.Select)
        if not inner_select:
            return

        has_window = bool(inner_select.find(exp.Window))
        if not has_window:
            return

        # Check if parent is a SELECT with WHERE filtering on window result
        parent = node.parent
        if isinstance(parent, exp.From):
            outer_select = parent.parent
            if isinstance(outer_select, exp.Select):
                where = outer_select.find(exp.Where)
                if where:
                    yield RuleMatch(
                        node=node,
                        context=context,
                        message="Subquery with window function - use QUALIFY instead",
                        matched_text="Subquery with ROW_NUMBER/window",
                    )


class NotUsingGroupByAllRule(ASTRule):
    """SQL-DUCK-002: Explicit GROUP BY instead of GROUP BY ALL.

    DuckDB supports GROUP BY ALL to automatically group by non-aggregated columns:
        -- Instead of:
        SELECT dept, region, SUM(sales)
        FROM orders
        GROUP BY dept, region

        -- Use:
        SELECT dept, region, SUM(sales)
        FROM orders
        GROUP BY ALL

    Detection:
    - Find GROUP BY with multiple columns matching SELECT list
    """

    rule_id = "SQL-DUCK-002"
    name = "Explicit GROUP BY"
    severity = "low"
    category = "duckdb"
    penalty = 5
    description = "Consider GROUP BY ALL for automatic grouping"
    suggestion = "DuckDB supports GROUP BY ALL to auto-group non-aggregated columns"
    dialects = ("duckdb",)

    target_node_types = (exp.Group,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Count GROUP BY columns
        group_cols = len(list(node.expressions)) if hasattr(node, 'expressions') else 0

        # Only suggest if 3+ GROUP BY columns (where ALL saves typing)
        if group_cols >= 3:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"GROUP BY with {group_cols} columns - consider GROUP BY ALL",
                matched_text=f"GROUP BY ({group_cols} columns)",
            )


class ListAggWithoutOrderRule(ASTRule):
    """SQL-DUCK-003: LIST_AGG/STRING_AGG without ORDER BY.

    DuckDB's list aggregation without ORDER BY is non-deterministic:
        SELECT LIST(name) FROM users GROUP BY dept

    Add ORDER BY for deterministic results:
        SELECT LIST(name ORDER BY name) FROM users GROUP BY dept

    Detection:
    - Find STRING_AGG, LIST, ARRAY_AGG without internal ORDER BY
    """

    rule_id = "SQL-DUCK-003"
    name = "List Aggregation Without ORDER"
    severity = "medium"
    category = "duckdb"
    penalty = 10
    description = "LIST/STRING_AGG without ORDER BY is non-deterministic"
    suggestion = "Add ORDER BY inside aggregation for deterministic results"
    dialects = ("duckdb",)

    # Note: DuckDB's LIST() parses as exp.List in sqlglot
    target_node_types = (exp.ArrayAgg, exp.GroupConcat, exp.Anonymous, exp.List)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check function name - DuckDB LIST() parses as exp.List
        is_list_agg = isinstance(node, (exp.ArrayAgg, exp.GroupConcat, exp.List))

        if isinstance(node, exp.Anonymous):
            func_name = str(node.this).lower() if node.this else ""
            if func_name in ("list", "list_agg", "string_agg", "array_agg"):
                is_list_agg = True

        if not is_list_agg:
            return

        # Check for ORDER BY within the aggregation
        has_order = node.find(exp.Order) is not None

        if not has_order:
            yield RuleMatch(
                node=node,
                context=context,
                message="List aggregation without ORDER BY - non-deterministic",
                matched_text=node.sql()[:60],
            )


class TempTableInsteadOfCTERule(ASTRule):
    """SQL-DUCK-004: Temp table when CTE would work better.

    DuckDB optimizes CTEs very well - they're not always materialized:
        -- Temp tables force materialization:
        CREATE TEMP TABLE t AS SELECT ...;
        SELECT * FROM t;

        -- CTEs are optimized:
        WITH t AS (SELECT ...) SELECT * FROM t

    Detection:
    - Find CREATE TEMPORARY TABLE statements
    """

    rule_id = "SQL-DUCK-004"
    name = "Temp Table Instead of CTE"
    severity = "low"
    category = "duckdb"
    penalty = 5
    description = "DuckDB optimizes CTEs well - temp table may be unnecessary"
    suggestion = "Consider using WITH (CTE) instead of temp table"
    dialects = ("duckdb",)

    target_node_types = (exp.Create,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if this is CREATE TEMPORARY TABLE
        if not isinstance(node.this, exp.Schema):
            return

        # Check for TEMPORARY keyword
        props = node.args.get('properties', [])
        is_temp = any(
            isinstance(p, exp.TemporaryProperty)
            for p in (props.expressions if hasattr(props, 'expressions') else [])
        ) if props else False

        # Also check for TEMP in the expression itself
        if not is_temp:
            sql_text = node.sql().upper()
            is_temp = 'TEMPORARY' in sql_text or 'TEMP ' in sql_text

        if is_temp:
            yield RuleMatch(
                node=node,
                context=context,
                message="Temp table - DuckDB CTEs are highly optimized",
                matched_text="CREATE TEMP TABLE",
            )


class NotUsingSampleRule(ASTRule):
    """SQL-DUCK-005: Large SELECT without SAMPLE for exploration.

    DuckDB supports TABLESAMPLE for efficient random sampling:
        -- Sample 10% of rows:
        SELECT * FROM large_table TABLESAMPLE 10%

        -- Sample 1000 rows:
        SELECT * FROM large_table USING SAMPLE 1000

    Detection:
    - Find SELECT * FROM large table without LIMIT or SAMPLE
    - Hard to detect programmatically, flag SELECT * for awareness
    """

    rule_id = "SQL-DUCK-005"
    name = "Consider SAMPLE Clause"
    severity = "low"
    category = "duckdb"
    penalty = 5
    description = "For data exploration, consider TABLESAMPLE or USING SAMPLE"
    suggestion = "DuckDB supports TABLESAMPLE 10% or USING SAMPLE 1000 ROWS"
    dialects = ("duckdb",)

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Skip if in subquery
        if context.in_subquery:
            return

        # Check for SELECT * without LIMIT
        has_star = any(isinstance(e, exp.Star) for e in node.expressions)
        has_limit = node.find(exp.Limit) is not None

        # Only suggest for SELECT * without LIMIT (likely exploration)
        if has_star and not has_limit:
            # Additional heuristic: no WHERE clause (full scan)
            has_where = node.find(exp.Where) is not None
            if not has_where:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="SELECT * without LIMIT - consider TABLESAMPLE for exploration",
                    matched_text="SELECT * FROM table",
                )


class NotUsingExcludeRule(ASTRule):
    """SQL-DUCK-006: SELECT * when EXCLUDE would help.

    DuckDB supports EXCLUDE to select all columns except some:
        SELECT * EXCLUDE (password, ssn) FROM users

    This is cleaner than listing all wanted columns.

    Detection:
    - Find SELECT with many columns (suggesting they want "most" columns)
    """

    rule_id = "SQL-DUCK-006"
    name = "Consider EXCLUDE Clause"
    severity = "low"
    category = "duckdb"
    penalty = 5
    description = "DuckDB supports SELECT * EXCLUDE (col) for most columns"
    suggestion = "Use SELECT * EXCLUDE (col1, col2) to select most columns"
    dialects = ("duckdb",)

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if SELECT has many explicit columns (10+)
        col_count = len(node.expressions)

        if col_count >= 10:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"SELECT with {col_count} columns - consider * EXCLUDE",
                matched_text=f"SELECT ({col_count} columns)",
            )


class SubqueryInsteadOfPivotRule(ASTRule):
    """SQL-DUCK-007: Manual pivot via CASE/subqueries.

    DuckDB supports native PIVOT:
        -- Instead of:
        SELECT id,
            MAX(CASE WHEN category = 'A' THEN value END) as A,
            MAX(CASE WHEN category = 'B' THEN value END) as B
        FROM t GROUP BY id

        -- Use:
        PIVOT t ON category USING MAX(value)

    Detection:
    - Find multiple CASE WHEN with same column in condition
    """

    rule_id = "SQL-DUCK-007"
    name = "Manual Pivot Pattern"
    severity = "medium"
    category = "duckdb"
    penalty = 10
    description = "Manual pivot with CASE - consider native PIVOT"
    suggestion = "DuckDB supports PIVOT t ON column USING aggregate(value)"
    dialects = ("duckdb",)

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Count CASE expressions with similar structure
        case_exprs = list(node.find_all(exp.Case))

        if len(case_exprs) >= 3:
            # Check if they reference same column in conditions
            condition_cols = set()
            for case in case_exprs:
                for when in case.find_all(exp.EQ):
                    cols = list(when.find_all(exp.Column))
                    for col in cols:
                        condition_cols.add(str(col.this).lower())

            # If same column appears in multiple CASE conditions, likely a pivot
            if condition_cols and len(case_exprs) >= 3:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"{len(case_exprs)} CASE expressions - consider PIVOT",
                    matched_text="SELECT with multiple CASE pivots",
                )


class NotUsingUnpivotRule(ASTRule):
    """SQL-DUCK-008: UNION ALL for unpivot instead of UNPIVOT.

    DuckDB supports native UNPIVOT:
        -- Instead of:
        SELECT id, 'A' as category, a as value FROM t
        UNION ALL
        SELECT id, 'B' as category, b as value FROM t

        -- Use:
        UNPIVOT t ON (a, b) INTO NAME category VALUE value

    Detection:
    - Find UNION ALL with same source table and literal category
    """

    rule_id = "SQL-DUCK-008"
    name = "UNION ALL for Unpivot"
    severity = "medium"
    category = "duckdb"
    penalty = 10
    description = "UNION ALL pattern suggests UNPIVOT could be used"
    suggestion = "DuckDB supports UNPIVOT t ON (cols) INTO NAME category VALUE val"
    dialects = ("duckdb",)

    target_node_types = (exp.Union,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if this is UNION ALL
        if not node.args.get('distinct') is False:
            # Not UNION ALL
            return

        # Count the branches
        branches = [node.this] + list(node.expressions) if hasattr(node, 'expressions') else [node.this]

        if len(branches) >= 3:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"UNION ALL with {len(branches)} branches - consider UNPIVOT",
                matched_text="UNION ALL (unpivot pattern)",
            )


class DirectParquetReadRule(ASTRule):
    """SQL-DUCK-009: Loading Parquet into table instead of direct query.

    DuckDB can query Parquet files directly:
        -- Instead of:
        CREATE TABLE t AS SELECT * FROM read_parquet('file.parquet');
        SELECT * FROM t WHERE x = 1;

        -- Query directly:
        SELECT * FROM read_parquet('file.parquet') WHERE x = 1;

    DuckDB pushes predicates into Parquet reader for efficiency.

    Detection:
    - Find CREATE TABLE ... SELECT * FROM read_parquet/read_csv
    """

    rule_id = "SQL-DUCK-009"
    name = "Consider Direct File Query"
    severity = "low"
    category = "duckdb"
    penalty = 5
    description = "DuckDB can query Parquet/CSV directly with predicate pushdown"
    suggestion = "Query read_parquet() directly - predicates are pushed down"
    dialects = ("duckdb",)

    target_node_types = (exp.Create,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if this is CREATE TABLE AS SELECT FROM read_parquet/csv
        select = node.find(exp.Select)
        if not select:
            return

        # Look for read_parquet or read_csv function
        for func in select.find_all(exp.Anonymous):
            func_name = str(func.this).lower() if func.this else ""
            if func_name in ("read_parquet", "read_csv", "read_csv_auto", "read_json"):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"CREATE TABLE from {func_name} - consider direct query",
                    matched_text=f"CREATE TABLE AS SELECT FROM {func_name}()",
                )
                return


class NotUsingAsOfJoinRule(ASTRule):
    """SQL-DUCK-010: Complex time-based join instead of ASOF JOIN.

    DuckDB supports ASOF JOIN for time-series data:
        -- Instead of complex window or correlated subquery:
        SELECT t1.*, t2.price
        FROM trades t1
        LEFT JOIN (
            SELECT *, ROW_NUMBER() OVER (...) as rn FROM quotes
        ) t2 ON t1.symbol = t2.symbol AND t2.rn = 1

        -- Use:
        SELECT * FROM trades t1
        ASOF JOIN quotes t2 ON t1.symbol = t2.symbol AND t1.time >= t2.time

    Detection:
    - Find JOINs with time-based inequalities
    """

    rule_id = "SQL-DUCK-010"
    name = "Consider ASOF JOIN"
    severity = "medium"
    category = "duckdb"
    penalty = 10
    description = "Time-based join pattern - consider ASOF JOIN"
    suggestion = "DuckDB supports ASOF JOIN for time-series lookups"
    dialects = ("duckdb",)

    target_node_types = (exp.Join,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Look for inequality conditions in JOIN that involve time-like columns
        on_clause = node.args.get('on')
        if not on_clause:
            return

        # Check for >= or <= comparisons
        for comp in on_clause.find_all((exp.GTE, exp.LTE)):
            # Check if either side references a time-like column
            cols = list(comp.find_all(exp.Column))
            for col in cols:
                col_name = str(col.this).lower() if col.this else ""
                if any(time_word in col_name for time_word in
                       ('time', 'date', 'timestamp', 'ts', 'created', 'updated', 'at')):
                    yield RuleMatch(
                        node=node,
                        context=context,
                        message="Time-based inequality in JOIN - consider ASOF JOIN",
                        matched_text=node.sql()[:60],
                    )
                    return


class CrossJoinUnnestWithWhereRule(ASTRule):
    """SQL-DUCK-011: CROSS JOIN with UNNEST and WHERE filter.

    DuckDB OPTIMIZER BUG: WHERE clauses are not pushed before CROSS JOIN UNNEST,
    causing full cross join before filtering (OOM on large tables).

    GitHub Issue #18653: When doing CROSS JOIN with unnest, the query planner
    will cross join ALL rows, not just those matching the WHERE clause.

        -- SLOW/OOM - DuckDB cross joins everything then filters:
        SELECT * FROM large_table
        CROSS JOIN unnest(large_table.items) AS item
        WHERE large_table.id = 47

        -- FAST - Filter first with CTE:
        WITH filtered AS (SELECT * FROM large_table WHERE id = 47)
        SELECT * FROM filtered
        CROSS JOIN unnest(filtered.items) AS item

    Detection:
    - Find CROSS JOIN with unnest function and WHERE clause on main table
    """

    rule_id = "SQL-DUCK-011"
    name = "CROSS JOIN UNNEST With WHERE"
    severity = "high"
    category = "duckdb"
    penalty = 20
    description = "DuckDB doesn't push WHERE before CROSS JOIN UNNEST - causes full cross join"
    suggestion = "Use CTE to filter BEFORE the CROSS JOIN UNNEST to avoid OOM"
    dialects = ("duckdb",)

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check for CROSS JOIN
        joins = list(node.find_all(exp.Join))
        has_cross_join = any(
            j.args.get('kind', '').upper() == 'CROSS' or
            (not j.args.get('on') and not j.args.get('using'))
            for j in joins
        )

        if not has_cross_join:
            return

        # Check for unnest function
        has_unnest = False
        for func in node.find_all(exp.Unnest):
            has_unnest = True
            break
        if not has_unnest:
            for func in node.find_all(exp.Anonymous):
                if str(func.this).lower() == 'unnest':
                    has_unnest = True
                    break

        if not has_unnest:
            return

        # Check for WHERE clause
        where = node.find(exp.Where)
        if where:
            yield RuleMatch(
                node=node,
                context=context,
                message="CROSS JOIN UNNEST with WHERE - filter not pushed down (OOM risk)",
                matched_text="CROSS JOIN unnest(...) WHERE",
            )


class WindowBlocksPredicatePushdownRule(ASTRule):
    """SQL-DUCK-012: Filter on window partition key after window function.

    DuckDB OPTIMIZER GAP: Predicates don't push through window functions,
    even when filtering on the partition key.

    GitHub Issue #10352: Predicate pushdown doesn't work with window functions.

        -- SLOW - Full scan, then window, then filter:
        SELECT * FROM (
            SELECT *, SUM(amount) OVER (PARTITION BY customer_id) as total
            FROM orders
        ) t WHERE customer_id = 123

        -- FAST - Filter before window:
        SELECT *, SUM(amount) OVER (PARTITION BY customer_id) as total
        FROM orders
        WHERE customer_id = 123

    Detection:
    - Find subquery with window function where outer WHERE filters on partition key
    """

    rule_id = "SQL-DUCK-012"
    name = "Window Blocks Predicate Pushdown"
    severity = "high"
    category = "duckdb"
    penalty = 15
    description = "Filter on window result - predicate not pushed through window function"
    suggestion = "Move WHERE filter inside the subquery, before the window function"
    dialects = ("duckdb",)

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Skip if in subquery (we want the outer query)
        if context.in_subquery:
            return

        # Check if FROM is a subquery
        from_clause = node.find(exp.From)
        if not from_clause:
            return

        subquery = from_clause.find(exp.Subquery)
        if not subquery:
            return

        # Check if subquery has window function
        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return

        windows = list(inner_select.find_all(exp.Window))
        if not windows:
            return

        # Check if outer query has WHERE
        where = node.find(exp.Where)
        if not where:
            return

        # Get partition columns from window functions
        partition_cols = set()
        for window in windows:
            partition = window.find(exp.PartitionedByProperty)
            if partition:
                for col in partition.find_all(exp.Column):
                    partition_cols.add(str(col.this).lower() if col.this else "")

        # Check if WHERE filters on any partition column
        for eq in where.find_all(exp.EQ):
            for col in eq.find_all(exp.Column):
                col_name = str(col.this).lower() if col.this else ""
                if col_name in partition_cols or any(pc in col_name for pc in partition_cols):
                    yield RuleMatch(
                        node=node,
                        context=context,
                        message="WHERE on window partition key - move filter inside subquery",
                        matched_text=f"WHERE {col_name} = ... (partition key)",
                    )
                    return


class ManyJoinsOnParquetRule(ASTRule):
    """SQL-DUCK-013: Multiple JOINs on Parquet files without native table.

    DuckDB OPTIMIZER GAP: Parquet files lack HLL statistics for cardinality
    estimation. With 4+ joins, estimation errors propagate exponentially,
    causing catastrophic join order choices.

    TPC-DS queries took 1.5 hours with bad join order vs 5 minutes with manual reorder.

        -- SLOW - No statistics for join optimization:
        SELECT * FROM read_parquet('a.parquet') a
        JOIN read_parquet('b.parquet') b ON a.id = b.a_id
        JOIN read_parquet('c.parquet') c ON b.id = c.b_id
        JOIN read_parquet('d.parquet') d ON c.id = d.c_id

        -- FASTER - Load to native tables first for HLL statistics:
        CREATE TABLE a AS SELECT * FROM read_parquet('a.parquet');
        -- etc, then join native tables

    Detection:
    - Find 4+ JOINs with read_parquet/read_csv in FROM clause
    """

    rule_id = "SQL-DUCK-013"
    name = "Many Joins on Parquet Files"
    severity = "high"
    category = "duckdb"
    penalty = 20
    description = "4+ JOINs on Parquet files - no HLL statistics for join optimization"
    suggestion = "Load into native DuckDB tables first for better cardinality estimation"
    dialects = ("duckdb",)

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Count joins
        joins = list(node.find_all(exp.Join))
        if len(joins) < 3:  # Need 4+ tables (3+ joins)
            return

        # Check for read_parquet/read_csv functions
        parquet_count = 0
        for func in node.find_all(exp.Anonymous):
            func_name = str(func.this).lower() if func.this else ""
            if func_name in ("read_parquet", "read_csv", "read_csv_auto", "read_json"):
                parquet_count += 1

        # Also check for .parquet file references in strings
        for literal in node.find_all(exp.Literal):
            if literal.is_string:
                val = str(literal.this).lower()
                if '.parquet' in val or '.csv' in val:
                    parquet_count += 1

        if parquet_count >= 2 and len(joins) >= 3:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{len(joins)+1} tables joined from files - load to native tables for statistics",
                matched_text=f"{len(joins)} JOINs on Parquet/CSV files",
            )


class GroupedTopNPatternRule(ASTRule):
    """SQL-DUCK-014: Inefficient grouped TOPN pattern.

    DuckDB OPTIMIZER GAP: Window-based grouped TOPN processes ALL rows before
    filtering, while LATERAL can stop early after finding N rows per group.

    EXECUTION MODEL DIFFERENCE:

    Window function:
    1. Must see ALL data before numbering rows
    2. Partitions and sorts entire dataset
    3. Computes ROW_NUMBER for ALL rows
    4. Then filters to keep only rn <= N

    LATERAL with LIMIT:
    1. For each group, seeks/scans that group's rows
    2. Sorts only that group
    3. Returns first N rows, STOPS ← early termination
    4. Repeats for next group

    For low NDV (few groups, many rows), LATERAL is dramatically faster:
    - Window: Process 7M rows, output 2K
    - LATERAL: 400 groups × (small seek + sort 17K + take 5)

    DuckDB CANNOT auto-transform because:
    1. Semantically different query structures
    2. Requires knowing NDV is small
    3. It's a semantic rewrite, not cost-based optimization

        -- SLOW - Window processes ALL 7M rows:
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC) as rn
            FROM sales
        ) t WHERE rn <= 5

        -- FAST - LATERAL stops after 5 rows per store:
        SELECT s.store_id, ls.*
        FROM stores s,
        LATERAL (
            SELECT * FROM sales WHERE store_sk = s.store_sk
            ORDER BY amount DESC LIMIT 5
        ) ls

        -- ALTERNATIVE - QUALIFY (cleaner but same execution as window):
        SELECT * FROM sales
        QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC) <= 5

    Detection:
    - Find ROW_NUMBER with PARTITION BY and outer filter on row number
    """

    rule_id = "SQL-DUCK-014"
    name = "Grouped TOPN Pattern"
    severity = "high"
    category = "duckdb"
    penalty = 15
    description = "Grouped TOPN via window processes ALL rows - LATERAL enables early termination"
    suggestion = "For low NDV grouping, use LATERAL with LIMIT for early termination"
    dialects = ("duckdb",)

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Look for subquery with ROW_NUMBER and outer WHERE on rn
        from_clause = node.find(exp.From)
        if not from_clause:
            return

        subquery = from_clause.find(exp.Subquery)
        if not subquery:
            return

        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return

        # Check for ranking window function (ROW_NUMBER, RANK, DENSE_RANK) with PARTITION BY
        # All have the same execution model problem - must process ALL rows before filtering
        has_ranking_partition = False
        for window in inner_select.find_all(exp.Window):
            # Check if it's a ranking function
            func = window.this
            if isinstance(func, (exp.RowNumber, exp.Rank, exp.DenseRank)):
                # Check for PARTITION BY - can be in args['partition_by'] or as PartitionedByProperty
                partition_by = window.args.get('partition_by')
                if partition_by or window.find(exp.PartitionedByProperty):
                    has_ranking_partition = True
                    break

        if not has_ranking_partition:
            return

        # Check if outer WHERE filters on ranking column with <= or < or =
        # Common aliases: rn, rk, row_num, row_number, rownum, rank, dense_rank
        ranking_aliases = ('rn', 'rk', 'row_num', 'row_number', 'rownum', 'rank', 'dense_rank', 'rnk')
        where = node.find(exp.Where)
        if where:
            for comp in where.find_all((exp.LTE, exp.LT, exp.EQ)):
                for col in comp.find_all(exp.Column):
                    col_name = str(col.this).lower() if col.this else ""
                    if col_name in ranking_aliases:
                        yield RuleMatch(
                            node=node,
                            context=context,
                            message="Grouped TOPN via window function - LATERAL with LIMIT enables early termination",
                            matched_text=f"WHERE {col_name} <= N pattern",
                        )
                        return


class MissingRedundantJoinFilterRule(ASTRule):
    """SQL-DUCK-015: Missing redundant filter for join optimization.

    DuckDB OPTIMIZER GAP: Filters are only pushed down, not pulled up.
    When filtering one side of a join, the optimizer doesn't infer the
    equivalent filter on the other side.

    GitHub Issue #112: Predicate pull-up is not implemented.

        -- SUBOPTIMAL - DuckDB doesn't infer t2.id = 5000:
        SELECT * FROM (
            SELECT * FROM t1 WHERE id = 5000
        ) a JOIN t2 ON a.id = t2.id

        -- BETTER - Add redundant filter explicitly:
        SELECT * FROM (
            SELECT * FROM t1 WHERE id = 5000
        ) a JOIN t2 ON a.id = t2.id
        WHERE t2.id = 5000  -- Redundant but helps optimizer

    Detection:
    - Find JOIN where one side has equality filter, other doesn't
    """

    rule_id = "SQL-DUCK-015"
    name = "Missing Redundant Join Filter"
    severity = "low"
    category = "duckdb"
    penalty = 5
    description = "DuckDB doesn't pull up filters - add redundant filters for join optimization"
    suggestion = "Add equivalent WHERE clause on joined table for better optimization"
    dialects = ("duckdb",)

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # This is a complex analysis - we check for subqueries with filters
        # joined to tables without equivalent filters

        from_clause = node.find(exp.From)
        if not from_clause:
            return

        # Check for subquery in FROM with WHERE
        subquery = from_clause.find(exp.Subquery)
        if not subquery:
            return

        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return

        inner_where = inner_select.find(exp.Where)
        if not inner_where:
            return

        # Get filtered columns from inner query
        inner_filter_cols = set()
        for eq in inner_where.find_all(exp.EQ):
            for col in eq.find_all(exp.Column):
                inner_filter_cols.add(str(col.this).lower() if col.this else "")

        if not inner_filter_cols:
            return

        # Check for JOIN
        joins = list(node.find_all(exp.Join))
        if not joins:
            return

        # Check if outer WHERE exists with same columns
        outer_where = node.find(exp.Where)
        outer_filter_cols = set()
        if outer_where:
            for eq in outer_where.find_all(exp.EQ):
                for col in eq.find_all(exp.Column):
                    outer_filter_cols.add(str(col.this).lower() if col.this else "")

        # Check join conditions for columns matching inner filter
        for join in joins:
            on_clause = join.args.get('on')
            if not on_clause:
                continue

            for eq in on_clause.find_all(exp.EQ):
                cols = [str(c.this).lower() for c in eq.find_all(exp.Column) if c.this]
                # If join is on a filtered column but outer doesn't have redundant filter
                for col in cols:
                    if col in inner_filter_cols and col not in outer_filter_cols:
                        yield RuleMatch(
                            node=node,
                            context=context,
                            message=f"Add redundant WHERE {col} = X for join optimization",
                            matched_text="JOIN on filtered column without redundant filter",
                        )
                        return


class LargePivotWithoutFilterRule(ASTRule):
    """SQL-DUCK-016: PIVOT on large dataset without pre-filtering.

    DuckDB LIMITATION: PIVOT can exhaust memory on large datasets because
    there's no pre-check for output size. With high cardinality pivot columns,
    this creates massive wide tables.

        -- DANGEROUS - May OOM with high cardinality:
        PIVOT large_table ON category USING SUM(value)

        -- SAFER - Filter or aggregate first:
        PIVOT (SELECT * FROM large_table WHERE year = 2024)
        ON category USING SUM(value)

    Detection:
    - Find PIVOT without subquery filter or LIMIT
    """

    rule_id = "SQL-DUCK-016"
    name = "PIVOT on Large Dataset"
    severity = "medium"
    category = "duckdb"
    penalty = 10
    description = "PIVOT without pre-filtering may exhaust memory on large datasets"
    suggestion = "Filter data before PIVOT to control output size"
    dialects = ("duckdb",)

    target_node_types = (exp.Pivot,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if PIVOT has a filtered source
        # The source is typically in node.this

        source = node.this
        if not source:
            return

        # If source is a simple table reference (not subquery with filter)
        if isinstance(source, exp.Table):
            yield RuleMatch(
                node=node,
                context=context,
                message="PIVOT on table without pre-filtering - OOM risk",
                matched_text="PIVOT table ON ...",
            )
        elif isinstance(source, exp.Subquery):
            # Check if subquery has WHERE
            inner = source.find(exp.Select)
            if inner and not inner.find(exp.Where):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="PIVOT on subquery without WHERE - consider filtering",
                    matched_text="PIVOT (SELECT * FROM ...) ON ...",
                )


class CountDistinctOnOrderedDataRule(ASTRule):
    """SQL-DUCK-017: COUNT DISTINCT on inherently ordered data.

    DuckDB OPTIMIZER GAP: COUNT DISTINCT repeatedly builds DISTINCT sets even
    when data is inherently ordered. This can cause execution time to skyrocket
    (800+ seconds vs near-instant with alternative approach).

        -- SLOW - Repeated DISTINCT set building:
        SELECT category, COUNT(DISTINCT user_id)
        FROM events
        GROUP BY category

        -- FASTER for pre-sorted data - use FIRST/LAST aggregates:
        SELECT category, COUNT(*)
        FROM (
            SELECT DISTINCT category, user_id FROM events
        ) GROUP BY category

    Detection:
    - Find COUNT(DISTINCT ...) in GROUP BY queries
    """

    rule_id = "SQL-DUCK-017"
    name = "COUNT DISTINCT Performance"
    severity = "medium"
    category = "duckdb"
    penalty = 10
    description = "COUNT DISTINCT in GROUP BY can be slow - consider pre-distinct subquery"
    suggestion = "For large data, use subquery with DISTINCT first, then COUNT(*)"
    dialects = ("duckdb",)

    target_node_types = (exp.Count,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if this is COUNT(DISTINCT ...)
        # In sqlglot, DISTINCT is wrapped in node.this, not in args
        has_distinct = (
            node.args.get('distinct') or
            isinstance(node.this, exp.Distinct)
        )
        if not has_distinct:
            return

        # Check if we're in a GROUP BY query
        select = node.find_ancestor(exp.Select)
        if not select:
            return

        if select.find(exp.Group):
            yield RuleMatch(
                node=node,
                context=context,
                message="COUNT DISTINCT in GROUP BY - may be slow on large data",
                matched_text="COUNT(DISTINCT ...) with GROUP BY",
            )


class NestedLoopJoinRiskRule(ASTRule):
    """SQL-DUCK-018: Subquery in filter condition causing nested loop join.

    DuckDB OPTIMIZER BUG: Subqueries in filter conditions can cause the optimizer
    to choose full sequential scan + nested loop join instead of hash join.

    GitHub Issue #10315: Full seq scan + nested loop join with subquery in filter.

        -- SLOW - May cause nested loop:
        SELECT * FROM orders
        WHERE customer_id IN (SELECT id FROM customers WHERE region = 'US')

        -- FASTER - Use explicit JOIN:
        SELECT o.* FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE c.region = 'US'

    Detection:
    - Find IN/EXISTS with correlated or semi-correlated subquery in WHERE
    """

    rule_id = "SQL-DUCK-018"
    name = "Nested Loop Join Risk"
    severity = "medium"
    category = "duckdb"
    penalty = 15
    description = "Subquery in WHERE may cause nested loop join instead of hash join"
    suggestion = "Rewrite as explicit JOIN for better optimizer choices"
    dialects = ("duckdb",)

    target_node_types = (exp.In,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if IN contains a subquery
        subquery = node.find(exp.Subquery)
        if not subquery:
            return

        # Check if we're in a WHERE clause
        where = node.find_ancestor(exp.Where)
        if not where:
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="IN (subquery) in WHERE - consider JOIN for better optimization",
            matched_text="WHERE col IN (SELECT ...)",
        )
