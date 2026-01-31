"""SQL Query Structure Parser - Full column lineage tracing for optimization."""

from dataclasses import dataclass, field
from typing import Optional
import re
import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage


@dataclass
class ColumnLineage:
    """Traces a column back to its source."""
    output_column: str
    output_table: str  # CTE name or "__output__"
    source_column: Optional[str] = None
    source_table: Optional[str] = None
    transformation: Optional[str] = None  # e.g., "SUM", "CAST", "CASE", "direct"
    expression: Optional[str] = None
    depends_on: list[str] = field(default_factory=list)  # Other columns this depends on


@dataclass
class TableNode:
    """A table or CTE in the query graph."""
    name: str
    node_type: str  # "base_table", "cte", "subquery", "output"
    alias: Optional[str] = None
    schema: Optional[str] = None
    columns_output: list[str] = field(default_factory=list)
    columns_used: list[str] = field(default_factory=list)  # Columns read from source tables
    source_tables: list[str] = field(default_factory=list)
    sql: str = ""
    source_range: Optional["SourceRange"] = None

    # Analysis hints for optimization
    has_filter: bool = False
    has_aggregation: bool = False
    has_window: bool = False
    has_distinct: bool = False
    has_order: bool = False
    has_limit: bool = False
    join_count: int = 0
    order_by: list[str] = field(default_factory=list)  # ORDER BY columns with direction

    # Estimated data flow
    row_multiplier: str = "unknown"  # "filter", "1:1", "1:N", "N:1", "N:M"


@dataclass
class JoinEdge:
    """A join relationship between tables."""
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str  # INNER, LEFT, RIGHT, FULL, CROSS
    operator: str = "="  # =, <, >, etc.
    source_range: Optional["SourceRange"] = None


@dataclass
class SourceRange:
    """Line range in the source SQL text."""
    start_line: int
    end_line: int
    start_col: Optional[int] = None
    end_col: Optional[int] = None


@dataclass
class QueryGraph:
    """Complete query structure as a graph for optimization analysis."""

    # Nodes (tables/CTEs)
    nodes: dict[str, TableNode] = field(default_factory=dict)

    # Column lineage: output_col -> [source columns]
    column_lineage: list[ColumnLineage] = field(default_factory=list)

    # Join edges
    joins: list[JoinEdge] = field(default_factory=list)

    # Data flow edges: source_table -> [target_tables]
    data_flow: dict[str, list[str]] = field(default_factory=dict)

    # CTE dependencies (source CTE -> dependent CTE)
    cte_edges: list[dict[str, str]] = field(default_factory=list)

    # Execution order (topologically sorted)
    execution_order: list[str] = field(default_factory=list)

    # Base tables (external data sources)
    base_tables: list[str] = field(default_factory=list)

    # Output columns from final SELECT
    output_columns: list[str] = field(default_factory=list)

    # Parse metadata
    dialect: str = "generic"
    parse_errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Export as JSON-serializable dict."""
        return {
            "nodes": {
                name: {
                    "name": node.name,
                    "type": node.node_type,
                    "alias": node.alias,
                    "schema": node.schema,
                    "columns_output": node.columns_output,
                    "columns_used": node.columns_used,
                    "source_tables": node.source_tables,
                    "sql": node.sql,
                    "line_range": (
                        {
                            "start_line": node.source_range.start_line,
                            "end_line": node.source_range.end_line,
                            "start_col": node.source_range.start_col,
                            "end_col": node.source_range.end_col,
                        }
                        if node.source_range
                        else None
                    ),
                    "analysis": {
                        "has_filter": node.has_filter,
                        "has_aggregation": node.has_aggregation,
                        "has_window": node.has_window,
                        "has_distinct": node.has_distinct,
                        "has_order": node.has_order,
                        "has_limit": node.has_limit,
                        "join_count": node.join_count,
                        "row_multiplier": node.row_multiplier,
                        "order_by": node.order_by,
                    }
                }
                for name, node in self.nodes.items()
            },
            "column_lineage": [
                {
                    "output": f"{cl.output_table}.{cl.output_column}",
                    "source": f"{cl.source_table}.{cl.source_column}" if cl.source_table else None,
                    "transformation": cl.transformation,
                    "expression": cl.expression,
                    "depends_on": cl.depends_on
                }
                for cl in self.column_lineage
            ],
            "joins": [
                {
                    "left": f"{j.left_table}.{j.left_column}",
                    "right": f"{j.right_table}.{j.right_column}",
                    "type": j.join_type,
                    "operator": j.operator,
                    "line_range": (
                        {
                            "start_line": j.source_range.start_line,
                            "end_line": j.source_range.end_line,
                            "start_col": j.source_range.start_col,
                            "end_col": j.source_range.end_col,
                        }
                        if j.source_range
                        else None
                    ),
                }
                for j in self.joins
            ],
            "data_flow": {
                "edges": [
                    {"from": src, "to": tgt}
                    for src, targets in self.data_flow.items()
                    for tgt in targets
                ],
                "cte_edges": self.cte_edges,
                "base_tables": self.base_tables,
                "execution_order": self.execution_order
            },
            "output_columns": self.output_columns,
            "summary": {
                "total_nodes": len(self.nodes),
                "cte_count": sum(1 for n in self.nodes.values() if n.node_type == "cte"),
                "base_table_count": len(self.base_tables),
                "join_count": len(self.joins),
                "output_column_count": len(self.output_columns),
                "max_depth": self._calculate_depth()
            },
            "parse_errors": self.parse_errors
        }

    def _calculate_depth(self) -> int:
        """Calculate maximum depth of data flow."""
        if not self.execution_order:
            return 0

        depths = {}
        for node_name in self.execution_order:
            node = self.nodes.get(node_name)
            if not node or not node.source_tables:
                depths[node_name] = 0
            else:
                max_source_depth = max(
                    (depths.get(src, 0) for src in node.source_tables),
                    default=0
                )
                depths[node_name] = max_source_depth + 1

        return max(depths.values()) if depths else 0


class SQLParser:
    """Parses SQL and builds a complete query graph with column lineage."""

    def __init__(self, dialect: str = "snowflake"):
        self.dialect = dialect
        self._cte_names: set[str] = set()
        self._table_aliases: dict[str, str] = {}  # alias -> real name
        self._source_sql: str = ""
        self._table_ranges: dict[str, SourceRange] = {}

    def parse(self, sql: str) -> QueryGraph:
        """Parse SQL and return complete query graph."""
        graph = QueryGraph(dialect=self.dialect)
        self._source_sql = sql

        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            self._table_ranges = self._collect_table_ranges(parsed, sql)

            # First pass: identify all CTEs
            self._identify_ctes(parsed)

            # Extract CTEs as nodes
            self._extract_cte_nodes(parsed, graph)

            # Extract main query
            self._extract_main_query(parsed, graph)

            # Extract subqueries (IN, EXISTS, scalar)
            self._extract_subqueries(parsed, graph)

            # Identify base tables
            self._identify_base_tables(graph)

            # Build data flow graph
            self._build_data_flow(graph)

            # Calculate execution order
            self._calculate_execution_order(graph)

            # Extract column lineage
            self._extract_column_lineage(parsed, graph)

            # Extract joins
            self._extract_all_joins(parsed, graph)

        except Exception as e:
            graph.parse_errors.append(f"Parse error: {str(e)}")
            self._fallback_parse(sql, graph)

        return graph

    def _identify_ctes(self, parsed: exp.Expression):
        """First pass to identify all CTE names."""
        self._cte_names = set()
        for cte in parsed.find_all(exp.CTE):
            if cte.alias:
                self._cte_names.add(cte.alias)

    def _extract_cte_nodes(self, parsed: exp.Expression, graph: QueryGraph):
        """Extract each CTE as a node."""
        for cte in parsed.find_all(exp.CTE):
            name = cte.alias
            select = cte.this

            node = TableNode(
                name=name,
                node_type="cte",
                sql=select.sql(dialect=self.dialect) if select else "",
                source_range=self._find_line_range(self._source_sql, cte.sql(dialect=self.dialect))
            )

            if select:
                # Extract output columns
                node.columns_output = self._extract_select_columns(select)

                # Extract source tables and columns used
                sources, columns_used = self._extract_sources_and_columns(select)
                node.source_tables = sources
                node.columns_used = columns_used

                # Analysis flags
                node.has_filter = bool(select.find(exp.Where))
                node.has_aggregation = self._has_aggregation(select)
                node.has_window = bool(select.find(exp.Window))
                node.has_distinct = bool(select.find(exp.Distinct))
                node.has_order = bool(select.find(exp.Order))
                node.has_limit = bool(select.find(exp.Limit))
                node.join_count = len(list(select.find_all(exp.Join)))
                node.order_by = self._extract_order_by(select)

                # Estimate row multiplier
                node.row_multiplier = self._estimate_row_multiplier(node)

            graph.nodes[name] = node

    def _extract_main_query(self, parsed: exp.Expression, graph: QueryGraph):
        """Extract the main SELECT as __output__ node."""
        # Find the outermost SELECT that's not in a CTE
        main_select = self._find_main_select(parsed)

        if main_select:
            node = TableNode(
                name="__output__",
                node_type="output",
                sql=main_select.sql(dialect=self.dialect),
                source_range=self._find_line_range(self._source_sql, main_select.sql(dialect=self.dialect))
            )

            node.columns_output = self._extract_select_columns(main_select)
            graph.output_columns = node.columns_output

            sources, columns_used = self._extract_sources_and_columns(main_select)
            node.source_tables = sources
            node.columns_used = columns_used

            node.has_filter = bool(main_select.find(exp.Where))
            node.has_aggregation = self._has_aggregation(main_select)
            node.has_window = bool(main_select.find(exp.Window))
            node.has_distinct = bool(main_select.find(exp.Distinct))
            node.has_order = bool(main_select.find(exp.Order))
            node.has_limit = bool(main_select.find(exp.Limit))
            node.join_count = len(list(main_select.find_all(exp.Join)))
            node.order_by = self._extract_order_by(main_select)
            node.row_multiplier = self._estimate_row_multiplier(node)

            graph.nodes["__output__"] = node

    def _extract_subqueries(self, parsed: exp.Expression, graph: QueryGraph):
        """Extract subqueries from WHERE clauses (IN, EXISTS, scalar subqueries)."""
        subquery_count = 0

        # Find all subqueries in the parsed tree
        for subquery in parsed.find_all(exp.Subquery):
            # Skip subqueries that are inside CTEs (already handled)
            if self._is_inside_cte(subquery):
                continue

            # Get the parent to determine subquery type
            parent = subquery.parent
            subquery_type = "subquery"

            if isinstance(parent, exp.In):
                subquery_type = "in_subquery"
            elif isinstance(parent, exp.Exists):
                subquery_type = "exists_subquery"
            elif isinstance(parent, (exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ)):
                subquery_type = "scalar_subquery"

            # Extract the SELECT inside the subquery
            inner_select = subquery.this
            if not isinstance(inner_select, exp.Select):
                continue

            subquery_count += 1
            name = f"__subquery_{subquery_count}__"

            node = TableNode(
                name=name,
                node_type=subquery_type,
                sql=inner_select.sql(dialect=self.dialect),
                source_range=self._find_line_range(
                    self._source_sql, subquery.sql(dialect=self.dialect)
                )
            )

            # Extract output columns
            node.columns_output = self._extract_select_columns(inner_select)

            # Extract source tables and columns used
            sources, columns_used = self._extract_sources_and_columns(inner_select)
            node.source_tables = sources
            node.columns_used = columns_used

            # Analysis flags
            node.has_filter = bool(inner_select.find(exp.Where))
            node.has_aggregation = self._has_aggregation(inner_select)
            node.has_window = bool(inner_select.find(exp.Window))
            node.has_distinct = bool(inner_select.find(exp.Distinct))
            node.join_count = len(list(inner_select.find_all(exp.Join)))
            node.row_multiplier = self._estimate_row_multiplier(node)

            graph.nodes[name] = node

    def _find_main_select(self, parsed: exp.Expression) -> Optional[exp.Select]:
        """Find the main SELECT statement (not inside a CTE)."""
        if isinstance(parsed, exp.Select):
            return parsed

        # For WITH...SELECT, traverse to find the final SELECT
        for node in parsed.walk():
            if isinstance(node, exp.Select):
                # Check if this SELECT is not inside a CTE
                if not self._is_inside_cte(node):
                    return node
        return None

    def _is_inside_cte(self, node: exp.Expression) -> bool:
        """Check if a node is inside a CTE definition."""
        parent = node.parent
        while parent:
            if isinstance(parent, exp.CTE):
                return True
            parent = parent.parent
        return False

    def _extract_select_columns(self, select: exp.Expression) -> list[str]:
        """Extract output column names from SELECT."""
        columns = []

        if hasattr(select, 'expressions'):
            for expr in select.expressions:
                if isinstance(expr, exp.Alias):
                    columns.append(expr.alias)
                elif isinstance(expr, exp.Column):
                    columns.append(expr.name)
                elif isinstance(expr, exp.Star):
                    columns.append("*")
                else:
                    # For complex expressions without alias, generate name
                    columns.append(expr.sql(dialect=self.dialect)[:50])

        return columns

    def _extract_sources_and_columns(self, select: exp.Expression) -> tuple[list[str], list[str]]:
        """Extract source tables and columns used."""
        tables = []
        columns = []

        # Get table references
        for table in select.find_all(exp.Table):
            name = table.name
            if table.alias:
                self._table_aliases[table.alias] = name
            if name not in tables:
                tables.append(name)

        # Get column references
        for col in select.find_all(exp.Column):
            table_ref = col.table
            col_name = col.name

            # Resolve alias to real table name
            if table_ref and table_ref in self._table_aliases:
                table_ref = self._table_aliases[table_ref]

            full_ref = f"{table_ref}.{col_name}" if table_ref else col_name
            if full_ref not in columns:
                columns.append(full_ref)

        return tables, columns

    def _has_aggregation(self, select: exp.Expression) -> bool:
        """Check for aggregation functions."""
        agg_types = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max,
                     exp.ArrayAgg, exp.GroupConcat, exp.Variance, exp.Stddev)
        return any(select.find_all(*agg_types)) or bool(select.find(exp.Group))

    def _extract_order_by(self, select: exp.Expression) -> list[str]:
        """Extract ORDER BY columns with direction.

        Returns list like ["col1 ASC", "col2 DESC"].
        """
        order = select.find(exp.Order)
        if not order:
            return []

        result = []
        for ordered in order.expressions:
            # Get the column expression
            col_expr = ordered.this
            col_name = col_expr.sql(dialect=self.dialect)

            # Get direction (DESC flag)
            desc = ordered.args.get("desc")
            if desc:
                result.append(f"{col_name} DESC")
            else:
                result.append(f"{col_name} ASC")

        return result

    def _estimate_row_multiplier(self, node: TableNode) -> str:
        """Estimate how this node affects row count."""
        if node.has_aggregation:
            return "N:1"  # Aggregation reduces rows
        if node.has_filter:
            return "filter"  # Filter reduces rows
        if node.has_distinct:
            return "filter"  # Distinct reduces rows
        if node.join_count > 0:
            return "1:N"  # Joins may multiply rows
        if node.has_limit:
            return "filter"
        return "1:1"  # Pass-through

    def _identify_base_tables(self, graph: QueryGraph):
        """Identify which tables are base tables (not CTEs)."""
        all_sources = set()
        for node in graph.nodes.values():
            all_sources.update(node.source_tables)

        # Base tables are sources that aren't CTEs
        graph.base_tables = [t for t in all_sources if t not in self._cte_names]

        # Add base table nodes
        for table_name in graph.base_tables:
            if table_name not in graph.nodes:
                graph.nodes[table_name] = TableNode(
                    name=table_name,
                    node_type="base_table",
                    source_range=self._table_ranges.get(table_name)
                )

    def _build_data_flow(self, graph: QueryGraph):
        """Build data flow edges between nodes."""
        for node_name, node in graph.nodes.items():
            for source in node.source_tables:
                if source not in graph.data_flow:
                    graph.data_flow[source] = []
                if node_name not in graph.data_flow[source]:
                    graph.data_flow[source].append(node_name)
                if node.node_type == "cte" and source in self._cte_names:
                    graph.cte_edges.append({"from": source, "to": node_name})

    def _calculate_execution_order(self, graph: QueryGraph):
        """Topologically sort nodes for execution order."""
        visited = set()
        order = []

        def visit(name: str):
            if name in visited:
                return
            visited.add(name)
            node = graph.nodes.get(name)
            if node:
                for source in node.source_tables:
                    visit(source)
            order.append(name)

        # Start from output and work backwards
        visit("__output__")

        # Add any disconnected nodes (shouldn't happen normally)
        for name in graph.nodes:
            visit(name)

        graph.execution_order = order

    def _extract_column_lineage(self, parsed: exp.Expression, graph: QueryGraph):
        """Extract detailed column lineage using sqlglot lineage."""
        try:
            # Build schema from our extracted info
            schema = {}
            for node_name, node in graph.nodes.items():
                if node.columns_output:
                    schema[node_name] = {col: "VARCHAR" for col in node.columns_output}

            # Get lineage for each output column
            for col_name in graph.output_columns:
                if col_name == "*":
                    continue

                try:
                    # Use sqlglot's lineage function
                    node = lineage(
                        column=col_name,
                        sql=parsed.sql(dialect=self.dialect),
                        dialect=self.dialect
                    )

                    # Walk the lineage tree
                    self._walk_lineage(node, col_name, "__output__", graph)

                except Exception:
                    # If lineage fails, create basic lineage from column references
                    self._basic_column_lineage(col_name, graph)

        except Exception as e:
            graph.parse_errors.append(f"Column lineage error: {str(e)}")

    def _walk_lineage(self, node, output_col: str, output_table: str, graph: QueryGraph):
        """Walk lineage tree and extract column dependencies."""
        if not node:
            return

        lineage_entry = ColumnLineage(
            output_column=output_col,
            output_table=output_table
        )

        # Extract source info from lineage node
        if hasattr(node, 'source') and node.source:
            lineage_entry.source_table = str(node.source.name) if hasattr(node.source, 'name') else None

        if hasattr(node, 'name'):
            lineage_entry.source_column = str(node.name)

        if hasattr(node, 'expression'):
            expr = node.expression
            lineage_entry.expression = expr.sql(dialect=self.dialect) if hasattr(expr, 'sql') else str(expr)

            # Determine transformation type
            if isinstance(expr, (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)):
                lineage_entry.transformation = type(expr).__name__.upper()
            elif isinstance(expr, exp.Cast):
                lineage_entry.transformation = "CAST"
            elif isinstance(expr, exp.Case):
                lineage_entry.transformation = "CASE"
            elif isinstance(expr, exp.Column):
                lineage_entry.transformation = "direct"
            else:
                lineage_entry.transformation = "expression"

        graph.column_lineage.append(lineage_entry)

        # Recurse into downstream (source columns)
        if hasattr(node, 'downstream') and node.downstream:
            for child in node.downstream:
                self._walk_lineage(child, output_col, output_table, graph)

    def _basic_column_lineage(self, col_name: str, graph: QueryGraph):
        """Create basic lineage when full analysis fails."""
        output_node = graph.nodes.get("__output__")
        if not output_node:
            return

        # Find matching columns in source tables
        for source_col in output_node.columns_used:
            if "." in source_col:
                table, col = source_col.rsplit(".", 1)
                if col == col_name or col_name in source_col:
                    graph.column_lineage.append(ColumnLineage(
                        output_column=col_name,
                        output_table="__output__",
                        source_column=col,
                        source_table=table,
                        transformation="direct"
                    ))

    def _extract_all_joins(self, parsed: exp.Expression, graph: QueryGraph):
        """Extract all join relationships including implicit comma-style joins."""
        # Build map of table aliases for the entire query
        for table in parsed.find_all(exp.Table):
            if table.alias:
                self._table_aliases[table.alias] = table.name

        # Collect ALL WHERE predicates that could be join conditions
        where_predicates = self._collect_all_where_predicates(parsed)

        # Track tables we've seen in order (for comma-join predicate matching)
        seen_tables = set()

        # First add the initial FROM table
        for select in parsed.find_all(exp.Select):
            from_clause = select.find(exp.From)
            if from_clause and isinstance(from_clause.this, exp.Table):
                initial_table = from_clause.this
                alias = initial_table.alias or initial_table.name
                seen_tables.add(alias)
                seen_tables.add(initial_table.name)

        for join in parsed.find_all(exp.Join):
            # SQLGlot uses 'side' for LEFT/RIGHT/FULL and 'kind' for INNER/CROSS/OUTER
            join_type = join.side or join.kind or "INNER"
            join_range = self._find_line_range(self._source_sql, join.sql(dialect=self.dialect))

            # Get right table
            right_table = None
            right_table_alias = None
            right_table_name = None
            if isinstance(join.this, exp.Table):
                right_table_alias = join.this.alias or join.this.name
                right_table_name = join.this.name
                right_table = right_table_alias
            elif isinstance(join.this, exp.Subquery):
                right_table = join.this.alias or "__subquery__"
                right_table_alias = right_table

            if join_range is None and right_table_name:
                join_range = self._find_join_range(self._source_sql, right_table_name)

            # Parse join condition
            on_clause = join.args.get("on")
            if on_clause and right_table:
                self._parse_join_condition(
                    on_clause,
                    right_table,
                    join_type,
                    graph,
                    join_range,
                    right_table_name,
                )
            elif right_table:
                # No ON clause - this is a comma-join. Look for predicate in WHERE
                # Find a predicate that links this table to a previously-seen table
                predicate = self._find_join_predicate(
                    right_table_alias, right_table_name, seen_tables, where_predicates
                )
                if predicate:
                    graph.joins.append(JoinEdge(
                        left_table=predicate["left_table"],
                        left_column=predicate["left_column"],
                        right_table=predicate["right_table"],
                        right_column=predicate["right_column"],
                        join_type="INNER",  # Comma-joins with WHERE predicates are INNER
                        operator="=",
                        source_range=predicate.get("source_range") or join_range,
                    ))
                else:
                    # True Cartesian join - no predicate found
                    graph.joins.append(JoinEdge(
                        left_table="",
                        left_column="",
                        right_table=right_table_name or right_table,
                        right_column="",
                        join_type="CROSS",  # Mark as CROSS if no predicate
                        operator="",
                        source_range=join_range,
                    ))

            # Add this table to seen tables for next iteration
            if right_table_alias:
                seen_tables.add(right_table_alias)
            if right_table_name:
                seen_tables.add(right_table_name)

    def _collect_all_where_predicates(self, parsed: exp.Expression) -> list:
        """Collect all equality predicates from WHERE that could be join conditions."""
        predicates = []

        for select in parsed.find_all(exp.Select):
            where_clause = select.find(exp.Where)
            if not where_clause:
                continue

            for eq in where_clause.find_all(exp.EQ):
                left_col = eq.left
                right_col = eq.right

                if not (isinstance(left_col, exp.Column) and isinstance(right_col, exp.Column)):
                    continue

                left_table_ref = left_col.table or ""
                right_table_ref = right_col.table or ""

                if not left_table_ref or not right_table_ref:
                    continue

                left_table = self._resolve_table_ref(left_table_ref)
                right_table = self._resolve_table_ref(right_table_ref)

                if left_table == right_table:
                    continue

                source_range = self._find_line_range(
                    self._source_sql,
                    eq.sql(dialect=self.dialect)
                )

                predicates.append({
                    "left_alias": left_table_ref,
                    "left_table": left_table,
                    "left_column": left_col.name,
                    "right_alias": right_table_ref,
                    "right_table": right_table,
                    "right_column": right_col.name,
                    "source_range": source_range,
                })

        return predicates

    def _find_join_predicate(
        self,
        table_alias: str,
        table_name: str,
        seen_tables: set,
        predicates: list
    ) -> Optional[dict]:
        """Find a join predicate linking a table to previously-seen tables.

        Returns the predicate with the table being joined on the right side.
        """
        for pred in predicates:
            # Check if this predicate involves our target table
            target_on_left = pred["left_alias"] == table_alias or pred["left_table"] == table_name
            target_on_right = pred["right_alias"] == table_alias or pred["right_table"] == table_name

            if not (target_on_left or target_on_right):
                continue

            # Check if the OTHER table in the predicate is one we've already seen
            if target_on_right:
                other_alias = pred["left_alias"]
                other_table = pred["left_table"]
                if other_alias in seen_tables or other_table in seen_tables:
                    # Return with target table on the right (natural join direction)
                    return pred
            elif target_on_left:
                other_alias = pred["right_alias"]
                other_table = pred["right_table"]
                if other_alias in seen_tables or other_table in seen_tables:
                    # Swap to put target table on the right
                    return {
                        "left_table": pred["right_table"],
                        "left_column": pred["right_column"],
                        "right_table": pred["left_table"],
                        "right_column": pred["left_column"],
                        "source_range": pred["source_range"],
                    }

        return None

    def _parse_join_condition(self, condition: exp.Expression, right_table: str,
                              join_type: str, graph: QueryGraph,
                              join_range: Optional[SourceRange],
                              right_table_name: Optional[str]):
        """Parse a join condition to extract column relationships."""
        if join_range is None and right_table_name:
            join_range = self._find_join_range(self._source_sql, right_table_name)
        # Handle simple equality: a.col = b.col
        if isinstance(condition, exp.EQ):
            left_col = condition.left
            right_col = condition.right

            if isinstance(left_col, exp.Column) and isinstance(right_col, exp.Column):
                left_table = self._resolve_table_ref(left_col.table or "")
                right_table_ref = right_col.table or right_table
                right_table_name = self._resolve_table_ref(right_table_ref)
                graph.joins.append(JoinEdge(
                    left_table=left_table,
                    left_column=left_col.name,
                    right_table=right_table_name,
                    right_column=right_col.name,
                    join_type=join_type,
                    operator="=",
                    source_range=join_range
                ))

        # Handle AND conditions (multiple join keys)
        elif isinstance(condition, exp.And):
            for child in [condition.left, condition.right]:
                self._parse_join_condition(
                    child,
                    right_table,
                    join_type,
                    graph,
                    join_range,
                    right_table_name,
                )

    def _fallback_parse(self, sql: str, graph: QueryGraph):
        """Fallback parsing when sqlglot fails."""
        # Extract CTE names
        cte_pattern = r'(\w+)\s+AS\s*\('
        for match in re.finditer(cte_pattern, sql, re.IGNORECASE):
            name = match.group(1).upper()
            if name not in ['SELECT', 'WITH', 'FROM', 'WHERE']:
                graph.nodes[name] = TableNode(
                    name=name,
                    node_type="cte",
                    source_range=self._find_line_range(sql, match.group(0))
                )
                self._cte_names.add(name)

        # Extract table references
        from_pattern = r'FROM\s+(\w+(?:\.\w+)?)'
        join_pattern = r'JOIN\s+(\w+(?:\.\w+)?)'

        tables = set()
        for pattern in [from_pattern, join_pattern]:
            for match in re.finditer(pattern, sql, re.IGNORECASE):
                tables.add(match.group(1))

        for table in tables:
            if table.upper() not in self._cte_names:
                graph.base_tables.append(table)
                graph.nodes[table] = TableNode(
                    name=table,
                    node_type="base_table",
                    source_range=self._find_identifier_range(sql, table)
                )

        graph.parse_errors.append("Used fallback regex parsing - lineage incomplete")

    def _collect_table_ranges(self, parsed: exp.Expression, sql: str) -> dict[str, SourceRange]:
        """Collect approximate line ranges for table references."""
        ranges: dict[str, SourceRange] = {}
        for table in parsed.find_all(exp.Table):
            name = table.name
            if name in ranges:
                continue
            table_range = self._find_line_range(sql, table.sql(dialect=self.dialect))
            if not table_range:
                table_range = self._find_identifier_range(sql, name)
            if table_range:
                ranges[name] = table_range
        return ranges

    def _find_join_range(self, sql: str, table_name: str) -> Optional[SourceRange]:
        """Find line range for a JOIN clause referencing a table."""
        if not table_name:
            return None
        pattern = rf"\bJOIN\s+{re.escape(table_name)}\b"
        match = re.search(pattern, sql, re.IGNORECASE)
        if not match:
            return None
        return self._range_from_match(sql, match)

    def _resolve_table_ref(self, table_ref: str) -> str:
        """Resolve table reference to base table name using aliases."""
        if not table_ref:
            return ""
        return self._table_aliases.get(table_ref, table_ref)

    def _find_identifier_range(self, sql: str, identifier: str) -> Optional[SourceRange]:
        """Find a line range for a bare identifier in SQL."""
        if not identifier:
            return None
        pattern = rf"\b{re.escape(identifier)}\b"
        match = re.search(pattern, sql, re.IGNORECASE)
        if not match:
            return None
        return self._range_from_match(sql, match)

    def _find_line_range(self, sql: str, snippet: str) -> Optional[SourceRange]:
        """Find line range for a snippet using whitespace-insensitive matching."""
        if not snippet:
            return None
        parts = [re.escape(part) for part in snippet.strip().split()]
        if not parts:
            return None
        pattern = r"\\s+".join(parts)
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        return self._range_from_match(sql, match)

    def _range_from_match(self, sql: str, match: re.Match) -> SourceRange:
        start = match.start()
        end = match.end()
        start_line = sql.count("\n", 0, start) + 1
        end_line = sql.count("\n", 0, end) + 1
        start_col = start - (sql.rfind("\n", 0, start) + 1) + 1
        end_col = end - (sql.rfind("\n", 0, end) + 1) + 1
        return SourceRange(
            start_line=start_line,
            end_line=end_line,
            start_col=start_col,
            end_col=end_col,
        )


def parse_sql(sql: str, dialect: str = "snowflake") -> dict:
    """Parse SQL and return query graph as dict.

    Args:
        sql: Raw SQL query string
        dialect: SQL dialect (snowflake, postgres, bigquery, tsql, databricks, etc.)

    Returns:
        Dictionary with complete query structure and column lineage
    """
    parser = SQLParser(dialect=dialect)
    graph = parser.parse(sql)
    return graph.to_dict()
