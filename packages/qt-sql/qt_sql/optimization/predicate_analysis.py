"""Multi-node predicate pushdown analysis for SQL DAG optimization.

This module traces filter predicates through DAG edges and identifies
opportunities to push them to source tables across multiple nodes.

Key concepts:
- Column lineage: Track which columns flow through each DAG edge
- Pushdown path: Route for pushing a predicate from filter_node to target_node
- Blocking operations: Aggregations, window functions that prevent pushdown

Usage:
    from qt_sql.optimization.sql_dag import SQLDag
    from qt_sql.optimization.predicate_analysis import analyze_pushdown_opportunities

    dag = SQLDag.from_sql(sql)
    analysis = analyze_pushdown_opportunities(dag)

    for path in analysis.paths:
        if path.pushable:
            print(f"Can push '{path.predicate_sql}' to {path.target_node}")
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

import sqlglot
from sqlglot import exp


@dataclass
class ColumnLineage:
    """Tracks the source of a column through the DAG.

    Attributes:
        output_column: The column name in the current node's output
        source_node: The node ID where this column originates
        source_column: The original column name in the source node
        expression: The full expression if this is a computed column
        passes_through_agg: Whether this column passes through an aggregation
        passes_through_window: Whether this column passes through a window function
        is_window_result: Whether this column IS the result of a window function
        is_agg_result: Whether this column IS the result of an aggregation
    """
    output_column: str
    source_node: str
    source_column: str
    expression: Optional[str] = None
    passes_through_agg: bool = False
    passes_through_window: bool = False
    is_window_result: bool = False
    is_agg_result: bool = False


@dataclass
class PushdownPath:
    """A path for pushing a predicate from filter_node to target_node.

    Attributes:
        filter_node: Node where predicate originates
        target_node: Node where predicate should be pushed
        column_chain: Column names through the path (may be aliased)
        predicate_sql: The original predicate SQL
        pushable: Whether it can be pushed (no aggregations in between)
        blocking_reason: Why it can't be pushed (if applicable)
        priority: Higher priority = more impact (based on scan size)
    """
    filter_node: str
    target_node: str
    column_chain: list[str]
    predicate_sql: str
    pushable: bool
    blocking_reason: Optional[str] = None
    priority: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "filter_node": self.filter_node,
            "target_node": self.target_node,
            "column_chain": self.column_chain,
            "predicate_sql": self.predicate_sql,
            "pushable": self.pushable,
            "blocking_reason": self.blocking_reason,
            "priority": self.priority,
        }


@dataclass
class PushdownAnalysis:
    """Complete pushdown analysis for a query DAG.

    Attributes:
        paths: All identified pushdown paths
        column_lineage: Mapping of node -> {output_col -> lineage info}
        nodes_with_filters: List of nodes that have WHERE/HAVING conditions
        source_nodes: List of nodes that scan base tables
    """
    paths: list[PushdownPath] = field(default_factory=list)
    column_lineage: dict[str, dict[str, ColumnLineage]] = field(default_factory=dict)
    nodes_with_filters: list[str] = field(default_factory=list)
    source_nodes: list[str] = field(default_factory=list)

    def get_pushable_paths(self) -> list[PushdownPath]:
        """Return only paths that can be pushed."""
        return [p for p in self.paths if p.pushable]

    def get_paths_for_node(self, target_node: str) -> list[PushdownPath]:
        """Get all pushdown paths targeting a specific node."""
        return [p for p in self.paths if p.target_node == target_node]

    def to_prompt_context(self) -> str:
        """Format analysis for inclusion in LLM prompts."""
        if not self.paths:
            return ""

        lines = ["## Multi-Node Predicate Pushdown Opportunities\n"]

        pushable = self.get_pushable_paths()
        if pushable:
            lines.append("### Pushable Predicates\n")
            lines.append("| From Node | To Node | Predicate | Column Path |")
            lines.append("|-----------|---------|-----------|-------------|")
            for p in sorted(pushable, key=lambda x: -x.priority):
                col_path = " → ".join(p.column_chain)
                # Truncate long predicates
                pred = p.predicate_sql[:60] + "..." if len(p.predicate_sql) > 60 else p.predicate_sql
                lines.append(f"| {p.filter_node} | {p.target_node} | `{pred}` | {col_path} |")
            lines.append("")

        blocked = [p for p in self.paths if not p.pushable]
        if blocked:
            lines.append("### Blocked Predicates (cannot push)\n")
            for p in blocked[:5]:  # Limit to 5
                lines.append(f"- `{p.predicate_sql[:50]}`: {p.blocking_reason}")
            lines.append("")

        return "\n".join(lines)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "paths": [p.to_dict() for p in self.paths],
            "nodes_with_filters": self.nodes_with_filters,
            "source_nodes": self.source_nodes,
            "pushable_count": len(self.get_pushable_paths()),
            "blocked_count": len(self.paths) - len(self.get_pushable_paths()),
        }


def analyze_pushdown_opportunities(dag: "SQLDag") -> PushdownAnalysis:
    """Analyze a DAG to find all predicate pushdown opportunities.

    Algorithm:
    1. Build column lineage map by traversing DAG edges
    2. For each node with filters, trace filter columns back to sources
    3. Check if path crosses aggregation/window boundaries (blocks pushdown)
    4. Return all valid pushdown paths

    Args:
        dag: The SQL DAG to analyze

    Returns:
        PushdownAnalysis with all discovered pushdown opportunities
    """
    analysis = PushdownAnalysis()

    if not dag.nodes:
        return analysis

    # Step 1: Build column lineage for each node
    analysis.column_lineage = _build_column_lineage(dag)

    # Step 2: Identify source nodes (nodes that scan base tables)
    analysis.source_nodes = _find_source_nodes(dag)

    # Step 3: Find nodes with filters
    analysis.nodes_with_filters = _find_nodes_with_filters(dag)

    # Step 4: For each filter, trace back to find pushdown opportunities
    for filter_node_id in analysis.nodes_with_filters:
        node = dag.nodes.get(filter_node_id)
        if not node:
            continue

        # Get predicates from this node
        predicates = _extract_predicates(node)

        for predicate, columns in predicates:
            # Try to trace each column back to source nodes
            paths = _trace_pushdown_paths(
                dag=dag,
                filter_node_id=filter_node_id,
                predicate_sql=predicate,
                predicate_columns=columns,
                column_lineage=analysis.column_lineage,
            )
            analysis.paths.extend(paths)

    # Deduplicate paths (same filter_node, target_node, predicate_sql)
    seen = set()
    unique_paths = []
    for path in analysis.paths:
        key = (path.filter_node, path.target_node, path.predicate_sql)
        if key not in seen:
            seen.add(key)
            unique_paths.append(path)
    analysis.paths = unique_paths

    return analysis


def _build_column_lineage(dag: "SQLDag") -> dict[str, dict[str, ColumnLineage]]:
    """Build column lineage map for the entire DAG.

    For each node, track where each output column comes from.

    Args:
        dag: The SQL DAG

    Returns:
        Dict of node_id -> {output_column -> ColumnLineage}
    """
    lineage: dict[str, dict[str, ColumnLineage]] = {}

    for node_id in dag.topological_order():
        node = dag.nodes.get(node_id)
        if not node:
            continue

        node_lineage: dict[str, ColumnLineage] = {}

        # Parse the node's SQL to extract column definitions
        try:
            if not node.sql:
                continue
            parsed = sqlglot.parse_one(node.sql)
        except Exception:
            continue

        # Check for aggregations and window functions
        has_agg = _has_aggregation(parsed)
        has_window = _has_window_function(parsed)

        # Extract SELECT expressions
        if isinstance(parsed, exp.Select):
            for select_expr in parsed.expressions:
                output_col, source_info = _extract_column_source(
                    select_expr, node, dag, lineage
                )
                if output_col and source_info:
                    source_info.passes_through_agg = has_agg
                    source_info.passes_through_window = has_window
                    node_lineage[output_col.lower()] = source_info

        lineage[node_id] = node_lineage

    return lineage


def _extract_column_source(
    select_expr: exp.Expression,
    node: "DagNode",
    dag: "SQLDag",
    current_lineage: dict[str, dict[str, ColumnLineage]],
) -> tuple[Optional[str], Optional[ColumnLineage]]:
    """Extract the source of a SELECT expression.

    Args:
        select_expr: A single SELECT expression
        node: Current DAG node
        dag: Full DAG
        current_lineage: Lineage built so far

    Returns:
        Tuple of (output_column_name, ColumnLineage) or (None, None)
    """
    # Get output column name
    if hasattr(select_expr, 'alias') and select_expr.alias:
        output_col = select_expr.alias
    elif isinstance(select_expr, exp.Column):
        output_col = select_expr.name
    elif isinstance(select_expr, exp.Alias):
        output_col = select_expr.alias
    else:
        return None, None

    # Check if this is a window function result
    is_window_result = False
    is_agg_result = False
    inner_expr = select_expr.this if isinstance(select_expr, exp.Alias) else select_expr
    if isinstance(inner_expr, exp.Window):
        is_window_result = True
    elif any(isinstance(inner_expr, agg) for agg in (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)):
        # Check if this aggregate is NOT in a window
        if not inner_expr.find_ancestor(exp.Window):
            is_agg_result = True

    # Find source column
    source_columns = list(select_expr.find_all(exp.Column))
    if not source_columns:
        # This is a constant or complex expression
        return output_col, ColumnLineage(
            output_column=output_col,
            source_node=node.id,
            source_column=output_col,
            expression=select_expr.sql(),
            is_window_result=is_window_result,
            is_agg_result=is_agg_result,
        )

    # Get the first column reference
    source_col = source_columns[0]
    source_name = source_col.name.lower() if source_col.name else None
    source_table = source_col.table.lower() if source_col.table else None

    if not source_name:
        return None, None

    # Check if this references a CTE or table
    if source_table:
        # Try to find in CTE refs
        for cte_ref in node.cte_refs:
            if cte_ref.lower() == source_table:
                # Trace back through CTE
                cte_lineage = current_lineage.get(cte_ref, {})
                if source_name in cte_lineage:
                    prev = cte_lineage[source_name]
                    return output_col, ColumnLineage(
                        output_column=output_col,
                        source_node=prev.source_node,
                        source_column=prev.source_column,
                        passes_through_agg=prev.passes_through_agg,
                        passes_through_window=prev.passes_through_window,
                        is_window_result=prev.is_window_result or is_window_result,
                        is_agg_result=prev.is_agg_result or is_agg_result,
                    )
                # Column from CTE but lineage not found
                return output_col, ColumnLineage(
                    output_column=output_col,
                    source_node=cte_ref,
                    source_column=source_name,
                    is_window_result=is_window_result,
                    is_agg_result=is_agg_result,
                )

    # Base table reference
    if node.tables:
        return output_col, ColumnLineage(
            output_column=output_col,
            source_node=node.id,
            source_column=source_name,
            is_window_result=is_window_result,
            is_agg_result=is_agg_result,
        )

    return output_col, ColumnLineage(
        output_column=output_col,
        source_node=node.id,
        source_column=source_name,
        is_window_result=is_window_result,
        is_agg_result=is_agg_result,
    )


def _find_source_nodes(dag: "SQLDag") -> list[str]:
    """Find nodes that scan base tables (not CTEs/subqueries)."""
    source_nodes = []
    for node_id, node in dag.nodes.items():
        if node.tables and not node.cte_refs:
            source_nodes.append(node_id)
        elif node.tables:
            # Has both tables and CTE refs - still a source for the tables
            source_nodes.append(node_id)
    return source_nodes


def _find_nodes_with_filters(dag: "SQLDag") -> list[str]:
    """Find nodes that have WHERE or HAVING conditions."""
    nodes_with_filters = []
    for node_id, node in dag.nodes.items():
        if node.filters:
            nodes_with_filters.append(node_id)
        elif node.sql:
            # Also check by parsing SQL
            try:
                parsed = sqlglot.parse_one(node.sql)
                if parsed.find(exp.Where) or parsed.find(exp.Having):
                    nodes_with_filters.append(node_id)
            except Exception:
                pass
    return nodes_with_filters


def _extract_predicates(node: "DagNode") -> list[tuple[str, list[str]]]:
    """Extract predicates and their column references from a node.

    Args:
        node: DAG node to extract predicates from

    Returns:
        List of (predicate_sql, [column_names]) tuples
    """
    predicates = []

    if not node.sql:
        return predicates

    try:
        parsed = sqlglot.parse_one(node.sql)
    except Exception:
        return predicates

    # Extract WHERE predicates
    where = parsed.find(exp.Where)
    if where:
        # Split AND conditions into individual predicates
        for pred in _split_and_conditions(where.this):
            pred_sql = pred.sql()
            columns = [col.name.lower() for col in pred.find_all(exp.Column) if col.name]
            if columns:
                predicates.append((pred_sql, columns))

    # Extract HAVING predicates (may be pushable if they reference non-aggregated columns)
    having = parsed.find(exp.Having)
    if having:
        for pred in _split_and_conditions(having.this):
            # Only include predicates that don't contain aggregations
            if not _has_aggregation(pred):
                pred_sql = pred.sql()
                columns = [col.name.lower() for col in pred.find_all(exp.Column) if col.name]
                if columns:
                    predicates.append((pred_sql, columns))

    return predicates


def _split_and_conditions(expr: exp.Expression) -> list[exp.Expression]:
    """Split AND-connected predicates into individual conditions."""
    conditions = []

    def collect(e):
        if isinstance(e, exp.And):
            collect(e.left)
            collect(e.right)
        else:
            conditions.append(e)

    collect(expr)
    return conditions


def _trace_pushdown_paths(
    dag: "SQLDag",
    filter_node_id: str,
    predicate_sql: str,
    predicate_columns: list[str],
    column_lineage: dict[str, dict[str, ColumnLineage]],
) -> list[PushdownPath]:
    """Trace a predicate back to find pushdown opportunities.

    Args:
        dag: SQL DAG
        filter_node_id: Node where the predicate exists
        predicate_sql: The SQL text of the predicate
        predicate_columns: Columns referenced in the predicate
        column_lineage: Pre-computed column lineage

    Returns:
        List of PushdownPath objects
    """
    paths = []

    # Get dependencies for this node
    dependencies = dag.get_dependencies(filter_node_id)
    if not dependencies:
        return paths

    # For each column in the predicate, trace back
    for col in predicate_columns:
        col_lower = col.lower()

        # Check each dependency
        for dep_id in dependencies:
            dep_lineage = column_lineage.get(dep_id, {})

            # Find if this column exists in the dependency
            if col_lower in dep_lineage:
                lineage = dep_lineage[col_lower]
                dep_node = dag.nodes.get(dep_id)

                # Determine if pushdown is blocked
                blocked = False
                blocking_reason = None

                # Block if column IS a window function result (cannot exist in source)
                if lineage.is_window_result:
                    blocked = True
                    blocking_reason = f"Column is a window function result in {dep_id}"

                # Block if column IS an aggregation result (cannot exist in source)
                if lineage.is_agg_result and not blocked:
                    blocked = True
                    blocking_reason = f"Column is an aggregation result in {dep_id}"

                # Check if we cross an aggregation boundary
                if lineage.passes_through_agg and not blocked:
                    # Special case: if predicate column IS the GROUP BY column, pushdown is safe
                    # Check both the filter column name AND the source column (handles aliasing)
                    is_groupby = (
                        _is_groupby_column(dep_node, col_lower) or
                        _is_groupby_column(dep_node, lineage.source_column)
                    )
                    if not is_groupby:
                        blocked = True
                        blocking_reason = f"Aggregation in {dep_id} blocks pushdown"

                # Window functions block pushdown UNLESS filter column is in PARTITION BY
                if lineage.passes_through_window:
                    # Check if column is in PARTITION BY of all window functions
                    is_partition = (
                        _is_partition_by_column(dep_node, col_lower) or
                        _is_partition_by_column(dep_node, lineage.source_column)
                    )
                    if not is_partition:
                        blocked = True
                        blocking_reason = f"Window function in {dep_id} blocks pushdown (column not in PARTITION BY)"

                # Create the pushdown path
                column_chain = [col]
                if lineage.source_column != col:
                    column_chain.append(lineage.source_column)

                target_node = lineage.source_node

                # Calculate priority based on whether target has base tables
                target = dag.nodes.get(target_node)
                priority = 10 if target and target.tables else 5

                path = PushdownPath(
                    filter_node=filter_node_id,
                    target_node=target_node,
                    column_chain=column_chain,
                    predicate_sql=predicate_sql,
                    pushable=not blocked,
                    blocking_reason=blocking_reason,
                    priority=priority,
                )
                paths.append(path)

            # Also recursively check further up the chain
            further_deps = dag.get_dependencies(dep_id)
            for further_id in further_deps:
                further_lineage = column_lineage.get(further_id, {})
                if col_lower in further_lineage:
                    f_lineage = further_lineage[col_lower]
                    further_node = dag.nodes.get(further_id)

                    blocked = False
                    blocking_reason = None

                    # Block if column IS a window/agg result
                    if f_lineage.is_window_result:
                        blocked = True
                        blocking_reason = f"Column is a window function result in {further_id}"

                    if f_lineage.is_agg_result and not blocked:
                        blocked = True
                        blocking_reason = f"Column is an aggregation result in {further_id}"

                    if f_lineage.passes_through_agg and not blocked:
                        # Check both filter column name AND source column (handles aliasing)
                        is_groupby = (
                            _is_groupby_column(further_node, col_lower) or
                            _is_groupby_column(further_node, f_lineage.source_column)
                        )
                        if not is_groupby:
                            blocked = True
                            blocking_reason = f"Aggregation in {further_id} blocks pushdown"

                    if f_lineage.passes_through_window and not blocked:
                        # Check if column is in PARTITION BY of all window functions
                        is_partition = (
                            _is_partition_by_column(further_node, col_lower) or
                            _is_partition_by_column(further_node, f_lineage.source_column)
                        )
                        if not is_partition:
                            blocked = True
                            blocking_reason = f"Window function in {further_id} blocks pushdown (column not in PARTITION BY)"

                    column_chain = [col]
                    if dep_lineage.get(col_lower) and dep_lineage[col_lower].source_column != col:
                        column_chain.append(dep_lineage[col_lower].source_column)
                    if f_lineage.source_column not in column_chain:
                        column_chain.append(f_lineage.source_column)

                    target = dag.nodes.get(f_lineage.source_node)
                    priority = 15 if target and target.tables else 5

                    path = PushdownPath(
                        filter_node=filter_node_id,
                        target_node=f_lineage.source_node,
                        column_chain=column_chain,
                        predicate_sql=predicate_sql,
                        pushable=not blocked,
                        blocking_reason=blocking_reason,
                        priority=priority,
                    )
                    paths.append(path)

    return paths


def _has_aggregation(expr: exp.Expression) -> bool:
    """Check if expression contains aggregation functions (not window aggregates).

    Window functions like SUM() OVER (...) are NOT counted as aggregations here
    because they don't reduce rows - they're handled separately.
    """
    agg_funcs = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max, exp.AggFunc)
    for node in expr.walk():
        if isinstance(node, agg_funcs):
            # Check if this aggregate is inside a window function
            parent = node.parent
            while parent:
                if isinstance(parent, exp.Window):
                    break  # This is a window aggregate, not a regular aggregate
                parent = parent.parent
            else:
                # Not inside a window - this is a regular aggregate
                return True
        # Also check for GROUP BY
        if isinstance(node, exp.Group):
            return True
    return False


def _has_window_function(expr: exp.Expression) -> bool:
    """Check if expression contains window functions."""
    for node in expr.walk():
        if isinstance(node, exp.Window):
            return True
    return False


def _is_groupby_column(node: "DagNode", column: str) -> bool:
    """Check if a column is in the GROUP BY clause of a node."""
    if not node or not node.sql:
        return False

    try:
        parsed = sqlglot.parse_one(node.sql)
        group = parsed.find(exp.Group)
        if group:
            for group_expr in group.expressions:
                if isinstance(group_expr, exp.Column):
                    if group_expr.name.lower() == column.lower():
                        return True
    except Exception:
        pass

    return False


def _is_partition_by_column(node: "DagNode", column: str) -> bool:
    """Check if a column is in ALL PARTITION BY clauses of window functions in a node.

    For pushdown to be safe, the column must be in the PARTITION BY of ALL
    window functions in the node (if there are multiple).

    Args:
        node: DAG node to check
        column: Column name to look for

    Returns:
        True if column is in PARTITION BY of all window functions
    """
    if not node or not node.sql:
        return False

    try:
        parsed = sqlglot.parse_one(node.sql)
        windows = list(parsed.find_all(exp.Window))

        if not windows:
            return False

        column_lower = column.lower()

        # Check if column is in PARTITION BY of ALL window functions
        for window in windows:
            partition_by = window.args.get('partition_by')
            if not partition_by:
                # Window has no PARTITION BY - filtering would change results
                return False

            # Check if our column is in this window's PARTITION BY
            found_in_partition = False
            for part_expr in partition_by:
                if isinstance(part_expr, exp.Column):
                    if part_expr.name.lower() == column_lower:
                        found_in_partition = True
                        break

            if not found_in_partition:
                return False

        return True

    except Exception:
        pass

    return False


def format_pushdown_table(analysis: PushdownAnalysis) -> str:
    """Format pushdown analysis as a markdown table for prompts.

    Args:
        analysis: PushdownAnalysis result

    Returns:
        Markdown-formatted table string
    """
    return analysis.to_prompt_context()
