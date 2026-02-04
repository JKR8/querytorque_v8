"""DAG utility functions for formatting and analysis.

This module provides helper functions to format QueryDag objects into
strings suitable for DSPy prompts and LLM consumption.
"""

import logging
from typing import List, Dict
from qt_sql.optimization.dag_v2 import QueryDag

logger = logging.getLogger(__name__)


def get_topological_order(dag: QueryDag) -> List[str]:
    """Get nodes in topological order (dependencies first).

    Performs a topological sort on the DAG so that each node appears
    after all of its dependencies. This ensures CTEs are listed before
    queries that reference them.

    Args:
        dag: QueryDag to sort

    Returns:
        List of node IDs in dependency order
    """
    # Build dependency map from edges
    deps = {}
    for node_id in dag.nodes:
        deps[node_id] = []
    for src, dst in dag.edges:
        if dst not in deps:
            deps[dst] = []
        deps[dst].append(src)

    # Topological sort using DFS
    result = []
    visited = set()
    temp = set()

    def visit(node):
        if node in temp:
            return  # Cycle detected (shouldn't happen in valid SQL)
        if node in visited:
            return
        temp.add(node)
        for dep in deps.get(node, []):
            visit(dep)
        temp.discard(node)
        visited.add(node)
        result.append(node)

    for node in deps:
        if node not in visited:
            visit(node)

    logger.debug(f"Topological order: {result}")
    return result


def build_dag_structure_string(dag: QueryDag) -> str:
    """Build DAG structure string for DSPy prompts.

    Formats the DAG structure in a human-readable format suitable for
    LLM consumption. Shows nodes with their properties and edges.

    Format:
        Nodes:
          [node_id] type=cte tables=[t1,t2] refs=[ref1] CORRELATED

        Edges:
          src → dst

    Args:
        dag: QueryDag to format

    Returns:
        Formatted DAG structure string
    """
    lines = ["Nodes:"]

    for node_id in get_topological_order(dag):
        node = dag.nodes[node_id]
        parts = [f"  [{node_id}]", f"type={node.node_type}"]

        if node.tables:
            parts.append(f"tables={node.tables}")
        if node.refs:
            parts.append(f"refs={node.refs}")
        if node.flags and "CORRELATED" in node.flags:
            parts.append("CORRELATED")

        lines.append(" ".join(parts))

    lines.append("\nEdges:")
    for src, dst in dag.edges:
        lines.append(f"  {src} → {dst}")

    logger.debug(f"DAG structure: {len(dag.nodes)} nodes, {len(dag.edges)} edges")
    return "\n".join(lines)


def build_node_sql_string(dag: QueryDag) -> str:
    """Build node SQL string for DSPy prompts.

    Formats the SQL for each node in the DAG, showing the actual SQL
    text for CTEs and main query in dependency order.

    Format:
        ### node_id
        ```sql
        SELECT ...
        ```

    Args:
        dag: QueryDag to format

    Returns:
        Formatted node SQL string
    """
    parts = []
    for node_id in get_topological_order(dag):
        node = dag.nodes[node_id]
        if node.sql:
            parts.append(f"### {node_id}\n```sql\n{node.sql.strip()}\n```")

    logger.debug(f"Node SQL: {len(parts)} nodes with SQL")
    return "\n\n".join(parts)
