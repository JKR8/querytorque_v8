"""Shared DAG formatting helpers for swarm prompts."""

from __future__ import annotations

from typing import Any, Dict, List


def append_dag_summary(
    lines: List[str],
    dag: Any,
    costs: Dict[str, Any],
    include_operations: bool = False,
) -> None:
    """Append a concise DAG summary table with cost percentages.

    Args:
        lines: List to append lines to
        dag: Parsed DAG from Phase 1
        costs: Per-node cost analysis
        include_operations: If True, add a "Key Operations" column
    """
    if not hasattr(dag, "nodes") or not dag.nodes:
        lines.append("(DAG not available)")
        return

    if include_operations:
        lines.append("| Node | Role | Cost % | Key Operations |")
        lines.append("|------|------|-------:|----------------|")
    else:
        lines.append("| Node | Role | Cost % |")
        lines.append("|------|------|-------:|")

    for node in dag.nodes:
        node_id = node.id if hasattr(node, "id") else str(node)
        role = ""
        if hasattr(node, "role"):
            role = node.role
        elif hasattr(node, "node_type"):
            role = node.node_type

        cost_pct = 0.0
        if isinstance(costs, dict) and node_id in costs:
            cost_info = costs[node_id]
            if isinstance(cost_info, dict):
                cost_pct = cost_info.get("cost_pct", cost_info.get("pct", 0.0))
            elif isinstance(cost_info, (int, float)):
                cost_pct = float(cost_info)

        if include_operations:
            ops = []
            if hasattr(node, "joins") and node.joins:
                ops.append(f"{len(node.joins)} join(s)")
            if hasattr(node, "filters") and node.filters:
                ops.append(f"{len(node.filters)} filter(s)")
            if hasattr(node, "aggregations") and node.aggregations:
                ops.append("aggregation")
            ops_str = ", ".join(ops) if ops else "\u2014"
            lines.append(f"| {node_id} | {role} | {cost_pct:.1f}% | {ops_str} |")
        else:
            lines.append(f"| {node_id} | {role} | {cost_pct:.1f}% |")
