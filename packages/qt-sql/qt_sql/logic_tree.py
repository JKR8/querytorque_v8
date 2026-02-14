"""Logic Tree generator — DAP Part 1 pre-fill from AST + cost data.

Auto-generates a Logic Tree text from any SQL query using the existing
query-structure/AST infrastructure. All nodes are marked [=] (original state) so the
LLM sees the current structure as context in exactly the format it should
output (with change markers).

Public API
----------
- build_logic_tree()          — single-query Logic Tree
- build_pipeline_logic_tree() — multi-statement pipeline Logic Tree
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# DAP operation vocabulary (from sql_rewrite_spec.md §3)
_OP_VOCAB = {
    "filter": "FILTER",
    "join": "JOIN",
    "scan": "SCAN",
    "agg": "AGG",
    "union": "UNION",
    "window": "WINDOW",
    "sort": "SORT",
    "dedup": "DEDUP",
    "case_map": "CASE_MAP",
    "compute": "COMPUTE",
}


def _fmt_rows(n: int) -> str:
    """Format row count: 557705 → '~557K', 28000000 → '~28M'."""
    if n >= 1_000_000:
        val = n / 1_000_000
        return f"~{val:.1f}M" if val != int(val) else f"~{int(val)}M"
    if n >= 1_000:
        return f"~{n // 1_000}K"
    return f"~{n}"


def _fmt_cost(pct: float) -> str:
    """Format cost percentage."""
    return f"Cost: {pct:.0f}%"


def _map_operations(meta: Dict[str, Any], flags: List[str]) -> List[str]:
    """Map extracted node metadata to DAP operation vocabulary strings.

    Renders explicit SUBQUERY nodes with type, inner tables, filters,
    and correlation predicates instead of flat 'correlated subquery' labels.
    """
    ops: List[str] = []

    # SCAN — base table references (subquery-internal tables excluded by analyst.py)
    deps = meta.get("dependencies", [])
    tables = [d for d in deps if not d.startswith("(")]
    if tables:
        for t in tables:
            ops.append(f"SCAN {t}")

    # JOIN
    joins = meta.get("joins", [])
    if joins:
        for j in joins[:2]:
            ops.append(f"JOIN ({j})")
        if len(joins) > 2:
            ops.append(f"JOIN (+{len(joins) - 2} more)")

    # FILTER (outer-level only)
    filters = meta.get("filters", [])
    if filters:
        for f in filters[:2]:
            ops.append(f"FILTER ({f})")
        if len(filters) > 2:
            ops.append(f"FILTER (+{len(filters) - 2} more)")

    # SUBQUERY nodes — explicit type + inner structure
    subqueries = meta.get("subqueries", [])
    # Deduplicate subqueries with same tables+filters (e.g. Q9 has 15 subqueries
    # but only 5 distinct bucket patterns)
    seen_sigs: Dict[str, int] = {}
    for sq in subqueries:
        sig = f"{','.join(sq['tables'])}|{','.join(sq['filters'][:2])}"
        seen_sigs[sig] = seen_sigs.get(sig, 0) + 1

    emitted_sigs: Dict[str, bool] = {}
    for sq in subqueries:
        sig = f"{','.join(sq['tables'])}|{','.join(sq['filters'][:2])}"
        if sig in emitted_sigs:
            continue
        emitted_sigs[sig] = True

        count = seen_sigs[sig]
        sq_type = sq["type"]
        if sq["correlated"]:
            sq_type = f"correlated {sq_type}"

        label = f"SUBQUERY ({sq_type})"
        if count > 1:
            label += f" x{count}"

        # Build child lines for this subquery
        children: List[str] = []
        for tbl in sq["tables"]:
            children.append(f"SCAN {tbl}")
        for filt in sq["filters"][:2]:
            children.append(f"FILTER {filt}")
        if sq["corr_pred"]:
            children.append(f"CORR-PRED: {sq['corr_pred']}")

        ops.append({"label": label, "children": children})

    # AGG
    if "GROUP_BY" in flags:
        ops.append("AGG (GROUP BY)")

    # UNION
    if "UNION_ALL" in flags:
        ops.append("UNION")

    # WINDOW
    if "WINDOW" in flags:
        ops.append("WINDOW")

    # SORT
    if meta.get("order_by"):
        ops.append(f"SORT ({meta['order_by']})")

    return ops


def _node_type_prefix(node_type: str) -> str:
    """Map node_type to DAP node type prefix."""
    mapping = {
        "cte": "[CTE]",
        "main": "[MAIN]",
        "subquery": "[SUB]",
    }
    return mapping.get(node_type, f"[{node_type.upper()}]")


def build_logic_tree(
    sql: str,
    dag: Any,  # Query structure from dag.py
    costs: Dict[str, Any],
    dialect: str = "duckdb",
    node_intents: Optional[Dict[str, str]] = None,
) -> str:
    """Build a Logic Tree text from a single query's structure + cost data.

    All nodes are marked [=] (original, unchanged).  The tree uses
    box-drawing characters for visual structure per DAP spec.

    Args:
        sql: Original SQL text (for context, not parsed here)
        dag: Query structure from builder.build()
        costs: Dict of {node_id: NodeCost} from CostAnalyzer
        dialect: SQL dialect
        node_intents: Optional {node_id: intent_string} for semantic labels

    Returns:
        Indented tree text string
    """
    from .prompter import compute_depths
    from .analyst import _extract_node_metadata

    if node_intents is None:
        node_intents = {}

    depths = compute_depths(dag)
    if not depths:
        return "QUERY: (empty)\n└── [MAIN] SELECT  [=]"

    # Build ordered list of (node_id, depth) sorted by depth then name
    ordered = sorted(depths.items(), key=lambda x: (x[1], x[0]))

    # Separate root (main_query, depth=max) from CTEs/subqueries
    max_depth = max(d for _, d in ordered)

    # Group by: CTEs first (sorted by depth), then main_query last
    cte_nodes = [(nid, d) for nid, d in ordered if nid != "main_query"]
    main_node = [nid for nid, _ in ordered if nid == "main_query"]

    all_nodes = cte_nodes + [(mid, max_depth) for mid in main_node]
    total = len(all_nodes)

    lines: List[str] = []
    lines.append("QUERY: (single statement)")

    for idx, (nid, _depth) in enumerate(all_nodes):
        is_last = idx == total - 1
        connector = "└──" if is_last else "├──"
        continuation = "    " if is_last else "│   "

        node = dag.nodes.get(nid)
        if not node:
            continue

        # Type prefix
        prefix = _node_type_prefix(node.node_type)

        # Cost info
        cost_obj = costs.get(nid)
        cost_pct = cost_obj.cost_pct if cost_obj and hasattr(cost_obj, "cost_pct") else 0
        row_est = cost_obj.row_estimate if cost_obj and hasattr(cost_obj, "row_estimate") else 0

        cost_str = _fmt_cost(cost_pct)

        # Check if node has subqueries (row_est reflects subquery scans, not output)
        base_flags = node.flags if hasattr(node, "flags") and node.flags else []
        meta = _extract_node_metadata(
            node.sql if hasattr(node, "sql") else "", dialect
        )
        has_subqueries = bool(meta.get("subqueries"))

        if row_est and has_subqueries and node.node_type == "main":
            rows_str = f"Processes: {_fmt_rows(row_est)} across subqueries"
        elif row_est:
            rows_str = f"Rows: {_fmt_rows(row_est)}"
        else:
            rows_str = ""

        # Build stats suffix
        stats_parts = [cost_str]
        if rows_str:
            stats_parts.append(rows_str)
        stats = "  ".join(stats_parts)

        # Intent label (if available)
        intent = node_intents.get(nid, "")
        intent_str = f"  — {intent}" if intent else ""

        # Header line
        lines.append(f"{connector} {prefix} {nid}  [=]  {stats}{intent_str}")

        # Operation lines (children of this node) — reuse meta from above
        operations = _map_operations(meta, base_flags)

        # Output columns
        out_cols = []
        if hasattr(node, "contract") and node.contract and node.contract.output_columns:
            out_cols = node.contract.output_columns[:8]

        # Render operations as child lines
        op_items = operations
        if out_cols:
            col_str = ", ".join(out_cols)
            if hasattr(node, "contract") and node.contract and len(node.contract.output_columns) > 8:
                col_str += ", ..."
            op_items = op_items + [f"OUTPUT ({col_str})"]

        for oi, op in enumerate(op_items):
            op_last = oi == len(op_items) - 1
            op_conn = "└──" if op_last else "├──"
            op_cont = "    " if op_last else "│   "

            if isinstance(op, dict):
                # Structured subquery node with children
                lines.append(f"{continuation}{op_conn} {op['label']}")
                children = op.get("children", [])
                for ci, child in enumerate(children):
                    child_last = ci == len(children) - 1
                    child_conn = "└──" if child_last else "├──"
                    lines.append(f"{continuation}{op_cont}{child_conn} {child}")
            else:
                lines.append(f"{continuation}{op_conn} {op}")

    return "\n".join(lines)


def build_pipeline_logic_tree(
    script_dag: Any,  # ScriptDAG from script_parser.py
    per_stmt_dags: Dict[str, Any],  # {target_table: query structure}
    per_stmt_costs: Dict[str, Dict],  # {target_table: {node_id: NodeCost}}
    dialect: str = "duckdb",
) -> str:
    """Build a multi-statement pipeline Logic Tree.

    Args:
        script_dag: ScriptDAG with statement ordering
        per_stmt_dags: Query structure per statement target table
        per_stmt_costs: Cost data per statement
        dialect: SQL dialect

    Returns:
        Pipeline Logic Tree text
    """
    from .analyst import _extract_node_metadata
    from .prompter import compute_depths

    lines: List[str] = []
    lines.append("PIPELINE: (multi-statement)")

    stmts = script_dag.optimization_targets()
    total_stmts = len(stmts)

    for si, stmt in enumerate(stmts):
        is_last_stmt = si == total_stmts - 1
        stmt_conn = "└──" if is_last_stmt else "├──"
        stmt_cont = "    " if is_last_stmt else "│   "

        target = getattr(stmt, "target_table", None) or f"stmt_{si}"
        lines.append(f"{stmt_conn} [STMT] {target}  [=]")

        # Get per-statement structure and costs
        dag = per_stmt_dags.get(target)
        costs = per_stmt_costs.get(target, {})
        if not dag:
            continue

        depths = compute_depths(dag)
        ordered = sorted(depths.items(), key=lambda x: (x[1], x[0]))
        cte_nodes = [(nid, d) for nid, d in ordered if nid != "main_query"]
        main_node = [nid for nid, _ in ordered if nid == "main_query"]
        all_nodes = cte_nodes + [(mid, 0) for mid in main_node]
        total_nodes = len(all_nodes)

        for ni, (nid, _) in enumerate(all_nodes):
            is_last_node = ni == total_nodes - 1
            node_conn = "└──" if is_last_node else "├──"
            node_cont = "    " if is_last_node else "│   "

            node = dag.nodes.get(nid)
            if not node:
                continue

            prefix = _node_type_prefix(node.node_type)
            cost_obj = costs.get(nid)
            cost_pct = cost_obj.cost_pct if cost_obj and hasattr(cost_obj, "cost_pct") else 0
            cost_str = _fmt_cost(cost_pct)

            lines.append(f"{stmt_cont}{node_conn} {prefix} {nid}  [=]  {cost_str}")

            # Operations
            base_flags = node.flags if hasattr(node, "flags") and node.flags else []
            meta = _extract_node_metadata(
                node.sql if hasattr(node, "sql") else "", dialect
            )
            operations = _map_operations(meta, base_flags)

            for oi, op in enumerate(operations):
                op_last = oi == len(operations) - 1
                op_conn = "└──" if op_last else "├──"
                op_cont_inner = "    " if op_last else "│   "
                if isinstance(op, dict):
                    lines.append(f"{stmt_cont}{node_cont}{op_conn} {op['label']}")
                    children = op.get("children", [])
                    for ci, child in enumerate(children):
                        child_last = ci == len(children) - 1
                        child_conn = "└──" if child_last else "├──"
                        lines.append(f"{stmt_cont}{node_cont}{op_cont_inner}{child_conn} {child}")
                else:
                    lines.append(f"{stmt_cont}{node_cont}{op_conn} {op}")

    return "\n".join(lines)
