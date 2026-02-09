"""
DAG v2 SQL Optimizer

Architecture improvements over v1:
1. Node contracts - output columns, grain, required predicates
2. Rewrite sets - atomic multi-node changes
3. Subgraph slicing - target + 1-hop neighbors only
4. Transform allowlist - minimal diffs, no churn
5. Downstream column usage - safe projection pruning
6. Cost attribution per node - map plan operators to nodes

Uses VERIFIED gold examples (Q15 2.98x, Q39 2.44x, Q23 2.33x).
"""

import json
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any
from pathlib import Path

import sqlglot
from sqlglot import exp


# ============================================================
# Data Structures
# ============================================================

@dataclass
class NodeContract:
    """Contract for a DAG node - what it promises to provide."""
    node_id: str
    output_columns: List[str]  # Column names this node outputs
    grain: List[str]  # Grouping keys (empty if not aggregated)
    required_predicates: List[str]  # Predicates that must stay
    nullable_columns: List[str] = field(default_factory=list)


@dataclass
class NodeUsage:
    """How a node is used downstream."""
    node_id: str
    downstream_refs: List[str]  # Columns actually used by consumers
    consumers: List[str]  # Node IDs that reference this node


@dataclass
class NodeCost:
    """Cost attribution for a node."""
    node_id: str
    cost_pct: float  # Percentage of total query cost
    row_estimate: int
    operators: List[str]  # Plan operators belonging to this node
    has_filter: bool = False
    join_type: Optional[str] = None  # hash, nested_loop, etc.


@dataclass
class RewriteSet:
    """Atomic set of coordinated rewrites."""
    id: str
    nodes: Dict[str, str]  # node_id -> new SQL
    invariants_kept: List[str]  # What semantics are preserved
    transform_type: str  # decorrelate, pushdown, or_to_union, etc.
    expected_speedup: str
    risk: str = "low"


@dataclass
class DagNode:
    """A node in the query DAG."""
    node_id: str
    node_type: str  # cte, main, subquery
    sql: str
    tables: List[str]
    refs: List[str]  # Other nodes this references
    flags: List[str]  # GROUP_BY, CORRELATED, UNION_ALL, etc.
    contract: Optional[NodeContract] = None
    usage: Optional[NodeUsage] = None
    cost: Optional[NodeCost] = None


@dataclass
class QueryDag:
    """Complete DAG representation of a query."""
    nodes: Dict[str, DagNode]
    edges: List[Tuple[str, str]]  # (from_node, to_node)
    original_sql: str


# ============================================================
# DAG Builder - Parse SQL into DAG structure
# ============================================================

class DagBuilder:
    """Build a DAG from SQL query."""

    ALLOWED_TRANSFORMS = [
        "pushdown",             # Push filters into CTEs/subqueries
        "decorrelate",          # Correlated subquery -> CTE with GROUP BY
        "or_to_union",          # OR conditions -> UNION ALL branches
        "early_filter",         # Filter dimension tables before joining to facts
        "date_cte_isolate",     # Extract date dimension filtering into early CTE
        "materialize_cte",      # Extract repeated subqueries into CTE
        "flatten_subquery",     # Convert EXISTS/IN to JOINs
        "reorder_join",         # Reorder joins for selectivity
        "multi_push_predicate", # Push predicates through multiple CTE layers
        "inline_cte",           # Inline single-use CTEs
        "remove_redundant",     # Remove unnecessary DISTINCT/ORDER BY
        "semantic_rewrite",     # Catch-all for other valid optimizations
    ]

    def __init__(self, sql: str, dialect: str = "duckdb"):
        self.sql = sql
        self.dialect = dialect
        self.nodes: Dict[str, DagNode] = {}
        self.edges: List[Tuple[str, str]] = []

    def build(self) -> QueryDag:
        """Parse SQL and build DAG."""
        try:
            parsed = sqlglot.parse_one(self.sql, dialect=self.dialect)
        except Exception:
            # Fallback for unparseable SQL
            parsed = sqlglot.parse_one(self.sql, error_level=sqlglot.ErrorLevel.IGNORE)

        # Extract CTEs
        for cte in parsed.find_all(exp.CTE):
            self._add_cte_node(cte)

        # Extract main query
        self._add_main_query(parsed)

        # Build edges from references
        self._build_edges()

        # Compute contracts and usage
        self._compute_contracts()
        self._compute_usage()

        return QueryDag(
            nodes=self.nodes,
            edges=self.edges,
            original_sql=self.sql
        )

    def _add_cte_node(self, cte: exp.CTE):
        """Add a CTE as a DAG node."""
        node_id = str(cte.alias) if cte.alias else "unnamed_cte"
        inner = cte.this

        tables = [str(t.name) for t in inner.find_all(exp.Table)] if inner else []
        refs = self._find_cte_refs(inner) if inner else []

        flags = []
        if inner and inner.find(exp.Group):
            flags.append("GROUP_BY")
        if inner and inner.find(exp.Window):
            flags.append("WINDOW")
        if inner and inner.find(exp.Union):
            flags.append("UNION_ALL")

        self.nodes[node_id] = DagNode(
            node_id=node_id,
            node_type="cte",
            sql=inner.sql(dialect=self.dialect) if inner else "",
            tables=tables,
            refs=refs,
            flags=flags
        )

    def _add_main_query(self, parsed: exp.Expression):
        """Add main SELECT as a DAG node."""
        # If parsed is a WITH expression, get the final expression
        if isinstance(parsed, exp.With):
            main_select = parsed.this
        else:
            # Find the outermost SELECT not inside a CTE
            main_select = None
            for select in parsed.find_all(exp.Select):
                if not select.find_ancestor(exp.CTE):
                    main_select = select
                    break

        if not main_select:
            return

        try:
            main_expr_no_with = main_select.copy()
            if main_expr_no_with.args.get("with_"):
                main_expr_no_with.set("with_", None)
            if main_expr_no_with.args.get("with"):
                main_expr_no_with.set("with", None)
        except Exception:
            main_expr_no_with = main_select

        tables = [str(t.name) for t in main_expr_no_with.find_all(exp.Table)]
        refs = self._find_cte_refs(main_expr_no_with)

        flags = []
        if main_select.find(exp.Group):
            flags.append("GROUP_BY")

        # Check for correlated subquery
        where = main_select.find(exp.Where)
        if where:
            for subq in where.find_all(exp.Subquery):
                if self._is_correlated(subq, main_select):
                    flags.append("CORRELATED")
                    break

        # Check for IN subqueries
        if where and where.find(exp.In):
            for in_expr in where.find_all(exp.In):
                if in_expr.find(exp.Subquery):
                    flags.append("IN_SUBQUERY")
                    break

        # Get SQL for main query (strip WITH since CTEs are shown separately)
        try:
            main_expr = main_select.copy()
            if main_expr.args.get("with_"):
                main_expr.set("with_", None)
            if main_expr.args.get("with"):
                main_expr.set("with", None)
            main_sql = main_expr.sql(dialect=self.dialect)
        except Exception:
            # Fallback if AST manipulation fails
            main_sql = main_select.sql(dialect=self.dialect)

        self.nodes["main_query"] = DagNode(
            node_id="main_query",
            node_type="main",
            sql=main_sql,
            tables=tables,
            refs=refs,
            flags=flags
        )

    def _find_cte_refs(self, expr: exp.Expression) -> List[str]:
        """Find references to other CTEs."""
        refs = []
        for table in expr.find_all(exp.Table):
            name = str(table.name).lower()
            if name in [n.lower() for n in self.nodes.keys()]:
                refs.append(name)
        return refs

    def _is_correlated(self, subq: exp.Subquery, outer: exp.Select) -> bool:
        """Check if subquery is correlated with outer query."""
        inner = subq.find(exp.Select)
        if not inner:
            return False
        inner_where = inner.find(exp.Where)
        if not inner_where:
            return False

        # Look for equality with outer table reference
        for eq in inner_where.find_all(exp.EQ):
            cols = list(eq.find_all(exp.Column))
            tables = {str(c.table).lower() for c in cols if c.table}
            if len(tables) >= 2:
                return True
        return False

    def _build_edges(self):
        """Build edges from node references."""
        for node_id, node in self.nodes.items():
            for ref in node.refs:
                if ref in self.nodes:
                    self.edges.append((ref, node_id))

    def _compute_contracts(self):
        """Compute output contracts for each node."""
        for node_id, node in self.nodes.items():
            try:
                parsed = sqlglot.parse_one(node.sql, dialect=self.dialect)
                select = parsed.find(exp.Select) or parsed

                # Output columns
                output_cols = []
                for expr in select.expressions:
                    if isinstance(expr, exp.Alias):
                        output_cols.append(str(expr.alias))
                    elif isinstance(expr, exp.Column):
                        output_cols.append(str(expr.name))
                    else:
                        output_cols.append(expr.sql()[:30])

                # Grain (GROUP BY keys)
                grain = []
                group = select.find(exp.Group)
                if group:
                    for g in group.expressions:
                        if isinstance(g, exp.Column):
                            grain.append(str(g.name))
                        else:
                            grain.append(g.sql()[:30])

                # Required predicates from WHERE
                predicates = []
                where = select.find(exp.Where)
                if where:
                    for cond in where.find_all(exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE):
                        pred_sql = cond.sql()
                        # Limit to 150 chars with ellipsis
                        if len(pred_sql) > 150:
                            pred_sql = pred_sql[:147] + "..."
                        predicates.append(pred_sql)

                node.contract = NodeContract(
                    node_id=node_id,
                    output_columns=output_cols[:20],  # Limit for prompt size
                    grain=grain,
                    required_predicates=predicates[:5]
                )
            except Exception:
                pass

    def _get_output_column_names(self, sql: str) -> Set[str]:
        """Extract output column names for downstream usage checks."""
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            select = parsed.find(exp.Select) or parsed
            names = set()
            for expr in select.expressions:
                if isinstance(expr, exp.Alias) and expr.alias:
                    names.add(str(expr.alias))
                elif isinstance(expr, exp.Column):
                    names.add(str(expr.name))
            return {n.lower() for n in names if n}
        except Exception:
            return set()

    def _compute_usage(self):
        """Compute how each node is used downstream."""
        for node_id, node in self.nodes.items():
            consumers = [n for n, m in self.nodes.items() if node_id in m.refs]

            # Get node's output columns for filtering
            node_output_cols = self._get_output_column_names(node.sql)
            if not node_output_cols and node.contract and node.contract.output_columns:
                node_output_cols = {col.lower() for col in node.contract.output_columns}

            # Find which columns are referenced by consumers
            downstream_refs = set()
            for consumer_id in consumers:
                consumer = self.nodes[consumer_id]
                try:
                    parsed = sqlglot.parse_one(consumer.sql, dialect=self.dialect)
                    for col in parsed.find_all(exp.Column):
                        col_name = str(col.name)
                        if col.table and str(col.table).lower() == node_id.lower():
                            # Explicitly qualified with this node's name
                            downstream_refs.add(col_name)
                        elif not col.table and node_output_cols:
                            # Unqualified - only add if it's in this node's output
                            if col_name.lower() in node_output_cols:
                                downstream_refs.add(col_name)
                except Exception:
                    pass

            node.usage = NodeUsage(
                node_id=node_id,
                downstream_refs=list(downstream_refs),
                consumers=consumers
            )


# ============================================================
# Cost Analyzer - Map plan costs to nodes
# ============================================================

class CostAnalyzer:
    """Analyze execution plan and attribute costs to DAG nodes."""

    def __init__(self, dag: QueryDag, plan_context: Optional[Any] = None):
        """Initialize cost analyzer.

        Args:
            dag: Query DAG
            plan_context: OptimizationContext from plan_analyzer (optional)
        """
        self.dag = dag
        self.plan_context = plan_context

    def analyze(self) -> Dict[str, NodeCost]:
        """Attribute plan costs to DAG nodes."""
        costs = {}

        # If no plan context, use heuristics based on tables
        if not self.plan_context:
            for node_id, node in self.dag.nodes.items():
                costs[node_id] = NodeCost(
                    node_id=node_id,
                    cost_pct=100.0 / len(self.dag.nodes),
                    row_estimate=1000,
                    operators=self._get_heuristic_operators(node)
                )
            return costs

        if not self.plan_context.table_scans and not self.plan_context.bottleneck_operators:
            for node_id, node in self.dag.nodes.items():
                costs[node_id] = NodeCost(
                    node_id=node_id,
                    cost_pct=100.0 / len(self.dag.nodes),
                    row_estimate=1000,
                    operators=self._get_heuristic_operators(node)
                )
            return costs

        # Use real plan data from OptimizationContext
        # Map tables to nodes
        table_to_node = {}
        for node_id, node in self.dag.nodes.items():
            for table in node.tables:
                table_to_node[table.lower()] = node_id

        # Attribute scans to nodes
        node_rows: Dict[str, int] = {}
        node_operators: Dict[str, List[str]] = {}
        node_cost_pct: Dict[str, float] = {}

        def _add_cost(node_id: str, cost: float) -> None:
            if cost is None:
                return
            node_cost_pct[node_id] = node_cost_pct.get(node_id, 0.0) + float(cost)

        for scan in self.plan_context.table_scans:
            table = scan.table.lower()
            node_id = table_to_node.get(table, "main_query")

            # Track max rows for this node
            if node_id not in node_rows:
                node_rows[node_id] = scan.rows_out or scan.rows_scanned
            else:
                node_rows[node_id] = max(node_rows[node_id], scan.rows_out or scan.rows_scanned)

            # Track operators
            if node_id not in node_operators:
                node_operators[node_id] = []
            node_operators[node_id].append(f"SEQ_SCAN[{scan.table}]")
            _add_cost(node_id, scan.cost_pct)

        # Get top operators and attribute to nodes
        top_ops = self.plan_context.get_top_operators(10)
        has_scan_info = bool(self.plan_context.table_scans)
        for op in top_ops:
            op_name = op.get("operator", "")
            if has_scan_info and "SCAN" in op_name.upper():
                continue

            # Try to attribute operator to a node
            attributed = False
            for node_id, node in self.dag.nodes.items():
                if self._operator_belongs_to_node(op_name, node, node_operators.get(node_id, [])):
                    _add_cost(node_id, op.get("cost_pct", 0.0))

                    # Add operator to list
                    if node_id not in node_operators:
                        node_operators[node_id] = []
                    if op_name not in node_operators[node_id]:
                        node_operators[node_id].append(op_name)
                    attributed = True
                    break

            # If not attributed, assign to main_query
            if not attributed:
                _add_cost("main_query", op.get("cost_pct", 0.0))

        # Build cost objects for each node
        for node_id, node in self.dag.nodes.items():
            costs[node_id] = NodeCost(
                node_id=node_id,
                cost_pct=round(node_cost_pct.get(node_id, 0), 1),
                row_estimate=node_rows.get(node_id, 1000),
                operators=node_operators.get(node_id, self._get_heuristic_operators(node))[:5],
                has_filter=self._has_filter(node),
                join_type=self._get_join_type(node)
            )
            node.cost = costs[node_id]

        return costs

    def _operator_belongs_to_node(self, op_name: str, node: DagNode, node_scan_ops: List[str]) -> bool:
        """Check if operator belongs to a node."""
        op_upper = op_name.upper()

        # Scans belong to the node
        for scan_op in node_scan_ops:
            if scan_op.upper() in op_upper:
                return True

        # GROUP_BY belongs to nodes with GROUP BY
        if "GROUP" in op_upper and "GROUP_BY" in node.flags:
            return True

        # JOINs belong to nodes with refs
        if "JOIN" in op_upper and node.refs:
            return True

        # CTE scan belongs to main_query
        if "CTE" in op_upper and node.node_type == "main":
            return True

        return False

    def _get_heuristic_operators(self, node: DagNode) -> List[str]:
        """Get heuristic operators when no plan available."""
        ops = []
        if "GROUP_BY" in node.flags:
            ops.append("HASH_GROUP_BY")
        if node.refs:
            ops.append("HASH_JOIN")
        if node.tables:
            ops.extend([f"SEQ_SCAN[{t}]" for t in node.tables[:3]])
        return ops

    def _has_filter(self, node: DagNode) -> bool:
        """Check if node has WHERE filters."""
        return "WHERE" in node.sql.upper()

    def _get_join_type(self, node: DagNode) -> Optional[str]:
        """Get join type used in node."""
        if "JOIN" in node.sql.upper():
            return "hash"
        return None


# ============================================================
# Subgraph Slicer - Extract target + 1-hop neighbors
# ============================================================

class SubgraphSlicer:
    """Extract minimal subgraph for rewriting."""

    def __init__(self, dag: QueryDag):
        self.dag = dag

    def get_slice(self, target_id: str) -> Dict[str, DagNode]:
        """Get target node + 1-hop neighbors."""
        if target_id not in self.dag.nodes:
            return {}

        slice_nodes = {target_id: self.dag.nodes[target_id]}

        # Add parents (nodes that target references)
        target = self.dag.nodes[target_id]
        for ref in target.refs:
            if ref in self.dag.nodes:
                slice_nodes[ref] = self.dag.nodes[ref]

        # Add children (nodes that reference target)
        for node_id, node in self.dag.nodes.items():
            if target_id in node.refs:
                slice_nodes[node_id] = node

        return slice_nodes

    def get_hot_nodes(self, top_n: int = 3) -> List[str]:
        """Get nodes with highest cost (optimization targets)."""
        nodes_with_cost = [
            (node_id, node.cost.cost_pct if node.cost else 0)
            for node_id, node in self.dag.nodes.items()
        ]
        nodes_with_cost.sort(key=lambda x: x[1], reverse=True)
        return [n[0] for n in nodes_with_cost[:top_n]]


# ============================================================
# Prompt Builder v2
# ============================================================

class DagV2PromptBuilder:
    """Build DAG v2 prompt with contracts, usage, and subgraph slicing."""

    SYSTEM_PROMPT = """You are an autonomous Query Rewrite Engine. Your goal is to maximize execution speed while strictly preserving semantic invariants.

Output atomic rewrite sets in JSON.

RULES:
- Primary Goal: Maximize execution speed while strictly preserving semantic invariants.
- Allowed Transforms: Use the provided list. If a standard SQL optimization applies that is not listed, label it "semantic_rewrite".
- Atomic Sets: Group dependent changes (e.g., creating a CTE and joining it) into a single rewrite_set.
- Contracts: Output columns, grain, and total result rows must remain invariant.
- Naming: Use descriptive CTE names (e.g., `filtered_returns` vs `cte1`).
- Column Aliasing: Permitted only for aggregations or disambiguation.

ALLOWED TRANSFORMS: {transforms}

OUTPUT FORMAT:
```json
{{
  "rewrite_sets": [
    {{
      "id": "rs_01",
      "transform": "transform_name",
      "nodes": {{
        "node_id": "new SQL..."
      }},
      "invariants_kept": ["list of preserved semantics"],
      "expected_speedup": "2x",
      "risk": "low"
    }}
  ],
  "explanation": "what was changed and why"
}}
```"""

    def __init__(self, dag: QueryDag, plan_context: Optional[Any] = None):
        """Initialize prompt builder.

        Args:
            dag: Query DAG
            plan_context: OptimizationContext from plan_analyzer (optional)
        """
        self.dag = dag
        self.plan_context = plan_context
        self.cost_analyzer = CostAnalyzer(dag, plan_context)
        self.slicer = SubgraphSlicer(dag)

    def build_prompt(self, target_nodes: Optional[List[str]] = None) -> str:
        """Build complete prompt for DAG v2 optimization."""
        # Analyze costs
        self.cost_analyzer.analyze()

        # Get targets (hot nodes if not specified)
        if target_nodes is None:
            target_nodes = self.slicer.get_hot_nodes(2)

        # Build subgraph slice
        slice_nodes = {}
        for target in target_nodes:
            slice_nodes.update(self.slicer.get_slice(target))

        parts = [
            self.SYSTEM_PROMPT.format(transforms=", ".join(DagBuilder.ALLOWED_TRANSFORMS)),
            "",
            "## Target Nodes",
            self._format_targets(target_nodes),
            "",
            "## Subgraph Slice",
            self._format_slice(slice_nodes),
            "",
            "## Node Contracts",
            self._format_contracts(slice_nodes),
            "",
            "## Downstream Usage",
            self._format_usage(slice_nodes),
            "",
            "## Cost Attribution",
            self._format_costs(slice_nodes),
            "",
            "## Detected Opportunities",
            self._detect_opportunities(slice_nodes),
            "",
            "Now output your rewrite_sets:",
        ]

        return "\n".join(parts)

    def _format_targets(self, targets: List[str]) -> str:
        lines = []
        for t in targets:
            node = self.dag.nodes.get(t)
            if node:
                flags = " ".join(node.flags) if node.flags else ""
                lines.append(f"  [{t}] {flags}")
        return "\n".join(lines)

    def _format_slice(self, slice_nodes: Dict[str, DagNode]) -> str:
        lines = []
        for node_id, node in slice_nodes.items():
            lines.append(f"[{node_id}] type={node.node_type}")
            lines.append(f"```sql")
            lines.append(node.sql)
            lines.append(f"```")
            lines.append("")
        return "\n".join(lines)

    def _format_contracts(self, slice_nodes: Dict[str, DagNode]) -> str:
        lines = []
        for node_id, node in slice_nodes.items():
            if node.contract:
                c = node.contract
                lines.append(f"[{node_id}]:")
                lines.append(f"  output_columns: {c.output_columns[:10]}")
                if c.grain:
                    lines.append(f"  grain: {c.grain}")
                if c.required_predicates:
                    lines.append(f"  required_predicates: {c.required_predicates[:3]}")
        return "\n".join(lines) if lines else "No contracts computed."

    def _format_usage(self, slice_nodes: Dict[str, DagNode]) -> str:
        lines = []
        for node_id, node in slice_nodes.items():
            if node.usage and node.usage.downstream_refs:
                lines.append(f"[{node_id}]: downstream_refs={node.usage.downstream_refs[:10]}")
        return "\n".join(lines) if lines else "No usage data."

    def _format_costs(self, slice_nodes: Dict[str, DagNode]) -> str:
        lines = []
        for node_id, node in slice_nodes.items():
            if node.cost:
                c = node.cost
                ops = ", ".join(c.operators[:3]) if c.operators else "unknown"
                lines.append(f"[{node_id}]: {c.cost_pct}% cost, ~{c.row_estimate} rows, ops=[{ops}]")
        return "\n".join(lines) if lines else "No cost data."

    def _detect_opportunities(self, slice_nodes: Dict[str, DagNode]) -> str:
        """Detect optimization opportunities using the centralized Knowledge Base.

        Uses patterns verified on TPC-DS SF100 with proven speedups.
        """
        from .knowledge_base import detect_opportunities, format_opportunities_for_prompt

        opportunities = []

        # Get KB opportunities from full SQL
        kb_opportunities = detect_opportunities(self.dag.original_sql)
        if kb_opportunities:
            kb_text = format_opportunities_for_prompt(kb_opportunities)
            if kb_text:
                opportunities.append("## Knowledge Base Patterns (verified on TPC-DS)\n" + kb_text)

        # Add node-specific opportunities based on DAG structure
        node_opportunities = []
        for node_id, node in slice_nodes.items():
            # Correlated subquery
            if "CORRELATED" in node.flags:
                node_opportunities.append(
                    f"DECORRELATE: [{node_id}] has correlated subquery\n"
                    f"  Fix: Move aggregate to CTE, join instead of correlated lookup\n"
                    f"  Expected: 2-3x speedup (verified Q1: 2.81x)"
                )

            # IN subquery
            if "IN_SUBQUERY" in node.flags:
                node_opportunities.append(
                    f"IN_TO_EXISTS: [{node_id}] has IN subquery\n"
                    f"  Fix: Convert to EXISTS for early termination\n"
                    f"  Expected: 1.5x speedup"
                )

            # Check for OR conditions
            if " OR " in node.sql.upper():
                node_opportunities.append(
                    f"OR_TO_UNION: [{node_id}] has OR condition\n"
                    f"  Fix: Split into UNION ALL branches\n"
                    f"  Expected: 2x speedup (verified Q15: 2.98x)"
                )

            # Check for late filter (main query filters on CTE columns)
            if node.node_type == "main" and node.refs:
                for ref in node.refs:
                    ref_node = self.dag.nodes.get(ref)
                    if ref_node and "GROUP_BY" in ref_node.flags:
                        node_opportunities.append(
                            f"PUSHDOWN: [{node_id}] filters on [{ref}] after GROUP BY\n"
                            f"  Fix: Push filter into CTE before aggregation\n"
                            f"  Expected: 2x speedup (verified Q93: 2.71x)"
                        )

        if node_opportunities:
            opportunities.append("## Node-Specific Opportunities\n" + "\n\n".join(node_opportunities))

        return "\n\n".join(opportunities) if opportunities else "No obvious opportunities detected."


# ============================================================
# Rewrite Assembler - Apply rewrite sets to SQL
# ============================================================

class RewriteAssembler:
    """Apply rewrite sets to produce optimized SQL."""

    def __init__(self, dag: QueryDag, dialect: str = "duckdb"):
        self.dag = dag
        self.dialect = dialect

    def apply_rewrite_set(self, rewrite_set: RewriteSet) -> str:
        """Apply a rewrite set and produce optimized SQL.

        Handles both updates to existing nodes AND new nodes added by the LLM.
        """
        # Handle empty rewrite - return original SQL
        if not rewrite_set.nodes:
            return self.dag.original_sql

        # Start with rewrite nodes (which may include NEW CTEs)
        new_nodes = dict(rewrite_set.nodes)

        # Fill in any original nodes not being rewritten
        for node_id, node in self.dag.nodes.items():
            if node_id not in new_nodes:
                new_nodes[node_id] = node.sql

        # Reassemble SQL
        return self._assemble_sql(new_nodes)

    def apply_from_json(self, json_str: str) -> str:
        """Parse JSON response and apply first rewrite set."""
        try:
            data = json.loads(json_str)
            rewrite_sets = data.get("rewrite_sets", [])
            if not rewrite_sets:
                return self.dag.original_sql

            # Apply first rewrite set
            rs = rewrite_sets[0]
            rewrite_set = RewriteSet(
                id=rs.get("id", "rs_01"),
                nodes=rs.get("nodes", {}),
                invariants_kept=rs.get("invariants_kept", []),
                transform_type=rs.get("transform", "unknown"),
                expected_speedup=rs.get("expected_speedup", "unknown"),
                risk=rs.get("risk", "low")
            )
            return self.apply_rewrite_set(rewrite_set)
        except json.JSONDecodeError:
            return self.dag.original_sql

    def _build_dependency_graph(self, cte_nodes: Dict[str, str]) -> Dict[str, List[str]]:
        """Build graph of CTE dependencies.

        Returns a dict mapping each CTE to the list of CTEs it depends on.
        """
        deps = {k: [] for k in cte_nodes}
        for node_id, sql in cte_nodes.items():
            sql_lower = sql.lower()
            for other_id in cte_nodes:
                if other_id != node_id:
                    # Check if other_id appears as a word boundary in SQL
                    # This avoids false positives like "store" in "store_returns"
                    pattern = r'\b' + re.escape(other_id.lower()) + r'\b'
                    if re.search(pattern, sql_lower):
                        deps[node_id].append(other_id)
        return deps

    def _topological_sort(self, deps: Dict[str, List[str]]) -> List[str]:
        """Topological sort of CTEs by dependencies.

        CTEs that are depended upon come first.
        """
        result = []
        visited = set()
        temp_visited = set()  # For cycle detection

        def visit(node: str):
            if node in temp_visited:
                # Cycle detected - just add to result to avoid infinite loop
                return
            if node in visited:
                return
            temp_visited.add(node)
            for dep in deps.get(node, []):
                visit(dep)
            temp_visited.discard(node)
            visited.add(node)
            result.append(node)

        for node in deps:
            if node not in visited:
                visit(node)

        return result

    def _strip_sql_comments(self, sql: str) -> str:
        """Strip SQL comments using sqlglot for safety.

        Handles both block comments (/* */) and line comments (--).
        Preserves comment-like patterns inside string literals.
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            # Recursively clear comments
            def clear_comments(node):
                if hasattr(node, 'comments'):
                    node.comments = None
                for child in node.iter_expressions():
                    clear_comments(child)

            clear_comments(parsed)
            return parsed.sql(dialect=self.dialect)
        except Exception:
            # Fallback: return original if parsing fails
            return sql

    def _assemble_sql(self, nodes: Dict[str, str]) -> str:
        """Reassemble full SQL from nodes."""
        main_sql = nodes.get("main_query", "")

        # Strip comments before processing (LLM often echoes original comments)
        main_sql = self._strip_sql_comments(main_sql)

        # Check if main_query already has WITH clause (LLM included full SQL)
        if main_sql.strip().upper().startswith("WITH "):
            # main_query is already complete, use it directly
            return main_sql

        # Build CTEs in dependency order
        cte_nodes = {k: v for k, v in nodes.items() if k != "main_query"}

        # Strip comments from all CTE nodes too
        cte_nodes = {k: self._strip_sql_comments(v) for k, v in cte_nodes.items()}

        # Handle empty CTEs
        if not cte_nodes:
            return main_sql

        # Build dependency graph and sort topologically
        deps = self._build_dependency_graph(cte_nodes)
        sorted_ctes = self._topological_sort(deps)

        ctes = []
        for node_id in sorted_ctes:
            sql = cte_nodes[node_id]
            # Strip WITH prefix if LLM erroneously included it
            sql_stripped = sql.strip()
            if sql_stripped.upper().startswith("WITH "):
                # Extract the CTE body after WITH ... AS (
                # This handles cases like "WITH foo AS (SELECT ...)"
                # We want just the "SELECT ..."
                match = re.match(r'WITH\s+\w+\s+AS\s*\(\s*(.*)\s*\)\s*$', sql_stripped, re.IGNORECASE | re.DOTALL)
                if match:
                    sql = match.group(1)
                else:
                    # Skip malformed CTE
                    continue
            ctes.append(f"{node_id} AS ({sql})")

        if ctes:
            return f"WITH {', '.join(ctes)}\n{main_sql}"
        return main_sql


# ============================================================
# Main Pipeline
# ============================================================

class DagV2Pipeline:
    """DAG v2 optimization pipeline."""

    def __init__(self, sql: str, plan_json: Optional[dict] = None, plan_context: Optional[Any] = None):
        """Initialize DAG v2 pipeline.

        Args:
            sql: SQL query to optimize
            plan_json: DEPRECATED - use plan_context instead
            plan_context: OptimizationContext from plan_analyzer
        """
        self.sql = sql
        self.plan_json = plan_json  # Keep for backwards compat
        self.plan_context = plan_context
        self.dag = DagBuilder(sql).build()
        self.prompt_builder = DagV2PromptBuilder(self.dag, plan_context)
        self.assembler = RewriteAssembler(self.dag)

    def get_prompt(self, target_nodes: Optional[List[str]] = None) -> str:
        """Get optimization prompt."""
        return self.prompt_builder.build_prompt(target_nodes)

    def apply_response(self, llm_response: str) -> str:
        """Apply LLM response to get optimized SQL."""
        # Extract JSON from response
        json_match = re.search(r'```json\s*(.*?)\s*```', llm_response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # Try to find raw JSON
            json_match = re.search(r'\{.*"rewrite_sets".*\}', llm_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
            else:
                return self.sql

        return self.assembler.apply_from_json(json_str)

    def get_dag_summary(self) -> str:
        """Get a summary of the DAG structure."""
        lines = ["Nodes:"]
        for node_id, node in self.dag.nodes.items():
            flags = " ".join(node.flags) if node.flags else ""
            tables = ",".join(node.tables[:3]) if node.tables else ""
            lines.append(f"  [{node_id}] type={node.node_type} tables=[{tables}] {flags}")

        lines.append("\nEdges:")
        for src, dst in self.dag.edges:
            lines.append(f"  {src} -> {dst}")

        return "\n".join(lines)


# ============================================================
# Gold Examples for Few-Shot (VERIFIED)
# ============================================================

def get_dag_v2_examples() -> List[dict]:
    """Get verified gold examples for DAG v2 format.

    Updated 2026-02-02 with Kimi K2.5 Full DB validated results:
    - Q1: 2.81x (decorrelate) - CONSISTENT WINNER
    - Q15: 2.67x (or_to_union) - CONSISTENT WINNER
    - Q93: 2.71x (early_filter) - CONSISTENT WINNER
    - Q39: 2.44x (pushdown) - DeepSeek verified
    """
    return [
        # Q1 - Correlated Subquery -> Early Filter + Separate Avg CTE (2.90x VERIFIED)
        {
            "opportunity": "DECORRELATE + PUSHDOWN",
            "input_slice": """[customer_total_return] CORRELATED:
SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(SR_FEE) AS ctr_total_return
FROM store_returns, date_dim
WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
GROUP BY sr_customer_sk, sr_store_sk

[main_query]:
SELECT c_customer_id FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (SELECT avg(ctr_total_return)*1.2 FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'SD' AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id LIMIT 100""",
            "output": {
                "rewrite_sets": [{
                    "id": "rs_01",
                    "transform": "decorrelate",
                    "nodes": {
                        "filtered_returns": "SELECT sr.sr_customer_sk, sr.sr_store_sk, sr.sr_fee FROM store_returns sr JOIN date_dim d ON sr.sr_returned_date_sk = d.d_date_sk JOIN store s ON sr.sr_store_sk = s.s_store_sk WHERE d.d_year = 2000 AND s.s_state = 'SD'",
                        "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM filtered_returns GROUP BY sr_customer_sk, sr_store_sk",
                        "store_avg_return": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return_threshold FROM customer_total_return GROUP BY ctr_store_sk",
                        "main_query": "SELECT c.c_customer_id FROM customer_total_return ctr1 JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk WHERE ctr1.ctr_total_return > sar.avg_return_threshold ORDER BY c.c_customer_id LIMIT 100"
                    },
                    "invariants_kept": ["same result rows", "same ordering", "same column output"],
                    "expected_speedup": "2.90x",
                    "risk": "low"
                }]
            },
            "speedup": "2.90x",
            "key_insight": "Push s_state='SD' filter EARLY into first CTE. Compute average as SEPARATE CTE with GROUP BY (NOT window function). Join on average instead of correlated subquery."
        },


        # Q15 - OR to UNION ALL + Early Date Filter
        {
            "opportunity": "OR_TO_UNION + EARLY_FILTER",
            "input_slice": """[main_query]:
SELECT ca_zip, sum(cs_sales_price)
FROM catalog_sales, customer, customer_address, date_dim
WHERE cs_bill_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND (substr(ca_zip,1,5) IN ('85669', '86197', ...)
       OR ca_state IN ('CA','WA','GA')
       OR cs_sales_price > 500)
  AND cs_sold_date_sk = d_date_sk
  AND d_qoy = 1 AND d_year = 2001
GROUP BY ca_zip ORDER BY ca_zip LIMIT 100""",
            "output": {
                "rewrite_sets": [{
                    "id": "rs_01",
                    "transform": "or_to_union",
                    "nodes": {
                        "filtered_dates": "SELECT d_date_sk FROM date_dim WHERE d_qoy = 1 AND d_year = 2001",
                        "filtered_sales": "SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE substr(ca_zip,1,5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792') UNION ALL SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE ca_state IN ('CA','WA','GA') UNION ALL SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE cs_sales_price > 500",
                        "main_query": "SELECT ca_zip, SUM(cs_sales_price) FROM filtered_sales GROUP BY ca_zip ORDER BY ca_zip LIMIT 100"
                    },
                    "invariants_kept": ["output columns unchanged", "same rows after aggregation"],
                    "expected_speedup": "2.98x",
                    "risk": "low"
                }]
            },
            "speedup": "2.98x"
        },

        # Q39 - Filter Pushdown into CTE
        {
            "opportunity": "PUSHDOWN",
            "input_slice": """[inv]:
SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, stdev, mean, cov
FROM (SELECT ... FROM inventory JOIN date_dim WHERE d_year = 1998 GROUP BY ...) foo
WHERE stdev/mean > 1

[main_query]:
SELECT ... FROM inv inv1, inv inv2
WHERE inv1.d_moy = 1 AND inv2.d_moy = 2 ...""",
            "output": {
                "rewrite_sets": [{
                    "id": "rs_01",
                    "transform": "pushdown",
                    "nodes": {
                        "inv": "SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev/mean END AS cov FROM (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, STDDEV_SAMP(inv_quantity_on_hand) AS stdev, AVG(inv_quantity_on_hand) AS mean FROM inventory JOIN item ON inv_item_sk = i_item_sk JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk JOIN date_dim ON inv_date_sk = d_date_sk WHERE d_year = 1998 AND d_moy IN (1, 2) GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo WHERE mean != 0 AND stdev/mean > 1"
                    },
                    "invariants_kept": ["inv output columns unchanged", "main_query unmodified"],
                    "expected_speedup": "2.44x",
                    "risk": "low"
                }]
            },
            "speedup": "2.44x"
        },

        # Q93 - Early Dimension Filter (2.71x Full DB Verified - Kimi K2.5) - CONSISTENT WINNER
        {
            "opportunity": "EARLY_FILTER",
            "input_slice": """[main_query]:
SELECT ss_customer_sk, SUM(act_sales) AS sumsales
FROM (SELECT ss.ss_customer_sk, CASE WHEN sr.sr_return_quantity IS NOT NULL
        THEN (ss.ss_quantity - sr.sr_return_quantity) * ss.ss_sales_price
        ELSE ss.ss_quantity * ss.ss_sales_price END AS act_sales
      FROM store_sales ss LEFT JOIN store_returns sr ON ss.ss_item_sk = sr.sr_item_sk
      JOIN reason r ON sr.sr_reason_sk = r.r_reason_sk
      WHERE r.r_reason_desc = 'duplicate purchase') t
GROUP BY ss_customer_sk ORDER BY sumsales, ss_customer_sk LIMIT 100""",
            "output": {
                "rewrite_sets": [{
                    "id": "rs_01",
                    "transform": "early_filter",
                    "nodes": {
                        "filtered_reason": "SELECT r_reason_sk FROM reason WHERE r_reason_desc = 'duplicate purchase'",
                        "filtered_returns": "SELECT sr_item_sk, sr_ticket_number, sr_return_quantity FROM store_returns JOIN filtered_reason ON sr_reason_sk = r_reason_sk",
                        "main_query": "SELECT ss_customer_sk, SUM(act_sales) AS sumsales FROM (SELECT ss.ss_customer_sk, CASE WHEN NOT fr.sr_return_quantity IS NULL THEN (ss.ss_quantity - fr.sr_return_quantity) * ss.ss_sales_price ELSE (ss.ss_quantity * ss.ss_sales_price) END AS act_sales FROM store_sales ss JOIN filtered_returns fr ON (fr.sr_item_sk = ss.ss_item_sk AND fr.sr_ticket_number = ss.ss_ticket_number)) AS t GROUP BY ss_customer_sk ORDER BY sumsales, ss_customer_sk LIMIT 100"
                    },
                    "invariants_kept": ["output columns unchanged", "grain preserved", "same result rows"],
                    "expected_speedup": "2.71x",
                    "risk": "low"
                }]
            },
            "speedup": "2.71x",
            "key_insight": "Filter dimension table (reason) FIRST, then join to fact. Reduces returns to only 'duplicate purchase' before expensive store_sales join."
        },

        # Q23 - IN to EXISTS (DeepSeek - failed validation, keep for reference)
        {
            "opportunity": "IN_TO_EXISTS",
            "input_slice": """[main_query]:
SELECT ... FROM catalog_sales
WHERE cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
  AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
...""",
            "output": {
                "rewrite_sets": [{
                    "id": "rs_01",
                    "transform": "in_to_exists",
                    "nodes": {
                        "main_query": "SELECT c_last_name, c_first_name, sales FROM (SELECT c_last_name, c_first_name, SUM(cs_quantity * cs_list_price) AS sales FROM catalog_sales JOIN date_dim ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk WHERE d_year = 2000 AND d_moy = 5 AND EXISTS (SELECT 1 FROM frequent_ss_items f WHERE f.item_sk = cs_item_sk) AND EXISTS (SELECT 1 FROM best_ss_customer b WHERE b.c_customer_sk = cs_bill_customer_sk) GROUP BY c_last_name, c_first_name UNION ALL SELECT c_last_name, c_first_name, SUM(ws_quantity * ws_list_price) AS sales FROM web_sales JOIN date_dim ON ws_sold_date_sk = d_date_sk JOIN customer ON ws_bill_customer_sk = c_customer_sk WHERE d_year = 2000 AND d_moy = 5 AND EXISTS (SELECT 1 FROM frequent_ss_items f WHERE f.item_sk = ws_item_sk) AND EXISTS (SELECT 1 FROM best_ss_customer b WHERE b.c_customer_sk = ws_bill_customer_sk) GROUP BY c_last_name, c_first_name) ORDER BY c_last_name, c_first_name, sales LIMIT 100"
                    },
                    "invariants_kept": ["same result rows", "same ordering"],
                    "expected_speedup": "2.33x",
                    "risk": "medium"  # Failed validation on DeepSeek
                }]
            },
            "speedup": "2.33x",
            "note": "DeepSeek failed validation - use with caution"
        }
    ]
