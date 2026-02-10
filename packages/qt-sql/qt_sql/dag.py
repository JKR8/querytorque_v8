"""
Logical Tree v2 SQL Optimizer

Data structures, Logical Tree builder, cost analyzer, and rewrite assembler.
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp


# ============================================================
# Data Structures
# ============================================================

@dataclass
class NodeContract:
    """Contract for a Logical Tree node - what it promises to provide."""
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
    node_contracts: Dict[str, List[str]] = field(default_factory=dict)  # node_id -> output columns


@dataclass
class LogicalTreeNode:
    """A node in the query Logical Tree."""
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
class QueryLogicalTree:
    """Complete Logical Tree representation of a query."""
    nodes: Dict[str, LogicalTreeNode]
    edges: List[Tuple[str, str]]  # (from_node, to_node)
    original_sql: str


# ============================================================
# Logical Tree Builder - Parse SQL into Logical Tree structure
# ============================================================

class LogicalTreeBuilder:
    """Build a Logical Tree from SQL query."""

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
        self.nodes: Dict[str, LogicalTreeNode] = {}
        self.edges: List[Tuple[str, str]] = []

    def build(self) -> QueryLogicalTree:
        """Parse SQL and build Logical Tree."""
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

        return QueryLogicalTree(
            nodes=self.nodes,
            edges=self.edges,
            original_sql=self.sql
        )

    def _add_cte_node(self, cte: exp.CTE):
        """Add a CTE as a Logical Tree node."""
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

        self.nodes[node_id] = LogicalTreeNode(
            node_id=node_id,
            node_type="cte",
            sql=inner.sql(dialect=self.dialect) if inner else "",
            tables=tables,
            refs=refs,
            flags=flags
        )

    def _add_main_query(self, parsed: exp.Expression):
        """Add main SELECT as a Logical Tree node."""
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

        self.nodes["main_query"] = LogicalTreeNode(
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
                # Use the parsed node itself if it's a Select, to avoid
                # finding a nested subquery's Select via DFS
                if isinstance(parsed, exp.Select):
                    select = parsed
                else:
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
                # Regex fallback: extract AS aliases from the SELECT clause
                alias_matches = re.findall(
                    r'\bAS\s+([a-zA-Z_]\w*)', node.sql[:500], re.IGNORECASE
                )
                if alias_matches:
                    node.contract = NodeContract(
                        node_id=node_id,
                        output_columns=alias_matches[:20],
                        grain=[],
                        required_predicates=[]
                    )

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
    """Analyze execution plan and attribute costs to Logical Tree nodes."""

    def __init__(self, dag: QueryLogicalTree, plan_context: Optional[Any] = None):
        """Initialize cost analyzer.

        Args:
            dag: Query Logical Tree
            plan_context: OptimizationContext from plan_analyzer (optional)
        """
        self.dag = dag
        self.plan_context = plan_context

    def analyze(self) -> Dict[str, NodeCost]:
        """Attribute plan costs to Logical Tree nodes."""
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

    def _operator_belongs_to_node(self, op_name: str, node: LogicalTreeNode, node_scan_ops: List[str]) -> bool:
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

    def _get_heuristic_operators(self, node: LogicalTreeNode) -> List[str]:
        """Get heuristic operators when no plan available."""
        ops = []
        if "GROUP_BY" in node.flags:
            ops.append("HASH_GROUP_BY")
        if node.refs:
            ops.append("HASH_JOIN")
        if node.tables:
            ops.extend([f"SEQ_SCAN[{t}]" for t in node.tables[:3]])
        return ops

    def _has_filter(self, node: LogicalTreeNode) -> bool:
        """Check if node has WHERE filters."""
        return "WHERE" in node.sql.upper()

    def _get_join_type(self, node: LogicalTreeNode) -> Optional[str]:
        """Get join type used in node."""
        if "JOIN" in node.sql.upper():
            return "hash"
        return None


# ============================================================
# Rewrite Assembler - Apply rewrite sets to SQL
# ============================================================

class RewriteAssembler:
    """Apply rewrite sets to produce optimized SQL."""

    def __init__(self, dag: QueryLogicalTree, dialect: str = "duckdb"):
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
