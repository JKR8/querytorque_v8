"""SQL DAG (Directed Acyclic Graph) for query optimization.

Parses SQL into a proper DAG structure where:
- Nodes = query scopes (CTEs, subqueries, main query)
- Edges = data dependencies (which node reads from which)

This enables:
1. Targeted node-level rewrites (not full SQL replacement)
2. Dependency-aware optimization ordering
3. Visibility into correlated subqueries
4. Clean LLM output format (rewrite specific nodes)

Usage:
    dag = SQLDag.from_sql(sql)
    print(dag.to_prompt())  # For LLM input

    # Apply rewrites
    rewrites = {"cte_name": "SELECT ...", "subquery_1": "SELECT ..."}
    new_sql = dag.apply_rewrites(rewrites)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import build_scope, traverse_scope, Scope, ScopeType


@dataclass
class DagNode:
    """A node in the SQL DAG representing a query scope."""

    id: str  # Unique identifier (cte name, "main_query", "subquery_N")
    node_type: str  # "cte", "main_query", "subquery", "union_branch"
    tables: list[str] = field(default_factory=list)  # Direct table references
    cte_refs: list[str] = field(default_factory=list)  # CTE references
    sql: str = ""  # The SQL for this node (SELECT statement)
    parent_id: Optional[str] = None  # Parent node (for subqueries)
    is_correlated: bool = False  # References columns from outer scope
    columns_out: list[str] = field(default_factory=list)  # Output columns
    filters: list[str] = field(default_factory=list)  # WHERE/HAVING conditions

    def __hash__(self):
        return hash(self.id)


@dataclass
class DagEdge:
    """An edge in the SQL DAG representing data flow."""

    source: str  # Node ID that provides data
    target: str  # Node ID that consumes data
    edge_type: str = "ref"  # "ref" (CTE ref), "table", "correlated"


@dataclass
class SQLDag:
    """Directed Acyclic Graph representation of a SQL query."""

    nodes: dict[str, DagNode] = field(default_factory=dict)
    edges: list[DagEdge] = field(default_factory=list)
    original_sql: str = ""
    _parsed: Optional[exp.Expression] = field(default=None, repr=False)
    _node_to_scope: dict[str, Scope] = field(default_factory=dict, repr=False)

    @classmethod
    def from_sql(cls, sql: str) -> "SQLDag":
        """Build a DAG from SQL query string."""
        dag = cls(original_sql=sql)

        try:
            parsed = sqlglot.parse_one(sql)
            dag._parsed = parsed
        except Exception as e:
            # Return empty DAG on parse error
            return dag

        # Track subquery counter for stable IDs
        subquery_count = 0
        union_branch_count = 0

        # First pass: collect CTE names
        cte_names = set()
        for cte in parsed.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias.lower() if isinstance(cte.alias, str) else cte.alias)

        # Traverse all scopes to build nodes
        for scope in traverse_scope(parsed):
            node_id = None
            node_type = None
            parent_id = None

            if scope.scope_type == ScopeType.CTE:
                # Find the CTE name
                for cte in parsed.find_all(exp.CTE):
                    if cte.this == scope.expression:
                        node_id = cte.alias
                        break
                if not node_id:
                    continue
                node_type = "cte"

            elif scope.scope_type == ScopeType.ROOT:
                # Check if main query is a UNION
                if isinstance(scope.expression, exp.Union):
                    # Handle UNION - create nodes for each branch
                    branches = _extract_union_branches(scope.expression)
                    for i, branch in enumerate(branches):
                        branch_id = f"main_query.union[{i}]"
                        branch_node = _build_node_from_select(
                            branch, branch_id, "union_branch", cte_names
                        )
                        branch_node.parent_id = "main_query"
                        dag.nodes[branch_id] = branch_node
                        dag._node_to_scope[branch_id] = scope

                        # Add edges for this branch
                        for ref in branch_node.cte_refs:
                            dag.edges.append(DagEdge(source=ref, target=branch_id, edge_type="ref"))

                    # Create a virtual main_query node
                    node_id = "main_query"
                    node_type = "union"
                    main_node = DagNode(
                        id=node_id,
                        node_type=node_type,
                        sql=scope.expression.sql(pretty=True),
                    )
                    dag.nodes[node_id] = main_node
                    dag._node_to_scope[node_id] = scope
                    continue
                else:
                    node_id = "main_query"
                    node_type = "main_query"

            elif scope.scope_type == ScopeType.SUBQUERY:
                subquery_count += 1
                node_id = f"subquery_{subquery_count}"
                node_type = "subquery"

                # Find parent scope
                if scope.parent:
                    for existing_id, existing_scope in dag._node_to_scope.items():
                        if existing_scope == scope.parent:
                            parent_id = existing_id
                            break
                    if not parent_id:
                        parent_id = "main_query"

            elif scope.scope_type == ScopeType.DERIVED_TABLE:
                subquery_count += 1
                node_id = f"derived_{subquery_count}"
                node_type = "derived_table"

            else:
                continue

            if not node_id:
                continue

            # Build the node
            node = _build_node_from_scope(scope, node_id, node_type, cte_names, parsed)
            node.parent_id = parent_id

            # Check for correlated references
            node.is_correlated = _is_correlated_scope(scope)

            dag.nodes[node_id] = node
            dag._node_to_scope[node_id] = scope

            # Add edges for CTE references
            for ref in node.cte_refs:
                dag.edges.append(DagEdge(source=ref, target=node_id, edge_type="ref"))

            # Add edges for correlated references
            if node.is_correlated and parent_id:
                dag.edges.append(DagEdge(source=parent_id, target=node_id, edge_type="correlated"))

        return dag

    def get_node(self, node_id: str) -> Optional[DagNode]:
        """Get a node by ID."""
        return self.nodes.get(node_id)

    def get_dependencies(self, node_id: str) -> list[str]:
        """Get IDs of nodes that this node depends on."""
        return [e.source for e in self.edges if e.target == node_id]

    def get_dependents(self, node_id: str) -> list[str]:
        """Get IDs of nodes that depend on this node."""
        return [e.target for e in self.edges if e.source == node_id]

    def topological_order(self) -> list[str]:
        """Return node IDs in topological order (dependencies first)."""
        visited = set()
        result = []

        def visit(node_id):
            if node_id in visited:
                return
            visited.add(node_id)
            for dep in self.get_dependencies(node_id):
                visit(dep)
            result.append(node_id)

        for node_id in self.nodes:
            visit(node_id)

        return result

    def to_prompt(self, include_sql: bool = True, plan_summary: Optional[dict] = None) -> str:
        """Format DAG for LLM prompt input."""
        lines = []

        # Execution plan (most important signal)
        if plan_summary:
            lines.append("## Execution Plan\n")
            if "top_operators" in plan_summary:
                lines.append("**Operators by cost:**")
                for op in plan_summary["top_operators"][:5]:
                    table_info = f" ({op['table']})" if op.get('table') and op['table'] != '-' else ""
                    lines.append(f"- {op['op']}{table_info}: {op['cost_pct']}% cost, "
                               f"{op.get('rows_scanned', 0) or op.get('rows_out', 0):,} rows")
                lines.append("")

            if "scans" in plan_summary:
                lines.append("**Table scans:**")
                for scan in plan_summary["scans"]:
                    table = scan.get("table", "")
                    if table.startswith("CTE") or table.startswith("COLUMN") or not table:
                        continue
                    rows = scan.get("rows", 0)
                    if scan.get("has_filter"):
                        lines.append(f"- {table}: {rows:,} rows (FILTERED)")
                    else:
                        lines.append(f"- {table}: {rows:,} rows (NO FILTER)")
                lines.append("")
            lines.append("---\n")

        # DAG structure
        lines.append("## Query DAG\n")
        lines.append("```")
        lines.append("Nodes:")

        for node_id in self.topological_order():
            node = self.nodes[node_id]
            parts = [f"  [{node_id}]"]
            parts.append(f"type={node.node_type}")
            if node.tables:
                parts.append(f"tables={node.tables}")
            if node.cte_refs:
                parts.append(f"refs={node.cte_refs}")
            if node.is_correlated:
                parts.append("CORRELATED")
            lines.append(" ".join(parts))

        lines.append("\nEdges:")
        for edge in self.edges:
            edge_label = f" ({edge.edge_type})" if edge.edge_type != "ref" else ""
            lines.append(f"  {edge.source} → {edge.target}{edge_label}")

        lines.append("```\n")

        # Node SQL (if requested)
        if include_sql:
            lines.append("## Node SQL\n")
            for node_id in self.topological_order():
                node = self.nodes[node_id]
                if node.sql:
                    lines.append(f"### {node_id}")
                    lines.append("```sql")
                    lines.append(node.sql.strip())
                    lines.append("```\n")

        return "\n".join(lines)

    def apply_rewrites(self, rewrites: dict[str, str]) -> str:
        """Apply node-level rewrites and return new SQL.

        Args:
            rewrites: Dict of {node_id: new_sql} for nodes to rewrite

        Returns:
            Complete rewritten SQL query
        """
        if not self._parsed:
            return self.original_sql

        # Clone the parsed tree
        result = self._parsed.copy()

        for node_id, new_sql in rewrites.items():
            node = self.nodes.get(node_id)
            if not node:
                continue

            try:
                new_expr = sqlglot.parse_one(new_sql)
            except Exception:
                continue

            if node.node_type == "cte":
                # Replace CTE body
                for cte in result.find_all(exp.CTE):
                    if cte.alias and cte.alias.lower() == node_id.lower():
                        cte.set("this", new_expr)
                        break

            elif node.node_type == "main_query":
                # Replace main query (preserve WITH clause)
                with_clause = result.find(exp.With)
                if with_clause:
                    new_expr.set("with", with_clause)
                result = new_expr

            elif node.node_type == "subquery":
                # Find and replace the subquery
                scope = self._node_to_scope.get(node_id)
                if scope and scope.expression:
                    # Find the Subquery wrapper in the tree
                    for subq in result.find_all(exp.Subquery):
                        if subq.this == scope.expression or _sql_matches(subq.this, scope.expression):
                            subq.set("this", new_expr)
                            break

            elif node.node_type == "union_branch":
                # Handle union branch rewrites
                match = re.match(r'main_query\.union\[(\d+)\]', node_id)
                if match:
                    branch_idx = int(match.group(1))
                    _replace_union_branch(result, branch_idx, new_expr)

        return result.sql(pretty=True)


def _build_node_from_scope(scope: Scope, node_id: str, node_type: str,
                           cte_names: set[str], parsed: exp.Expression = None) -> DagNode:
    """Build a DagNode from a sqlglot Scope."""
    tables = []
    cte_refs_set = set()  # Use set to deduplicate
    cte_names_lower = {c.lower() for c in cte_names}

    for alias, source in scope.sources.items():
        if isinstance(source, exp.Table):
            table_name = source.name
            if table_name and table_name.lower() not in cte_names_lower:
                if table_name not in tables:
                    tables.append(table_name)
            elif table_name and table_name.lower() in cte_names_lower:
                # This is a CTE reference
                cte_refs_set.add(table_name)
        elif isinstance(source, Scope):
            # This is a reference to a CTE - find the actual CTE name
            # by matching the source scope's expression to a CTE definition
            if parsed:
                for cte in parsed.find_all(exp.CTE):
                    if cte.this == source.expression:
                        cte_refs_set.add(cte.alias)
                        break

    cte_refs = list(cte_refs_set)

    # Get SQL for this node
    # For main query (ROOT scope), strip the WITH clause to show just the SELECT
    if scope.expression:
        expr_to_render = scope.expression
        if node_type == "main_query" and hasattr(scope.expression, 'this'):
            # For a query with WITH clause, the structure is:
            # Select/Union with .args['with'] containing the WITH clause
            # We want to render just the SELECT without WITH
            expr_copy = scope.expression.copy()
            if 'with' in expr_copy.args:
                expr_copy.set('with', None)
            sql = expr_copy.sql(pretty=True)
        else:
            sql = scope.expression.sql(pretty=True)
    else:
        sql = ""

    # Extract output columns
    columns_out = []
    if isinstance(scope.expression, exp.Select):
        for expr in scope.expression.expressions[:10]:  # Limit to first 10
            if hasattr(expr, 'alias') and expr.alias:
                columns_out.append(expr.alias)
            elif isinstance(expr, exp.Column):
                columns_out.append(expr.name)

    # Extract filters
    filters = []
    if isinstance(scope.expression, exp.Select):
        where = scope.expression.find(exp.Where)
        if where:
            filters.append(str(where.this)[:100])

    return DagNode(
        id=node_id,
        node_type=node_type,
        tables=tables,
        cte_refs=cte_refs,
        sql=sql,
        columns_out=columns_out,
        filters=filters,
    )


def _build_node_from_select(select_expr: exp.Expression, node_id: str,
                            node_type: str, cte_names: set[str]) -> DagNode:
    """Build a DagNode from a SELECT expression (for union branches)."""
    tables = []
    cte_refs = []

    for table in select_expr.find_all(exp.Table):
        table_name = table.name
        if table_name:
            if table_name.lower() in {c.lower() for c in cte_names}:
                cte_refs.append(table_name)
            else:
                tables.append(table_name)

    sql = select_expr.sql(pretty=True)

    return DagNode(
        id=node_id,
        node_type=node_type,
        tables=tables,
        cte_refs=cte_refs,
        sql=sql,
    )


def _is_correlated_scope(scope: Scope) -> bool:
    """Check if a scope has correlated references to outer scope."""
    if not scope.parent:
        return False

    # Get columns referenced in this scope
    for col in scope.expression.find_all(exp.Column):
        table = col.table
        if table:
            # Check if this table is from the parent scope
            parent_sources = {alias.lower() for alias in scope.parent.sources.keys()}
            if table.lower() in parent_sources:
                return True

    return False


def _extract_union_branches(union: exp.Union) -> list[exp.Expression]:
    """Extract all branches from a UNION."""
    branches = []

    def collect(node):
        if isinstance(node, exp.Union):
            collect(node.left)
            collect(node.right)
        else:
            branches.append(node)

    collect(union)
    return branches


def _replace_union_branch(parsed: exp.Expression, branch_idx: int,
                          new_expr: exp.Expression) -> None:
    """Replace a specific branch in a UNION."""
    branches = []

    # Find the main query (could be inside WITH)
    main = parsed
    for node in parsed.walk():
        if isinstance(node, exp.Union):
            parent = node.parent
            in_cte = False
            while parent:
                if isinstance(parent, exp.CTE):
                    in_cte = True
                    break
                parent = parent.parent
            if not in_cte:
                main = node
                break

    if not isinstance(main, exp.Union):
        return

    # Collect branches
    def collect(node):
        if isinstance(node, exp.Union):
            collect(node.left)
            collect(node.right)
        else:
            branches.append(node)

    collect(main)

    if branch_idx < len(branches):
        # Replace the branch
        old_branch = branches[branch_idx]
        old_branch.replace(new_expr)


def _sql_matches(expr1: exp.Expression, expr2: exp.Expression) -> bool:
    """Check if two expressions represent the same SQL."""
    return expr1.sql() == expr2.sql()


def build_dag_prompt(sql: str, plan_summary: Optional[dict] = None) -> str:
    """Build a complete optimization prompt using DAG structure.

    Args:
        sql: The SQL query to optimize
        plan_summary: Optional execution plan summary

    Returns:
        Complete prompt string for LLM
    """
    dag = SQLDag.from_sql(sql)

    lines = [
        "Optimize this SQL query by rewriting specific nodes.\n",
    ]

    # Add DAG representation
    lines.append(dag.to_prompt(include_sql=True, plan_summary=plan_summary))

    # Optimization patterns
    lines.append("""
---

## Optimization Patterns

These patterns have produced >2x speedups:

1. **Filter pushdown**: Move filters from main_query into CTEs that scan the filtered dimension
2. **Correlated → Window**: Replace correlated subquery with window function in the CTE
3. **Join elimination**: Remove joins where only FK existence is checked, add IS NOT NULL
4. **Scan consolidation**: Merge multiple scans of same table using CASE WHEN
5. **UNION ALL decomposition**: Split complex OR into separate queries + UNION ALL

---

## Output Format

Return JSON with rewrites for each node you want to change:

```json
{
  "rewrites": {
    "node_id": "SELECT ... (complete rewritten SQL for this node)",
    "another_node": "SELECT ..."
  },
  "explanation": "What was optimized and why"
}
```

### Rules
1. Only include nodes you're actually changing
2. Each rewrite must be a complete, valid SELECT statement
3. Preserve column names/aliases for downstream compatibility
4. For CTEs, only provide the body (not "WITH cte AS (...)")
5. Verify semantic equivalence - results must be identical

### Node IDs
""")

    # List available node IDs
    lines.append("Available nodes to rewrite:")
    for node_id in dag.topological_order():
        node = dag.nodes[node_id]
        lines.append(f"- `{node_id}` ({node.node_type})")

    lines.append("\n---\n")

    # Original SQL for reference
    lines.append("## Original SQL\n```sql")
    lines.append(sql.strip())
    lines.append("```")

    return "\n".join(lines)
