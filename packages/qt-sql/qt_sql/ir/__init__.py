"""IR — Intermediate Representation for multi-statement SQL scripts.

Public API::

    # Build IR from SQL text
    script_ir = build_script_ir(sql, Dialect.DUCKDB)

    # Run all detectors (label nodes for patching)
    detections = detect_all(script_ir)

    # Load + apply a patch plan
    plan = load_patch_plan("plans/latest_date_parametrise_v1.yaml")
    result = apply_patch_plan(script_ir, plan)
    print(result.output_sql)

    # Render current state back to SQL
    print(render_script(script_ir))
"""
from .schema import (
    CTEIR,
    Dialect,
    DuplicateGroup,
    ExprIR,
    ExprKind,
    FromIR,
    FromKind,
    JoinIR,
    JoinType,
    QueryIR,
    ReferenceIndex,
    ScriptIR,
    StatementIR,
    StatementKind,
    SymbolTable,
    UseSite,
)
from .patch_schema import (
    Gate,
    GateKind,
    PatchOp,
    PatchPayload,
    PatchPlan,
    PatchResult,
    PatchStep,
    PatchTarget,
)
from .builder import build_script_ir
from .patch_engine import apply_patch_plan, render_script


def load_patch_plan(path: str) -> PatchPlan:
    """Load a PatchPlan from a YAML file."""
    import yaml
    from pathlib import Path

    data = yaml.safe_load(Path(path).read_text())
    return _dict_to_plan(data)


def detect_all(script_ir: ScriptIR) -> dict:
    """Run all detectors and return labelled UseSites by category."""
    from .detectors.latest_date import detect_latest_date_filters
    from .detectors.duplicate_expr import (
        detect_duplicate_expressions,
        detect_haversine_duplicates,
        detect_cross_join_on_true,
    )

    return {
        "latest_date_filters": detect_latest_date_filters(script_ir),
        "duplicate_expressions": detect_duplicate_expressions(script_ir),
        "haversine_duplicates": detect_haversine_duplicates(script_ir),
        "cross_joins_on_true": detect_cross_join_on_true(script_ir),
    }


def dict_to_plan(data: dict) -> PatchPlan:
    """Public entry: deserialise a dict (from JSON/YAML) into a PatchPlan."""
    return _dict_to_plan(data)


def render_ir_node_map(script_ir: ScriptIR) -> str:
    """Render a concise node map of the IR for worker prompts.

    Returns a human-readable tree showing statement IDs, CTE names,
    FROM tables, WHERE predicates, and GROUP BY clauses — enough
    context for a worker to target patch operations by node ID.
    """
    lines: list[str] = []

    for stmt in script_ir.statements:
        kind_label = stmt.kind.value.upper()
        lines.append(f"{stmt.id} [{kind_label}]")

        query = stmt.query
        if query is None:
            continue

        # Render CTEs
        for cte in query.with_ctes:
            cq = cte.query
            lines.append(f"  CTE: {cte.name}  (via {cq.id})")
            _render_query_summary(cq, lines, indent=4)

        # Render main query body
        lines.append(f"  MAIN QUERY (via {query.id})")
        _render_query_summary(query, lines, indent=4)

    # Footer: available patch operations + targeting
    lines.append("")
    lines.append(
        "Patch operations: insert_cte, replace_expr_subtree, "
        "replace_where_predicate, delete_expr_subtree"
    )
    lines.append('Target nodes by: by_node_id (e.g. "{}")'.format(
        script_ir.statements[0].id if script_ir.statements else "S0"
    ))

    return "\n".join(lines)


def _render_query_summary(query: QueryIR, lines: list[str], indent: int = 4) -> None:
    """Render FROM, WHERE, GROUP BY summary for a QueryIR node."""
    pad = " " * indent

    # FROM clause
    from_parts = _collect_from_tables(query.from_clause)
    if from_parts:
        lines.append(f"{pad}FROM: {', '.join(from_parts)}")

    # WHERE clause (truncated)
    if query.where:
        where_text = query.where.sql_text
        if len(where_text) > 120:
            where_text = where_text[:117] + "..."
        lines.append(f"{pad}WHERE: {where_text}")

    # GROUP BY
    if query.group_by:
        gb_cols = [e.sql_text for e in query.group_by]
        lines.append(f"{pad}GROUP BY: {', '.join(gb_cols)}")

    # ORDER BY
    if query.order_by:
        ob_cols = [e.expr.sql_text for e in query.order_by]
        lines.append(f"{pad}ORDER BY: {', '.join(ob_cols)}")


def _collect_from_tables(from_ir) -> list[str]:
    """Recursively collect table names from a FromIR node."""
    if from_ir is None:
        return []

    if from_ir.kind == FromKind.TABLE and from_ir.table:
        alias = f" {from_ir.table.alias}" if from_ir.table.alias else ""
        return [f"{from_ir.table.name}{alias}"]
    elif from_ir.kind == FromKind.SUBQUERY and from_ir.subquery:
        alias = f" {from_ir.subquery.alias}" if from_ir.subquery else ""
        return [f"(subquery){alias}"]
    elif from_ir.kind == FromKind.JOIN and from_ir.join:
        left = _collect_from_tables(from_ir.join.left)
        right = _collect_from_tables(from_ir.join.right)
        return left + right

    return []


def _dict_to_plan(data: dict) -> PatchPlan:
    """Deserialise a dict (from YAML) into a PatchPlan."""
    steps = []
    for s in data.get("steps", []):
        target_d = s.get("target", {})
        payload_d = s.get("payload", {}) or {}
        gates_d = s.get("gates", [])

        steps.append(
            PatchStep(
                step_id=s["step_id"],
                op=PatchOp(s["op"]),
                target=PatchTarget(
                    by_node_id=target_d.get("by_node_id"),
                    by_label=target_d.get("by_label"),
                    by_anchor_hash=target_d.get("by_anchor_hash"),
                    by_path=target_d.get("by_path"),
                ),
                payload=PatchPayload(
                    sql_fragment=payload_d.get("sql_fragment"),
                    expr_sql=payload_d.get("expr_sql"),
                    cte_name=payload_d.get("cte_name"),
                    cte_query_sql=payload_d.get("cte_query_sql"),
                ),
                gates=[
                    Gate(kind=GateKind(g["kind"]), args=g.get("args", {}))
                    for g in gates_d
                ],
                description=s.get("description"),
            )
        )

    return PatchPlan(
        plan_id=data["plan_id"],
        dialect=Dialect(data["dialect"]),
        steps=steps,
        preconditions=[
            Gate(kind=GateKind(g["kind"]), args=g.get("args", {}))
            for g in data.get("preconditions", [])
        ],
        postconditions=[
            Gate(kind=GateKind(g["kind"]), args=g.get("args", {}))
            for g in data.get("postconditions", [])
        ],
        target_script_id=data.get("target_script_id"),
        description=data.get("description"),
    )
