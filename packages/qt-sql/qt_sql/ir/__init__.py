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
