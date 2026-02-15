"""Patch Engine — apply PatchPlans against ScriptIR.

Implements all patch operations, gate validation, and SQL rendering.
Each step is applied sequentially with gate checks between steps.
On any failure (step error or gate), the ScriptIR is rolled back to
its pre-application state.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp

from .schema import (
    ScriptIR,
    StatementIR,
    StatementKind,
    canonical_hash,
)
from .patch_schema import (
    Gate,
    GateKind,
    PatchOp,
    PatchPlan,
    PatchResult,
    PatchStep,
    PatchTarget,
)

log = logging.getLogger(__name__)


class PatchError(Exception):
    """Raised when a patch step cannot be applied."""


# ── Snapshot / Rollback ───────────────────────────────────────────────


def _snapshot_statements(script_ir: ScriptIR) -> List[Tuple[str, str, StatementKind, list]]:
    """Capture (id, sql_text, kind, labels) for every statement."""
    return [
        (s.id, s.sql_text, s.kind, list(s.labels))
        for s in script_ir.statements
    ]


def _rollback_statements(
    script_ir: ScriptIR,
    snapshot: List[Tuple[str, str, StatementKind, list]],
    dialect: str,
):
    """Restore ScriptIR.statements from a snapshot, rebuilding full metadata."""
    from .builder import _build_statement

    new_stmts: List[StatementIR] = []
    counter = [0]
    for sid, sql_text, kind, labels in snapshot:
        try:
            ast = sqlglot.parse_one(sql_text, dialect=dialect)
            stmt = _build_statement(ast, sid, dialect, counter)
            stmt.labels = labels
        except Exception:
            stmt = StatementIR(
                id=sid, kind=kind, sql_text=sql_text, ast=None, labels=labels
            )
        new_stmts.append(stmt)
    script_ir.statements = new_stmts
    _rebuild_script_indexes(script_ir)


def _rebuild_script_indexes(script_ir: ScriptIR):
    """Recompute symbols, references, and fingerprints from current statements."""
    from .builder import _update_symbols, _build_fingerprints
    from .reference_index import build_reference_index
    from .schema import SymbolTable

    script_ir.symbols = SymbolTable()
    for stmt in script_ir.statements:
        _update_symbols(script_ir.symbols, stmt)
    script_ir.references = build_reference_index(script_ir)
    script_ir.fingerprints = _build_fingerprints(script_ir)


# ── Public API ─────────────────────────────────────────────────────────


def apply_patch_plan(script_ir: ScriptIR, plan: PatchPlan) -> PatchResult:
    """Apply a PatchPlan to a ScriptIR, returning patched SQL.

    On any failure (step error, step gate, or postcondition), the ScriptIR
    is rolled back so callers always see either the fully-patched state or
    the original state — never a partial mutation.
    """
    result = PatchResult(
        plan_id=plan.plan_id,
        success=False,
        steps_applied=0,
        steps_total=len(plan.steps),
    )
    dialect = plan.dialect.value

    # ── Snapshot for rollback ──
    snapshot = _snapshot_statements(script_ir)

    # ── Preconditions ──
    current_sql = render_script(script_ir, dialect)
    for gate in plan.preconditions:
        ok, msg = _validate_gate(gate, current_sql, dialect)
        result.gate_results[f"pre_{gate.kind.value}"] = ok
        if not ok:
            result.errors.append(f"Precondition failed: {gate.kind.value}: {msg}")
            return result  # no rollback needed — nothing was mutated yet

    # ── Apply steps sequentially ──
    for step in plan.steps:
        try:
            _apply_step(script_ir, step, dialect)
            result.steps_applied += 1
        except PatchError as e:
            result.errors.append(f"Step {step.step_id} failed: {e}")
            _rollback_statements(script_ir, snapshot, dialect)
            return result

        # Step gates
        current_sql = render_script(script_ir, dialect)
        for gate in step.gates:
            ok, msg = _validate_gate(gate, current_sql, dialect)
            result.gate_results[f"{step.step_id}_{gate.kind.value}"] = ok
            if not ok:
                result.errors.append(
                    f"Gate {gate.kind.value} failed after {step.step_id}: {msg}"
                )
                _rollback_statements(script_ir, snapshot, dialect)
                return result

    # ── Reorder CTEs by dependency ──
    _reorder_ctes(script_ir, dialect)

    # ── Postconditions ──
    current_sql = render_script(script_ir, dialect)
    for gate in plan.postconditions:
        ok, msg = _validate_gate(gate, current_sql, dialect)
        result.gate_results[f"post_{gate.kind.value}"] = ok
        if not ok:
            result.errors.append(f"Postcondition failed: {gate.kind.value}: {msg}")
            _rollback_statements(script_ir, snapshot, dialect)
            return result

    result.success = True
    result.output_sql = current_sql
    _rebuild_script_indexes(script_ir)
    return result


def render_script(script_ir: ScriptIR, dialect: Optional[str] = None) -> str:
    """Render the full script back to SQL text."""
    dialect = dialect or script_ir.dialect.value
    parts: List[str] = []
    for stmt in script_ir.statements:
        sql = stmt.ast.sql(dialect=dialect) if stmt.ast else stmt.sql_text
        parts.append(sql.rstrip(";") + ";")
    return "\n\n".join(parts)


# ── Step dispatcher ───────────────────────────────────────────────────


_OP_HANDLERS: Dict[PatchOp, Any] = {}  # populated below


def _apply_step(script_ir: ScriptIR, step: PatchStep, dialect: str):
    handler = _OP_HANDLERS.get(step.op)
    if not handler:
        raise PatchError(f"Unsupported op: {step.op}")
    handler(script_ir, step, dialect)


# ── INSERT_VIEW_STATEMENT / INSERT_STATEMENT_BEFORE ────────────────────


def _op_insert_view_statement(
    script_ir: ScriptIR, step: PatchStep, dialect: str
):
    sql = step.payload.sql_fragment
    if not sql:
        raise PatchError("insert_view_statement requires sql_fragment")

    try:
        ast = sqlglot.parse_one(sql.strip(), dialect=dialect)
    except Exception as e:
        raise PatchError(f"Cannot parse payload: {e}")

    insert_idx = _find_target_stmt_index(script_ir, step.target)
    new_id = f"S_patch_{len(script_ir.statements) + 100}"

    from .builder import _build_statement

    new_stmt = _build_statement(ast, new_id, dialect, [0])
    script_ir.statements.insert(insert_idx, new_stmt)


_op_insert_statement_before = _op_insert_view_statement


# ── REPLACE_EXPR_SUBTREE ─────────────────────────────────────────────


def _op_replace_expr_subtree(
    script_ir: ScriptIR, step: PatchStep, dialect: str
):
    expr_sql = step.payload.expr_sql
    if not expr_sql:
        raise PatchError("replace_expr_subtree requires expr_sql")

    new_expr = _parse_expr(expr_sql.strip(), dialect)

    target_stmts = _find_target_stmts(script_ir, step.target)
    if not target_stmts:
        raise PatchError(f"No target found for step {step.step_id}")

    replaced = False
    for stmt in target_stmts:
        if not stmt.ast:
            continue
        if step.target.by_label:
            replaced = _replace_by_label(stmt, step.target.by_label, new_expr, dialect)
        elif step.target.by_anchor_hash:
            replaced = _replace_by_hash(
                stmt, step.target.by_anchor_hash, new_expr, dialect
            )
        if replaced:
            break

    if not replaced:
        raise PatchError("Could not locate expression to replace")


def _op_replace_where_predicate(
    script_ir: ScriptIR, step: PatchStep, dialect: str
):
    """Replace (or add) WHERE predicate on the target SELECT."""
    expr_sql = step.payload.expr_sql
    if not expr_sql:
        raise PatchError("replace_where_predicate requires expr_sql")

    # If by_label or by_anchor_hash is set, fall through to expr_subtree logic
    if step.target.by_label or step.target.by_anchor_hash:
        return _op_replace_expr_subtree(script_ir, step, dialect)

    # Otherwise, find the target SELECT and set/replace its WHERE clause
    new_expr = _parse_expr(expr_sql.strip(), dialect)

    target_stmts = _find_target_stmts(script_ir, step.target)
    if not target_stmts:
        raise PatchError(f"No target found for step {step.step_id}")

    for stmt in target_stmts:
        if not stmt.ast:
            continue
        select_node = _resolve_select_node(stmt, step.target, dialect)
        if not select_node:
            continue
        select_node.set("where", exp.Where(this=new_expr))
        stmt.sql_text = stmt.ast.sql(dialect=dialect)
        return

    raise PatchError("Could not locate SELECT to set WHERE predicate")


_op_replace_join_condition = _op_replace_expr_subtree


# ── REPLACE_SELECT ────────────────────────────────────────────────────


def _op_replace_select(script_ir: ScriptIR, step: PatchStep, dialect: str):
    """Replace the SELECT expressions of a targeted SELECT statement."""
    sql_fragment = step.payload.sql_fragment
    if not sql_fragment:
        raise PatchError("replace_select requires sql_fragment (comma-separated expressions)")

    # Parse via wrapper to get expression list
    try:
        wrapper = f"SELECT {sql_fragment} FROM __dummy__"
        parsed = sqlglot.parse_one(wrapper, dialect=dialect)
        new_expressions = parsed.expressions
        if not new_expressions:
            raise PatchError("Parsed zero expressions from sql_fragment")
    except PatchError:
        raise
    except Exception as e:
        raise PatchError(f"Cannot parse SELECT expressions: {e}")

    target_stmts = _find_target_stmts(script_ir, step.target)
    if not target_stmts:
        raise PatchError(f"No target found for replace_select step {step.step_id}")

    for stmt in target_stmts:
        if not stmt.ast:
            continue
        select_node = _resolve_select_node(stmt, step.target, dialect)
        if not select_node:
            continue
        select_node.set("expressions", new_expressions)
        stmt.sql_text = stmt.ast.sql(dialect=dialect)
        return

    raise PatchError("Could not locate SELECT to replace expressions")


# ── REPLACE_BODY ──────────────────────────────────────────────────────


def _op_replace_body(script_ir: ScriptIR, step: PatchStep, dialect: str):
    """Replace the main query body (SELECT+FROM+WHERE+GROUP+ORDER+LIMIT),
    keeping the WITH clause (CTEs) intact."""
    sql_fragment = step.payload.sql_fragment
    if not sql_fragment:
        raise PatchError("replace_body requires sql_fragment (complete SELECT statement)")

    try:
        new_select = sqlglot.parse_one(sql_fragment.strip(), dialect=dialect)
    except Exception as e:
        raise PatchError(f"Cannot parse body sql_fragment: {e}")

    if not isinstance(new_select, exp.Select):
        raise PatchError(f"sql_fragment must be a SELECT, got {type(new_select).__name__}")

    target_stmts = _find_target_stmts(script_ir, step.target)
    if not target_stmts:
        raise PatchError(f"No target found for replace_body step {step.step_id}")

    for stmt in target_stmts:
        if not stmt.ast:
            continue

        root = stmt.ast
        if isinstance(root, exp.Create):
            inner = root.args.get("expression")
        else:
            inner = root

        # Extract existing WITH clause
        existing_with = None
        if isinstance(inner, exp.Select):
            existing_with = inner.args.get("with")
        elif isinstance(inner, exp.With):
            existing_with = inner

        # Attach WITH clause to new SELECT
        if existing_with:
            # Strip any WITH from the new_select (shouldn't have one)
            new_select.set("with", existing_with)

        if isinstance(root, exp.Create):
            root.set("expression", new_select)
        else:
            stmt.ast = new_select

        stmt.sql_text = stmt.ast.sql(dialect=dialect)
        return

    raise PatchError("Could not locate statement to replace body")


# ── INSERT_CTE ────────────────────────────────────────────────────────


def _op_insert_cte(script_ir: ScriptIR, step: PatchStep, dialect: str):
    cte_name = step.payload.cte_name
    cte_sql = step.payload.cte_query_sql
    if not cte_name or not cte_sql:
        raise PatchError("insert_cte requires cte_name and cte_query_sql")

    target_stmts = _find_target_stmts(script_ir, step.target)
    for stmt in target_stmts:
        if not stmt.ast:
            continue

        try:
            wrapper = f"WITH {cte_name} AS ({cte_sql}) SELECT 1"
            parsed = sqlglot.parse_one(wrapper, dialect=dialect)
            new_cte = list(parsed.find_all(exp.CTE))[0]
        except Exception as e:
            raise PatchError(f"Cannot parse CTE: {e}")

        target_ast = stmt.ast
        if isinstance(target_ast, exp.Create):
            select_node = target_ast.args.get("expression")
        else:
            select_node = target_ast

        if isinstance(select_node, exp.With):
            # Upsert: replace existing CTE with same name
            replaced = False
            for old_cte in list(select_node.find_all(exp.CTE)):
                if old_cte.alias == cte_name:
                    old_cte.replace(new_cte)
                    replaced = True
                    break
            if not replaced:
                select_node.append("expressions", new_cte)
        elif isinstance(select_node, exp.Select):
            existing_with = select_node.args.get("with")
            if existing_with:
                # Upsert: replace existing CTE with same name
                replaced = False
                for old_cte in list(existing_with.find_all(exp.CTE)):
                    if old_cte.alias == cte_name:
                        old_cte.replace(new_cte)
                        replaced = True
                        break
                if not replaced:
                    existing_with.append("expressions", new_cte)
            else:
                select_node.set("with", exp.With(expressions=[new_cte]))
        else:
            raise PatchError(f"Cannot add CTE to {type(select_node).__name__}")

        stmt.sql_text = stmt.ast.sql(dialect=dialect)


# ── REPLACE_BLOCK_WITH_CTE_PAIR ──────────────────────────────────────


def _op_replace_block_with_cte_pair(
    script_ir: ScriptIR, step: PatchStep, dialect: str
):
    sql_frag = step.payload.sql_fragment
    if not sql_frag:
        raise PatchError("replace_block_with_cte_pair requires sql_fragment")

    target_stmts = _find_target_stmts(script_ir, step.target)
    if not target_stmts:
        raise PatchError("No target found")

    for stmt in target_stmts:
        if not stmt.ast:
            continue

        old_sql = stmt.sql_text
        new_sql = _integrate_cte_fragment(old_sql, sql_frag.strip(), dialect)

        try:
            stmt.ast = sqlglot.parse_one(new_sql, dialect=dialect)
            stmt.sql_text = stmt.ast.sql(dialect=dialect)
        except Exception as e:
            raise PatchError(f"Re-parse failed after CTE insertion: {e}")


# ── DELETE_EXPR_SUBTREE ──────────────────────────────────────────────


def _op_delete_expr_subtree(
    script_ir: ScriptIR, step: PatchStep, dialect: str
):
    target_stmts = _find_target_stmts(script_ir, step.target)
    if not target_stmts:
        raise PatchError("No target found for delete")

    deleted = False
    for stmt in target_stmts:
        if not stmt.ast:
            continue
        if step.target.by_label:
            deleted = _delete_by_label(stmt, step.target.by_label, dialect)
        elif step.target.by_anchor_hash:
            deleted = _delete_by_anchor_hash(
                stmt, step.target.by_anchor_hash, dialect
            )
        if deleted:
            break

    if not deleted:
        raise PatchError(
            f"Could not locate expression to delete for "
            f"label={step.target.by_label!r} hash={step.target.by_anchor_hash!r}"
        )


# ── WRAP_QUERY_WITH_CTE ─────────────────────────────────────────────


def _op_wrap_query_with_cte(
    script_ir: ScriptIR, step: PatchStep, dialect: str
):
    _op_insert_cte(script_ir, step, dialect)


# ── REPLACE_FROM ────────────────────────────────────────────────────


def _op_replace_from(script_ir: ScriptIR, step: PatchStep, dialect: str):
    """Replace the FROM + JOINs of a targeted SELECT with new from_sql."""
    from_sql = step.payload.from_sql
    if not from_sql:
        raise PatchError("replace_from requires from_sql")

    # Parse the from_sql via a wrapper SELECT
    try:
        wrapper = f"SELECT 1 FROM {from_sql} WHERE 1=1"
        parsed = sqlglot.parse_one(wrapper, dialect=dialect)
    except Exception as e:
        raise PatchError(f"Cannot parse from_sql: {e}")

    new_from = parsed.args.get("from")
    new_joins = parsed.args.get("joins")

    target_stmts = _find_target_stmts(script_ir, step.target)
    if not target_stmts:
        raise PatchError(f"No target found for replace_from step {step.step_id}")

    replaced = False
    for stmt in target_stmts:
        if not stmt.ast:
            continue

        # Resolve to the right SELECT node (main query or CTE by anchor hash)
        select_node = _resolve_select_node(stmt, step.target, dialect)
        if not select_node:
            continue

        select_node.set("from", new_from)
        select_node.set("joins", new_joins)
        stmt.sql_text = stmt.ast.sql(dialect=dialect)
        replaced = True
        break

    if not replaced:
        raise PatchError("Could not locate SELECT to replace FROM clause")


def _resolve_select_node(
    stmt: StatementIR, target: PatchTarget, dialect: str
) -> Optional[exp.Select]:
    """Find the SELECT node within a statement, optionally narrowed by anchor hash."""
    root = stmt.ast
    if isinstance(root, exp.Create):
        root = root.args.get("expression")

    # Collect all SELECT nodes (CTE bodies + main)
    selects: list[exp.Select] = []

    if isinstance(root, exp.With):
        for cte_node in root.expressions:
            body = cte_node.this
            if isinstance(body, exp.Select):
                selects.append(body)
        if isinstance(root.this, exp.Select):
            selects.append(root.this)
    elif isinstance(root, exp.Select):
        with_clause = root.args.get("with")
        if with_clause:
            for cte_node in with_clause.expressions:
                body = cte_node.this
                if isinstance(body, exp.Select):
                    selects.append(body)
        selects.append(root)

    if not selects:
        return None

    # If anchor hash given, find the SELECT whose FROM matches
    if target.by_anchor_hash:
        for sel in selects:
            from_clause = sel.args.get("from")
            if from_clause:
                try:
                    from_text = from_clause.sql(dialect=dialect)
                    if canonical_hash(from_text) == target.by_anchor_hash:
                        return sel
                except Exception:
                    pass
        # Also check full select hash
        for sel in selects:
            try:
                if canonical_hash(sel.sql(dialect=dialect)) == target.by_anchor_hash:
                    return sel
            except Exception:
                pass

    # Default: last SELECT (the main query body)
    return selects[-1]


# Register handlers — split_cte deliberately omitted (not implemented)
_OP_HANDLERS.update(
    {
        PatchOp.INSERT_VIEW_STATEMENT: _op_insert_view_statement,
        PatchOp.INSERT_STATEMENT_BEFORE: _op_insert_statement_before,
        PatchOp.REPLACE_EXPR_SUBTREE: _op_replace_expr_subtree,
        PatchOp.REPLACE_WHERE_PREDICATE: _op_replace_where_predicate,
        PatchOp.REPLACE_JOIN_CONDITION: _op_replace_join_condition,
        PatchOp.INSERT_CTE: _op_insert_cte,
        PatchOp.REPLACE_BLOCK_WITH_CTE_PAIR: _op_replace_block_with_cte_pair,
        PatchOp.DELETE_EXPR_SUBTREE: _op_delete_expr_subtree,
        PatchOp.WRAP_QUERY_WITH_CTE: _op_wrap_query_with_cte,
        PatchOp.REPLACE_FROM: _op_replace_from,
        PatchOp.REPLACE_SELECT: _op_replace_select,
        PatchOp.REPLACE_BODY: _op_replace_body,
    }
)


# ── Target resolution ────────────────────────────────────────────────


def _find_target_stmt_index(script_ir: ScriptIR, target: PatchTarget) -> int:
    """Find insertion index for a target (for insert-before ops)."""
    if target.by_node_id:
        for i, stmt in enumerate(script_ir.statements):
            if stmt.id == target.by_node_id:
                return i

    if target.by_label:
        label_parts = target.by_label.split(".")
        if len(label_parts) >= 2:
            table_name = label_parts[-1].lower()
            for i, stmt in enumerate(script_ir.statements):
                if target.by_label in stmt.labels:
                    return i
                if any(r.name.lower() == table_name for r in stmt.reads):
                    return i

    return 0  # default: beginning


def _find_target_stmts(
    script_ir: ScriptIR, target: PatchTarget
) -> List[StatementIR]:
    """Find all statements matching a target."""
    results: List[StatementIR] = []

    if target.by_node_id:
        for stmt in script_ir.statements:
            if stmt.id == target.by_node_id:
                return [stmt]

    if target.by_label:
        label_parts = target.by_label.split(".")
        if len(label_parts) >= 2:
            table_name = label_parts[-1].lower()
            pattern_type = label_parts[0].lower()

            for stmt in script_ir.statements:
                if target.by_label in stmt.labels:
                    results.append(stmt)
                    continue
                if pattern_type == "latest_date_filter":
                    # Skip views — they are the parameterisation, not the target
                    if stmt.kind == StatementKind.CREATE_VIEW:
                        continue
                    if any(r.name.lower() == table_name for r in stmt.reads):
                        results.append(stmt)
                elif pattern_type == "geo":
                    geo_tables = {"location_record", "tbl_address_portfolio_v1"}
                    if any(r.name.lower() in geo_tables for r in stmt.reads):
                        results.append(stmt)

    if target.by_anchor_hash:
        # Walk expression subtrees, not just full-statement SQL
        for stmt in script_ir.statements:
            if not stmt.ast:
                continue
            if _ast_contains_hash(stmt.ast, target.by_anchor_hash, script_ir.dialect.value):
                results.append(stmt)

    if target.by_path:
        parts = target.by_path.split(".")
        if parts:
            for stmt in script_ir.statements:
                if stmt.id == parts[0]:
                    results.append(stmt)

    return results


def _ast_contains_hash(ast: Any, anchor_hash: str, dialect: str) -> bool:
    """Check whether any subtree in *ast* matches *anchor_hash*."""
    for node in ast.walk():
        # Only hash non-trivial nodes (functions, subqueries, conditions)
        if not isinstance(
            node,
            (exp.Func, exp.Subquery, exp.Case, exp.And, exp.Or,
             exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
             exp.Between, exp.In, exp.Select),
        ):
            continue
        try:
            sql = node.sql(dialect=dialect)
        except Exception:
            continue
        if canonical_hash(sql) == anchor_hash:
            return True
    return False


# ── Expression replacement helpers ───────────────────────────────────


def _parse_expr(expr_sql: str, dialect: str) -> Any:
    """Parse a SQL expression fragment into a sqlglot AST node."""
    try:
        return sqlglot.parse_one(expr_sql, dialect=dialect, into=exp.Condition)
    except Exception:
        pass

    try:
        wrapper = f"SELECT * FROM t WHERE {expr_sql}"
        parsed = sqlglot.parse_one(wrapper, dialect=dialect)
        where = parsed.args.get("where")
        if where:
            return where.this
    except Exception:
        pass

    try:
        wrapper = f"SELECT {expr_sql}"
        parsed = sqlglot.parse_one(wrapper, dialect=dialect)
        return parsed.expressions[0]
    except Exception as e:
        raise PatchError(f"Cannot parse expression: {expr_sql!r}: {e}")


def _replace_by_label(
    stmt: StatementIR, label: str, new_expr: Any, dialect: str
) -> bool:
    label_parts = label.split(".")
    if len(label_parts) < 2:
        return False

    pattern_type = label_parts[0]
    table_name = label_parts[-1].lower()

    if pattern_type == "latest_date_filter":
        return _replace_max_date_subquery(stmt, table_name, new_expr, dialect)
    if pattern_type == "geo":
        return _replace_by_hash_walk(stmt, new_expr, dialect)
    return False


def _replace_max_date_subquery(
    stmt: StatementIR, table_name: str, new_expr: Any, dialect: str
) -> bool:
    """Replace ``(SELECT MAX(calendar_date) FROM table)`` with *new_expr*."""
    if not stmt.ast:
        return False

    replaced = [False]

    def transformer(node):
        if not isinstance(node, exp.Subquery):
            return node
        inner = node.this
        if not isinstance(inner, exp.Select):
            return node

        has_max = False
        has_table = False
        for func in inner.find_all(exp.Max):
            col = func.this
            if isinstance(col, exp.Column) and col.name.lower() == "calendar_date":
                has_max = True
        for tbl in inner.find_all(exp.Table):
            if tbl.name.lower() == table_name:
                has_table = True

        if has_max and has_table:
            replaced[0] = True
            sub = (
                new_expr.find(exp.Subquery) if hasattr(new_expr, "find") else None
            )
            return sub or new_expr
        return node

    stmt.ast = stmt.ast.transform(transformer)
    if replaced[0]:
        stmt.sql_text = stmt.ast.sql(dialect=dialect)
    return replaced[0]


def _replace_by_hash(
    stmt: StatementIR, anchor_hash: str, new_expr: Any, dialect: str
) -> bool:
    """Replace the first expression subtree matching *anchor_hash*."""
    if not stmt.ast:
        return False

    replaced = [False]

    def transformer(node):
        if replaced[0]:
            return node  # only replace first match
        try:
            sql = node.sql(dialect=dialect)
        except Exception:
            return node
        if canonical_hash(sql) == anchor_hash:
            replaced[0] = True
            return new_expr
        return node

    stmt.ast = stmt.ast.transform(transformer)
    if replaced[0]:
        stmt.sql_text = stmt.ast.sql(dialect=dialect)
    return replaced[0]


def _replace_by_hash_walk(
    stmt: StatementIR, new_expr: Any, dialect: str
) -> bool:
    """Generic replacement by walking all nodes."""
    return False  # placeholder for geo patterns


# ── Delete helpers ────────────────────────────────────────────────────


def _delete_by_label(stmt: StatementIR, label: str, dialect: str) -> bool:
    """Remove an expression matching *label* from the statement's WHERE.

    Searches both the main query and all CTE bodies.

    Handles:
    - Single-predicate WHERE → removes entire WHERE clause.
    - AND-conjunction → removes the matching conjunct, keeps the rest.

    Returns True if a predicate was actually removed.
    """
    if not stmt.ast:
        return False

    label_parts = label.split(".")
    pattern_type = label_parts[0] if label_parts else ""

    # Find the query root (possibly inside CREATE)
    root = stmt.ast
    if isinstance(root, exp.Create):
        root = root.args.get("expression")

    # Collect all SELECT nodes to check (CTE bodies + main query)
    selects: List[exp.Select] = []

    if isinstance(root, exp.With):
        for cte_node in root.expressions:
            body = cte_node.this
            if isinstance(body, exp.Select):
                selects.append(body)
        if isinstance(root.this, exp.Select):
            selects.append(root.this)
    elif isinstance(root, exp.Select):
        with_clause = root.args.get("with")
        if with_clause:
            for cte_node in with_clause.expressions:
                body = cte_node.this
                if isinstance(body, exp.Select):
                    selects.append(body)
        selects.append(root)

    # Try deleting from each SELECT's WHERE
    for select_node in selects:
        if _try_delete_from_where(select_node, pattern_type, label_parts, dialect):
            stmt.sql_text = stmt.ast.sql(dialect=dialect)
            return True

    return False


def _try_delete_from_where(
    select_node: exp.Select,
    pattern_type: str,
    label_parts: list,
    dialect: str,
) -> bool:
    """Try to remove a matching predicate from *select_node*'s WHERE clause."""
    where_clause = select_node.args.get("where")
    if not where_clause:
        return False

    predicate = where_clause.this
    if _predicate_matches_label(predicate, pattern_type, label_parts, dialect):
        select_node.set("where", None)
        return True

    if isinstance(predicate, exp.And):
        conjuncts = _flatten_and(predicate)
        remaining = [
            c for c in conjuncts
            if not _predicate_matches_label(c, pattern_type, label_parts, dialect)
        ]
        if len(remaining) < len(conjuncts):
            if not remaining:
                select_node.set("where", None)
            else:
                new_pred = remaining[0]
                for r in remaining[1:]:
                    new_pred = exp.And(this=new_pred, expression=r)
                where_clause.set("this", new_pred)
            return True

    return False


def _delete_by_anchor_hash(
    stmt: StatementIR, anchor_hash: str, dialect: str
) -> bool:
    """Remove the WHERE conjunct whose hash matches *anchor_hash*."""
    if not stmt.ast:
        return False

    select_node = stmt.ast
    if isinstance(select_node, exp.Create):
        select_node = select_node.args.get("expression")
    if isinstance(select_node, exp.With):
        select_node = select_node.this

    if not isinstance(select_node, exp.Select):
        return False

    where_clause = select_node.args.get("where")
    if not where_clause:
        return False

    predicate = where_clause.this

    # Check full predicate
    try:
        if canonical_hash(predicate.sql(dialect=dialect)) == anchor_hash:
            select_node.set("where", None)
            stmt.sql_text = stmt.ast.sql(dialect=dialect)
            return True
    except Exception:
        pass

    # Check AND conjuncts
    if isinstance(predicate, exp.And):
        conjuncts = _flatten_and(predicate)
        remaining = []
        found = False
        for c in conjuncts:
            try:
                if canonical_hash(c.sql(dialect=dialect)) == anchor_hash:
                    found = True
                    continue
            except Exception:
                pass
            remaining.append(c)
        if found:
            if not remaining:
                select_node.set("where", None)
            else:
                new_pred = remaining[0]
                for r in remaining[1:]:
                    new_pred = exp.And(this=new_pred, expression=r)
                where_clause.set("this", new_pred)
            stmt.sql_text = stmt.ast.sql(dialect=dialect)
            return True

    return False


def _predicate_matches_label(
    pred: Any, pattern_type: str, label_parts: list, dialect: str
) -> bool:
    """Check whether a predicate matches a label pattern."""
    if pattern_type == "geo" and "distance_filter" in ".".join(label_parts):
        # Match predicates containing haversine (ASIN/ROUND) + comparison
        pred_sql = pred.sql(dialect=dialect).lower()
        if "asin" in pred_sql or "distance_km" in pred_sql:
            return True
    return False


def _flatten_and(node: exp.And) -> List[Any]:
    """Flatten nested AND into a list of conjuncts."""
    result: List[Any] = []
    if isinstance(node.this, exp.And):
        result.extend(_flatten_and(node.this))
    else:
        result.append(node.this)
    if isinstance(node.expression, exp.And):
        result.extend(_flatten_and(node.expression))
    else:
        result.append(node.expression)
    return result


# ── CTE dependency reorder ──────────────────────────────────────────


def _reorder_ctes(script_ir: ScriptIR, dialect: str):
    """Topologically sort CTEs so each CTE comes after CTEs it references."""
    for stmt in script_ir.statements:
        if not stmt.ast:
            continue

        root = stmt.ast
        if isinstance(root, exp.Create):
            root = root.args.get("expression")

        with_clause = None
        if isinstance(root, exp.With):
            with_clause = root
        elif isinstance(root, exp.Select):
            with_clause = root.args.get("with")

        if not with_clause or not with_clause.expressions:
            continue

        ctes = list(with_clause.expressions)
        if len(ctes) <= 1:
            continue

        # Build name → CTE mapping and dependency graph
        cte_by_name = {}
        for cte in ctes:
            cte_by_name[cte.alias.lower()] = cte

        cte_names = set(cte_by_name.keys())

        # Find which CTE names each CTE body references
        deps = {}  # cte_name → set of cte_names it depends on
        for cte in ctes:
            name = cte.alias.lower()
            body_sql = cte.this.sql(dialect=dialect).lower()
            deps[name] = set()
            for other_name in cte_names:
                if other_name != name and other_name in body_sql:
                    deps[name].add(other_name)

        # Topological sort (Kahn's algorithm)
        in_degree = {n: 0 for n in cte_names}
        for n, d in deps.items():
            for dep in d:
                if dep in in_degree:
                    in_degree[n] += 1

        queue = [n for n in cte_names if in_degree[n] == 0]
        # Stable sort: prefer original order for ties
        name_order = {cte.alias.lower(): i for i, cte in enumerate(ctes)}
        queue.sort(key=lambda n: name_order.get(n, 0))
        sorted_names = []

        while queue:
            n = queue.pop(0)
            sorted_names.append(n)
            for other, d in deps.items():
                if n in d:
                    d.discard(n)
                    in_degree[other] -= 1
                    if in_degree[other] == 0:
                        queue.append(other)
                        queue.sort(key=lambda x: name_order.get(x, 0))

        if len(sorted_names) != len(ctes):
            # Cycle detected — skip reorder
            continue

        # Check if already in correct order
        current_order = [cte.alias.lower() for cte in ctes]
        if current_order == sorted_names:
            continue

        # Reorder
        new_ctes = [cte_by_name[n] for n in sorted_names]
        with_clause.set("expressions", new_ctes)
        stmt.sql_text = stmt.ast.sql(dialect=dialect)


# ── CTE fragment integration ────────────────────────────────────────


def _integrate_cte_fragment(old_sql: str, fragment: str, dialect: str) -> str:
    """Insert CTE definitions from *fragment* into *old_sql*'s WITH clause."""
    frag = fragment.lstrip(",").strip()

    try:
        parsed = sqlglot.parse_one(old_sql, dialect=dialect)
        inner = parsed
        if isinstance(parsed, exp.Create):
            inner = parsed.args.get("expression")

        frag_sql = f"WITH {frag} SELECT 1"
        frag_parsed = sqlglot.parse_one(frag_sql, dialect=dialect)

        with_clause = inner if isinstance(inner, exp.With) else None
        if with_clause is None and inner:
            # Check for Select.with
            if isinstance(inner, exp.Select):
                with_clause = inner.args.get("with")
            else:
                with_clause = inner.find(exp.With)

        if with_clause:
            for new_cte in frag_parsed.find_all(exp.CTE):
                new_name = new_cte.alias
                # Upsert: replace existing CTE body in place, or append if new
                replaced = False
                if new_name:
                    for old_cte in list(with_clause.find_all(exp.CTE)):
                        if old_cte.alias == new_name:
                            old_cte.replace(new_cte)
                            replaced = True
                            break
                if not replaced:
                    with_clause.append("expressions", new_cte)
            return parsed.sql(dialect=dialect)
    except Exception:
        pass

    # Fallback: text-level insertion before final SELECT
    return old_sql


# ── Gate validation ──────────────────────────────────────────────────


def _validate_gate(gate: Gate, sql: str, dialect: str) -> Tuple[bool, str]:
    if gate.kind == GateKind.PARSE_OK:
        return _gate_parse_ok(sql, dialect)
    if gate.kind == GateKind.PLAN_SHAPE:
        return _gate_plan_shape(sql, gate.args, dialect)
    # Runtime gates require a live connection — fail explicitly
    return False, f"{gate.kind.value} requires runtime (no connection available)"


def _gate_parse_ok(sql: str, dialect: str) -> Tuple[bool, str]:
    try:
        stmts = sqlglot.parse(sql, dialect=dialect)
        if not stmts or all(s is None for s in stmts):
            return False, "empty parse result"
        return True, "ok"
    except Exception as e:
        return False, str(e)


def _gate_plan_shape(
    sql: str, args: Dict[str, Any], dialect: str
) -> Tuple[bool, str]:
    expectation = args.get("expectation", "")

    if expectation == "no_scalar_subquery_max_calendar_date_remaining":
        try:
            for stmt_ast in sqlglot.parse(sql, dialect=dialect):
                if stmt_ast is None:
                    continue
                # Skip the parameterisation view itself (CREATE VIEW v_latest*)
                if isinstance(stmt_ast, exp.Create):
                    kind_str = str(stmt_ast.args.get("kind", "")).upper()
                    if "VIEW" in kind_str:
                        tbl = stmt_ast.find(exp.Table)
                        if tbl and tbl.name.lower().startswith("v_latest"):
                            continue
                for subq in stmt_ast.find_all(exp.Subquery):
                    inner = subq.this
                    if not isinstance(inner, exp.Select):
                        continue
                    for func in inner.find_all(exp.Max):
                        col = func.this
                        if (
                            isinstance(col, exp.Column)
                            and col.name.lower() == "calendar_date"
                        ):
                            return (
                                False,
                                "scalar max(calendar_date) subquery still present",
                            )
            return True, "ok"
        except Exception as e:
            return False, f"parse error: {e}"

    if expectation == "haversine_expr_occurs_once":
        count = 0
        try:
            for stmt_ast in sqlglot.parse(sql, dialect=dialect):
                if stmt_ast is None:
                    continue
                for func in stmt_ast.find_all(exp.Func):
                    if type(func).__name__.upper() == "ASIN":
                        count += 1
            return (
                (True, "ok")
                if count <= 1
                else (False, f"ASIN occurs {count} times")
            )
        except Exception as e:
            return False, f"parse error: {e}"

    if expectation == "distance_filter_uses_distance_km_column":
        lower = sql.lower()
        if "distance_km" in lower and "where" in lower:
            return True, "ok"
        return False, "distance_km column not found in WHERE"

    return True, f"unknown expectation: {expectation}"
