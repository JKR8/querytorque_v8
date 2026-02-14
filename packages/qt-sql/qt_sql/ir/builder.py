"""IR Builder — Parse multi-statement SQL into ScriptIR.

Uses sqlglot for parsing.  Supports DuckDB / PostgreSQL / Snowflake dialects.
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional

import sqlglot
from sqlglot import exp

from .schema import (
    CTEIR,
    Dialect,
    DialectFeatures,
    ExprIR,
    ExprKind,
    FingerprintIndex,
    FromIR,
    FromKind,
    JoinIR,
    JoinType,
    MaterializationHint,
    OrderItemIR,
    QueryIR,
    ReferenceIndex,
    RelationRef,
    ScriptIR,
    StatementIR,
    StatementKind,
    SubqueryRefIR,
    SymbolTable,
    TableRefIR,
    WindowSpecIR,
    canonical_hash,
)

log = logging.getLogger(__name__)


# ── Public API ─────────────────────────────────────────────────────────


def build_script_ir(
    sql: str,
    dialect: Dialect = Dialect.DUCKDB,
    script_id: str = "script_0",
) -> ScriptIR:
    """Parse multi-statement SQL into a ScriptIR."""
    dialect_str = dialect.value

    try:
        asts = sqlglot.parse(sql, dialect=dialect_str)
    except Exception as e:
        log.warning("sqlglot.parse failed, semicolon-split fallback: %s", e)
        asts = _fallback_parse(sql, dialect_str)

    script = ScriptIR(script_id=script_id, dialect=dialect)
    counter = [0]  # mutable pre-order counter for stable IDs

    for idx, ast in enumerate(asts):
        if ast is None:
            continue
        stmt_id = f"S{idx}"
        stmt = _build_statement(ast, stmt_id, dialect_str, counter)
        script.statements.append(stmt)
        _update_symbols(script.symbols, stmt)

    # Reference index (lazy import to avoid circular)
    from .reference_index import build_reference_index

    script.references = build_reference_index(script)

    # Fingerprints
    script.fingerprints = _build_fingerprints(script)

    return script


# ── Fallback parser ────────────────────────────────────────────────────


def _fallback_parse(sql: str, dialect: str) -> List:
    results = []
    for chunk in sql.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            results.append(sqlglot.parse_one(chunk, dialect=dialect))
        except Exception:
            log.warning("Skipping unparseable chunk: %.80s...", chunk)
    return results


# ── Statement ──────────────────────────────────────────────────────────


def _build_statement(
    ast: Any, stmt_id: str, dialect: str, counter: list
) -> StatementIR:
    kind = _classify_statement(ast)
    sql_text = ast.sql(dialect=dialect)

    stmt = StatementIR(id=stmt_id, kind=kind, sql_text=sql_text, ast=ast)
    # Extract writes FIRST so we can exclude write targets from reads
    stmt.writes = _extract_writes(ast, kind)
    write_names = {w.name.lower() for w in stmt.writes}
    stmt.reads = _extract_reads(ast, exclude_names=write_names)

    # Build QueryIR for query-bearing statements
    if kind in (
        StatementKind.SELECT,
        StatementKind.CREATE_TABLE_AS,
        StatementKind.CREATE_VIEW,
    ):
        query_ast = _extract_query_ast(ast, kind)
        if query_ast:
            query_id = f"Q_{stmt_id}"
            stmt.query = _build_query_ir(query_ast, query_id, dialect, counter)

    return stmt


def _classify_statement(ast: Any) -> StatementKind:
    if isinstance(ast, exp.Create):
        kind_str = str(ast.args.get("kind", "")).upper()
        if "VIEW" in kind_str:
            return StatementKind.CREATE_VIEW
        if "TABLE" in kind_str:
            expression = ast.args.get("expression")
            if expression and isinstance(
                expression, (exp.Select, exp.Union, exp.With)
            ):
                return StatementKind.CREATE_TABLE_AS
            return StatementKind.CREATE_TABLE
        return StatementKind.OTHER_DDL
    if isinstance(ast, exp.Drop):
        return StatementKind.DROP_TABLE
    if isinstance(ast, exp.Insert):
        return StatementKind.INSERT
    if isinstance(ast, (exp.Select, exp.Union, exp.With)):
        return StatementKind.SELECT
    return StatementKind.OTHER_DDL


def _extract_query_ast(ast: Any, kind: StatementKind) -> Optional[Any]:
    if kind == StatementKind.SELECT:
        return ast
    if kind in (StatementKind.CREATE_TABLE_AS, StatementKind.CREATE_VIEW):
        return ast.args.get("expression")
    return None


# ── QueryIR ────────────────────────────────────────────────────────────


def _build_query_ir(
    ast: Any, query_id: str, dialect: str, counter: list
) -> QueryIR:
    query = QueryIR(id=query_id, _ast_node=ast)

    # Peel WITH wrapper — extract CTEs first
    # Two shapes: standalone exp.With, or exp.Select with a "with" arg
    select_ast = ast
    with_clause = None
    if isinstance(ast, exp.With):
        with_clause = ast
        select_ast = ast.this
    elif isinstance(ast, exp.Select) and ast.args.get("with"):
        with_clause = ast.args["with"]
        select_ast = ast

    if with_clause is not None:
        for cte_node in with_clause.expressions:
            cte_name = cte_node.alias
            cte_body = cte_node.this
            cte_qid = f"CTE_{query_id}_{cte_name}"
            cte_query = _build_query_ir(cte_body, cte_qid, dialect, counter)

            mat_hint = None
            mat_flag = cte_node.args.get("materialized")
            if mat_flag is True:
                mat_hint = MaterializationHint.MATERIALIZE
            elif mat_flag is False:
                mat_hint = MaterializationHint.INLINE

            query.with_ctes.append(
                CTEIR(name=cte_name, query=cte_query, materialization_hint=mat_hint)
            )

    if not isinstance(select_ast, exp.Select):
        # UNION / INTERSECT etc — store raw, no deeper decomposition
        return query

    # SELECT list
    for i, sel_expr in enumerate(select_ast.expressions):
        eid = _next_id(counter, query_id, f"sel_{i}")
        query.select_list.append(_build_expr_ir(sel_expr, eid, dialect))

    # FROM + JOINs
    query.from_clause = _build_from_ir(select_ast, dialect, counter, query_id)

    # WHERE
    where_node = select_ast.args.get("where")
    if where_node:
        eid = _next_id(counter, query_id, "where")
        query.where = _build_expr_ir(where_node.this, eid, dialect)

    # GROUP BY
    group_node = select_ast.args.get("group")
    if group_node:
        for i, g_expr in enumerate(group_node.expressions):
            eid = _next_id(counter, query_id, f"grp_{i}")
            query.group_by.append(_build_expr_ir(g_expr, eid, dialect))

    # HAVING
    having_node = select_ast.args.get("having")
    if having_node:
        eid = _next_id(counter, query_id, "having")
        query.having = _build_expr_ir(having_node.this, eid, dialect)

    # ORDER BY
    order_node = select_ast.args.get("order")
    if order_node:
        for i, o_expr in enumerate(order_node.expressions):
            eid = _next_id(counter, query_id, f"ord_{i}")
            inner = o_expr.this if isinstance(o_expr, exp.Ordered) else o_expr
            desc = isinstance(o_expr, exp.Ordered) and bool(
                o_expr.args.get("desc")
            )
            query.order_by.append(
                OrderItemIR(
                    expr=_build_expr_ir(inner, eid, dialect), desc=desc
                )
            )

    # LIMIT
    limit_node = select_ast.args.get("limit")
    if limit_node:
        eid = _next_id(counter, query_id, "limit")
        query.limit = _build_expr_ir(limit_node.this, eid, dialect)

    # Dialect features
    query.dialect_features = _detect_dialect_features(select_ast)

    return query


# ── ExprIR ─────────────────────────────────────────────────────────────


def _build_expr_ir(node: Any, expr_id: str, dialect: str) -> ExprIR:
    kind = _classify_expr(node)
    try:
        sql_text = node.sql(dialect=dialect)
    except Exception:
        sql_text = str(node)

    props: dict = {}

    if kind == ExprKind.FUNC:
        props["func_name"] = (
            node.name
            if isinstance(node, exp.Anonymous)
            else type(node).__name__
        )
    elif kind == ExprKind.AGG:
        props["func_name"] = type(node).__name__
        if isinstance(node, exp.Count) and node.args.get("distinct"):
            props["distinct"] = True
    elif kind == ExprKind.BINOP:
        props["operator"] = type(node).__name__
    elif kind == ExprKind.SUBQUERY:
        props["is_scalar"] = _is_scalar_subquery(node)
    elif kind == ExprKind.WINDOW_FUNC:
        props["func_name"] = (
            type(node.this).__name__ if node.this else ""
        )
    elif kind == ExprKind.CASE:
        props["has_else"] = node.args.get("default") is not None

    return ExprIR(
        id=expr_id,
        kind=kind,
        sql_text=sql_text,
        props=props,
        _ast_node=node,
    )


_AGG_TYPES = (
    exp.Count,
    exp.Sum,
    exp.Avg,
    exp.Min,
    exp.Max,
    exp.ArrayAgg,
    exp.GroupConcat,
)

_BINOP_TYPES = (
    exp.And,
    exp.Or,
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.Is,
    exp.Like,
    exp.ILike,
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.Mod,
    exp.Between,
    exp.In,
)


def _classify_expr(node: Any) -> ExprKind:
    if isinstance(node, exp.Column):
        return ExprKind.COL
    if isinstance(node, exp.Literal):
        return ExprKind.LIT
    if isinstance(node, _AGG_TYPES):
        return ExprKind.AGG
    if isinstance(node, exp.Window):
        return ExprKind.WINDOW_FUNC
    if isinstance(node, exp.Case):
        return ExprKind.CASE
    if isinstance(node, exp.Cast):
        return ExprKind.CAST
    if isinstance(node, exp.Subquery):
        return ExprKind.SUBQUERY
    if isinstance(node, (exp.Not, exp.Neg, exp.Paren)):
        return ExprKind.UNOP
    if isinstance(node, _BINOP_TYPES):
        return ExprKind.BINOP
    if isinstance(node, exp.Alias):
        return ExprKind.ALIAS
    if isinstance(node, exp.Star):
        return ExprKind.STAR
    if isinstance(node, exp.Func):
        return ExprKind.FUNC
    return ExprKind.OTHER


def _is_scalar_subquery(node: Any) -> bool:
    inner = node.this if hasattr(node, "this") else node
    if isinstance(inner, exp.Select):
        has_agg = any(
            isinstance(e, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max))
            for e in inner.find_all(exp.Func)
        )
        has_group = inner.args.get("group") is not None
        return has_agg and not has_group
    return False


# ── FromIR / JoinIR ───────────────────────────────────────────────────


def _build_from_ir(
    select_ast: Any, dialect: str, counter: list, query_id: str
) -> Optional[FromIR]:
    from_node = select_ast.args.get("from")
    if not from_node:
        return None

    base = from_node.this
    result = _source_to_from_ir(base, dialect, counter, query_id)

    # Process direct JOINs (not inside subqueries)
    for join_node in select_ast.args.get("joins") or []:
        join_type = _classify_join_type(join_node)
        right = _source_to_from_ir(join_node.this, dialect, counter, query_id)

        condition = None
        on_clause = join_node.args.get("on")
        if on_clause:
            eid = _next_id(counter, query_id, "jcond")
            condition = _build_expr_ir(on_clause, eid, dialect)

        hints: dict = {}
        if condition and condition.sql_text.strip() in ("1 = 1", "1=1", "TRUE"):
            hints["is_cross_join_emulated"] = True

        result = FromIR(
            kind=FromKind.JOIN,
            join=JoinIR(
                join_type=join_type,
                left=result,
                right=right,
                condition=condition,
                hints=hints,
            ),
        )

    return result


def _source_to_from_ir(
    node: Any, dialect: str, counter: list, query_id: str
) -> FromIR:
    if isinstance(node, exp.Table):
        alias = node.alias if node.alias and node.alias != node.name else None
        schema = getattr(node, "db", None) or None
        return FromIR(
            kind=FromKind.TABLE,
            table=TableRefIR(name=node.name, alias=alias, schema=schema),
        )
    if isinstance(node, exp.Subquery):
        return FromIR(
            kind=FromKind.SUBQUERY,
            subquery=SubqueryRefIR(
                query=QueryIR(
                    id=_next_id(counter, query_id, "from_subq")
                ),
                alias=node.alias,
            ),
        )
    # Fallback
    return FromIR(
        kind=FromKind.TABLE,
        table=TableRefIR(name=node.sql(dialect=dialect)),
    )


def _classify_join_type(join_node: Any) -> JoinType:
    join_str = join_node.sql().upper()
    if "CROSS" in join_str:
        return JoinType.CROSS
    if "LEFT" in join_str:
        return JoinType.LEFT
    if "RIGHT" in join_str:
        return JoinType.RIGHT
    if "FULL" in join_str:
        return JoinType.FULL
    return JoinType.INNER


# ── Dialect features ──────────────────────────────────────────────────


def _detect_dialect_features(ast: Any) -> DialectFeatures:
    f = DialectFeatures()
    for node in ast.walk():
        if isinstance(node, exp.Window) and isinstance(
            node.this, exp.RowNumber
        ):
            f.uses_window_row_number = True
        elif isinstance(node, exp.Interval):
            f.uses_interval = True
        elif isinstance(node, exp.Filter):
            f.uses_filter_agg = True
    return f


# ── Reads / Writes extraction ────────────────────────────────────────


def _extract_reads(ast: Any, exclude_names: set | None = None) -> list:
    reads = []
    seen: set = set()
    exclude = exclude_names or set()
    for table in ast.find_all(exp.Table):
        name = table.name.lower()
        if name and name not in seen and name not in exclude:
            seen.add(name)
            alias = (
                table.alias
                if table.alias and table.alias != table.name
                else None
            )
            schema = getattr(table, "db", None) or None
            reads.append(RelationRef(name=table.name, schema=schema, alias=alias))
    return reads


def _extract_writes(ast: Any, kind: StatementKind) -> list:
    if kind in (
        StatementKind.CREATE_VIEW,
        StatementKind.CREATE_TABLE_AS,
        StatementKind.CREATE_TABLE,
        StatementKind.DROP_TABLE,
    ):
        tbl = ast.find(exp.Table)
        if tbl:
            return [RelationRef(name=tbl.name)]
    if kind == StatementKind.INSERT:
        tbl = ast.find(exp.Table)
        if tbl:
            return [RelationRef(name=tbl.name)]
    return []


# ── Symbol table ──────────────────────────────────────────────────────


def _update_symbols(symbols: SymbolTable, stmt: StatementIR):
    if stmt.kind == StatementKind.CREATE_VIEW:
        for w in stmt.writes:
            columns: list = []
            if stmt.query and stmt.query.select_list:
                for sel in stmt.query.select_list:
                    node = sel._ast_node
                    if isinstance(node, exp.Alias):
                        columns.append(node.alias)
                    elif isinstance(node, exp.Column):
                        columns.append(node.name)
                    else:
                        columns.append(sel.sql_text)
            symbols.add(w.name, "view", stmt.id, columns)
    elif stmt.kind in (StatementKind.CREATE_TABLE_AS, StatementKind.CREATE_TABLE):
        for w in stmt.writes:
            symbols.add(w.name, "table", stmt.id)


# ── Fingerprints ──────────────────────────────────────────────────────


def _build_fingerprints(script: ScriptIR) -> FingerprintIndex:
    fp = FingerprintIndex()
    for stmt in script.statements:
        fp.hashes[stmt.id] = canonical_hash(stmt.sql_text)
    return fp


# ── ID generation ─────────────────────────────────────────────────────


def _next_id(counter: list, prefix: str, suffix: str) -> str:
    counter[0] += 1
    return f"E_{prefix}_{suffix}_{counter[0]}"
