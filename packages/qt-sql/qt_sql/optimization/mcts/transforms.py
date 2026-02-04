"""Deterministic AST transformation library for Hybrid MCTS (KB transforms).

All transforms are AST-only (no LLM) and must:
- Operate on a deepcopy of the input AST
- Return a new SQL string
- Return None if the rule is a no-op
"""

from __future__ import annotations

import copy
from typing import Callable, Optional

import sqlglot
from sqlglot import exp
from sqlglot.optimizer import pushdown_predicates, unnest_subqueries, optimize_joins

from qt_sql.optimization.knowledge_base import TransformID, get_transform_ids
from qt_sql.rewriters.boolean_optimizer import OrToUnionRewriter
from qt_sql.rewriters.correlated_subquery import CorrelatedSubqueryToJoinRewriter
from qt_sql.rewriters.cte_optimizer import CTEInliner, UnusedCTERemover
from qt_sql.rewriters.repeated_subquery import RepeatedSubqueryToCTERewriter
from qt_sql.rewriters.in_subquery import InSubqueryToJoinRewriter
from qt_sql.rewriters.simplification import RedundantDistinctRemover, RedundantPredicateRemover

Transformation = Callable[[exp.Expression], exp.Expression]

def _apply_rewriter(node: exp.Expression, rewriter_cls) -> Optional[exp.Expression]:
    rewriter = rewriter_cls()
    if not rewriter.can_rewrite(node):
        return None
    result = rewriter.rewrite(node)
    if not result.success:
        return None
    if result.rewritten_node is not None:
        return result.rewritten_node
    if result.rewritten_sql:
        return sqlglot.parse_one(result.rewritten_sql, dialect="duckdb")
    return None


def _split_and(expr: exp.Expression) -> list[exp.Expression]:
    parts: list[exp.Expression] = []

    def _walk(e: exp.Expression) -> None:
        if isinstance(e, exp.And):
            _walk(e.left)
            _walk(e.right)
        else:
            parts.append(e)

    _walk(expr)
    return parts


def _combine_and(parts: list[exp.Expression]) -> Optional[exp.Expression]:
    if not parts:
        return None
    expr = parts[0]
    for part in parts[1:]:
        expr = exp.And(this=expr, expression=part)
    return expr


def _get_cte_names(node: exp.Expression) -> set[str]:
    names: set[str] = set()
    with_clause = node.find(exp.With)
    if not with_clause:
        return names
    for cte in with_clause.find_all(exp.CTE):
        if cte.alias:
            names.add(str(cte.alias).lower())
    return names


def _unique_cte_name(node: exp.Expression, base: str) -> str:
    existing = _get_cte_names(node)
    name = base
    i = 1
    while name.lower() in existing:
        name = f"{base}_{i}"
        i += 1
    return name


def _replace_table_name(node: exp.Expression, old: str, new: str) -> None:
    for table in node.find_all(exp.Table):
        if str(table.this).lower() == old.lower():
            table.set("this", exp.to_identifier(new))


def _replace_table_refs(node: exp.Expression, old: str, new: str) -> None:
    for col in node.find_all(exp.Column):
        if col.table and col.table.lower() == old.lower():
            col.set("table", exp.to_identifier(new))


def _remove_unused_ctes(node: exp.Expression) -> exp.Expression:
    with_clause = node.args.get("with_")
    if not with_clause:
        return node

    used_tables = {str(t.this).lower() for t in node.find_all(exp.Table)}
    remaining = []
    for cte in with_clause.find_all(exp.CTE):
        if not cte.alias:
            continue
        name = str(cte.alias).lower()
        if name in used_tables:
            remaining.append(cte)

    if not remaining:
        node.set("with_", None)
        return node

    with_clause.set("expressions", remaining)
    node.set("with_", with_clause)
    return node


def _transform_or_to_union(node: exp.Expression) -> Optional[exp.Expression]:
    return _apply_rewriter(node, OrToUnionRewriter)


def _transform_correlated_to_cte(node: exp.Expression) -> Optional[exp.Expression]:
    # Uses correlated subquery -> join rewrite (deterministic AST)
    return _apply_rewriter(node, CorrelatedSubqueryToJoinRewriter)


def _transform_date_cte_isolate(node: exp.Expression) -> Optional[exp.Expression]:
    if not isinstance(node, exp.Select):
        return None

    date_dim_tables = []
    for table in node.find_all(exp.Table):
        if str(table.this).lower() == "date_dim":
            date_dim_tables.append(table)

    if not date_dim_tables:
        return None

    date_dim = date_dim_tables[0]
    date_alias = date_dim.alias_or_name or "date_dim"
    date_cols = {
        "d_year",
        "d_qoy",
        "d_moy",
        "d_month",
        "d_date",
        "d_date_sk",
        "d_week_seq",
        "d_week",
    }

    where = node.find(exp.Where)
    if not where or not where.this:
        return None

    predicates = _split_and(where.this)
    date_preds: list[exp.Expression] = []
    other_preds: list[exp.Expression] = []

    def _is_date_pred(expr: exp.Expression) -> bool:
        for col in expr.find_all(exp.Column):
            col_name = col.name.lower() if col.name else ""
            table_name = col.table.lower() if col.table else ""
            if table_name in {date_alias.lower(), "date_dim"} and col_name in date_cols:
                return True
            if not table_name and col_name in date_cols:
                return True
        return False

    for pred in predicates:
        if _is_date_pred(pred):
            date_preds.append(pred)
        else:
            other_preds.append(pred)

    if not date_preds:
        return None

    cte_name = _unique_cte_name(node, "date_filter")
    cte_select = exp.Select(expressions=[exp.Column(this=exp.to_identifier("d_date_sk"))])
    cte_select = cte_select.from_(exp.Table(this=exp.to_identifier("date_dim")))
    def _strip_alias(pred: exp.Expression) -> exp.Expression:
        pred = pred.copy()
        for col in pred.find_all(exp.Column):
            if col.table and col.table.lower() in {date_alias.lower(), "date_dim"}:
                col.set("table", None)
        return pred

    cte_where = _combine_and([_strip_alias(p) for p in date_preds])
    if cte_where is not None:
        cte_select.set("where", exp.Where(this=cte_where))

    # Replace date_dim table with CTE in outer query only
    _replace_table_name(node, "date_dim", cte_name)

    if date_dim.alias:
        date_dim.set("alias", date_dim.alias)

    # Remove date predicates from outer WHERE
    remaining = _combine_and([p.copy() for p in other_preds])
    if remaining is None:
        node.set("where", None)
    else:
        node.set("where", exp.Where(this=remaining))

    # Attach CTE using sqlglot's helper so it renders correctly
    node = node.with_(cte_name, cte_select, append=True, copy=False)
    return node


def _transform_push_pred(node: exp.Expression) -> Optional[exp.Expression]:
    return pushdown_predicates.pushdown_predicates(node)


def _transform_materialize_cte(node: exp.Expression) -> Optional[exp.Expression]:
    # Uses repeated subquery to CTE extraction (materialization hint not supported in sqlglot)
    return _apply_rewriter(node, RepeatedSubqueryToCTERewriter)


def _transform_flatten_subq(node: exp.Expression) -> Optional[exp.Expression]:
    # Try IN subquery to join first, then correlated subquery to join, then unnest
    rewritten = _apply_rewriter(node, InSubqueryToJoinRewriter)
    if rewritten is not None:
        return rewritten
    rewritten = _apply_rewriter(node, CorrelatedSubqueryToJoinRewriter)
    if rewritten is not None:
        return rewritten
    return unnest_subqueries.unnest_subqueries(node)


def _transform_reorder_join(node: exp.Expression) -> Optional[exp.Expression]:
    return optimize_joins.optimize_joins(node)


def _transform_inline_cte(node: exp.Expression) -> Optional[exp.Expression]:
    rewritten = _apply_rewriter(node, CTEInliner)
    if rewritten is None:
        return None
    # Clean up any now-unused CTEs left behind
    cleaned = _apply_rewriter(rewritten, UnusedCTERemover)
    cleaned = cleaned or rewritten
    return _remove_unused_ctes(cleaned)


def _transform_remove_redundant(node: exp.Expression) -> Optional[exp.Expression]:
    updated = _apply_rewriter(node, RedundantDistinctRemover) or node
    updated = _apply_rewriter(updated, RedundantPredicateRemover) or updated
    return updated


def _transform_consolidate_scans(node: exp.Expression) -> Optional[exp.Expression]:
    """Consolidate two CTE scans of the same table into one CASE-based scan."""
    if not isinstance(node, exp.Select):
        return None

    with_clause = node.find(exp.With)
    if not with_clause:
        return None

    ctes = [cte for cte in with_clause.find_all(exp.CTE)]
    if len(ctes) < 2:
        return None

    # Only handle the first two CTEs for a conservative rewrite
    cte_a, cte_b = ctes[0], ctes[1]
    if not cte_a.alias or not cte_b.alias:
        return None

    name_a = str(cte_a.alias).lower()
    name_b = str(cte_b.alias).lower()

    def _extract_simple_cte(cte: exp.CTE):
        inner = cte.find(exp.Select)
        if inner is None:
            return None
        from_table = inner.args.get("from_")
        if not from_table or not isinstance(from_table.this, exp.Table):
            return None
        table_name = str(from_table.this.this)

        group = inner.args.get("group")
        if not group:
            return None
        group_cols = [g for g in group.expressions if isinstance(g, exp.Column)]
        if not group_cols:
            return None

        agg_aliases = [e for e in inner.expressions if isinstance(e, exp.Alias)]
        if len(agg_aliases) != 1:
            return None
        agg_expr = agg_aliases[0].this
        if not isinstance(agg_expr, exp.Sum):
            return None
        agg_arg = agg_expr.this

        where = inner.args.get("where")
        if not where:
            return None

        return {
            "table": table_name,
            "group_cols": group_cols,
            "agg_alias": str(agg_aliases[0].alias),
            "agg_arg": agg_arg,
            "where": where.this,
        }

    a = _extract_simple_cte(cte_a)
    b = _extract_simple_cte(cte_b)
    if not a or not b:
        return None

    if a["table"].lower() != b["table"].lower():
        return None

    # Ensure group-by columns match by name
    a_groups = [c.name.lower() for c in a["group_cols"]]
    b_groups = [c.name.lower() for c in b["group_cols"]]
    if a_groups != b_groups:
        return None

    consolidated_name = _unique_cte_name(node, "scan_consolidated")
    select_exprs = [c.copy() for c in a["group_cols"]]

    def _case_sum(cond: exp.Expression, arg: exp.Expression, alias: str) -> exp.Alias:
        case = exp.Case(
            ifs=[exp.If(this=cond.copy(), true=arg.copy())],
            default=exp.Null(),
        )
        return exp.Alias(this=exp.Sum(this=case), alias=exp.to_identifier(alias))

    select_exprs.append(_case_sum(a["where"], a["agg_arg"], a["agg_alias"]))
    select_exprs.append(_case_sum(b["where"], b["agg_arg"], b["agg_alias"]))

    consolidated_select = exp.Select(expressions=select_exprs)
    consolidated_select = consolidated_select.from_(exp.Table(this=exp.to_identifier(a["table"])))
    consolidated_select.set("group", exp.Group(expressions=[c.copy() for c in a["group_cols"]]))

    consolidated_cte = exp.CTE(
        this=consolidated_select,
        alias=exp.TableAlias(this=exp.to_identifier(consolidated_name)),
    )

    # Replace CTEs in WITH
    remaining = [cte for cte in ctes if cte not in (cte_a, cte_b)]
    with_clause.set("expressions", remaining + [consolidated_cte])

    # Update main query: replace references to cte_a/cte_b with consolidated
    _replace_table_name(node, name_a, consolidated_name)
    _replace_table_name(node, name_b, consolidated_name)
    _replace_table_refs(node, name_a, consolidated_name)
    _replace_table_refs(node, name_b, consolidated_name)

    return node


TRANSFORM_REGISTRY: dict[str, Transformation] = {
    TransformID.OR_TO_UNION.value: _transform_or_to_union,
    TransformID.CORRELATED_TO_CTE.value: _transform_correlated_to_cte,
    TransformID.DATE_CTE_ISOLATION.value: _transform_date_cte_isolate,
    TransformID.PUSH_PREDICATE.value: _transform_push_pred,
    TransformID.CONSOLIDATE_SCANS.value: _transform_consolidate_scans,
    TransformID.MATERIALIZE_CTE.value: _transform_materialize_cte,
    TransformID.FLATTEN_SUBQUERY.value: _transform_flatten_subq,
    TransformID.REORDER_JOIN.value: _transform_reorder_join,
    TransformID.INLINE_CTE.value: _transform_inline_cte,
    TransformID.REMOVE_REDUNDANT.value: _transform_remove_redundant,
}


def get_all_transform_ids() -> list[str]:
    """Return all registered transform IDs."""
    # Respect KB order
    ids = get_transform_ids()
    return [tid for tid in ids if tid in TRANSFORM_REGISTRY]


def apply_transform(sql: str, rule_name: str, dialect: str = "duckdb") -> Optional[str]:
    """Apply a deterministic transform. Returns new SQL or None if no-op."""
    rule = TRANSFORM_REGISTRY.get(rule_name)
    if rule is None:
        raise ValueError(f"Unknown transform: {rule_name}")

    parsed = sqlglot.parse_one(sql, dialect=dialect)
    original_sql = parsed.sql(dialect=dialect)

    try:
        new_ast = rule(copy.deepcopy(parsed))
        if new_ast is None:
            return None
        new_sql = new_ast.sql(dialect=dialect)
    except Exception:
        return None

    if new_sql.strip() == original_sql.strip():
        return None

    return new_sql
