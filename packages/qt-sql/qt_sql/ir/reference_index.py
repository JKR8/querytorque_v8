"""Reference Index — collect all cross-script references from a ScriptIR.

Populates relation_reads, scalar_subqueries, function_calls, and
duplicate_expr_groups for safe global rewrites.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from sqlglot import exp

from .schema import (
    DuplicateGroup,
    ReferenceIndex,
    ScriptIR,
    StatementIR,
    UseSite,
    canonical_hash,
)

log = logging.getLogger(__name__)


def build_reference_index(script: ScriptIR) -> ReferenceIndex:
    """Build reference index from ScriptIR."""
    idx = ReferenceIndex()
    dialect = script.dialect.value

    for stmt in script.statements:
        _collect_relation_reads(idx, stmt)
        _collect_scalar_subqueries(idx, stmt, dialect)
        _collect_function_calls(idx, stmt, dialect)

    idx.duplicate_expr_groups = _find_duplicate_expressions(script, dialect)
    return idx


# ── Relation reads ─────────────────────────────────────────────────────


def _collect_relation_reads(idx: ReferenceIndex, stmt: StatementIR):
    for rel in stmt.reads:
        name = rel.name.lower()
        site = UseSite(
            statement_id=stmt.id,
            path=f"{stmt.id}.reads.{name}",
            snippet_hash=canonical_hash(rel.name),
        )
        idx.relation_reads.setdefault(name, []).append(site)


# ── Scalar subqueries ─────────────────────────────────────────────────


def _collect_scalar_subqueries(
    idx: ReferenceIndex, stmt: StatementIR, dialect: str
):
    if not stmt.ast:
        return

    for subq in stmt.ast.find_all(exp.Subquery):
        parent = subq.parent
        # Skip FROM / JOIN subqueries — those are derived tables, not scalar
        if isinstance(parent, (exp.From, exp.Join)):
            continue

        sql_text = subq.sql(dialect=dialect)
        site = UseSite(
            statement_id=stmt.id,
            path=_subquery_path(subq, stmt),
            snippet_hash=canonical_hash(sql_text),
        )
        idx.scalar_subqueries.append(site)


def _subquery_path(subq: exp.Subquery, stmt: StatementIR) -> str:
    parent = subq.parent
    if isinstance(parent, exp.Where):
        return f"{stmt.id}.where.subquery"
    if isinstance(parent, exp.EQ):
        gp = parent.parent
        if isinstance(gp, exp.Where):
            return f"{stmt.id}.where.eq.subquery"
        return f"{stmt.id}.pred.eq.subquery"
    if isinstance(parent, exp.Select):
        return f"{stmt.id}.select.subquery"
    return f"{stmt.id}.subquery"


# ── Function calls ────────────────────────────────────────────────────


def _collect_function_calls(
    idx: ReferenceIndex, stmt: StatementIR, dialect: str
):
    if not stmt.ast:
        return

    for func in stmt.ast.find_all(exp.Func):
        func_name = _func_name(func).lower()
        if not func_name:
            continue
        sql_text = func.sql(dialect=dialect)
        site = UseSite(
            statement_id=stmt.id,
            path=f"{stmt.id}.func.{func_name}",
            snippet_hash=canonical_hash(sql_text),
        )
        idx.function_calls.setdefault(func_name, []).append(site)


def _func_name(func: exp.Func) -> str:
    if isinstance(func, exp.Anonymous):
        return func.name
    return type(func).__name__


# ── Duplicate expression detection ────────────────────────────────────

MIN_COMPLEXITY = 50  # minimum chars to be "expensive"


def _find_duplicate_expressions(
    script: ScriptIR, dialect: str
) -> List[DuplicateGroup]:
    """Hash all non-trivial expression subtrees; return groups with 2+ hits."""
    expr_map: Dict[str, List[Tuple[str, str]]] = defaultdict(list)

    for stmt in script.statements:
        if not stmt.ast:
            continue
        for node in stmt.ast.walk():
            # Only consider functions and CASE (skip structural nodes)
            if not isinstance(node, (exp.Func, exp.Case)):
                continue
            sql = node.sql(dialect=dialect)
            if len(sql) < MIN_COMPLEXITY:
                continue
            h = canonical_hash(sql)
            expr_map[h].append((stmt.id, sql))

    groups: List[DuplicateGroup] = []
    for h, entries in expr_map.items():
        if len(entries) >= 2:
            sites = [
                UseSite(statement_id=sid, path=f"{sid}.expr", snippet_hash=h)
                for sid, _ in entries
            ]
            groups.append(
                DuplicateGroup(
                    canonical_hash=h,
                    canonical_sql=entries[0][1],
                    sites=sites,
                )
            )

    groups.sort(key=lambda g: len(g.canonical_sql), reverse=True)
    return groups
