"""Detector: Latest-date scalar subqueries.

Finds all (SELECT MAX(calendar_date) FROM X) and labels them as
``latest_date_filter.{table_name}``.
"""
from __future__ import annotations

import logging
from typing import List

from sqlglot import exp

from ..schema import ScriptIR, UseSite, canonical_hash

log = logging.getLogger(__name__)


def detect_latest_date_filters(script_ir: ScriptIR) -> List[UseSite]:
    """Label every scalar MAX(calendar_date) subquery.

    Returns UseSites with ``labels=["latest_date_filter.<table>"]``.
    Also attaches labels to the owning StatementIR.
    """
    sites: List[UseSite] = []
    dialect = script_ir.dialect.value

    for stmt in script_ir.statements:
        if not stmt.ast:
            continue

        for subq in stmt.ast.find_all(exp.Subquery):
            inner = subq.this
            if not isinstance(inner, exp.Select):
                continue

            # Look for MAX(calendar_date)
            has_max_cal = False
            for func in inner.find_all(exp.Max):
                col = func.this
                if isinstance(col, exp.Column) and col.name.lower() == "calendar_date":
                    has_max_cal = True
                    break
            if not has_max_cal:
                continue

            # Find the source table
            table_name = None
            for tbl in inner.find_all(exp.Table):
                table_name = tbl.name.lower()
                break
            if not table_name:
                continue

            label = f"latest_date_filter.{table_name}"
            sql_text = subq.sql(dialect=dialect)

            site = UseSite(
                statement_id=stmt.id,
                path=f"{stmt.id}.where.eq.subquery",
                snippet_hash=canonical_hash(sql_text),
                labels=[label],
            )
            sites.append(site)

            if label not in stmt.labels:
                stmt.labels.append(label)

            log.debug("Found latest-date filter: %s in %s", label, stmt.id)

    return sites
