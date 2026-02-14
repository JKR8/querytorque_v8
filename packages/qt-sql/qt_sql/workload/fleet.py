"""Fleet-Level Pattern Detection — Tier 1 actions.

Analyzes all queries together to detect shared patterns:
- Shared scans (→ index/clustering key recommendation)
- Config opportunities (→ global SET changes)
- Statistics staleness (→ ANALYZE recommendations)
- Shared subexpressions (→ materialized view candidates)
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class FleetAction:
    """A fleet-level action that benefits multiple queries."""
    action_type: str          # index | config | statistics | materialized_view
    action: str               # DDL or SET command
    rationale: str
    queries_affected: List[str] = field(default_factory=list)
    estimated_impact: str = ""


@dataclass
class FleetAnalysis:
    """Result of fleet-level pattern detection."""
    actions: List[FleetAction] = field(default_factory=list)
    shared_scans: Dict[str, List[str]] = field(default_factory=dict)  # table → query_ids
    config_opportunities: List[FleetAction] = field(default_factory=list)
    statistics_stale: List[FleetAction] = field(default_factory=list)


def detect_fleet_patterns(
    queries: List[Dict[str, Any]],
    engine: str = "postgres",
) -> FleetAnalysis:
    """Detect fleet-level optimization patterns across all queries.

    Args:
        queries: List of dicts with keys:
            - query_id: str
            - sql: str
            - tables: List[str] (optional — extracted from SQL if missing)
            - filter_columns: List[str] (optional)
            - spill_detected: bool (optional)
            - bad_estimates: bool (optional)
            - estimate_tables: List[str] (optional — tables with bad estimates)
        engine: Target engine name

    Returns:
        FleetAnalysis with detected patterns and recommended actions.
    """
    analysis = FleetAnalysis()

    # Detect shared table scans
    table_queries: Dict[str, Set[str]] = defaultdict(set)
    filter_col_queries: Dict[str, Set[str]] = defaultdict(set)

    for q in queries:
        qid = q["query_id"]
        tables = q.get("tables", _extract_tables_from_sql(q.get("sql", "")))
        for t in tables:
            table_queries[t].add(qid)
        for col in q.get("filter_columns", []):
            filter_col_queries[col].add(qid)

    # Shared scans: tables scanned by 5+ queries → index candidate
    for table, qids in table_queries.items():
        if len(qids) >= 5:
            analysis.shared_scans[table] = sorted(qids)
            analysis.actions.append(FleetAction(
                action_type="index",
                action=f"Consider index on frequently-scanned table: {table}",
                rationale=f"Scanned by {len(qids)} queries",
                queries_affected=sorted(qids),
            ))

    # Config opportunities: N queries with same bottleneck
    spill_queries = [q["query_id"] for q in queries if q.get("spill_detected")]
    if len(spill_queries) >= 3 and engine in ("postgres", "postgresql"):
        action = FleetAction(
            action_type="config",
            action="SET work_mem = '256MB'",
            rationale=f"{len(spill_queries)} queries show spill — increase work_mem fleet-wide",
            queries_affected=spill_queries,
        )
        analysis.config_opportunities.append(action)
        analysis.actions.append(action)

    # Statistics staleness: bad estimates across queries for same table
    estimate_tables: Dict[str, Set[str]] = defaultdict(set)
    for q in queries:
        if q.get("bad_estimates"):
            for t in q.get("estimate_tables", []):
                estimate_tables[t].add(q["query_id"])

    for table, qids in estimate_tables.items():
        if len(qids) >= 3:
            action = FleetAction(
                action_type="statistics",
                action=f"ANALYZE {table}",
                rationale=f"Estimates off by >10x for {table} across {len(qids)} queries",
                queries_affected=sorted(qids),
            )
            analysis.statistics_stale.append(action)
            analysis.actions.append(action)

    logger.info(
        f"Fleet analysis: {len(analysis.actions)} actions detected "
        f"({len(analysis.shared_scans)} shared scans, "
        f"{len(analysis.config_opportunities)} config, "
        f"{len(analysis.statistics_stale)} stats)"
    )

    return analysis


def _extract_tables_from_sql(sql: str) -> List[str]:
    """Best-effort table extraction from SQL text.

    Uses sqlglot if available, falls back to regex.
    """
    try:
        import sqlglot
        parsed = sqlglot.parse_one(sql)
        tables = []
        for table in parsed.find_all(sqlglot.exp.Table):
            name = table.name
            if name and name.lower() not in ("dual", "generate_series"):
                tables.append(name.lower())
        return list(set(tables))
    except Exception:
        pass

    # Fallback: basic regex
    import re
    pattern = r'(?:FROM|JOIN)\s+(\w+)'
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return list(set(m.lower() for m in matches if m.lower() not in ("select", "where", "as")))
