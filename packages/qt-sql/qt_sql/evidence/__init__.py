"""Evidence Bundle — unified pre-computed evidence from query profiles.

Wraps existing extractors (qerror.py, explain_signals.py, pipeline._parse_logical_tree)
into a single structured bundle. No new extraction logic — just unification.

The orchestrator enriches memory.budget and memory.status from the scenario card.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class SpillInfo:
    """Spill detection from runtime profile."""
    detected: bool = False
    details: str = ""
    root_cause: str = ""


@dataclass
class PruningInfo:
    """Partition/index pruning quality."""
    ratio: str = ""
    status: str = ""  # good | poor | terrible
    blocking_factor: str = ""


@dataclass
class MemoryInfo:
    """Memory usage from runtime profile."""
    peak: str = ""
    budget: str = ""        # Enriched by orchestrator from scenario card
    status: str = ""        # Enriched by orchestrator: within_budget | over_budget


@dataclass
class EstimateInfo:
    """Cardinality estimation accuracy."""
    worst_node: str = ""
    ratio: str = ""
    direction: str = ""     # OVER | UNDER
    locus: str = ""         # SCAN | JOIN | AGGREGATE


@dataclass
class ServiceEligibility:
    """Whether an engine service could help."""
    service: str = ""
    eligible: bool = False
    reason: str = ""


@dataclass
class CostSpineEntry:
    """One operator in the cost spine."""
    node_id: str = ""
    operator: str = ""
    cost_pct: float = 0.0
    estimated_rows: int = 0
    actual_rows: int = 0
    notes: str = ""


@dataclass
class RuntimeProfile:
    """Runtime profile extracted from EXPLAIN ANALYZE."""
    spill: SpillInfo = field(default_factory=SpillInfo)
    pruning: PruningInfo = field(default_factory=PruningInfo)
    memory: MemoryInfo = field(default_factory=MemoryInfo)
    estimates: EstimateInfo = field(default_factory=EstimateInfo)
    service_eligibility: List[ServiceEligibility] = field(default_factory=list)


@dataclass
class EvidenceBundle:
    """Unified evidence bundle per query.

    Assembled from existing extractors:
    - cost_spine: from pipeline._parse_logical_tree() per-node costs
    - runtime_profile.estimates: from qerror.py Q-Error analysis (85% accurate)
    - runtime_profile.spill: from explain_signals.py
    - runtime_profile.pruning: from explain_signals.py
    - runtime_profile.memory: from pg_tuning.build_resource_envelope() (PG only)
    """
    query_id: str = ""
    query_sql: str = ""
    cost_spine: List[CostSpineEntry] = field(default_factory=list)
    runtime_profile: RuntimeProfile = field(default_factory=RuntimeProfile)
    frequency: Optional[int] = None        # executions_per_day (workload mode)
    current_cost: Optional[str] = None     # estimated_monthly (workload mode)
    vital_signs_text: str = ""             # Compact text from extract_vital_signs


def extract_evidence_bundle(
    query_id: str,
    query_sql: str,
    explain_text: Optional[str] = None,
    plan_json: Optional[Any] = None,
    qerror_analysis: Optional[Any] = None,
    vital_signs: Optional[str] = None,
    resource_envelope: Optional[str] = None,
    dialect: str = "duckdb",
) -> EvidenceBundle:
    """Assemble evidence bundle from existing extractor outputs.

    This function wraps outputs from existing extractors into a unified
    structure. It does NOT perform new extraction — callers should run
    extractors first and pass their results.

    Args:
        query_id: Query identifier
        query_sql: Original SQL text
        explain_text: Formatted EXPLAIN plan text
        plan_json: Raw plan JSON (for cost spine extraction)
        qerror_analysis: QErrorAnalysis from qerror.py
        vital_signs: Compact vital signs text from explain_signals.py
        resource_envelope: Text from pg_tuning.build_resource_envelope()
        dialect: SQL dialect
    """
    bundle = EvidenceBundle(
        query_id=query_id,
        query_sql=query_sql,
        vital_signs_text=vital_signs or "",
    )

    # Extract Q-Error info into estimates
    if qerror_analysis is not None:
        bundle.runtime_profile.estimates = EstimateInfo(
            worst_node=getattr(qerror_analysis, "worst_node_type", ""),
            ratio=f"{getattr(qerror_analysis, 'max_q_error', 0):.0f}x",
            direction=getattr(qerror_analysis, "direction", ""),
            locus=getattr(qerror_analysis, "locus", ""),
        )

    # Parse vital signs for spill/pruning indicators
    if vital_signs:
        vs_lower = vital_signs.lower()
        if "spill" in vs_lower or "external merge" in vs_lower or "temp" in vs_lower:
            bundle.runtime_profile.spill = SpillInfo(
                detected=True,
                details=_extract_line(vital_signs, "spill"),
            )
        if "pruning" in vs_lower or "rows removed" in vs_lower:
            bundle.runtime_profile.pruning = PruningInfo(
                status="poor" if "poor" in vs_lower or "terrible" in vs_lower else "unknown",
            )

    # Extract cost spine from plan_json if available
    if plan_json:
        bundle.cost_spine = _extract_cost_spine(plan_json, dialect)

    # Memory info from resource envelope
    if resource_envelope:
        bundle.runtime_profile.memory.peak = resource_envelope

    return bundle


def _extract_line(text: str, keyword: str) -> str:
    """Extract the first line containing a keyword."""
    for line in text.split("\n"):
        if keyword.lower() in line.lower():
            return line.strip()
    return ""


def _extract_cost_spine(
    plan_json: Any, dialect: str
) -> List[CostSpineEntry]:
    """Extract top cost-contributing operators from plan JSON.

    Handles both DuckDB (dict) and PostgreSQL (list) plan formats.
    Returns top 5 operators by cost percentage.
    """
    entries: List[CostSpineEntry] = []

    try:
        if isinstance(plan_json, list) and plan_json:
            # PostgreSQL format: [{"Plan": {...}}]
            _walk_pg_plan(plan_json[0].get("Plan", {}), entries)
        elif isinstance(plan_json, dict):
            # DuckDB format: {"children": [...], "name": "...", ...}
            _walk_duckdb_plan(plan_json, entries)
    except Exception as e:
        logger.warning(f"Cost spine extraction failed: {e}")

    # Sort by cost_pct descending, return top 5
    entries.sort(key=lambda e: e.cost_pct, reverse=True)
    return entries[:5]


def _walk_pg_plan(node: Dict, entries: List[CostSpineEntry], depth: int = 0) -> None:
    """Walk PostgreSQL plan tree, extracting cost info."""
    if not isinstance(node, dict):
        return

    total_cost = node.get("Total Cost", 0)
    actual_rows = node.get("Actual Rows", 0)
    plan_rows = node.get("Plan Rows", 0)
    node_type = node.get("Node Type", "Unknown")

    if total_cost > 0:
        entry = CostSpineEntry(
            node_id=f"d{depth}",
            operator=node_type,
            cost_pct=0,  # Will be normalized using Total Cost
            estimated_rows=plan_rows,
            actual_rows=actual_rows,
        )
        entry._raw_cost = total_cost  # type: ignore[attr-defined]
        entries.append(entry)

    for child in node.get("Plans", []):
        _walk_pg_plan(child, entries, depth + 1)

    # Normalize cost_pct after full walk using Total Cost, not row counts
    if depth == 0 and entries:
        root_cost = node.get("Total Cost", 0) or 1
        for e in entries:
            e.cost_pct = round(e._raw_cost / max(root_cost, 1) * 100, 1)


def _walk_duckdb_plan(node: Dict, entries: List[CostSpineEntry], depth: int = 0) -> None:
    """Walk DuckDB plan tree, extracting cost info."""
    if not isinstance(node, dict):
        return

    name = node.get("name", "Unknown")
    timing = node.get("timing", 0)
    cardinality = node.get("cardinality", 0)
    extra_info = node.get("extra_info", "")

    if timing > 0 or cardinality > 0:
        entry = CostSpineEntry(
            node_id=f"d{depth}",
            operator=name,
            cost_pct=0,  # Will be normalized using timing
            estimated_rows=0,
            actual_rows=cardinality,
            notes=extra_info[:100] if extra_info else "",
        )
        entry._raw_cost = timing  # type: ignore[attr-defined]
        entries.append(entry)

    for child in node.get("children", []):
        _walk_duckdb_plan(child, entries, depth + 1)

    # Normalize by timing (not row counts)
    if depth == 0 and entries:
        total_timing = sum(e._raw_cost for e in entries) or 1  # type: ignore[attr-defined]
        for e in entries:
            e.cost_pct = round(e._raw_cost / total_timing * 100, 1)  # type: ignore[attr-defined]


def render_evidence_for_prompt(bundle: EvidenceBundle) -> str:
    """Render evidence bundle as text for LLM prompt injection."""
    lines = ["[EVIDENCE BUNDLE]"]

    if bundle.vital_signs_text:
        lines.append("")
        lines.append("VITAL SIGNS:")
        lines.append(bundle.vital_signs_text)

    # Cost spine
    if bundle.cost_spine:
        lines.append("")
        lines.append("COST SPINE (top operators):")
        for entry in bundle.cost_spine:
            parts = [f"  {entry.operator}"]
            if entry.actual_rows:
                parts.append(f"rows={entry.actual_rows:,}")
            if entry.notes:
                parts.append(entry.notes[:60])
            lines.append(" | ".join(parts))

    # Runtime profile
    rp = bundle.runtime_profile
    if rp.spill.detected:
        lines.append(f"\nSPILL: {rp.spill.details or 'detected'}")
    if rp.estimates.ratio:
        lines.append(
            f"\nESTIMATES: worst={rp.estimates.worst_node} "
            f"ratio={rp.estimates.ratio} "
            f"direction={rp.estimates.direction} locus={rp.estimates.locus}"
        )
    if rp.memory.peak:
        lines.append(f"\nMEMORY: peak={rp.memory.peak}")
        if rp.memory.budget:
            lines.append(f"  budget={rp.memory.budget} status={rp.memory.status}")

    return "\n".join(lines)
