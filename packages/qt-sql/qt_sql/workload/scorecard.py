"""Workload Scorecard — final deliverable for workload optimization.

Compiles per-query results + fleet actions + business case into
a structured scorecard with markdown rendering.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class QueryScore:
    """Per-query result in the scorecard."""
    query_id: str
    tier: str                    # SKIP | TIER_1 | TIER_2 | TIER_3
    status: str                  # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR | SKIP
    speedup: float = 1.0
    technique: str = ""
    latency_before_ms: float = 0
    latency_after_ms: float = 0
    fits_scenario: bool = True
    escalation_level: int = 0   # 0=none, 1=constraint_feedback, 2=human_escalation, 3=accept
    failure_reason: str = ""


@dataclass
class FleetActionScore:
    """Fleet-level action in the scorecard."""
    action: str
    action_type: str
    queries_affected: int = 0
    impact: str = ""


@dataclass
class BusinessCase:
    """Business case calculator."""
    original_cost_monthly: str = ""
    optimized_cost_monthly: str = ""
    savings_monthly: str = ""
    savings_annual: str = ""
    methodology: str = "Benchmark-validated query optimization with workload-aware triage"


@dataclass
class WorkloadScorecard:
    """Complete workload optimization scorecard."""
    workload_id: str = ""
    client: str = ""
    date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    # Sizing
    original_warehouse: str = ""
    target_warehouse: str = ""
    achieved_warehouse: str = ""

    # Fleet actions
    fleet_actions: List[FleetActionScore] = field(default_factory=list)

    # Query results
    query_scores: List[QueryScore] = field(default_factory=list)

    # Business case
    business_case: BusinessCase = field(default_factory=BusinessCase)

    # Residuals
    residuals: List[QueryScore] = field(default_factory=list)

    @property
    def total_queries(self) -> int:
        return len(self.query_scores)

    @property
    def skipped(self) -> int:
        return sum(1 for q in self.query_scores if q.status == "SKIP")

    @property
    def wins(self) -> int:
        return sum(1 for q in self.query_scores if q.status in ("WIN", "IMPROVED"))

    @property
    def failures(self) -> int:
        return sum(1 for q in self.query_scores if q.status in ("REGRESSION", "ERROR"))

    @property
    def pass_rate(self) -> float:
        total = self.total_queries - self.skipped
        if total == 0:
            return 1.0
        passing = sum(
            1 for q in self.query_scores
            if q.status in ("WIN", "IMPROVED", "NEUTRAL") and q.fits_scenario
        )
        return passing / total


def compile_scorecard(
    query_results: List[Dict[str, Any]],
    fleet_actions: Optional[List[Dict[str, Any]]] = None,
    workload_id: str = "",
    original_warehouse: str = "",
    target_warehouse: str = "",
) -> WorkloadScorecard:
    """Compile query results into a workload scorecard.

    Args:
        query_results: List of per-query result dicts with keys:
            query_id, tier, status, speedup, technique, latency_before_ms,
            latency_after_ms, fits_scenario, escalation_level, failure_reason
        fleet_actions: Optional list of fleet action dicts
        workload_id: Workload identifier
        original_warehouse: Original warehouse/instance size
        target_warehouse: Target warehouse/instance size
    """
    scorecard = WorkloadScorecard(
        workload_id=workload_id,
        original_warehouse=original_warehouse,
        target_warehouse=target_warehouse,
    )

    for qr in query_results:
        qs = QueryScore(
            query_id=qr.get("query_id", ""),
            tier=qr.get("tier", ""),
            status=qr.get("status", "NEUTRAL"),
            speedup=qr.get("speedup", 1.0),
            technique=qr.get("technique", ""),
            latency_before_ms=qr.get("latency_before_ms", 0),
            latency_after_ms=qr.get("latency_after_ms", 0),
            fits_scenario=qr.get("fits_scenario", True),
            escalation_level=qr.get("escalation_level", 0),
            failure_reason=qr.get("failure_reason", ""),
        )
        scorecard.query_scores.append(qs)

        # Track residuals (queries that didn't fit)
        if not qs.fits_scenario or qs.escalation_level >= 2:
            scorecard.residuals.append(qs)

    if fleet_actions:
        for fa in fleet_actions:
            scorecard.fleet_actions.append(FleetActionScore(
                action=fa.get("action", ""),
                action_type=fa.get("action_type", ""),
                queries_affected=fa.get("queries_affected", 0),
                impact=fa.get("impact", ""),
            ))

    # Determine achieved warehouse
    if scorecard.pass_rate >= 0.95:
        scorecard.achieved_warehouse = target_warehouse
    else:
        scorecard.achieved_warehouse = original_warehouse

    return scorecard


def render_scorecard_markdown(scorecard: WorkloadScorecard) -> str:
    """Render scorecard as markdown."""
    lines = [
        "# Workload Optimization Scorecard",
        "",
        f"**Workload:** {scorecard.workload_id}",
        f"**Date:** {scorecard.date}",
        "",
        "## Sizing",
        "",
        f"| Original | Target | Achieved |",
        f"|----------|--------|----------|",
        f"| {scorecard.original_warehouse} | {scorecard.target_warehouse} | {scorecard.achieved_warehouse} |",
        "",
    ]

    # Fleet actions
    if scorecard.fleet_actions:
        lines.extend([
            "## Fleet Actions",
            "",
            "| Action | Type | Queries Affected | Impact |",
            "|--------|------|-----------------|--------|",
        ])
        for fa in scorecard.fleet_actions:
            lines.append(
                f"| {fa.action} | {fa.action_type} | {fa.queries_affected} | {fa.impact} |"
            )
        lines.append("")

    # Summary
    lines.extend([
        "## Summary",
        "",
        f"- **Total queries:** {scorecard.total_queries}",
        f"- **Skipped (already pass):** {scorecard.skipped}",
        f"- **Wins/Improved:** {scorecard.wins}",
        f"- **Failures:** {scorecard.failures}",
        f"- **Pass rate:** {scorecard.pass_rate:.0%}",
        f"- **Residuals:** {len(scorecard.residuals)}",
        "",
    ])

    # Per-query results table
    lines.extend([
        "## Per-Query Results",
        "",
        "| Query | Tier | Status | Speedup | Technique | Fits? |",
        "|-------|------|--------|---------|-----------|-------|",
    ])
    for qs in scorecard.query_scores:
        fits = "Yes" if qs.fits_scenario else "No"
        lines.append(
            f"| {qs.query_id} | {qs.tier} | {qs.status} | "
            f"{qs.speedup:.2f}x | {qs.technique} | {fits} |"
        )
    lines.append("")

    # Residuals
    if scorecard.residuals:
        lines.extend([
            "## Residuals (queries that could not fit target)",
            "",
        ])
        for r in scorecard.residuals:
            lines.append(f"- **{r.query_id}**: {r.failure_reason or 'No viable rewrite found'}")
            if r.escalation_level >= 3:
                lines.append(f"  Recommendation: Keep on {scorecard.original_warehouse}")
        lines.append("")

    # Business case
    bc = scorecard.business_case
    if bc.savings_monthly:
        lines.extend([
            "## Business Case",
            "",
            f"- **Original cost:** {bc.original_cost_monthly}/month",
            f"- **Optimized cost:** {bc.optimized_cost_monthly}/month",
            f"- **Monthly savings:** {bc.savings_monthly}",
            f"- **Annual savings:** {bc.savings_annual}",
            f"- **Methodology:** {bc.methodology}",
        ])

    return "\n".join(lines)
