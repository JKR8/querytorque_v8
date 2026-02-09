"""Probe schemas — data structures for frontier probe results.

AttackResult: A single probe attack (rewrite + validation outcome)
DiscoverySummary: Gaps discovered per query
ProbeResult: Full probe output for one query
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class AttackResult:
    """A single probe attack — one rewrite targeting one optimizer weakness."""

    attack_id: int  # 1-4 within a probe
    target_node: str  # EXPLAIN node targeted
    gap_hypothesis: str  # testable claim about optimizer weakness
    structural_preconditions: str  # generalizable detection rule
    mechanism: str  # how rewrite exploits the gap
    expected_plan_change: str  # what EXPLAIN should show after
    semantic_risk: str  # what could break
    optimized_sql: str  # the attack SQL

    # Populated after validation
    status: str = ""  # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR | FAIL
    speedup: float = 0.0
    error_messages: list[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "attack_id": self.attack_id,
            "target_node": self.target_node,
            "gap_hypothesis": self.gap_hypothesis,
            "structural_preconditions": self.structural_preconditions,
            "mechanism": self.mechanism,
            "expected_plan_change": self.expected_plan_change,
            "semantic_risk": self.semantic_risk,
            "optimized_sql": self.optimized_sql,
            "status": self.status,
            "speedup": self.speedup,
            "error_messages": self.error_messages,
        }


@dataclass
class DiscoverySummary:
    """Gaps discovered from probing a single query."""

    new_gaps: list[str] = field(default_factory=list)
    extended_gaps: list[str] = field(default_factory=list)
    negative_results: list[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "new_gaps": self.new_gaps,
            "extended_gaps": self.extended_gaps,
            "negative_results": self.negative_results,
        }


@dataclass
class ProbeResult:
    """Full probe output for one query — attacks + discovery."""

    query_id: str
    engine: str
    original_sql: str
    attacks: list[AttackResult] = field(default_factory=list)
    discovery_summary: Optional[DiscoverySummary] = None
    probe_response: str = ""  # raw LLM response for audit
    round_num: int = 0

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "query_id": self.query_id,
            "engine": self.engine,
            "round_num": self.round_num,
            "n_attacks": len(self.attacks),
            "attacks": [a.to_dict() for a in self.attacks],
        }
        if self.discovery_summary:
            d["discovery_summary"] = self.discovery_summary.to_dict()
        return d

    @property
    def n_wins(self) -> int:
        return sum(1 for a in self.attacks if a.status == "WIN")

    @property
    def n_improved(self) -> int:
        return sum(1 for a in self.attacks if a.status == "IMPROVED")

    @property
    def best_speedup(self) -> float:
        if not self.attacks:
            return 0.0
        return max(a.speedup for a in self.attacks)
