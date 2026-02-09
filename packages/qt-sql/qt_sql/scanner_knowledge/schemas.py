"""Scanner knowledge schemas — Layer 1 (Observation) and Layer 2 (Finding).

Layer 1: ScannerObservation — one JSONL line per (query, flags) pair.
         Machine-generated from explore/scan data, no interpretation.

Layer 2: ScannerFinding — LLM-extracted claim about engine behavior.
         Evidence-backed, human-reviewed before downstream use.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Flag → category mapping ──────────────────────────────────────────────

FLAG_CATEGORIES: Dict[str, str] = {
    "enable_nestloop": "join_method",
    "enable_hashjoin": "join_method",
    "enable_mergejoin": "join_method",
    "enable_seqscan": "scan_method",
    "enable_indexscan": "scan_method",
    "enable_bitmapscan": "scan_method",
    "work_mem": "memory",
    "hash_mem_multiplier": "memory",
    "max_parallel_workers_per_gather": "parallelism",
    "jit": "jit",
    "random_page_cost": "cost_model",
    "effective_cache_size": "cost_model",
    "join_collapse_limit": "join_order",
    "from_collapse_limit": "join_order",
}


def derive_category(flags: Dict[str, str]) -> str:
    """Assign category from which flags are toggled.

    Single-category flags → that category.
    Multiple categories → "compound".
    """
    categories = set()
    for flag_name in flags:
        cat = FLAG_CATEGORIES.get(flag_name)
        if cat:
            categories.add(cat)
    if len(categories) == 1:
        return categories.pop()
    if len(categories) > 1:
        return "compound"
    return "unknown"


def derive_combo_name(flags: Dict[str, str]) -> str:
    """Deterministic combo name: sorted flag keys joined with '+'."""
    return "+".join(sorted(flags.keys()))


# ── Layer 1: ScannerObservation ──────────────────────────────────────────

@dataclass
class ScannerObservation:
    """One JSONL line — raw observation from explore/scan/stacking."""

    # Identity
    query_id: str
    flags: Dict[str, str]       # Canonical merge key
    source: str                 # "explore" | "scan" | "explore+scan" | "stacking"

    # Category for grouping
    category: str               # "join_method" | "scan_method" | "memory" | etc.

    # Derived combo name (human-readable, never used as key)
    combo_name: str

    # 2-3 sentence summary
    summary: str

    # Plan-space observation (from explore, nullable)
    plan_changed: Optional[bool] = None
    cost_ratio: Optional[float] = None

    # Wall-clock observation (from scan, nullable)
    wall_speedup: Optional[float] = None
    baseline_ms: Optional[float] = None
    combo_ms: Optional[float] = None
    rows_match: Optional[bool] = None

    # Vulnerability classification
    vulnerability_types: List[str] = field(default_factory=list)

    # Query-level context (denormalized)
    n_plan_changers: Optional[int] = None
    n_distinct_plans: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query_id": self.query_id,
            "flags": self.flags,
            "source": self.source,
            "category": self.category,
            "combo_name": self.combo_name,
            "summary": self.summary,
            "plan_changed": self.plan_changed,
            "cost_ratio": self.cost_ratio,
            "wall_speedup": self.wall_speedup,
            "baseline_ms": self.baseline_ms,
            "combo_ms": self.combo_ms,
            "rows_match": self.rows_match,
            "vulnerability_types": self.vulnerability_types,
            "n_plan_changers": self.n_plan_changers,
            "n_distinct_plans": self.n_distinct_plans,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ScannerObservation:
        return cls(
            query_id=d["query_id"],
            flags=d["flags"],
            source=d["source"],
            category=d["category"],
            combo_name=d["combo_name"],
            summary=d["summary"],
            plan_changed=d.get("plan_changed"),
            cost_ratio=d.get("cost_ratio"),
            wall_speedup=d.get("wall_speedup"),
            baseline_ms=d.get("baseline_ms"),
            combo_ms=d.get("combo_ms"),
            rows_match=d.get("rows_match"),
            vulnerability_types=d.get("vulnerability_types", []),
            n_plan_changers=d.get("n_plan_changers"),
            n_distinct_plans=d.get("n_distinct_plans"),
        )

    def to_jsonl(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    def merge_key(self) -> tuple:
        """Canonical merge key for deduplication."""
        return (self.query_id, frozenset(self.flags.items()))


# ── Layer 2: ScannerFinding ──────────────────────────────────────────────

@dataclass
class ScannerFinding:
    """One JSON object — LLM-extracted claim about engine behavior."""

    id: str                         # "SF-001"
    claim: str                      # Human-readable claim
    category: str                   # "join_sensitivity" | "memory" | etc.

    # Evidence
    supporting_queries: List[str]
    evidence_summary: str
    evidence_count: int
    contradicting_count: int

    # Boundary conditions
    boundaries: List[str]

    # Mechanism
    mechanism: str

    # Confidence
    confidence: str                 # "high" | "medium" | "low"
    confidence_rationale: str

    # What workers should do
    implication: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "claim": self.claim,
            "category": self.category,
            "supporting_queries": self.supporting_queries,
            "evidence_summary": self.evidence_summary,
            "evidence_count": self.evidence_count,
            "contradicting_count": self.contradicting_count,
            "boundaries": self.boundaries,
            "mechanism": self.mechanism,
            "confidence": self.confidence,
            "confidence_rationale": self.confidence_rationale,
            "implication": self.implication,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ScannerFinding:
        return cls(
            id=d["id"],
            claim=d["claim"],
            category=d["category"],
            supporting_queries=d.get("supporting_queries", []),
            evidence_summary=d.get("evidence_summary", ""),
            evidence_count=d.get("evidence_count", 0),
            contradicting_count=d.get("contradicting_count", 0),
            boundaries=d.get("boundaries", []),
            mechanism=d.get("mechanism", ""),
            confidence=d.get("confidence", "medium"),
            confidence_rationale=d.get("confidence_rationale", ""),
            implication=d.get("implication", ""),
        )
