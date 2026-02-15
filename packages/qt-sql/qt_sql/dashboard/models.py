"""Data models for the forensic intelligence dashboard.

JSON-serializable dataclasses that serve as the API contract between
the collector pipeline and the frontend.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Forensic per-query types
# ---------------------------------------------------------------------------

@dataclass
class ForensicTransformMatch:
    """A single transform match for a query."""
    id: str
    overlap: float              # 0.0-1.0
    gap: str = ""               # Engine blind spot targeted
    family: str = ""            # A-F family code


@dataclass
class QErrorEntry:
    """Per-query cardinality estimation error (PG/DuckDB only)."""
    severity: str = ""          # CATASTROPHIC_BLINDNESS / MAJOR / MODERATE / MINOR / ACCURATE
    direction: str = ""         # UNDER_EST / OVER_EST / ZERO_EST
    worst_node: str = ""        # Operator with largest error
    worst_est: int = 0          # Estimated cardinality at worst node
    worst_act: int = 0          # Actual cardinality at worst node
    max_q_error: float = 0.0    # Maximum q-error ratio
    locus: str = ""             # Where errors occurred (JOIN, SCAN, etc.)
    pathology_routing: str = "" # Matched pathology codes (P0,P2,...)
    structural_flags: str = ""  # Pipe-separated flags
    n_signals: int = 0          # Number of estimation mismatches


@dataclass
class ForensicQuery:
    """Per-query forensic intelligence — primary data unit for the Forensic tab."""
    query_id: str               # Canonical q{N}
    runtime_ms: float           # From EXPLAIN timing (-1 if unavailable)
    bucket: str                 # HIGH / MEDIUM / LOW / SKIP

    # Structural analysis
    top_overlap: float = 0.0    # Best transform overlap ratio (0.0-1.0)
    tractability: int = 0       # Count of transforms with >=60% overlap (capped at 4)
    n_matches: int = 0          # Count of transforms with >=25% overlap
    top_transform: str = ""     # Best matching transform ID
    priority_score: float = 0.0 # Composite priority score

    # Transform detail
    matched_transforms: List[ForensicTransformMatch] = field(default_factory=list)

    # Cost context (populated after sorting by runtime)
    pct_of_total: float = 0.0   # Runtime as fraction of total workload
    cumulative_pct: float = 0.0 # Cumulative fraction for Pareto
    cost_rank: int = 0          # 1-based rank by runtime (1 = most expensive)

    # Q-Error (optional — PG/DuckDB only)
    qerror: Optional[QErrorEntry] = None

    # Structural flags from q-error analysis
    structural_flags: List[str] = field(default_factory=list)

    # EXPLAIN availability
    has_explain: bool = False
    explain_text: str = ""      # Truncated EXPLAIN plan text for drawer


# ---------------------------------------------------------------------------
# Engine profile types
# ---------------------------------------------------------------------------

@dataclass
class EngineStrength:
    id: str
    summary: str
    implication: str = ""


@dataclass
class EngineGap:
    id: str
    priority: str = ""
    what: str = ""
    why: str = ""
    opportunity: str = ""
    what_worked: Any = ""       # str or list
    n_queries_matched: int = 0  # Computed: queries with transform targeting this gap
    matched_query_ids: List[str] = field(default_factory=list)


@dataclass
class EngineProfile:
    engine: str
    version_tested: str = ""
    briefing_note: str = ""
    strengths: List[EngineStrength] = field(default_factory=list)
    gaps: List[EngineGap] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Master container
# ---------------------------------------------------------------------------

@dataclass
class WorkloadProfile:
    """Master container — everything the dashboard needs."""
    benchmark_name: str
    engine: str
    collected_at: str                           # ISO timestamp
    forensic: ForensicSummary
    execution: ExecutionSummary
    impact: ImpactSummary


# ---------------------------------------------------------------------------
# Legacy + shared types
# ---------------------------------------------------------------------------

@dataclass
class CostEntry:
    """Legacy cost entry — kept for backward compat with existing frontend."""
    query_id: str
    runtime_ms: float
    pct_of_total: float                         # 0.0-1.0
    cumulative_pct: float                       # running total for Pareto
    bucket: str
    detected_patterns: List[str] = field(default_factory=list)


@dataclass
class PatternStat:
    pattern_id: str
    pattern_name: str
    query_count: int
    avg_overlap: float
    target_gap: str = ""                        # Engine blind spot targeted


@dataclass
class PatternCoverage:
    queries_with_detection: int
    queries_without_detection: int
    top_patterns: List[PatternStat] = field(default_factory=list)


@dataclass
class ResourceProfile:
    """Aggregated resource state from PG system profile."""
    shared_buffers: str
    work_mem_default: str
    max_parallel_workers: int
    effective_cache_size: str
    storage_type: str                           # "SSD" or "HDD"


# ---------------------------------------------------------------------------
# Forensic aggregate
# ---------------------------------------------------------------------------

@dataclass
class ForensicSummary:
    total_queries: int
    total_runtime_ms: float
    queries: List[ForensicQuery] = field(default_factory=list)
    cost_concentration: List[CostEntry] = field(default_factory=list)
    bucket_distribution: Dict[str, int] = field(default_factory=dict)
    pattern_coverage: PatternCoverage = field(
        default_factory=lambda: PatternCoverage(0, 0))
    engine_profile: Optional[EngineProfile] = None
    resource_profile: Optional[ResourceProfile] = None
    dominant_pathology: str = ""
    estimated_opportunity_ms: float = 0.0


# ---------------------------------------------------------------------------
# Execution types
# ---------------------------------------------------------------------------

@dataclass
class RunSummary:
    run_id: str
    timestamp: str
    mode: str
    total_queries: int
    completed: int
    status_counts: Dict[str, int] = field(default_factory=dict)
    elapsed_seconds: float = 0.0
    total_speedup_weighted: float = 0.0


@dataclass
class QueryResult:
    query_id: str
    status: str                                 # WIN/IMPROVED/NEUTRAL/REGRESSION/ERROR
    speedup: float = 0.0
    baseline_ms: float = 0.0
    optimized_ms: float = 0.0
    transform_used: str = ""
    set_local_commands: List[str] = field(default_factory=list)
    worker_id: Optional[int] = None


@dataclass
class ExecutionSummary:
    runs: List[RunSummary] = field(default_factory=list)
    latest_results: Dict[str, QueryResult] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Impact types
# ---------------------------------------------------------------------------

@dataclass
class ResourceImpact:
    """Aggregate resource changes from all SET LOCAL commands."""
    queries_with_set_local: int = 0
    work_mem_total: str = "0MB"
    work_mem_peak_factor: float = 0.0
    parallel_workers_total: int = 0
    conflicts: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ImpactSummary:
    total_baseline_ms: float = 0.0
    total_optimized_ms: float = 0.0
    total_savings_ms: float = 0.0
    total_savings_pct: float = 0.0
    status_counts: Dict[str, int] = field(default_factory=dict)
    regressions: List[QueryResult] = field(default_factory=list)
    resource_impact: Optional[ResourceImpact] = None
