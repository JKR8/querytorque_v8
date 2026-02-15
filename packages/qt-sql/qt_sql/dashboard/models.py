"""Data models for the forensic intelligence dashboard.

JSON-serializable dataclasses that serve as the API contract between
the collector pipeline and the frontend.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class WorkloadProfile:
    """Master container — everything the dashboard needs."""
    benchmark_name: str
    engine: str
    collected_at: str                           # ISO timestamp
    forensic: ForensicSummary
    execution: ExecutionSummary
    impact: ImpactSummary


@dataclass
class CostEntry:
    query_id: str
    runtime_ms: float
    pct_of_total: float                         # 0.0–1.0
    cumulative_pct: float                       # running total for Pareto
    bucket: str
    detected_patterns: List[str] = field(default_factory=list)


@dataclass
class PatternStat:
    pattern_id: str
    pattern_name: str
    query_count: int
    avg_overlap: float


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


@dataclass
class ForensicSummary:
    total_queries: int
    total_runtime_ms: float
    cost_concentration: List[CostEntry] = field(default_factory=list)
    bucket_distribution: Dict[str, int] = field(default_factory=dict)
    pattern_coverage: PatternCoverage = field(
        default_factory=lambda: PatternCoverage(0, 0))
    resource_profile: Optional[ResourceProfile] = None


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
