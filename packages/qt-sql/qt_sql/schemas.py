"""QueryTorque schemas.

Data structures for the optimization pipeline:
- Validation: ValidationStatus, ValidationResult
- Pipeline: BenchmarkConfig, EdgeContract, NodeRewriteResult, PipelineResult
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional


class OptimizationMode(str, Enum):
    """Optimization mode selection."""
    BEAM = "beam"           # Automated search: analyst → N workers → validate → snipe
    REASONING = "reasoning"  # Analyst-only 2-shot reasoning loop


class ValidationStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"


@dataclass
class ValidationResult:
    worker_id: int
    status: ValidationStatus
    speedup: float
    error: Optional[str]
    optimized_sql: str
    errors: list[str] = None  # All errors for learning
    error_category: Optional[str] = None  # syntax | semantic | timeout | execution | unknown
    explain_plan: Optional[str] = None  # EXPLAIN ANALYZE plan text for candidate

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


# =============================================================================
# Pipeline Data Structures (5-phase logical-tree pipeline)
# =============================================================================


@dataclass
class ColumnMismatch:
    """Column count/name mismatch details."""
    original_columns: List[str]
    rewrite_columns: List[str]
    missing: List[str]  # In original but not rewrite
    extra: List[str]    # In rewrite but not original


@dataclass
class RowCountDiff:
    """Row count difference on mini dataset."""
    original_count: int
    rewrite_count: int
    diff: int  # rewrite - original
    sample_pct: float  # What % of data was tested


@dataclass
class ValueDiff:
    """Single value difference."""
    row_index: int
    column: str
    original_value: Any
    rewrite_value: Any


@dataclass
class SemanticValidationResult:
    """Result from 3-tier mini validation."""
    tier_passed: int  # 0 (failed all) | 1 (structural) | 2 (logic) | 3 (all)
    passed: bool      # True if tier_passed >= 2
    errors: List[str]

    # Tier 1 failures (structural)
    syntax_error: Optional[str] = None
    column_mismatch: Optional[ColumnMismatch] = None

    # Tier 2 failures (logic on mini dataset)
    row_count_diff: Optional[RowCountDiff] = None
    value_diffs: Optional[List[ValueDiff]] = None
    sql_diff: Optional[str] = None  # Unified diff for LLM

    # Timing
    validation_time_ms: float = 0.0


@dataclass
class BenchmarkConfig:
    """Configuration loaded from benchmarks/<name>/config.json."""
    engine: str           # "duckdb" | "postgresql" | "snowflake"
    benchmark: str        # "tpcds" | "dsb"
    db_path_or_dsn: str   # DuckDB path or PostgreSQL DSN
    benchmark_dsn: str    # DSN/path used for benchmarking (may differ from EXPLAIN DSN)
    scale_factor: int
    timeout_seconds: int
    validation_method: str  # "race" | "3-run" | "5-run"
    n_queries: int
    workers_state_0: int
    workers_state_n: int
    promote_threshold: float
    race_min_runtime_ms: float = 2000.0   # Race only triggers if original >= this
    race_min_margin: float = 0.05         # Candidate must beat original by this fraction

    # Semantic validation options
    semantic_validation_enabled: bool = False
    semantic_sample_pct: float = 2.0  # TABLESAMPLE percentage
    semantic_timeout_ms: int = 30_000  # 30s max per mini query

    # Tiered patch mode (analyst + worker split) — legacy flag
    tiered_patch_enabled: bool = False
    # Legacy per-role model fields (deprecated; global pipeline model is authoritative)
    analyst_model: Optional[str] = None
    worker_model: Optional[str] = None
    target_speedup: float = 100.0
    snipe_rounds: int = 2  # Number of snipe rounds after initial analyst iteration

    # Beam execution mode (single-mode runtime; legacy values tolerated)
    beam_mode: str = "beam"       # canonical: "beam" | "reasoning" (legacy: "wide", "auto", "focused")
    enable_reasoning_mode: bool = False  # Safety gate: reasoning path disabled unless explicitly enabled
    wide_max_probes: int = 16     # Max probes for wide mode
    wide_worker_parallelism: int = 8  # Max concurrent worker LLM calls per query in beam mode
    focused_max_sorties: int = 5  # Legacy field (unused in single-mode runtime)
    wide_dispatcher_model: Optional[str] = None  # Legacy override field (deprecated)
    wide_worker_model: Optional[str] = None  # Legacy override field (deprecated)

    @classmethod
    def from_file(cls, config_path: str | Path) -> BenchmarkConfig:
        """Load config from a JSON file."""
        path = Path(config_path)
        data = json.loads(path.read_text())
        db_path_or_dsn = data.get("db_path") or data.get("dsn", "")
        return cls(
            engine=data["engine"],
            benchmark=data["benchmark"],
            db_path_or_dsn=db_path_or_dsn,
            benchmark_dsn=data.get("benchmark_dsn") or db_path_or_dsn,
            scale_factor=data.get("scale_factor", 10),
            timeout_seconds=data.get("timeout_seconds", 300),
            validation_method=data.get("validation_method", "race"),
            n_queries=data.get("n_queries", 99),
            workers_state_0=data.get("workers_state_0", 5),
            workers_state_n=data.get("workers_state_n", 1),
            promote_threshold=data.get("promote_threshold", 1.05),
            race_min_runtime_ms=data.get("race_min_runtime_ms", 2000.0),
            race_min_margin=data.get("race_min_margin", 0.05),
            semantic_validation_enabled=data.get("semantic_validation_enabled", False),
            semantic_sample_pct=data.get("semantic_sample_pct", 2.0),
            semantic_timeout_ms=data.get("semantic_timeout_ms", 30_000),
            tiered_patch_enabled=data.get("tiered_patch_enabled", False),
            analyst_model=data.get("analyst_model"),
            worker_model=data.get("worker_model"),
            target_speedup=data.get("target_speedup", 100.0),
            snipe_rounds=data.get("snipe_rounds", 2),
            beam_mode=data.get("beam_mode", "beam"),
            enable_reasoning_mode=data.get("enable_reasoning_mode", False),
            wide_max_probes=data.get("wide_max_probes", 16),
            wide_worker_parallelism=data.get("wide_worker_parallelism", 8),
            focused_max_sorties=data.get("focused_max_sorties", 5),
            wide_dispatcher_model=data.get("wide_dispatcher_model"),
            wide_worker_model=data.get("wide_worker_model"),
        )


@dataclass
class EdgeContract:
    """Contract for a logical-tree edge — what flows between nodes."""
    columns: List[str]
    grain: str
    filters: List[str]
    cardinality_estimate: Optional[int] = None


@dataclass
class NodeRewriteResult:
    """Result from rewriting a single node in Phase 3."""
    node_id: str
    status: str           # "rewritten" | "skipped" | "error" | "kept_original"
    original_sql: str
    rewritten_sql: Optional[str] = None
    output_contract: Optional[EdgeContract] = None
    pattern_applied: Optional[str] = None
    retries: int = 0


@dataclass
class PromotionAnalysis:
    """Analysis generated when promoting a winning query to the next state.

    Captures what the transform did, why it worked, and what to try next.
    Included in the prompt for the next state so the LLM has full context.
    """
    query_id: str
    original_sql: str
    optimized_sql: str
    speedup: float
    transforms: List[str]
    analysis: str            # LLM-generated reasoning about what the transform did
    suggestions: str         # LLM-generated ideas for further optimization
    state_promoted_from: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query_id": self.query_id,
            "original_sql": self.original_sql,
            "optimized_sql": self.optimized_sql,
            "speedup": self.speedup,
            "transforms": self.transforms,
            "analysis": self.analysis,
            "suggestions": self.suggestions,
            "state_promoted_from": self.state_promoted_from,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PromotionAnalysis:
        return cls(
            query_id=data["query_id"],
            original_sql=data["original_sql"],
            optimized_sql=data["optimized_sql"],
            speedup=data.get("speedup", 0.0),
            transforms=data.get("transforms", []),
            analysis=data.get("analysis", ""),
            suggestions=data.get("suggestions", ""),
            state_promoted_from=data.get("state_promoted_from", 0),
        )


@dataclass
class PipelineResult:
    """Complete result from a single query through the 5-phase pipeline."""
    query_id: str
    status: str           # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR
    speedup: float
    original_sql: str
    optimized_sql: str
    nodes_rewritten: List[str] = field(default_factory=list)
    transforms_applied: List[str] = field(default_factory=list)
    prompt: Optional[str] = None
    response: Optional[str] = None  # LLM rewrite response
    analysis: Optional[str] = None  # Raw analyst LLM response
    analysis_prompt: Optional[str] = None  # Analyst prompt sent to LLM
    analysis_formatted: Optional[str] = None  # Formatted analysis injected into rewrite prompt


@dataclass
class WorkerResult:
    """Result from a single optimization worker."""
    worker_id: int
    strategy: str
    examples_used: List[str]
    optimized_sql: str
    speedup: float
    status: str
    transforms: List[str] = field(default_factory=list)
    hint: str = ""
    error_message: Optional[str] = None
    error_messages: List[str] = field(default_factory=list)
    error_category: Optional[str] = None
    exploratory: bool = False  # True for Worker 4 (exploration budget)
    set_local_config: Optional[Dict[str, Any]] = None  # PG tuning: {"params": {...}, "reasoning": "..."}
    set_local_commands: List[str] = field(default_factory=list)  # ["SET LOCAL work_mem = '256MB'", ...]
    explain_plan: Optional[str] = None  # EXPLAIN ANALYZE plan text for optimized query
    semantic_validation: Optional[SemanticValidationResult] = None  # Semantic validation result from pre-validation

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "worker_id": self.worker_id,
            "strategy": self.strategy,
            "examples_used": self.examples_used,
            "optimized_sql": self.optimized_sql,
            "speedup": self.speedup,
            "status": self.status,
            "transforms": self.transforms,
            "hint": self.hint,
            "error_message": self.error_message,
            "error_messages": self.error_messages,
            "error_category": self.error_category,
        }
        if self.exploratory:
            d["exploratory"] = True
        if self.set_local_config:
            d["set_local_config"] = self.set_local_config
        if self.set_local_commands:
            d["set_local_commands"] = self.set_local_commands
        if self.explain_plan:
            d["explain_plan"] = self.explain_plan
        if self.semantic_validation:
            # Convert semantic validation to dict format
            sem = self.semantic_validation
            sem_dict = {
                "tier_passed": sem.tier_passed,
                "passed": sem.passed,
                "errors": sem.errors,
                "validation_time_ms": sem.validation_time_ms,
            }
            if sem.syntax_error:
                sem_dict["syntax_error"] = sem.syntax_error
            if sem.column_mismatch:
                cm = sem.column_mismatch
                sem_dict["column_mismatch"] = {
                    "original_columns": cm.original_columns,
                    "rewrite_columns": cm.rewrite_columns,
                    "missing": cm.missing,
                    "extra": cm.extra,
                }
            if sem.row_count_diff:
                rcd = sem.row_count_diff
                sem_dict["row_count_diff"] = {
                    "original_count": rcd.original_count,
                    "rewrite_count": rcd.rewrite_count,
                    "diff": rcd.diff,
                    "sample_pct": rcd.sample_pct,
                }
            if sem.value_diffs:
                sem_dict["value_diffs"] = [
                    {
                        "row_index": vd.row_index,
                        "column": vd.column,
                        "original_value": str(vd.original_value),
                        "rewrite_value": str(vd.rewrite_value),
                    }
                    for vd in sem.value_diffs[:10]  # Limit to first 10
                ]
            if sem.sql_diff:
                sem_dict["sql_diff"] = sem.sql_diff[:1000]  # Truncate for serialization
            d["semantic_validation"] = sem_dict
        return d


@dataclass
class RunMeta:
    """Metadata for a benchmark run — full traceability for every API-cost run."""
    run_id: str
    started_at: str
    finished_at: str = ""
    duration_seconds: float = 0.0
    git_sha: str = ""
    git_branch: str = ""
    git_dirty: bool = False
    model: str = ""
    provider: str = ""
    config_snapshot: Dict[str, Any] = field(default_factory=dict)
    workers: int = 4
    queries_attempted: int = 0
    queries_improved: int = 0
    total_api_calls: int = 0
    estimated_cost_usd: float = 0.0
    validation_method: str = ""
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "git_sha": self.git_sha,
            "git_branch": self.git_branch,
            "git_dirty": self.git_dirty,
            "model": self.model,
            "provider": self.provider,
            "config_snapshot": self.config_snapshot,
            "workers": self.workers,
            "queries_attempted": self.queries_attempted,
            "queries_improved": self.queries_improved,
            "total_api_calls": self.total_api_calls,
            "estimated_cost_usd": self.estimated_cost_usd,
            "validation_method": self.validation_method,
            "notes": self.notes,
        }

    @classmethod
    def from_file(cls, path: str | Path) -> "RunMeta":
        data = json.loads(Path(path).read_text())
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def save(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict(), indent=2))

    @staticmethod
    def generate_run_id() -> str:
        return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    @staticmethod
    def capture_git_info() -> Dict[str, Any]:
        """Capture current git SHA, branch, dirty status."""
        import subprocess
        info: Dict[str, Any] = {"git_sha": "", "git_branch": "", "git_dirty": False}
        try:
            info["git_sha"] = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=5,
            ).stdout.strip()
            info["git_branch"] = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True, text=True, timeout=5,
            ).stdout.strip()
            dirty_check = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True, text=True, timeout=5,
            )
            info["git_dirty"] = bool(dirty_check.stdout.strip())
        except Exception:
            pass
        return info


@dataclass
class SessionResult:
    """Result from an optimization session (any mode)."""
    query_id: str
    mode: str  # OptimizationMode value
    best_speedup: float
    best_sql: str
    original_sql: str
    best_transforms: List[str] = field(default_factory=list)
    status: str = ""  # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR
    iterations: List[Any] = field(default_factory=list)
    n_iterations: int = 0
    n_api_calls: int = 0
    beam_cost_usd: float = 0.0
    beam_cost_priced_calls: int = 0
    beam_cost_unpriced_calls: int = 0
    beam_token_totals: Dict[str, int] = field(default_factory=dict)
    api_call_costs: List[Dict[str, Any]] = field(default_factory=list)
    config_changes: List[str] = field(default_factory=list)
