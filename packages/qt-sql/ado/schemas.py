"""ADO schemas (standalone).

Data structures for the ADO pipeline:
- Validation: ValidationStatus, ValidationResult
- Pipeline: BenchmarkConfig, EdgeContract, NodeRewriteResult, PipelineResult
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


class OptimizationMode(str, Enum):
    """Optimization mode selection for the ADO pipeline."""
    STANDARD = "standard"   # Fast: skip analyst, single iteration
    EXPERT = "expert"       # Iterative with analyst failure analysis (default)
    SWARM = "swarm"         # Multi-worker fan-out with snipe refinement


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

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


# =============================================================================
# Pipeline Data Structures (5-phase DAG pipeline)
# =============================================================================


@dataclass
class BenchmarkConfig:
    """Configuration loaded from benchmarks/<name>/config.json."""
    engine: str           # "duckdb" | "postgresql" | "snowflake"
    benchmark: str        # "tpcds" | "dsb"
    db_path_or_dsn: str   # DuckDB path or PostgreSQL DSN
    scale_factor: int
    timeout_seconds: int
    validation_method: str  # "3-run" | "5-run"
    n_queries: int
    workers_state_0: int
    workers_state_n: int
    promote_threshold: float

    @classmethod
    def from_file(cls, config_path: str | Path) -> BenchmarkConfig:
        """Load config from a JSON file."""
        path = Path(config_path)
        data = json.loads(path.read_text())
        return cls(
            engine=data["engine"],
            benchmark=data["benchmark"],
            db_path_or_dsn=data.get("db_path") or data.get("dsn", ""),
            scale_factor=data.get("scale_factor", 10),
            timeout_seconds=data.get("timeout_seconds", 300),
            validation_method=data.get("validation_method", "3-run"),
            n_queries=data.get("n_queries", 99),
            workers_state_0=data.get("workers_state_0", 5),
            workers_state_n=data.get("workers_state_n", 1),
            promote_threshold=data.get("promote_threshold", 1.05),
        )


@dataclass
class EdgeContract:
    """Contract for a DAG edge — what flows between nodes."""
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
    """Result from a single optimization worker (used in swarm mode)."""
    worker_id: int
    strategy: str
    examples_used: List[str]
    optimized_sql: str
    speedup: float
    status: str
    transforms: List[str] = field(default_factory=list)
    hint: str = ""
    error_message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "strategy": self.strategy,
            "examples_used": self.examples_used,
            "optimized_sql": self.optimized_sql,
            "speedup": self.speedup,
            "status": self.status,
            "transforms": self.transforms,
            "hint": self.hint,
            "error_message": self.error_message,
        }


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
