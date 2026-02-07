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


# =============================================================================
# Seed + Run + Blackboard Data Structures
# =============================================================================


@dataclass
class ChecklistItem:
    """Single item in the seed manifest checklist."""
    status: str = "pending"  # pending | pass | fail
    count: int = 0
    errors: List[str] = field(default_factory=list)
    missing: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"status": self.status}
        if self.count:
            d["count"] = self.count
        if self.errors:
            d["errors"] = self.errors
        if self.missing:
            d["missing"] = self.missing
        if self.extra:
            d.update(self.extra)
        return d


@dataclass
class SeedManifest:
    """Manifest for a benchmark seed folder (seed/manifest.yaml)."""
    name: str
    engine: str
    scale_factor: int = 10
    created: str = ""

    # Checklist items
    db_connection: ChecklistItem = field(default_factory=ChecklistItem)
    queries_loaded: ChecklistItem = field(default_factory=ChecklistItem)
    queries_parseable: ChecklistItem = field(default_factory=ChecklistItem)
    explains_gathered: ChecklistItem = field(default_factory=ChecklistItem)
    intents_attached: ChecklistItem = field(default_factory=ChecklistItem)
    catalog_rules_loaded: ChecklistItem = field(default_factory=ChecklistItem)
    faiss_index_ready: ChecklistItem = field(default_factory=ChecklistItem)

    # Validation config
    validation_method: str = "3-run"
    timeout_seconds: int = 300

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "engine": self.engine,
            "scale_factor": self.scale_factor,
            "created": self.created,
            "checklist": {
                "db_connection": self.db_connection.to_dict(),
                "queries_loaded": self.queries_loaded.to_dict(),
                "queries_parseable": self.queries_parseable.to_dict(),
                "explains_gathered": self.explains_gathered.to_dict(),
                "intents_attached": self.intents_attached.to_dict(),
                "catalog_rules_loaded": self.catalog_rules_loaded.to_dict(),
                "faiss_index_ready": self.faiss_index_ready.to_dict(),
            },
            "validation": {
                "method": self.validation_method,
                "timeout_seconds": self.timeout_seconds,
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SeedManifest":
        checklist = data.get("checklist", {})
        validation = data.get("validation", {})

        def _load_item(key: str) -> ChecklistItem:
            raw = checklist.get(key, {})
            return ChecklistItem(
                status=raw.get("status", "pending"),
                count=raw.get("count", 0),
                errors=raw.get("errors", []),
                missing=raw.get("missing", []),
                extra={k: v for k, v in raw.items()
                       if k not in ("status", "count", "errors", "missing")},
            )

        return cls(
            name=data.get("name", ""),
            engine=data.get("engine", ""),
            scale_factor=data.get("scale_factor", 10),
            created=data.get("created", ""),
            db_connection=_load_item("db_connection"),
            queries_loaded=_load_item("queries_loaded"),
            queries_parseable=_load_item("queries_parseable"),
            explains_gathered=_load_item("explains_gathered"),
            intents_attached=_load_item("intents_attached"),
            catalog_rules_loaded=_load_item("catalog_rules_loaded"),
            faiss_index_ready=_load_item("faiss_index_ready"),
            validation_method=validation.get("method", "3-run"),
            timeout_seconds=validation.get("timeout_seconds", 300),
        )


@dataclass
class RunConfig:
    """Configuration for a named optimization run."""
    name: str
    created: str = ""
    mode: str = "swarm"  # standard | expert | swarm
    n_workers: int = 4
    parent_run: Optional[str] = None
    query_filter: Optional[List[str]] = None
    target_speedup: float = 2.0
    max_iterations: int = 3

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "created": self.created,
            "mode": self.mode,
            "n_workers": self.n_workers,
            "parent_run": self.parent_run,
            "query_filter": self.query_filter,
            "target_speedup": self.target_speedup,
            "max_iterations": self.max_iterations,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunConfig":
        return cls(
            name=data.get("name", ""),
            created=data.get("created", ""),
            mode=data.get("mode", "swarm"),
            n_workers=data.get("n_workers", 4),
            parent_run=data.get("parent_run"),
            query_filter=data.get("query_filter"),
            target_speedup=data.get("target_speedup", 2.0),
            max_iterations=data.get("max_iterations", 3),
        )


@dataclass
class BlackboardEntry:
    """Knowledge entry captured after each worker optimization attempt.

    Stored in runs/<name>/blackboard/raw/<query_id>/worker_<N>.json.
    """
    query_id: str
    worker_id: int
    run_name: str
    timestamp: str

    # Context
    query_intent: str = ""
    query_fingerprint: str = ""
    examples_used: List[str] = field(default_factory=list)
    strategy: str = ""

    # Outcome
    status: str = ""  # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR
    speedup: float = 0.0
    transforms_applied: List[str] = field(default_factory=list)
    error_category: Optional[str] = None
    error_messages: List[str] = field(default_factory=list)

    # Knowledge (the valuable part)
    what_worked: Optional[str] = None
    why_it_worked: Optional[str] = None
    what_failed: Optional[str] = None
    why_it_failed: Optional[str] = None
    principle: Optional[str] = None

    # Metadata
    reviewed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "query_id": self.query_id,
            "worker_id": self.worker_id,
            "run_name": self.run_name,
            "timestamp": self.timestamp,
            "query_intent": self.query_intent,
            "query_fingerprint": self.query_fingerprint,
            "examples_used": self.examples_used,
            "strategy": self.strategy,
            "status": self.status,
            "speedup": self.speedup,
            "transforms_applied": self.transforms_applied,
            "error_category": self.error_category,
            "error_messages": self.error_messages,
            "what_worked": self.what_worked,
            "why_it_worked": self.why_it_worked,
            "what_failed": self.what_failed,
            "why_it_failed": self.why_it_failed,
            "principle": self.principle,
            "reviewed": self.reviewed,
        }
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BlackboardEntry":
        return cls(
            query_id=data.get("query_id", ""),
            worker_id=data.get("worker_id", 0),
            run_name=data.get("run_name", ""),
            timestamp=data.get("timestamp", ""),
            query_intent=data.get("query_intent", ""),
            query_fingerprint=data.get("query_fingerprint", ""),
            examples_used=data.get("examples_used", []),
            strategy=data.get("strategy", ""),
            status=data.get("status", ""),
            speedup=data.get("speedup", 0.0),
            transforms_applied=data.get("transforms_applied", []),
            error_category=data.get("error_category"),
            error_messages=data.get("error_messages", []),
            what_worked=data.get("what_worked"),
            why_it_worked=data.get("why_it_worked"),
            what_failed=data.get("what_failed"),
            why_it_failed=data.get("why_it_failed"),
            principle=data.get("principle"),
            reviewed=data.get("reviewed", False),
        )


@dataclass
class KnowledgePrinciple:
    """A verified optimization principle from collated blackboard entries."""
    id: str
    name: str
    what: str
    why: str
    when: str
    when_not: str = ""
    verified_speedups: List[float] = field(default_factory=list)
    avg_speedup: float = 0.0
    queries: List[str] = field(default_factory=list)
    transforms: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "what": self.what,
            "why": self.why,
            "when": self.when,
            "when_not": self.when_not,
            "verified_speedups": self.verified_speedups,
            "avg_speedup": self.avg_speedup,
            "queries": self.queries,
            "transforms": self.transforms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KnowledgePrinciple":
        return cls(**{k: data.get(k, v.default if hasattr(v, 'default') else "")
                      for k, v in cls.__dataclass_fields__.items()
                      if k in data})


@dataclass
class KnowledgeAntiPattern:
    """A verified anti-pattern from collated blackboard entries."""
    id: str
    name: str
    mechanism: str
    observed_regressions: List[float] = field(default_factory=list)
    queries: List[str] = field(default_factory=list)
    avoid_when: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "mechanism": self.mechanism,
            "observed_regressions": self.observed_regressions,
            "queries": self.queries,
            "avoid_when": self.avoid_when,
        }


@dataclass
class GlobalKnowledge:
    """Collated global knowledge for a dataset."""
    dataset: str
    last_updated: str = ""
    source_runs: List[str] = field(default_factory=list)
    principles: List[KnowledgePrinciple] = field(default_factory=list)
    anti_patterns: List[KnowledgeAntiPattern] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset": self.dataset,
            "last_updated": self.last_updated,
            "source_runs": self.source_runs,
            "principles": [p.to_dict() for p in self.principles],
            "anti_patterns": [a.to_dict() for a in self.anti_patterns],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GlobalKnowledge":
        return cls(
            dataset=data.get("dataset", ""),
            last_updated=data.get("last_updated", ""),
            source_runs=data.get("source_runs", []),
            principles=[
                KnowledgePrinciple.from_dict(p)
                for p in data.get("principles", [])
            ],
            anti_patterns=[
                KnowledgeAntiPattern(
                    id=a.get("id", ""),
                    name=a.get("name", ""),
                    mechanism=a.get("mechanism", ""),
                    observed_regressions=a.get("observed_regressions", []),
                    queries=a.get("queries", []),
                    avoid_when=a.get("avoid_when", ""),
                )
                for a in data.get("anti_patterns", [])
            ],
        )
