"""Build learning blackboard from swarm batch or global best-of-all-sources.

Mode 1 (Swarm): Reads a swarm batch directory and produces:
  1. blackboard/raw/<query_id>/worker_NN.json  — one BlackboardEntry per worker
  2. blackboard/collated.json                  — KnowledgePrinciple[] + KnowledgeAntiPattern[]
  3. benchmarks/<name>/knowledge/<dataset>.json — GlobalKnowledge for prompt injection
     (also copied to batch_dir/knowledge/ for provenance)

  Engine/dataset auto-detected from benchmarks/<name>/config.json.

Mode 2 (Global --global): Aggregates the BEST optimization per query across ALL
historical sources (Swarm, Retry4W, Retry3W, Kimi, V2, Evo, analyst_mode, etc.)
with full provenance: optimized SQL, model, run, reasoning, transforms.
  Output: benchmarks/duckdb_tpcds/knowledge/duckdb_tpcds.json

No LLM calls. Purely deterministic extraction from existing files.

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    # DuckDB TPC-DS swarm blackboard
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.build_blackboard \\
        packages/qt-sql/ado/benchmarks/duckdb_tpcds/swarm_batch_20260208_102033
    # PostgreSQL DSB swarm blackboard
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.build_blackboard \\
        packages/qt-sql/ado/benchmarks/postgres_dsb/swarm_batch_20260208_142643
    # Global blackboard (DuckDB TPC-DS only)
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.build_blackboard --global
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Schemas (matching feature/ado-blackboard worktree exactly)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class BlackboardEntry:
    """Knowledge entry captured after each worker optimization attempt."""

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
    status: str = ""  # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR | FAIL
    speedup: float = 0.0
    transforms_applied: List[str] = field(default_factory=list)
    error_category: Optional[str] = None
    error_messages: List[str] = field(default_factory=list)

    # Knowledge
    what_worked: Optional[str] = None
    why_it_worked: Optional[str] = None
    what_failed: Optional[str] = None
    why_it_failed: Optional[str] = None
    principle: Optional[str] = None

    # Metadata
    reviewed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
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
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            what=data.get("what", ""),
            why=data.get("why", ""),
            when=data.get("when", ""),
            when_not=data.get("when_not", ""),
            verified_speedups=data.get("verified_speedups", []),
            avg_speedup=data.get("avg_speedup", 0.0),
            queries=data.get("queries", []),
            transforms=data.get("transforms", []),
        )


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


# ─────────────────────────────────────────────────────────────────────────────
# Global blackboard schemas
# ─────────────────────────────────────────────────────────────────────────────


def normalize_query_id(raw: str) -> str:
    """Normalize query IDs: 'query_88' → 'q88', '88' → 'q88', 'q88' → 'q88', 'query_23a' → 'q23a'."""
    raw = raw.strip().lower()
    # Strip 'query_' prefix from swarm dirs
    if raw.startswith("query_"):
        raw = raw[6:]
    # Strip 'q' prefix if already present, then re-add
    if raw.startswith("q"):
        return raw
    # Bare number or number+letter
    return f"q{raw}"


@dataclass
class SourceAttempt:
    """A single optimization attempt from any source."""

    source: str  # "Swarm", "Retry4W", "Retry3W", "Kimi", "V2", "Evo", "analyst_mode", etc.
    run: str  # e.g. "swarm_batch_20260208_102033", "retry_4worker_20260206_004710"
    model: str  # "deepseek-reasoner", "kimi-k2.5", etc.
    worker_id: Optional[int] = None
    iteration: int = 0
    strategy: str = ""

    speedup: float = 0.0
    original_ms: float = 0.0
    optimized_ms: float = 0.0
    status: str = ""  # "pass", "fail", "error", etc.
    rows_match: bool = True

    optimized_sql: Optional[str] = None
    original_sql: Optional[str] = None
    reasoning: Optional[str] = None
    changes_description: Optional[str] = None
    transforms: List[str] = field(default_factory=list)
    examples_used: List[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "run": self.run,
            "model": self.model,
            "worker_id": self.worker_id,
            "iteration": self.iteration,
            "strategy": self.strategy,
            "speedup": self.speedup,
            "original_ms": self.original_ms,
            "optimized_ms": self.optimized_ms,
            "status": self.status,
            "rows_match": self.rows_match,
            "has_sql": self.optimized_sql is not None,
            "has_reasoning": self.reasoning is not None,
            "transforms": self.transforms,
        }


@dataclass
class GlobalBlackboardQuery:
    """Best-of entry for a single query in the global blackboard."""

    query_id: str
    status: str  # from leaderboard: WIN, IMPROVED, NEUTRAL, REGRESSION
    best_speedup: float
    original_ms: float
    optimized_ms: float

    original_sql: Optional[str] = None
    optimized_sql: Optional[str] = None
    transforms: List[str] = field(default_factory=list)
    principle: str = ""
    changes_description: Optional[str] = None
    reasoning: Optional[str] = None

    provenance: Dict[str, Any] = field(default_factory=dict)
    rows_match: bool = True
    all_attempts: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query_id": self.query_id,
            "status": self.status,
            "best_speedup": self.best_speedup,
            "original_ms": self.original_ms,
            "optimized_ms": self.optimized_ms,
            "original_sql": self.original_sql,
            "optimized_sql": self.optimized_sql,
            "transforms": self.transforms,
            "principle": self.principle,
            "changes_description": self.changes_description,
            "reasoning": self.reasoning,
            "provenance": self.provenance,
            "rows_match": self.rows_match,
            "all_attempts": self.all_attempts,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Transform → principle mapping (from gold examples)
# ─────────────────────────────────────────────────────────────────────────────

TRANSFORM_PRINCIPLES: Dict[str, str] = {
    "decorrelate": "Correlated subqueries re-execute per outer row; converting to JOIN eliminates per-row overhead",
    "early_filter": "Applying selective filters early reduces intermediate row counts before expensive operations",
    "pushdown": "Pushing predicates closer to table scans reduces data volume in upper operators",
    "date_cte_isolate": "Pre-filtering date dimension into CTE reduces hash join probe table from 73K to ~365 rows",
    "dimension_cte_isolate": "Pre-filtering all dimension tables into CTEs avoids repeated full-table scans",
    "prefetch_fact_join": "Pre-joining filtered dimensions with fact table before aggregation reduces join input",
    "multi_dimension_prefetch": "Pre-filtering multiple dimension tables in parallel reduces join fan-out",
    "multi_date_range_cte": "Separate CTEs for each date alias avoids ambiguous multi-way date joins",
    "single_pass_aggregation": "Consolidating repeated scans into CASE aggregates reduces I/O from N scans to 1",
    "or_to_union": "Converting OR to UNION ALL lets optimizer choose independent index paths per branch",
    "intersect_to_exists": "Replacing INTERSECT with EXISTS avoids materializing full intermediate sets",
    "materialize_cte": "Materializing a CTE used multiple times prevents redundant re-computation",
    "union_cte_split": "Splitting complex UNION into separate CTEs enables per-branch optimization",
}

# Principle "when" hints (from gold examples + benchmark experience)
PRINCIPLE_WHEN: Dict[str, str] = {
    "decorrelate": "Query has correlated subquery in WHERE or SELECT that references outer table",
    "early_filter": "Selective filters on dimension tables are applied late, after expensive joins",
    "pushdown": "WHERE predicates reference columns from tables deep in the join tree",
    "date_cte_isolate": "Query joins date_dim on multiple conditions (year, month, etc.) with fact tables",
    "dimension_cte_isolate": "Query joins 2+ dimension tables that could each be pre-filtered independently",
    "prefetch_fact_join": "Query joins filtered dates/dims with large fact table; pre-join reduces probe size",
    "multi_dimension_prefetch": "Query references multiple dimension tables (date + store, date + item, etc.)",
    "multi_date_range_cte": "Query uses the same date_dim table under 2+ aliases (d1, d2, d3) with different filters",
    "single_pass_aggregation": "Query has repeated scans of the same fact table with different WHERE filters",
    "or_to_union": "WHERE clause has OR conditions over different dimension keys (≤3 branches)",
    "intersect_to_exists": "Query uses INTERSECT or EXCEPT to combine subquery results",
    "materialize_cte": "Same subexpression appears 2+ times in the query (as subquery or CTE ref)",
    "union_cte_split": "Query has UNION/UNION ALL of complex subqueries that share common table references",
}

# Strategy name → likely primary transform mapping
STRATEGY_TRANSFORM_MAP: Dict[str, str] = {
    "conservative_predicate_pushdown": "pushdown",
    "conservative_early_reduction": "early_filter",
    "conservative_filter_pushdown": "pushdown",
    "moderate_dimension_isolation": "dimension_cte_isolate",
    "moderate_date_isolation": "date_cte_isolate",
    "aggressive_multi_cte_prefetch": "prefetch_fact_join",
    "aggressive_single_pass_restructure": "single_pass_aggregation",
    "aggressive_cte_restructure": "prefetch_fact_join",
    "novel_correlation_elimination": "decorrelate",
    "novel_structural_transform": "or_to_union",
    "novel_set_operation_transform": "intersect_to_exists",
}

# Known transform keywords to scan for in response text
KNOWN_TRANSFORMS = set(TRANSFORM_PRINCIPLES.keys())


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1: Extract BlackboardEntry records from swarm batch
# ─────────────────────────────────────────────────────────────────────────────


def classify_status(worker: Dict[str, Any]) -> str:
    """Classify worker outcome into WIN|IMPROVED|NEUTRAL|REGRESSION|ERROR|FAIL."""
    if worker.get("error"):
        return "ERROR"
    if not worker.get("rows_match", True):
        return "FAIL"
    speedup = worker.get("speedup", 0.0)
    if speedup >= 2.0:
        return "WIN"
    if speedup >= 1.1:
        return "IMPROVED"
    if speedup >= 0.95:
        return "NEUTRAL"
    return "REGRESSION"


def categorize_error(error_msg: str) -> str:
    """Categorize an error message into a standard category."""
    if not error_msg:
        return "unknown"
    lower = error_msg.lower()
    if any(kw in lower for kw in ("parser error", "syntax error", "unterminated")):
        return "syntax"
    if any(kw in lower for kw in ("binder error", "catalog error", "not found", "not in group by")):
        return "semantic"
    if any(kw in lower for kw in ("timeout", "timed out", "cancelled")):
        return "timeout"
    if any(kw in lower for kw in ("runtime error", "out of memory", "not implemented")):
        return "execution"
    return "unknown"


def extract_changes_section(response_text: str) -> Optional[str]:
    """Extract the 'Changes:' section from a worker response."""
    if not response_text:
        return None
    match = re.search(
        r'Changes?:\s*(.+?)(?:\nExpected|\n```|\n##|\Z)',
        response_text, re.IGNORECASE | re.DOTALL,
    )
    if match:
        return match.group(1).strip()[:500]
    return None


def extract_transforms_from_response(response_text: str) -> List[str]:
    """Scan response text for known transform keywords."""
    if not response_text:
        return []
    found = []
    lower = response_text.lower()
    for t in KNOWN_TRANSFORMS:
        # Match transform name with word boundaries (underscores count as word chars)
        pattern = r'\b' + re.escape(t) + r'\b'
        if re.search(pattern, lower):
            found.append(t)
    return found


def extract_transforms(
    assignment: Optional[Dict[str, Any]],
    response_text: str,
    strategy: str,
    original_sql: Optional[str] = None,
    optimized_sql: Optional[str] = None,
) -> List[str]:
    """4-tier transform extraction: assignments → response regex → strategy → SQL diff.

    Tier 4 (SQL-diff inference) is the final fallback when Tiers 1-3 fail.
    """
    # Tier 1: assignments.json examples list
    if assignment:
        examples = assignment.get("examples", [])
        if examples:
            # Filter to known transforms only
            known = [e for e in examples if e in KNOWN_TRANSFORMS]
            if known:
                return known

    # Tier 2: scan response text for known keywords
    from_text = extract_transforms_from_response(response_text)
    if from_text:
        return from_text

    # Tier 3: strategy name fallback
    if strategy:
        mapped = STRATEGY_TRANSFORM_MAP.get(strategy)
        if mapped:
            return [mapped]

    # Tier 4: SQL-diff inference (structural analysis)
    if original_sql and optimized_sql:
        from .sql_rewriter import infer_transforms_from_sql_diff
        inferred = infer_transforms_from_sql_diff(original_sql, optimized_sql)
        if inferred:
            return inferred

    return []


def relabel_query(original_sql: str, optimized_sql: str, dialect: str = "duckdb") -> List[str]:
    """Relabel a query's transforms using SQL-diff inference.

    Utility for re-labeling queries with placeholder/unknown labels.
    Uses the enhanced infer_transforms_from_sql_diff() for structural detection.

    Args:
        original_sql: Original query SQL
        optimized_sql: Optimized query SQL
        dialect: SQL dialect for parsing

    Returns:
        List of inferred transform names
    """
    from .sql_rewriter import infer_transforms_from_sql_diff
    return infer_transforms_from_sql_diff(original_sql, optimized_sql, dialect=dialect)


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    """Load a JSON file, returning None on failure."""
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def load_text(path: Path) -> str:
    """Load a text file, returning empty string on failure."""
    try:
        return path.read_text()
    except Exception:
        return ""


def extract_query_entries(
    query_dir: Path,
    run_name: str,
    timestamp: str,
) -> List[BlackboardEntry]:
    """Extract all BlackboardEntry records for a single query directory."""
    query_id = query_dir.name
    entries: List[BlackboardEntry] = []

    # Load shared data
    assignments_data = load_json(query_dir / "assignments.json")
    assignments_by_worker: Dict[int, Dict[str, Any]] = {}
    if assignments_data and isinstance(assignments_data, list):
        for a in assignments_data:
            wid = a.get("worker_id", 0)
            assignments_by_worker[wid] = a

    regression_warnings = load_json(query_dir / "regression_warnings.json")
    reanalyze = load_json(query_dir / "reanalyze_parsed.json")

    # Map iteration → benchmark file and worker response files
    iter_configs = [
        # (iter, benchmark_file, worker_id_range, response_file_pattern)
        (0, "benchmark_iter0.json", range(1, 5), "worker_{}_response.txt"),
        (1, "benchmark_iter1.json", [5], "snipe_worker_response.txt"),
        (2, "benchmark_iter2.json", [6], "final_worker_response.txt"),
    ]

    for iteration, bench_file, worker_ids, resp_pattern in iter_configs:
        bench_path = query_dir / bench_file
        bench_data = load_json(bench_path)
        if not bench_data:
            continue

        workers = bench_data.get("workers", [])
        worker_by_id = {w["worker_id"]: w for w in workers}

        for wid in worker_ids:
            worker = worker_by_id.get(wid)
            if not worker:
                continue

            # Get response text
            if iteration == 0:
                resp_file = resp_pattern.format(wid)
            elif iteration == 1:
                resp_file = "snipe_worker_response.txt"
            else:
                resp_file = "final_worker_response.txt"
            response_text = load_text(query_dir / resp_file)

            # Get assignment/strategy
            if iteration == 0:
                assignment = assignments_by_worker.get(wid)
            elif iteration == 1:
                # Snipe worker uses best worker's assignment context
                best_iter0 = bench_data.get("best_worker_id")
                assignment = assignments_by_worker.get(best_iter0)
            else:
                # Final worker: try reanalyze examples as pseudo-assignment
                assignment = None
                if reanalyze:
                    reanalyze_examples = reanalyze.get("examples", [])
                    if reanalyze_examples:
                        assignment = {"examples": reanalyze_examples}
            strategy = (assignment or {}).get("strategy", "")

            # For iter2, try to get strategy from reanalyze
            if iteration == 2 and reanalyze:
                strategy = reanalyze.get("refined_strategy", strategy) or strategy

            # Classify status
            status = classify_status(worker)
            speedup = worker.get("speedup", 0.0)

            # Load SQL for Tier 4 inference fallback
            original_sql_text = load_text(query_dir / "original.sql") or None
            if iteration == 0:
                opt_sql_file = f"worker_{wid}_sql.sql"
            elif iteration == 1:
                opt_sql_file = "snipe_worker_sql.sql"
            else:
                opt_sql_file = "final_worker_sql.sql"
            optimized_sql_text = load_text(query_dir / opt_sql_file) or None

            # Extract transforms (now with Tier 4 SQL-diff fallback)
            transforms = extract_transforms(
                assignment, response_text, strategy,
                original_sql=original_sql_text,
                optimized_sql=optimized_sql_text,
            )

            # For iter2, also try reanalyze examples if still empty
            if iteration == 2 and reanalyze and not transforms:
                reanalyze_examples = reanalyze.get("examples", [])
                transforms = [e for e in reanalyze_examples if e in KNOWN_TRANSFORMS]

            # Error handling
            error_msg = worker.get("error", "")
            error_category = categorize_error(error_msg) if error_msg else None
            error_messages = [error_msg] if error_msg else []

            # Knowledge extraction
            what_worked = None
            why_it_worked = None
            what_failed = None
            why_it_failed = None
            principle = None

            if status in ("WIN", "IMPROVED"):
                changes = extract_changes_section(response_text)
                transform_str = ", ".join(transforms) if transforms else "unknown"
                if changes:
                    what_worked = f"Applied {transform_str}: {changes}"
                else:
                    what_worked = f"Applied {transform_str} achieving {speedup:.2f}x speedup"

                # Look up known principles
                principles_text = [
                    TRANSFORM_PRINCIPLES[t] for t in transforms if t in TRANSFORM_PRINCIPLES
                ]
                if principles_text:
                    why_it_worked = "; ".join(principles_text)

                # Map to principle name
                for t in transforms:
                    if t in TRANSFORM_PRINCIPLES:
                        principle = t
                        break
                if not principle and transforms:
                    principle = transforms[0]

            elif status == "ERROR":
                what_failed = error_msg[:500] if error_msg else "Unknown error"
                category_explanations = {
                    "syntax": "SQL syntax error in the rewritten query",
                    "semantic": "Binder/catalog error — referenced column or table not found after rewrite",
                    "timeout": "Rewritten query timed out (likely regression)",
                    "execution": "Runtime error during query execution",
                }
                why_it_failed = category_explanations.get(
                    error_category or "", f"Error category: {error_category}"
                )

            elif status == "FAIL":
                what_failed = f"Row count mismatch: optimized query produced {worker.get('row_count', '?')} rows"
                why_it_failed = "Semantic mismatch — rewrite changed query semantics, producing different results"

            elif status == "REGRESSION":
                changes = extract_changes_section(response_text)
                if changes:
                    what_failed = f"Regression ({speedup:.2f}x): {changes}"
                else:
                    what_failed = f"Regression: {speedup:.2f}x slower than baseline"

                # Check regression_warnings for mechanism
                if regression_warnings:
                    mechanisms = [
                        rw.get("mechanism", "")
                        for rw in regression_warnings
                        if rw.get("mechanism")
                    ]
                    if mechanisms:
                        why_it_failed = mechanisms[0][:500]

                # Check reanalyze for failure analysis
                if not why_it_failed and reanalyze:
                    failure_analysis = reanalyze.get("failure_analysis", "")
                    if failure_analysis:
                        why_it_failed = failure_analysis[:500]

                if not why_it_failed:
                    why_it_failed = "Rewrite increased execution time — likely added overhead or prevented optimizer optimizations"

            entry = BlackboardEntry(
                query_id=query_id,
                worker_id=wid,
                run_name=run_name,
                timestamp=timestamp,
                examples_used=(assignment or {}).get("examples", []),
                strategy=strategy[:200] if isinstance(strategy, str) else "",
                status=status,
                speedup=speedup,
                transforms_applied=transforms,
                error_category=error_category,
                error_messages=error_messages,
                what_worked=what_worked,
                why_it_worked=why_it_worked,
                what_failed=what_failed,
                why_it_failed=why_it_failed,
                principle=principle,
                reviewed=True,  # Auto-reviewed: we have benchmark ground truth
            )
            entries.append(entry)

    return entries


def phase1_extract(batch_dir: Path) -> List[BlackboardEntry]:
    """Phase 1: Extract all BlackboardEntry records from a swarm batch."""
    run_name = batch_dir.name
    timestamp = datetime.now().isoformat()

    # Find all query directories
    query_dirs = sorted(
        [d for d in batch_dir.iterdir() if d.is_dir() and d.name.startswith("query")],
        key=lambda d: d.name,
    )

    logger.info(f"Phase 1: Extracting from {len(query_dirs)} query directories")

    all_entries: List[BlackboardEntry] = []
    for qdir in query_dirs:
        entries = extract_query_entries(qdir, run_name, timestamp)
        all_entries.extend(entries)

    # Write raw entries
    raw_dir = batch_dir / "blackboard" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    queries_written = set()
    for entry in all_entries:
        entry_dir = raw_dir / entry.query_id
        entry_dir.mkdir(parents=True, exist_ok=True)
        path = entry_dir / f"worker_{entry.worker_id:02d}.json"
        path.write_text(json.dumps(entry.to_dict(), indent=2))
        queries_written.add(entry.query_id)

    # Status summary
    status_counts: Dict[str, int] = defaultdict(int)
    for e in all_entries:
        status_counts[e.status] += 1

    logger.info(
        f"Phase 1 complete: {len(all_entries)} entries across {len(queries_written)} queries"
    )
    logger.info(
        f"  Status breakdown: {dict(sorted(status_counts.items()))}"
    )

    return all_entries


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2: Collate entries into principles + anti-patterns
# ─────────────────────────────────────────────────────────────────────────────


def phase2_collate(
    entries: List[BlackboardEntry],
    batch_dir: Path,
) -> tuple[List[KnowledgePrinciple], List[KnowledgeAntiPattern]]:
    """Phase 2: Collate raw entries into principles and anti-patterns."""

    # ── Principle extraction (from WIN + IMPROVED) ──
    win_entries = [e for e in entries if e.status in ("WIN", "IMPROVED")]
    principle_groups: Dict[str, List[BlackboardEntry]] = defaultdict(list)
    for e in win_entries:
        key = e.principle or (e.transforms_applied[0] if e.transforms_applied else "unknown")
        principle_groups[key].append(e)

    # Also collect regression entries per transform for when_not
    regression_entries = [e for e in entries if e.status == "REGRESSION"]
    regression_by_transform: Dict[str, List[BlackboardEntry]] = defaultdict(list)
    for e in regression_entries:
        for t in e.transforms_applied:
            regression_by_transform[t].append(e)

    principles: List[KnowledgePrinciple] = []
    for pid, group in sorted(principle_groups.items()):
        speedups = [e.speedup for e in group]
        avg_speedup = sum(speedups) / len(speedups) if speedups else 0.0
        queries = sorted(set(e.query_id for e in group))
        transforms = sorted(set(t for e in group for t in e.transforms_applied))

        # Pick best what/why from highest-speedup entry
        best_entry = max(group, key=lambda e: e.speedup)
        what = best_entry.what_worked or f"Applied {pid}"
        why = best_entry.why_it_worked or TRANSFORM_PRINCIPLES.get(pid, "")
        when = PRINCIPLE_WHEN.get(pid, "")

        # Cross-reference regressions for when_not
        when_not_parts = []
        if pid in regression_by_transform:
            reg_queries = sorted(set(
                e.query_id for e in regression_by_transform[pid]
            ))
            reg_speedups = [e.speedup for e in regression_by_transform[pid]]
            worst = min(reg_speedups)
            when_not_parts.append(
                f"Caused regression on {', '.join(reg_queries)} "
                f"(worst: {worst:.2f}x)"
            )
            for re_entry in regression_by_transform[pid]:
                if re_entry.why_it_failed:
                    when_not_parts.append(re_entry.why_it_failed[:200])
                    break
        when_not = "; ".join(when_not_parts)

        # Clean up the name
        name = pid.replace("_", " ").title()

        principles.append(KnowledgePrinciple(
            id=pid,
            name=name,
            what=what[:500],
            why=why[:500],
            when=when,
            when_not=when_not,
            verified_speedups=sorted(speedups, reverse=True),
            avg_speedup=round(avg_speedup, 3),
            queries=queries,
            transforms=transforms,
        ))

    # Sort by avg_speedup descending
    principles.sort(key=lambda p: p.avg_speedup, reverse=True)

    # ── Anti-pattern extraction (from REGRESSION + ERROR + FAIL) ──
    anti_patterns: List[KnowledgeAntiPattern] = []

    # Group regressions by first transform
    regression_groups: Dict[str, List[BlackboardEntry]] = defaultdict(list)
    for e in regression_entries:
        key = e.transforms_applied[0] if e.transforms_applied else "unknown_regression"
        regression_groups[key].append(e)

    for key, group in sorted(regression_groups.items()):
        speedups = [e.speedup for e in group]
        queries = sorted(set(e.query_id for e in group))
        worst = min(group, key=lambda e: e.speedup)
        mechanism = worst.why_it_failed or worst.what_failed or f"Regression from {key}"

        anti_patterns.append(KnowledgeAntiPattern(
            id=f"regression_{key}",
            name=f"Regression: {key.replace('_', ' ').title()}",
            mechanism=mechanism[:500],
            observed_regressions=sorted(speedups),
            queries=queries,
            avoid_when=f"Applying {key} to queries similar to {', '.join(queries)}",
        ))

    # Group errors by error_category
    error_entries = [e for e in entries if e.status == "ERROR"]
    error_groups: Dict[str, List[BlackboardEntry]] = defaultdict(list)
    for e in error_entries:
        key = e.error_category or "unknown"
        error_groups[key].append(e)

    for key, group in sorted(error_groups.items()):
        queries = sorted(set(e.query_id for e in group))
        # Pick most informative error message
        sample = max(group, key=lambda e: len(e.error_messages[0]) if e.error_messages else 0)
        mechanism = sample.what_failed or f"Error category: {key}"

        anti_patterns.append(KnowledgeAntiPattern(
            id=f"error_{key}",
            name=f"Error Pattern: {key.replace('_', ' ').title()}",
            mechanism=mechanism[:500],
            observed_regressions=[0.0] * len(group),  # Errors have 0 speedup
            queries=queries,
            avoid_when=f"Watch for {key} errors when rewriting queries with complex joins/aliases",
        ))

    # Group fails by transform (semantic mismatches)
    fail_entries = [e for e in entries if e.status == "FAIL"]
    if fail_entries:
        fail_groups: Dict[str, List[BlackboardEntry]] = defaultdict(list)
        for e in fail_entries:
            key = e.transforms_applied[0] if e.transforms_applied else "unknown"
            fail_groups[key].append(e)

        for key, group in sorted(fail_groups.items()):
            queries = sorted(set(e.query_id for e in group))
            anti_patterns.append(KnowledgeAntiPattern(
                id=f"semantic_mismatch_{key}",
                name=f"Semantic Mismatch: {key.replace('_', ' ').title()}",
                mechanism="Rewrite changed query semantics — different row counts or values returned",
                observed_regressions=[0.0] * len(group),
                queries=queries,
                avoid_when=f"Applying {key} to queries where semantic equivalence is hard to verify",
            ))

    # Write collated output
    collated = {
        "principles": [p.to_dict() for p in principles],
        "anti_patterns": [a.to_dict() for a in anti_patterns],
        "summary": {
            "n_principles": len(principles),
            "n_anti_patterns": len(anti_patterns),
            "total_wins": len([e for e in entries if e.status == "WIN"]),
            "total_improved": len([e for e in entries if e.status == "IMPROVED"]),
            "total_neutral": len([e for e in entries if e.status == "NEUTRAL"]),
            "total_regression": len(regression_entries),
            "total_error": len(error_entries),
            "total_fail": len(fail_entries),
        },
    }

    collated_path = batch_dir / "blackboard" / "collated.json"
    collated_path.write_text(json.dumps(collated, indent=2))

    logger.info(
        f"Phase 2 complete: {len(principles)} principles, {len(anti_patterns)} anti-patterns"
    )
    logger.info(f"  Top 5 principles by avg speedup:")
    for p in principles[:5]:
        logger.info(
            f"    {p.id}: {p.avg_speedup:.2f}x avg "
            f"({len(p.verified_speedups)} observations, {len(p.queries)} queries)"
        )

    return principles, anti_patterns


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3: Write GlobalKnowledge (merge if exists)
# ─────────────────────────────────────────────────────────────────────────────


def phase3_global(
    principles: List[KnowledgePrinciple],
    anti_patterns: List[KnowledgeAntiPattern],
    batch_dir: Path,
    dataset: str = "duckdb_tpcds",
    benchmark_dir: Optional[Path] = None,
) -> GlobalKnowledge:
    """Phase 3: Write GlobalKnowledge to benchmark-level knowledge dir.

    Canonical output: benchmark_dir/knowledge/<dataset>.json
    Batch copy:       batch_dir/knowledge/<dataset>.json (for provenance)
    """
    if benchmark_dir is None:
        benchmark_dir = batch_dir.resolve().parent

    # Canonical knowledge path (benchmark level)
    knowledge_dir = benchmark_dir / "knowledge"
    knowledge_dir.mkdir(parents=True, exist_ok=True)
    knowledge_path = knowledge_dir / f"{dataset}.json"

    run_name = batch_dir.name

    # Merge with existing if present
    if knowledge_path.exists():
        logger.info("Merging with existing knowledge file")
        existing = GlobalKnowledge.from_dict(
            json.loads(knowledge_path.read_text())
        )

        # Merge principles
        existing_by_id = {p.id: p for p in existing.principles}
        for p in principles:
            if p.id in existing_by_id:
                ep = existing_by_id[p.id]
                # Union speedups and queries
                all_speedups = sorted(
                    set(ep.verified_speedups + p.verified_speedups), reverse=True
                )
                all_queries = sorted(set(ep.queries + p.queries))
                all_transforms = sorted(set(ep.transforms + p.transforms))
                avg = sum(all_speedups) / len(all_speedups) if all_speedups else 0.0

                existing_by_id[p.id] = KnowledgePrinciple(
                    id=p.id,
                    name=p.name,
                    what=p.what if len(p.what) >= len(ep.what) else ep.what,
                    why=p.why if len(p.why) >= len(ep.why) else ep.why,
                    when=p.when or ep.when,
                    when_not=p.when_not or ep.when_not,
                    verified_speedups=all_speedups,
                    avg_speedup=round(avg, 3),
                    queries=all_queries,
                    transforms=all_transforms,
                )
            else:
                existing_by_id[p.id] = p

        merged_principles = sorted(
            existing_by_id.values(), key=lambda p: p.avg_speedup, reverse=True
        )

        # Merge anti-patterns
        existing_ap_by_id = {a.id: a for a in existing.anti_patterns}
        for a in anti_patterns:
            if a.id in existing_ap_by_id:
                ea = existing_ap_by_id[a.id]
                existing_ap_by_id[a.id] = KnowledgeAntiPattern(
                    id=a.id,
                    name=a.name,
                    mechanism=a.mechanism if len(a.mechanism) >= len(ea.mechanism) else ea.mechanism,
                    observed_regressions=sorted(set(ea.observed_regressions + a.observed_regressions)),
                    queries=sorted(set(ea.queries + a.queries)),
                    avoid_when=a.avoid_when or ea.avoid_when,
                )
            else:
                existing_ap_by_id[a.id] = a

        merged_anti_patterns = list(existing_ap_by_id.values())
        source_runs = sorted(set(existing.source_runs + [run_name]))

        gk = GlobalKnowledge(
            dataset=existing.dataset,
            last_updated=datetime.now().isoformat(),
            source_runs=source_runs,
            principles=list(merged_principles),
            anti_patterns=merged_anti_patterns,
        )
    else:
        gk = GlobalKnowledge(
            dataset=dataset,
            last_updated=datetime.now().isoformat(),
            source_runs=[run_name],
            principles=principles,
            anti_patterns=anti_patterns,
        )

    # Write canonical knowledge file
    knowledge_path.write_text(json.dumps(gk.to_dict(), indent=2))
    logger.info(f"Phase 3 complete: wrote {knowledge_path}")

    # Write batch-level copy for provenance
    batch_knowledge_dir = batch_dir / "knowledge"
    batch_knowledge_dir.mkdir(parents=True, exist_ok=True)
    batch_copy = batch_knowledge_dir / f"{dataset}.json"
    batch_copy.write_text(json.dumps(gk.to_dict(), indent=2))
    logger.info(f"  Batch copy: {batch_copy}")

    logger.info(
        f"  {len(gk.principles)} principles, {len(gk.anti_patterns)} anti-patterns, "
        f"{len(gk.source_runs)} source runs"
    )

    return gk


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────


def validate_output(gk: GlobalKnowledge, batch_dir: Path) -> bool:
    """Validate the generated GlobalKnowledge."""
    ok = True

    # Check all principle IDs non-empty and have verified_speedups
    for p in gk.principles:
        if not p.id:
            logger.error("Principle with empty ID found")
            ok = False
        if not p.verified_speedups:
            logger.error(f"Principle {p.id} has no verified speedups")
            ok = False

    # Check all query references exist in batch directory
    all_query_refs = set()
    for p in gk.principles:
        all_query_refs.update(p.queries)
    for a in gk.anti_patterns:
        all_query_refs.update(a.queries)

    for qid in all_query_refs:
        if not (batch_dir / qid).is_dir():
            logger.warning(f"Query reference {qid} not found in batch directory")

    # Check raw entries exist
    raw_dir = batch_dir / "blackboard" / "raw"
    if raw_dir.exists():
        raw_queries = [d.name for d in raw_dir.iterdir() if d.is_dir()]
        logger.info(f"Validation: {len(raw_queries)} queries in blackboard/raw/")
    else:
        logger.error("blackboard/raw/ directory not found")
        ok = False

    # Check collated.json
    collated_path = batch_dir / "blackboard" / "collated.json"
    if collated_path.exists():
        collated = json.loads(collated_path.read_text())
        logger.info(
            f"Validation: collated.json has "
            f"{len(collated.get('principles', []))} principles, "
            f"{len(collated.get('anti_patterns', []))} anti-patterns"
        )
    else:
        logger.error("blackboard/collated.json not found")
        ok = False

    # Verify top principles match known winners
    known_top = {"single_pass_aggregation", "date_cte_isolate", "early_filter",
                 "prefetch_fact_join", "or_to_union", "decorrelate",
                 "dimension_cte_isolate", "multi_dimension_prefetch"}
    found_top = {p.id for p in gk.principles[:10]}
    overlap = known_top & found_top
    logger.info(
        f"Validation: {len(overlap)}/{len(known_top)} known top principles "
        f"found in top 10: {sorted(overlap)}"
    )

    return ok


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark config detection (for engine/dataset-aware output paths)
# ─────────────────────────────────────────────────────────────────────────────

_QT_DIR = Path(__file__).resolve().parent  # .../qt_sql/


def _detect_benchmark_config(batch_dir: Path) -> Dict[str, Any]:
    """Detect engine/dataset from config.json in the parent benchmark dir.

    batch_dir is e.g. benchmarks/postgres_dsb/swarm_batch_20260208_142643/
    so parent is benchmarks/postgres_dsb/ which contains config.json.
    """
    parent = batch_dir.resolve().parent  # benchmarks/<benchmark>/
    config_path = parent / "config.json"
    if config_path.exists():
        cfg = json.loads(config_path.read_text())
        engine = cfg.get("engine", "duckdb")
        benchmark = cfg.get("benchmark", parent.name)
        dataset = f"{engine}_{benchmark}"
        return {
            "engine": engine,
            "benchmark": benchmark,
            "dataset": dataset,
            "benchmark_dir": parent,
        }
    # Fallback for duckdb_tpcds (no config.json needed)
    return {
        "engine": "duckdb",
        "benchmark": "tpcds",
        "dataset": "duckdb_tpcds",
        "benchmark_dir": parent,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Global blackboard: Source readers
# ─────────────────────────────────────────────────────────────────────────────

# Resolve project root (for --global mode, run from project root)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # ado/ → qt-sql/ → packages/ → root
_ADO_BENCHMARKS = Path(__file__).resolve().parent / "benchmarks" / "duckdb_tpcds"
_RESEARCH = _PROJECT_ROOT / "research"
_CONSOLIDATED = _RESEARCH / "CONSOLIDATED_BENCHMARKS"
_RETRY_ARCHIVE = _RESEARCH / "retry_archive"

# Default paths for each source
_DEFAULT_SWARM_BATCH = _ADO_BENCHMARKS / "swarm_batch_20260208_102033"
_DEFAULT_RETRY4W_DIR = _RETRY_ARCHIVE / "retry_neutrals"
_DEFAULT_RETRY3W_DIR = _RETRY_ARCHIVE / "retry_collect"
_DEFAULT_KIMI_DIRS = [
    _CONSOLIDATED / "kimi_q1-q30_optimization",
    _CONSOLIDATED / "kimi_q31-q99_optimization",
]
_DEFAULT_V2_DIR = _CONSOLIDATED / "benchmark_output_v2"
_DEFAULT_MASTER_CSV = _CONSOLIDATED / "DuckDB_TPC-DS_Master_v3_20260206.csv"
_DEFAULT_SQL_CSV = _CONSOLIDATED / "DuckDB_TPC-DS_SQL_v1_20260205.csv"
_DEFAULT_LEADERBOARD = _ADO_BENCHMARKS / "leaderboard.json"


def read_swarm_source(batch_dir: Path) -> Dict[str, List[SourceAttempt]]:
    """Read swarm batch directory, return attempts keyed by normalized query_id."""
    result: Dict[str, List[SourceAttempt]] = defaultdict(list)
    run_name = batch_dir.name

    query_dirs = sorted(
        [d for d in batch_dir.iterdir() if d.is_dir() and d.name.startswith("query")],
        key=lambda d: d.name,
    )

    for qdir in query_dirs:
        qid = normalize_query_id(qdir.name)

        # Load assignments
        assignments_data = load_json(qdir / "assignments.json")
        assignments_by_worker: Dict[int, Dict[str, Any]] = {}
        if assignments_data and isinstance(assignments_data, list):
            for a in assignments_data:
                assignments_by_worker[a.get("worker_id", 0)] = a

        # Original SQL
        original_sql = load_text(qdir / "original.sql") or None

        # Iterate over all benchmark iterations
        iter_configs = [
            (0, "benchmark_iter0.json", range(1, 5), "worker_{}_response.txt", "worker_{}_sql.sql"),
            (1, "benchmark_iter1.json", [5], "snipe_worker_response.txt", "snipe_worker_sql.sql"),
            (2, "benchmark_iter2.json", [6], "final_worker_response.txt", "final_worker_sql.sql"),
        ]

        for iteration, bench_file, worker_ids, resp_pattern, sql_pattern in iter_configs:
            bench_data = load_json(qdir / bench_file)
            if not bench_data:
                continue

            baseline_ms = bench_data.get("baseline_trimmed_mean_ms", 0.0)
            workers = {w["worker_id"]: w for w in bench_data.get("workers", [])}

            for wid in worker_ids:
                worker = workers.get(wid)
                if not worker:
                    continue

                # Response text
                if iteration == 0:
                    resp_file = resp_pattern.format(wid)
                    sql_file = sql_pattern.format(wid)
                elif iteration == 1:
                    resp_file = "snipe_worker_response.txt"
                    sql_file = "snipe_worker_sql.sql"
                else:
                    resp_file = "final_worker_response.txt"
                    sql_file = "final_worker_sql.sql"

                response_text = load_text(qdir / resp_file) or None
                optimized_sql = load_text(qdir / sql_file) or None

                # Strategy + assignment
                assignment = assignments_by_worker.get(wid) if iteration == 0 else None
                strategy = (assignment or {}).get("strategy", "")
                examples = (assignment or {}).get("examples", [])

                # Transforms (with Tier 4 SQL-diff fallback)
                transforms = extract_transforms(
                    assignment, response_text or "", strategy,
                    original_sql=original_sql,
                    optimized_sql=optimized_sql,
                )
                changes = extract_changes_section(response_text or "")

                speedup = worker.get("speedup", 0.0)
                trimmed_ms = worker.get("trimmed_mean_ms", 0.0)
                error = worker.get("error", "") or None

                result[qid].append(SourceAttempt(
                    source="Swarm",
                    run=run_name,
                    model="deepseek-reasoner",
                    worker_id=wid,
                    iteration=iteration,
                    strategy=strategy[:200],
                    speedup=speedup,
                    original_ms=baseline_ms,
                    optimized_ms=trimmed_ms,
                    status=worker.get("status", ""),
                    rows_match=worker.get("rows_match", True),
                    optimized_sql=optimized_sql,
                    original_sql=original_sql,
                    reasoning=response_text,
                    changes_description=changes,
                    transforms=transforms,
                    examples_used=examples,
                    error=error,
                ))

    logger.info(f"  Swarm: {sum(len(v) for v in result.values())} attempts across {len(result)} queries")
    return dict(result)


def read_retry4w_source(archive_dir: Path) -> Dict[str, List[SourceAttempt]]:
    """Read Retry4W (retry_neutrals) source."""
    result: Dict[str, List[SourceAttempt]] = defaultdict(list)

    # Find details JSON
    details_files = sorted(archive_dir.glob("retry_4worker_*_details.json"))
    if not details_files:
        logger.warning(f"  Retry4W: no details JSON found in {archive_dir}")
        return dict(result)

    details_path = details_files[0]
    run_name = details_path.stem  # e.g. "retry_4worker_20260206_004710"
    details = load_json(details_path)
    if not details:
        return dict(result)

    # Worker examples config
    worker_examples = details.get("config", {}).get("worker_examples", {})

    # Find the latest validation JSON for SF10 speedups
    validation_files = sorted(archive_dir.glob("validation_*.json"))
    validation_by_qid: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for vf in validation_files:
        vdata = load_json(vf)
        if not vdata:
            continue
        for vr in vdata.get("results", []):
            qid = normalize_query_id(vr.get("query_id", ""))
            validation_by_qid[qid].append(vr)

    for query_result in details.get("results", []):
        qid = normalize_query_id(query_result.get("query_id", ""))

        # Per-query directory with SQL + response files
        qdir = archive_dir / query_result.get("query_id", qid)
        if not qdir.is_dir():
            # Try normalized form
            qdir = archive_dir / qid

        original_sql_text = load_text(qdir / "original.sql") if qdir.is_dir() else None

        for worker_data in query_result.get("all_workers", []):
            wid = worker_data.get("worker_id", 0)

            # SQL from per-query dir files
            sql_from_file = None
            response_text = None
            if qdir.is_dir():
                sql_from_file = load_text(qdir / f"w{wid}_optimized.sql") or None
                response_text = load_text(qdir / f"w{wid}_response.txt") or None

            # SQL fallback from details JSON (inline)
            optimized_sql = sql_from_file or worker_data.get("optimized_sql") or None

            # Speedup from validation (more accurate than details)
            speedup = 0.0
            val_entries = validation_by_qid.get(qid, [])
            for ve in val_entries:
                if ve.get("worker_id") == wid:
                    speedup = ve.get("speedup", 0.0)
                    break
            if speedup == 0.0:
                speedup = worker_data.get("speedup", 0.0)

            examples = worker_data.get("examples_used", [])
            if not examples:
                examples = worker_examples.get(str(wid), [])

            transforms = extract_transforms_from_response(response_text or "")
            if not transforms:
                transforms = [e for e in examples if e in KNOWN_TRANSFORMS]
            changes = extract_changes_section(response_text or "")

            result[qid].append(SourceAttempt(
                source="Retry4W",
                run=run_name,
                model="deepseek-reasoner",
                worker_id=wid,
                speedup=speedup,
                original_ms=worker_data.get("original_time_ms", 0.0),
                optimized_ms=worker_data.get("optimized_time_ms", 0.0),
                status=worker_data.get("status", ""),
                rows_match=True,
                optimized_sql=optimized_sql,
                original_sql=original_sql_text,
                reasoning=response_text,
                changes_description=changes,
                transforms=transforms,
                examples_used=examples,
                error=worker_data.get("error_message"),
            ))

    logger.info(f"  Retry4W: {sum(len(v) for v in result.values())} attempts across {len(result)} queries")
    return dict(result)


def read_retry3w_source(archive_dir: Path) -> Dict[str, List[SourceAttempt]]:
    """Read Retry3W (retry_collect) source."""
    result: Dict[str, List[SourceAttempt]] = defaultdict(list)

    details_files = sorted(archive_dir.glob("retry_3worker_*_details.json"))
    if not details_files:
        logger.warning(f"  Retry3W: no details JSON found in {archive_dir}")
        return dict(result)

    details_path = details_files[0]
    run_name = details_path.stem
    details = load_json(details_path)
    if not details:
        return dict(result)

    worker_examples = details.get("config", {}).get("worker_examples", {})

    # Validation JSONs
    validation_files = sorted(archive_dir.glob("validation_*.json"))
    validation_by_qid: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for vf in validation_files:
        vdata = load_json(vf)
        if not vdata:
            continue
        for vr in vdata.get("results", []):
            qid = normalize_query_id(vr.get("query_id", ""))
            validation_by_qid[qid].append(vr)

    for query_result in details.get("results", []):
        qid = normalize_query_id(query_result.get("query_id", ""))

        qdir = archive_dir / query_result.get("query_id", qid)
        if not qdir.is_dir():
            qdir = archive_dir / qid

        original_sql_text = load_text(qdir / "original.sql") if qdir.is_dir() else None

        for worker_data in query_result.get("all_workers", []):
            wid = worker_data.get("worker_id", 0)

            # SQL from per-query dir or inline in details JSON
            sql_from_file = None
            response_text = None
            if qdir.is_dir():
                sql_from_file = load_text(qdir / f"w{wid}_optimized.sql") or None
                response_text = load_text(qdir / f"w{wid}_response.txt") or None

            optimized_sql = sql_from_file or worker_data.get("optimized_sql") or None

            # Speedup from validation
            speedup = 0.0
            for ve in validation_by_qid.get(qid, []):
                if ve.get("worker_id") == wid:
                    speedup = ve.get("speedup", 0.0)
                    break
            if speedup == 0.0:
                speedup = worker_data.get("speedup", 0.0)

            examples = worker_data.get("examples_used", [])
            if not examples:
                examples = worker_examples.get(str(wid), [])

            transforms = extract_transforms_from_response(response_text or "")
            if not transforms and optimized_sql:
                transforms = extract_transforms_from_response(optimized_sql)
            if not transforms:
                transforms = [e for e in examples if e in KNOWN_TRANSFORMS]

            result[qid].append(SourceAttempt(
                source="Retry3W",
                run=run_name,
                model="deepseek-reasoner",
                worker_id=wid,
                speedup=speedup,
                original_ms=worker_data.get("original_time_ms", 0.0),
                optimized_ms=worker_data.get("optimized_time_ms", 0.0),
                status=worker_data.get("status", ""),
                rows_match=True,
                optimized_sql=optimized_sql,
                original_sql=original_sql_text,
                reasoning=response_text,
                transforms=transforms,
                examples_used=examples,
                error=worker_data.get("error_message"),
            ))

    logger.info(f"  Retry3W: {sum(len(v) for v in result.values())} attempts across {len(result)} queries")
    return dict(result)


def read_kimi_source(kimi_dirs: List[Path]) -> Dict[str, List[SourceAttempt]]:
    """Read Kimi K2.5 source directories."""
    result: Dict[str, List[SourceAttempt]] = defaultdict(list)

    for kimi_dir in kimi_dirs:
        if not kimi_dir.is_dir():
            continue
        for qdir in sorted(kimi_dir.iterdir()):
            if not qdir.is_dir() or not qdir.name.startswith("q"):
                continue

            qid = normalize_query_id(qdir.name)

            # Read result.json for structured data
            result_data = load_json(qdir / "result.json")
            original_sql = None
            optimized_sql = None
            reasoning = None
            error = None

            if result_data:
                original_sql = result_data.get("original_sql")
                optimized_sql = result_data.get("optimized_sql")
                reasoning = result_data.get("raw_response")
                error = result_data.get("error")
            else:
                # Fallback to individual files
                original_sql = load_text(qdir / "original.sql") or None
                optimized_sql = load_text(qdir / "output_optimized.sql") or None
                reasoning = load_text(qdir / "output_raw.txt") or None

            transforms = extract_transforms_from_response(reasoning or "")
            changes = extract_changes_section(reasoning or "")

            result[qid].append(SourceAttempt(
                source="Kimi",
                run="kimi_k2.5",
                model="kimi-k2.5",
                speedup=0.0,  # Will be filled from Master CSV or leaderboard
                status=result_data.get("status", "") if result_data else "",
                optimized_sql=optimized_sql,
                original_sql=original_sql,
                reasoning=reasoning,
                changes_description=changes,
                transforms=transforms,
                error=error,
            ))

    logger.info(f"  Kimi: {sum(len(v) for v in result.values())} attempts across {len(result)} queries")
    return dict(result)


def read_v2_source(v2_dir: Path) -> Dict[str, List[SourceAttempt]]:
    """Read V2 Standard benchmark output."""
    result: Dict[str, List[SourceAttempt]] = defaultdict(list)

    if not v2_dir.is_dir():
        logger.warning(f"  V2: directory not found: {v2_dir}")
        return dict(result)

    for qdir in sorted(v2_dir.iterdir()):
        if not qdir.is_dir() or not qdir.name.startswith("q"):
            continue

        qid = normalize_query_id(qdir.name)

        status_data = load_json(qdir / "status.json")
        original_sql = load_text(qdir / "original.sql") or None
        optimized_sql = load_text(qdir / "final_optimized.sql") or None
        reasoning = None

        # Try to read attempt response
        attempt_resp = load_json(qdir / "attempt_1_response.json")
        if attempt_resp:
            # May contain raw_response or just the structured response
            if isinstance(attempt_resp, dict):
                reasoning = attempt_resp.get("raw_response") or json.dumps(attempt_resp)[:2000]
            elif isinstance(attempt_resp, str):
                reasoning = attempt_resp

        transforms = extract_transforms_from_response(reasoning or "")
        changes = extract_changes_section(reasoning or "")

        result[qid].append(SourceAttempt(
            source="V2",
            run="benchmark_output_v2",
            model="deepseek-reasoner",
            speedup=0.0,  # Will be filled from Master CSV or leaderboard
            status=(status_data or {}).get("final_status", ""),
            optimized_sql=optimized_sql,
            original_sql=original_sql,
            reasoning=reasoning,
            changes_description=changes,
            transforms=transforms,
        ))

    logger.info(f"  V2: {sum(len(v) for v in result.values())} attempts across {len(result)} queries")
    return dict(result)


def read_master_csv_source(csv_path: Path) -> Dict[str, Dict[str, Any]]:
    """Read Master CSV v3 for Evo/DSR1/Kimi timings. Returns {qid: {source_columns}}."""
    result: Dict[str, Dict[str, Any]] = {}

    if not csv_path.is_file():
        logger.warning(f"  Master CSV: file not found: {csv_path}")
        return result

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qnum = row.get("Query_Num", "").strip()
            if not qnum:
                continue
            qid = normalize_query_id(qnum)
            result[qid] = {
                "classification": row.get("Classification", ""),
                # Kimi columns
                "kimi_speedup": _float(row.get("Kimi_Speedup", "")),
                "kimi_original_ms": _float(row.get("Kimi_Original_ms", "")),
                "kimi_optimized_ms": _float(row.get("Kimi_Optimized_ms", "")),
                "kimi_status": row.get("Kimi_Status", ""),
                # Evo columns
                "evo_speedup": _float(row.get("Evo_Best_Speedup", "")),
                "evo_status": row.get("Evo_Status", ""),
                # DSR1 columns
                "dsr1_speedup": _float(row.get("DSR1_Speedup", "")),
                "dsr1_original_ms": _float(row.get("DSR1_Original_ms", "")),
                "dsr1_optimized_ms": _float(row.get("DSR1_Optimized_ms", "")),
                "dsr1_status": row.get("DSR1_Status", ""),
                # Retry3W columns
                "retry3w_sf10_speedup": _float(row.get("Retry3W_SF10_Speedup", "")),
                "retry3w_best_worker": row.get("Retry3W_Best_Worker", ""),
                "retry3w_status": row.get("Retry3W_Status", ""),
            }

    logger.info(f"  Master CSV: {len(result)} query rows loaded")
    return result


def _float(s: str) -> float:
    """Safe float parse."""
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def read_sql_csv_fallback(csv_path: Path) -> Dict[str, Dict[str, str]]:
    """Read SQL CSV for original + optimized SQL as fallback. Returns {qid: {original, optimized}}."""
    result: Dict[str, Dict[str, str]] = {}

    if not csv_path.is_file():
        logger.warning(f"  SQL CSV: file not found: {csv_path}")
        return result

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qnum = row.get("Query_Num", "").strip()
            if not qnum:
                continue
            qid = normalize_query_id(qnum)
            original = row.get("SQL_Original", "").strip()
            optimized = row.get("SQL_Optimized", "").strip()
            if original or optimized:
                # Keep the longest version if multiple rows exist per query
                existing = result.get(qid)
                if existing:
                    if len(optimized) > len(existing.get("optimized", "")):
                        result[qid] = {"original": original, "optimized": optimized}
                else:
                    result[qid] = {"original": original, "optimized": optimized}

    logger.info(f"  SQL CSV: {len(result)} queries with SQL fallback")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Global blackboard: Aggregate + build
# ─────────────────────────────────────────────────────────────────────────────


def build_global_blackboard() -> Path:
    """Build the global blackboard from all historical sources.

    Returns path to the output JSON file.
    """
    logger.info("=" * 70)
    logger.info("BUILDING GLOBAL BLACKBOARD")
    logger.info("=" * 70)

    # ── Step 1: Read all sources ──
    logger.info("")
    logger.info("Step 1: Reading sources...")

    all_attempts: Dict[str, List[SourceAttempt]] = defaultdict(list)

    # Source 1: Swarm
    if _DEFAULT_SWARM_BATCH.is_dir():
        swarm = read_swarm_source(_DEFAULT_SWARM_BATCH)
        for qid, attempts in swarm.items():
            all_attempts[qid].extend(attempts)
    else:
        logger.warning(f"  Swarm batch not found: {_DEFAULT_SWARM_BATCH}")

    # Source 2: Retry4W
    if _DEFAULT_RETRY4W_DIR.is_dir():
        retry4w = read_retry4w_source(_DEFAULT_RETRY4W_DIR)
        for qid, attempts in retry4w.items():
            all_attempts[qid].extend(attempts)
    else:
        logger.warning(f"  Retry4W not found: {_DEFAULT_RETRY4W_DIR}")

    # Source 3: Retry3W
    if _DEFAULT_RETRY3W_DIR.is_dir():
        retry3w = read_retry3w_source(_DEFAULT_RETRY3W_DIR)
        for qid, attempts in retry3w.items():
            all_attempts[qid].extend(attempts)
    else:
        logger.warning(f"  Retry3W not found: {_DEFAULT_RETRY3W_DIR}")

    # Source 4: Kimi
    kimi_found = [d for d in _DEFAULT_KIMI_DIRS if d.is_dir()]
    if kimi_found:
        kimi = read_kimi_source(kimi_found)
        for qid, attempts in kimi.items():
            all_attempts[qid].extend(attempts)
    else:
        logger.warning("  Kimi dirs not found")

    # Source 5: V2
    if _DEFAULT_V2_DIR.is_dir():
        v2 = read_v2_source(_DEFAULT_V2_DIR)
        for qid, attempts in v2.items():
            all_attempts[qid].extend(attempts)
    else:
        logger.warning(f"  V2 not found: {_DEFAULT_V2_DIR}")

    # Source 6: Master CSV (for Evo + timing enrichment)
    master_csv = read_master_csv_source(_DEFAULT_MASTER_CSV)

    # Source 7: SQL CSV fallback
    sql_fallback = read_sql_csv_fallback(_DEFAULT_SQL_CSV)

    total_attempts = sum(len(v) for v in all_attempts.values())
    logger.info(f"  Total: {total_attempts} attempts across {len(all_attempts)} queries")

    # ── Step 2: Load leaderboard (oracle for best-per-query) ──
    logger.info("")
    logger.info("Step 2: Loading leaderboard...")
    leaderboard = load_json(_DEFAULT_LEADERBOARD)
    if not leaderboard:
        logger.error(f"  Cannot load leaderboard: {_DEFAULT_LEADERBOARD}")
        sys.exit(1)

    lb_queries = leaderboard.get("queries", [])
    logger.info(f"  Leaderboard: {len(lb_queries)} queries")

    # Enrich Kimi attempts with timings from Master CSV
    for qid, attempts in all_attempts.items():
        csv_data = master_csv.get(qid, {})
        for a in attempts:
            if a.source == "Kimi" and a.speedup == 0.0:
                a.speedup = csv_data.get("kimi_speedup", 0.0)
                if a.original_ms == 0.0:
                    a.original_ms = csv_data.get("kimi_original_ms", 0.0)
                if a.optimized_ms == 0.0:
                    a.optimized_ms = csv_data.get("kimi_optimized_ms", 0.0)

    # ── Step 3: Select best per query ──
    logger.info("")
    logger.info("Step 3: Selecting best per query...")

    global_queries: Dict[str, GlobalBlackboardQuery] = {}
    with_sql = 0
    with_reasoning = 0
    source_runs = set()

    for lb_entry in lb_queries:
        qid = normalize_query_id(lb_entry.get("query_id", ""))
        lb_source = lb_entry.get("source", "")
        lb_speedup = lb_entry.get("speedup", 0.0)
        lb_transforms = lb_entry.get("transforms", [])

        attempts = all_attempts.get(qid, [])

        # Find the best matching attempt from the winning source
        best_attempt = _find_best_attempt(attempts, lb_source, lb_speedup)

        # Build provenance
        provenance: Dict[str, Any] = {"source": lb_source}
        if best_attempt:
            provenance.update({
                "run": best_attempt.run,
                "model": best_attempt.model,
                "worker_id": best_attempt.worker_id,
                "iteration": best_attempt.iteration,
                "strategy": best_attempt.strategy,
            })
            source_runs.add(best_attempt.run)
        else:
            provenance.update({"run": lb_source, "model": "unknown"})

        # Get SQL: from best attempt → SQL CSV fallback
        optimized_sql = None
        original_sql = None
        reasoning = None
        changes_desc = None
        transforms = lb_transforms

        if best_attempt:
            optimized_sql = best_attempt.optimized_sql
            original_sql = best_attempt.original_sql
            reasoning = best_attempt.reasoning
            changes_desc = best_attempt.changes_description
            if best_attempt.transforms and not transforms:
                transforms = best_attempt.transforms

        # SQL CSV fallback
        if not optimized_sql and qid in sql_fallback:
            optimized_sql = sql_fallback[qid].get("optimized")
        if not original_sql and qid in sql_fallback:
            original_sql = sql_fallback[qid].get("original")

        # Determine principle (first known transform)
        principle = ""
        for t in transforms:
            if t in TRANSFORM_PRINCIPLES:
                principle = t
                break
        if not principle and transforms:
            principle = transforms[0]

        # Build all_attempts summary
        all_attempt_summaries = []
        for a in attempts:
            all_attempt_summaries.append(a.to_dict())
        # Sort by speedup descending
        all_attempt_summaries.sort(key=lambda x: x.get("speedup", 0.0), reverse=True)

        if optimized_sql:
            with_sql += 1
        if reasoning:
            with_reasoning += 1

        global_queries[qid] = GlobalBlackboardQuery(
            query_id=qid,
            status=lb_entry.get("status", ""),
            best_speedup=lb_speedup,
            original_ms=lb_entry.get("original_ms", 0.0),
            optimized_ms=lb_entry.get("optimized_ms", 0.0),
            original_sql=original_sql,
            optimized_sql=optimized_sql,
            transforms=transforms,
            principle=principle,
            changes_description=changes_desc,
            reasoning=reasoning,
            provenance=provenance,
            rows_match=lb_entry.get("rows_match", True),
            all_attempts=all_attempt_summaries,
        )

    logger.info(f"  Built {len(global_queries)} query entries")
    logger.info(f"  With SQL: {with_sql}, With reasoning: {with_reasoning}")

    # ── Step 4: Collate principles + anti-patterns ──
    logger.info("")
    logger.info("Step 4: Collating principles and anti-patterns...")

    # Convert all attempts to BlackboardEntry for reuse of phase2_collate logic
    bb_entries: List[BlackboardEntry] = []
    for qid, attempts in all_attempts.items():
        for a in attempts:
            # Only include attempts with meaningful speedup data
            if a.speedup == 0.0 and not a.error:
                continue
            status = classify_status({
                "error": a.error or "",
                "rows_match": a.rows_match,
                "speedup": a.speedup,
            })
            principle = ""
            for t in a.transforms:
                if t in TRANSFORM_PRINCIPLES:
                    principle = t
                    break
            if not principle and a.transforms:
                principle = a.transforms[0]

            bb_entries.append(BlackboardEntry(
                query_id=qid,
                worker_id=a.worker_id or 0,
                run_name=a.run,
                timestamp=datetime.now().isoformat(),
                strategy=a.strategy,
                status=status,
                speedup=a.speedup,
                transforms_applied=a.transforms,
                error_category=categorize_error(a.error or "") if a.error else None,
                error_messages=[a.error] if a.error else [],
                what_worked=a.changes_description if status in ("WIN", "IMPROVED") else None,
                why_it_worked=(
                    "; ".join(TRANSFORM_PRINCIPLES[t] for t in a.transforms if t in TRANSFORM_PRINCIPLES)
                    if status in ("WIN", "IMPROVED") else None
                ),
                what_failed=a.error if status in ("ERROR", "FAIL", "REGRESSION") else None,
                principle=principle,
                reviewed=True,
            ))

    # Use a temporary dir for collate output (we don't need those files)
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        (tmppath / "blackboard").mkdir()
        principles, anti_patterns = phase2_collate(bb_entries, tmppath)

    logger.info(f"  {len(principles)} principles, {len(anti_patterns)} anti-patterns")

    # ── Step 5: Build summary and write output ──
    logger.info("")
    logger.info("Step 5: Writing output...")

    status_counts: Dict[str, int] = defaultdict(int)
    total_speedup = 0.0
    for gq in global_queries.values():
        status_counts[gq.status] += 1
        total_speedup += gq.best_speedup

    avg_speedup = total_speedup / len(global_queries) if global_queries else 0.0

    output = {
        "dataset": "duckdb_tpcds",
        "engine": "duckdb",
        "scale_factor": 10,
        "last_updated": datetime.now().isoformat(),
        "source_runs": sorted(source_runs),
        "summary": {
            "total": len(global_queries),
            "with_sql": with_sql,
            "with_reasoning": with_reasoning,
            "wins": status_counts.get("WIN", 0),
            "improved": status_counts.get("IMPROVED", 0),
            "neutral": status_counts.get("NEUTRAL", 0),
            "regression": status_counts.get("REGRESSION", 0),
            "avg_speedup": round(avg_speedup, 4),
        },
        "queries": {qid: gq.to_dict() for qid, gq in sorted(global_queries.items())},
        "principles": [p.to_dict() for p in principles],
        "anti_patterns": [a.to_dict() for a in anti_patterns],
    }

    # Write to benchmarks/duckdb_tpcds/knowledge/duckdb_tpcds.json
    knowledge_dir = _ADO_BENCHMARKS / "knowledge"
    knowledge_dir.mkdir(parents=True, exist_ok=True)
    output_path = knowledge_dir / "duckdb_tpcds.json"
    output_path.write_text(json.dumps(output, indent=2))

    logger.info(f"  Output: {output_path}")
    logger.info(f"  Size: {output_path.stat().st_size / 1024:.1f} KB")

    # ── Step 6: Validate ──
    logger.info("")
    logger.info("Step 6: Validating...")

    ok = True

    # Check query count
    if len(global_queries) != len(lb_queries):
        logger.warning(
            f"  Query count mismatch: {len(global_queries)} vs {len(lb_queries)} in leaderboard"
        )
    else:
        logger.info(f"  All {len(global_queries)} queries present")

    # Spot-check speedups
    mismatches = 0
    for lb_entry in lb_queries[:10]:
        qid = normalize_query_id(lb_entry.get("query_id", ""))
        gq = global_queries.get(qid)
        if gq and abs(gq.best_speedup - lb_entry.get("speedup", 0.0)) > 0.01:
            logger.warning(
                f"  Speedup mismatch for {qid}: {gq.best_speedup} vs {lb_entry.get('speedup')}"
            )
            mismatches += 1

    if mismatches == 0:
        logger.info("  Speedup spot-checks passed (top 10)")

    # Check SQL coverage
    logger.info(f"  SQL coverage: {with_sql}/{len(global_queries)} ({100*with_sql/len(global_queries):.0f}%)")
    logger.info(f"  Reasoning coverage: {with_reasoning}/{len(global_queries)} ({100*with_reasoning/len(global_queries):.0f}%)")

    # Check top winners have SQL
    top_5_qids = [normalize_query_id(q["query_id"]) for q in lb_queries[:5]]
    for qid in top_5_qids:
        gq = global_queries.get(qid)
        if gq and not gq.optimized_sql:
            logger.warning(f"  Top winner {qid} ({gq.best_speedup}x) missing SQL")
        elif gq:
            logger.info(f"  Top winner {qid}: {gq.best_speedup}x ✓ SQL + provenance={gq.provenance.get('source')}")

    # Check principles
    logger.info(f"  Principles: {len(principles)}, Anti-patterns: {len(anti_patterns)}")

    logger.info("")
    logger.info("=" * 70)
    logger.info("GLOBAL BLACKBOARD BUILD COMPLETE")
    logger.info(f"  Queries: {len(global_queries)} ({status_counts})")
    logger.info(f"  Avg speedup: {avg_speedup:.4f}x")
    logger.info(f"  With SQL: {with_sql}, With reasoning: {with_reasoning}")
    logger.info(f"  Output: {output_path}")

    return output_path


def _find_best_attempt(
    attempts: List[SourceAttempt],
    target_source: str,
    target_speedup: float,
) -> Optional[SourceAttempt]:
    """Find the attempt that best matches the leaderboard winner.

    Strategy: match by source name, then pick the one closest to target speedup.
    """
    # Map leaderboard source names to SourceAttempt source names
    source_map = {
        "Swarm": "Swarm",
        "Retry4W": "Retry4W",
        "Retry3W": "Retry3W",
        "Kimi": "Kimi",
        "V2": "V2",
        "Evo": "Evo",
        "analyst_mode": "analyst_mode",
        "unvalidated": "unvalidated",
        "state_0": "state_0",
    }
    mapped_source = source_map.get(target_source, target_source)

    # Filter to matching source
    matched = [a for a in attempts if a.source == mapped_source]
    if not matched:
        # Try any source with similar speedup
        matched = attempts

    if not matched:
        return None

    # Find closest speedup match (with SQL preferred)
    def sort_key(a: SourceAttempt) -> tuple:
        has_sql = 1 if a.optimized_sql else 0
        has_reasoning = 1 if a.reasoning else 0
        speedup_diff = -abs(a.speedup - target_speedup)
        return (has_sql, has_reasoning, speedup_diff)

    matched.sort(key=sort_key, reverse=True)
    return matched[0]


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4: Auto-promote winners to gold examples
# ─────────────────────────────────────────────────────────────────────────────


def phase4_promote_winners(
    batch_dir: Path,
    min_speedup: float = 2.0,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """Phase 4: Auto-promote high-speedup winners to gold examples.

    Scans batch results for candidates meeting promotion criteria:
    - speedup >= min_speedup (default 2.0x)
    - Has both original_sql and optimized_sql
    - Has at least one known transform
    - Transform is not already a gold example with a higher speedup

    Writes promoted examples to:
    - ado/examples/{engine}/<transform>.json (ADO)
    - qt_sql/optimization/examples/<transform>.json (V5 CLI)

    Args:
        batch_dir: Path to swarm batch directory
        min_speedup: Minimum speedup to qualify for promotion
        dry_run: If True, only report what would be promoted without writing

    Returns:
        List of promotion records (transform, query_id, speedup, paths)
    """
    bench_cfg = _detect_benchmark_config(batch_dir)
    engine = bench_cfg["engine"]

    # Locate example directories
    ado_examples_dir = _QT_DIR / "examples" / engine
    v5_examples_dir = ado_examples_dir  # consolidated: examples now in one place

    logger.info(f"Phase 4: Auto-promote winners (min_speedup={min_speedup}x, dry_run={dry_run})")

    # Collect existing gold example speedups per transform
    existing_speedups: Dict[str, float] = {}
    if ado_examples_dir.is_dir():
        for ex_path in ado_examples_dir.glob("*.json"):
            try:
                ex_data = json.loads(ex_path.read_text())
                ex_id = ex_data.get("id", ex_path.stem)
                speedup_str = ex_data.get("verified_speedup", "0x")
                existing_speedups[ex_id] = float(speedup_str.replace("x", ""))
            except Exception:
                pass

    # Scan all query directories for promotion candidates
    candidates: List[Dict[str, Any]] = []

    query_dirs = sorted(
        [d for d in batch_dir.iterdir() if d.is_dir() and d.name.startswith("query")],
        key=lambda d: d.name,
    )

    for qdir in query_dirs:
        query_id = qdir.name

        # Check all benchmark iterations for winners
        iter_configs = [
            (0, "benchmark_iter0.json", range(1, 5), "worker_{}_sql.sql"),
            (1, "benchmark_iter1.json", [5], "snipe_worker_sql.sql"),
            (2, "benchmark_iter2.json", [6], "final_worker_sql.sql"),
        ]

        original_sql = load_text(qdir / "original.sql")
        if not original_sql:
            continue

        for iteration, bench_file, worker_ids, sql_pattern in iter_configs:
            bench_data = load_json(qdir / bench_file)
            if not bench_data:
                continue

            workers = {w["worker_id"]: w for w in bench_data.get("workers", [])}

            for wid in worker_ids:
                worker = workers.get(wid)
                if not worker:
                    continue

                speedup = worker.get("speedup", 0.0)
                if speedup < min_speedup:
                    continue

                if not worker.get("rows_match", True):
                    continue

                if worker.get("error"):
                    continue

                # Get optimized SQL
                if iteration == 0:
                    sql_file = sql_pattern.format(wid)
                elif iteration == 1:
                    sql_file = "snipe_worker_sql.sql"
                else:
                    sql_file = "final_worker_sql.sql"

                optimized_sql = load_text(qdir / sql_file)
                if not optimized_sql:
                    continue

                # Get response text for changes description
                if iteration == 0:
                    resp_file = f"worker_{wid}_response.txt"
                elif iteration == 1:
                    resp_file = "snipe_worker_response.txt"
                else:
                    resp_file = "final_worker_response.txt"
                response_text = load_text(qdir / resp_file)

                # Get transforms
                assignments_data = load_json(qdir / "assignments.json")
                assignment = None
                if assignments_data and isinstance(assignments_data, list):
                    for a in assignments_data:
                        if a.get("worker_id") == wid:
                            assignment = a
                            break

                strategy = (assignment or {}).get("strategy", "")
                transforms = extract_transforms(
                    assignment, response_text, strategy,
                    original_sql=original_sql,
                    optimized_sql=optimized_sql,
                )

                if not transforms:
                    continue

                # Use primary transform as the example ID
                primary_transform = transforms[0]

                # Skip if existing example already has a higher speedup
                if primary_transform in existing_speedups:
                    if existing_speedups[primary_transform] >= speedup:
                        continue

                candidates.append({
                    "transform": primary_transform,
                    "query_id": query_id,
                    "worker_id": wid,
                    "iteration": iteration,
                    "speedup": speedup,
                    "original_sql": original_sql,
                    "optimized_sql": optimized_sql,
                    "response_text": response_text,
                    "transforms": transforms,
                    "strategy": strategy,
                })

    # Deduplicate: keep highest speedup per transform
    best_per_transform: Dict[str, Dict[str, Any]] = {}
    for c in candidates:
        t = c["transform"]
        if t not in best_per_transform or c["speedup"] > best_per_transform[t]["speedup"]:
            best_per_transform[t] = c

    promoted: List[Dict[str, Any]] = []

    for transform, candidate in sorted(best_per_transform.items()):
        speedup = candidate["speedup"]
        query_id = candidate["query_id"]
        qnum = query_id.replace("query_", "")

        logger.info(
            f"  {'[DRY RUN] ' if dry_run else ''}Promoting {transform} "
            f"from {query_id} ({speedup:.2f}x)"
        )

        # Build gold example JSON
        changes = extract_changes_section(candidate.get("response_text", "") or "")
        principle_text = TRANSFORM_PRINCIPLES.get(transform, "")
        when_text = PRINCIPLE_WHEN.get(transform, "")

        example_data = {
            "id": transform,
            "name": transform.replace("_", " ").title(),
            "description": changes or f"Apply {transform} optimization pattern",
            "benchmark_queries": [f"Q{qnum}"],
            "verified_speedup": f"{speedup:.2f}x",
            "principle": principle_text,
            "when": when_text,
            "example": {
                "opportunity": transform.upper().replace("_", " "),
                "input_slice": candidate["original_sql"][:1000],
                "output": {
                    "rewrite_sets": [
                        {
                            "id": "rs_01",
                            "transform": transform,
                            "nodes": {
                                "main_query": candidate["optimized_sql"][:3000],
                            },
                            "invariants_kept": [
                                "same result values",
                                "same column output",
                            ],
                            "expected_speedup": f"{speedup:.2f}x",
                            "risk": "low" if speedup < 3.0 else "medium",
                        }
                    ]
                },
                "key_insight": (
                    f"Principle: {transform.replace('_', ' ').title()} — "
                    f"{principle_text} "
                    f"Achieved {speedup:.2f}x speedup on {query_id}."
                ),
            },
            "original_sql": candidate["original_sql"],
            "optimized_sql": candidate["optimized_sql"],
            "optimized_source": f"auto_promoted_{candidate.get('strategy', 'swarm')[:50]}",
            "benchmark_query_num": int(qnum) if qnum.isdigit() else 0,
        }

        record = {
            "transform": transform,
            "query_id": query_id,
            "speedup": speedup,
            "action": "update" if transform in existing_speedups else "create",
        }

        if not dry_run:
            # Write to ADO examples
            ado_examples_dir.mkdir(parents=True, exist_ok=True)
            ado_path = ado_examples_dir / f"{transform}.json"
            ado_path.write_text(json.dumps(example_data, indent=2))
            record["ado_path"] = str(ado_path)

            # Write to V5 CLI examples
            v5_examples_dir.mkdir(parents=True, exist_ok=True)
            v5_path = v5_examples_dir / f"{transform}.json"
            v5_path.write_text(json.dumps(example_data, indent=2))
            record["v5_path"] = str(v5_path)

            logger.info(f"    Wrote: {ado_path.name} + {v5_path.name}")
        else:
            record["ado_path"] = str(ado_examples_dir / f"{transform}.json")
            record["v5_path"] = str(v5_examples_dir / f"{transform}.json")

        promoted.append(record)

    if promoted:
        logger.info(
            f"Phase 4 complete: {len(promoted)} examples "
            f"{'would be ' if dry_run else ''}promoted"
        )
        if not dry_run:
            logger.info("  Run `python3 -m ado.faiss_builder` to rebuild the tag index")
    else:
        logger.info("Phase 4 complete: no new examples qualify for promotion")

    return promoted


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────


def main():
    if "--global" in sys.argv:
        build_global_blackboard()
        return

    if "--promote-only" in sys.argv:
        # Run only phase 4 (auto-promote) on an existing batch
        args = [a for a in sys.argv[1:] if not a.startswith("--")]
        if not args:
            print("Usage: python3 -m ado.build_blackboard --promote-only <swarm_batch_dir>")
            sys.exit(1)
        batch_dir = Path(args[0])
        dry_run = "--dry-run" in sys.argv
        promoted = phase4_promote_winners(batch_dir, dry_run=dry_run)
        logger.info(f"Promoted {len(promoted)} examples")
        return

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 -m ado.build_blackboard <swarm_batch_dir>   # Swarm blackboard")
        print("  python3 -m ado.build_blackboard --global            # Global blackboard")
        print("  python3 -m ado.build_blackboard --promote-only <dir> # Auto-promote winners only")
        print()
        print("Flags:")
        print("  --dry-run    Show what would be promoted without writing")
        print()
        print("Examples:")
        print("  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.build_blackboard \\")
        print("      packages/qt-sql/ado/benchmarks/duckdb_tpcds/swarm_batch_20260208_102033")
        print("  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.build_blackboard --global")
        sys.exit(1)

    batch_dir = Path(sys.argv[1])
    if not batch_dir.is_dir():
        logger.error(f"Not a directory: {batch_dir}")
        sys.exit(1)

    # Detect benchmark config (engine, dataset, output paths)
    bench_cfg = _detect_benchmark_config(batch_dir)
    dataset = bench_cfg["dataset"]
    benchmark_dir = bench_cfg["benchmark_dir"]

    logger.info(f"Building blackboard from: {batch_dir}")
    logger.info(f"  Engine: {bench_cfg['engine']}, Benchmark: {bench_cfg['benchmark']}, Dataset: {dataset}")
    logger.info("=" * 70)

    # Phase 1: Extract
    entries = phase1_extract(batch_dir)
    logger.info("")

    # Phase 2: Collate
    principles, anti_patterns = phase2_collate(entries, batch_dir)
    logger.info("")

    # Phase 3: Global knowledge
    gk = phase3_global(principles, anti_patterns, batch_dir, dataset=dataset, benchmark_dir=benchmark_dir)
    logger.info("")

    # Validate
    logger.info("=" * 70)
    logger.info("Running validation...")
    valid = validate_output(gk, batch_dir)
    if valid:
        logger.info("Validation PASSED")
    else:
        logger.warning("Validation had warnings/errors — check output above")

    # Phase 4: Auto-promote winners
    logger.info("")
    promoted = phase4_promote_winners(batch_dir, dry_run="--dry-run" in sys.argv)
    logger.info("")

    # Final summary
    knowledge_path = benchmark_dir / "knowledge" / f"{dataset}.json"
    logger.info("")
    logger.info("=" * 70)
    logger.info("BLACKBOARD BUILD COMPLETE")
    logger.info(f"  Raw entries:    {len(entries)}")
    logger.info(f"  Principles:     {len(principles)}")
    logger.info(f"  Anti-patterns:  {len(anti_patterns)}")
    logger.info(f"  Promoted:       {len(promoted)}")
    logger.info(f"  Output dirs:")
    logger.info(f"    blackboard/raw/     — {len(set(e.query_id for e in entries))} queries")
    logger.info(f"    blackboard/collated.json")
    logger.info(f"    {knowledge_path}")


if __name__ == "__main__":
    main()
