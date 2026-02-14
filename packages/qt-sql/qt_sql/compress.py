"""Compress Stage — deduplicate and rank candidates post-validation.

After fan-out validation produces 4 candidate rewrites with timing data,
the compress stage:
1. Deduplicates candidates with equivalent AST (whitespace-only differences)
2. Scores each by: Impact × Confidence × Invasiveness (1-125 range)
3. Returns ranked candidates for sniper selection

This is a POST-validation step. Race validation, semantic pre-validation,
and EXPLAIN collection are all unchanged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ScoredCandidate:
    """A candidate with compress scoring."""
    worker_id: int
    sql: str
    speedup: float
    transforms: List[str] = field(default_factory=list)
    set_local_commands: List[str] = field(default_factory=list)

    # Scoring components (1-5 each)
    impact_score: int = 1
    confidence_score: int = 1
    invasiveness_score: int = 5  # 5 = least invasive = best

    @property
    def composite_score(self) -> int:
        """Impact × Confidence × Invasiveness. Range: 1-125."""
        return self.impact_score * self.confidence_score * self.invasiveness_score


@dataclass
class CompressResult:
    """Result of compress stage."""
    ranked_candidates: List[ScoredCandidate] = field(default_factory=list)
    n_original: int = 0
    n_after_dedup: int = 0
    n_duplicates_removed: int = 0


def compress_candidates(
    candidates: List[Dict[str, Any]],
    validation_tier: str = "race",
    dialect: str = "duckdb",
) -> CompressResult:
    """Deduplicate and rank candidates.

    Args:
        candidates: List of dicts with keys: worker_id, sql, speedup,
                   transforms, set_local_commands
        validation_tier: How candidates were validated:
                        "race" | "sequential_3run" | "single_run" | "cost_only" | "parse_only"
        dialect: SQL dialect for AST normalization

    Returns:
        CompressResult with ranked, deduplicated candidates.
    """
    result = CompressResult(n_original=len(candidates))

    if not candidates:
        return result

    # Step 1: Deduplicate by normalized AST
    unique = _dedup_candidates(candidates, dialect)
    result.n_after_dedup = len(unique)
    result.n_duplicates_removed = result.n_original - result.n_after_dedup

    if result.n_duplicates_removed > 0:
        logger.info(
            f"Compress: removed {result.n_duplicates_removed} duplicate candidates "
            f"({result.n_original} → {result.n_after_dedup})"
        )

    # Step 2: Score each candidate
    confidence = _confidence_from_tier(validation_tier)

    scored = []
    for c in unique:
        sc = ScoredCandidate(
            worker_id=c.get("worker_id", 0),
            sql=c.get("sql", ""),
            speedup=c.get("speedup", 1.0),
            transforms=c.get("transforms", []),
            set_local_commands=c.get("set_local_commands", []),
            impact_score=_impact_score(c.get("speedup", 1.0)),
            confidence_score=confidence,
            invasiveness_score=_invasiveness_score(
                c.get("set_local_commands", []),
                c.get("transforms", []),
            ),
        )
        scored.append(sc)

    # Step 3: Rank by composite score (descending), break ties by fewer transforms
    scored.sort(
        key=lambda s: (s.composite_score, -len(s.transforms)),
        reverse=True,
    )

    result.ranked_candidates = scored
    return result


def _dedup_candidates(
    candidates: List[Dict[str, Any]],
    dialect: str,
) -> List[Dict[str, Any]]:
    """Remove candidates with identical normalized AST.

    Uses sqlglot for normalization. Falls back to whitespace-normalized
    string comparison if sqlglot fails.
    """
    seen_normalized: Dict[str, Dict[str, Any]] = {}

    for c in candidates:
        sql = c.get("sql", "")
        normalized = _normalize_sql(sql, dialect)

        if normalized in seen_normalized:
            # Keep the one with higher speedup
            existing = seen_normalized[normalized]
            if c.get("speedup", 0) > existing.get("speedup", 0):
                seen_normalized[normalized] = c
        else:
            seen_normalized[normalized] = c

    return list(seen_normalized.values())


def _normalize_sql(sql: str, dialect: str) -> str:
    """Normalize SQL for deduplication comparison.

    Tries sqlglot AST normalization first, falls back to
    whitespace normalization.
    """
    try:
        import sqlglot
        dialect_map = {
            "postgres": "postgres",
            "postgresql": "postgres",
            "duckdb": "duckdb",
            "snowflake": "snowflake",
        }
        sg_dialect = dialect_map.get(dialect.lower(), dialect.lower())
        parsed = sqlglot.parse_one(sql, dialect=sg_dialect)
        return parsed.sql(dialect=sg_dialect)
    except Exception:
        # Fallback: whitespace normalization
        return " ".join(sql.split()).strip().upper()


def _impact_score(speedup: float) -> int:
    """Score 1-5 based on validated speedup.

    5: >2x speedup validated
    4: 1.5-2x speedup validated
    3: 1.2-1.5x speedup validated
    2: 1.1-1.2x speedup validated
    1: <1.1x or estimate-only
    """
    if speedup >= 2.0:
        return 5
    elif speedup >= 1.5:
        return 4
    elif speedup >= 1.2:
        return 3
    elif speedup >= 1.1:
        return 2
    else:
        return 1


def _confidence_from_tier(tier: str) -> int:
    """Score 1-5 based on validation tier.

    5: Race-validated (simultaneous execution)
    4: Sequential 3-run validated
    3: Single-run validated
    2: Cost-estimate only
    1: No validation (parse-only)
    """
    tier_scores = {
        "race": 5,
        "sequential_3run": 4,
        "single_run": 3,
        "cost_only": 2,
        "parse_only": 1,
    }
    return tier_scores.get(tier, 3)


def _invasiveness_score(
    set_local_commands: List[str],
    transforms: List[str],
) -> int:
    """Score 5 (least invasive) to 1 (most invasive).

    5: Query rewrite only (no config changes)
    4: Query rewrite + SET LOCAL config
    3: Query rewrite + statistics refresh
    2: Query rewrite + index creation
    1: Query rewrite + schema changes
    """
    if not set_local_commands:
        return 5

    # Check what kind of config changes
    cmds_lower = " ".join(set_local_commands).lower()
    if "create index" in cmds_lower or "cluster" in cmds_lower:
        return 2
    if "analyze" in cmds_lower or "statistics" in cmds_lower:
        return 3
    if set_local_commands:
        return 4

    return 5
