"""Beam session analyzer — scan beam sessions and extract promotion candidates.

Parses beam session output directories and surfaces high-speedup wins and
instructive regressions for human review and promotion to gold examples.

Usage:
    from qt_sql.beam_analyzer import analyze_beam_sessions
    candidates = analyze_beam_sessions(Path("benchmarks/duckdb_tpcds/beam_sessions"))
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class PromotionCandidate:
    """A single optimization attempt from beam that may qualify for promotion."""

    query_id: str
    patch_id: str
    iteration: int
    family: str  # A-F
    transform: str
    speedup: float
    status: str  # WIN/REGRESSION/NEUTRAL/IMPROVED/FAIL/ERROR
    original_sql: str
    optimized_sql: str
    original_ms: float
    patch_ms: float
    explain_before: str  # Original EXPLAIN (if available)
    explain_after: str  # Patch EXPLAIN (if available)
    hypothesis: str  # Analyst reasoning from targets.txt
    semantic_passed: bool
    error: Optional[str]
    engine: str  # duckdb/postgresql/snowflake
    benchmark: str  # tpcds/dsb/etc.
    session_dir: str  # Path to session directory (for traceability)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    """Load JSON file, returning None on failure."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("Failed to load %s: %s", path, e)
        return None


def _find_original_sql(session_dir: Path, benchmark_dir: Optional[Path]) -> str:
    """Find original SQL for a session.

    Looks in:
    1. The analyst prompt (contains original SQL)
    2. The benchmark queries/ directory (queries/{query_id}.sql)
    """
    # Try to extract query_id from session directory name
    # Format: query{NNN}_{scenario}_{iN}_{timestamp}
    dir_name = session_dir.name
    match = re.match(r"(query\d+)", dir_name)
    if not match:
        return ""

    query_stem = match.group(1)

    # Try benchmark queries directory
    if benchmark_dir:
        for sql_file in sorted(benchmark_dir.glob(f"queries/{query_stem}*.sql")):
            try:
                return sql_file.read_text(encoding="utf-8").strip()
            except OSError:
                continue

    return ""


def _extract_hypothesis(
    session_dir: Path, iteration: int, patch_id: str
) -> str:
    """Extract analyst hypothesis for a specific patch from targets.txt."""
    targets_file = session_dir / f"iter{iteration}_targets.txt"
    targets = _load_json(targets_file)
    if not targets or not isinstance(targets, list):
        return ""

    for target in targets:
        if target.get("target_id") == patch_id:
            return target.get("hypothesis", "")

    # For synthetic workers (syn_w1, syn_w2, etc.), no direct target match
    return ""


def analyze_beam_sessions(
    sessions_dir: Path,
    benchmark_dir: Optional[Path] = None,
    min_speedup: float = 2.0,
    max_regression: float = 0.90,
) -> Dict[str, List[PromotionCandidate]]:
    """Scan all beam sessions and extract promotion candidates.

    Args:
        sessions_dir: Path to beam_sessions/ directory
        benchmark_dir: Path to benchmark root (for resolving original SQL)
        min_speedup: Minimum speedup for win candidates (default: 2.0x)
        max_regression: Maximum speedup for regression candidates (default: 0.90x)

    Returns:
        {
          "wins": [candidates with speedup >= min_speedup],
          "regressions": [candidates with speedup < max_regression, correct SQL]
        }
    """
    if not sessions_dir.exists():
        logger.warning("Sessions directory not found: %s", sessions_dir)
        return {"wins": [], "regressions": []}

    # Infer benchmark_dir from sessions_dir if not provided
    if benchmark_dir is None:
        # sessions_dir is typically benchmark_dir/beam_sessions
        candidate_parent = sessions_dir.parent
        if (candidate_parent / "config.json").exists():
            benchmark_dir = candidate_parent

    wins: List[PromotionCandidate] = []
    regressions: List[PromotionCandidate] = []
    sessions_scanned = 0

    for session_path in sorted(sessions_dir.iterdir()):
        if not session_path.is_dir():
            continue

        # Load metadata
        metadata = _load_json(session_path / "metadata.json")
        if not metadata:
            logger.debug("Skipping %s — no metadata.json", session_path.name)
            continue

        sessions_scanned += 1
        engine = metadata.get("engine", "unknown")
        benchmark = metadata.get("benchmark", "unknown")

        # Find original SQL
        original_sql = _find_original_sql(session_path, benchmark_dir)

        # Scan all iteration result files
        for result_file in sorted(session_path.glob("iter*_result.txt")):
            result = _load_json(result_file)
            if not result:
                continue

            iteration = result.get("iteration", 0)

            for patch in result.get("patches", []):
                patch_id = patch.get("patch_id", "")
                speedup = patch.get("speedup")
                status = patch.get("status", "")
                semantic_passed = patch.get("semantic_passed", False)
                error = patch.get("error")

                if speedup is None:
                    continue

                # Extract EXPLAIN data
                explains = result.get("explains", {})
                explain_after = explains.get(patch_id, "")

                # Original explain — check for "original" key or first target
                explain_before = explains.get("original", "")

                # Extract hypothesis from targets
                hypothesis = _extract_hypothesis(session_path, iteration, patch_id)

                candidate = PromotionCandidate(
                    query_id=metadata.get("query_id", session_path.name),
                    patch_id=patch_id,
                    iteration=iteration,
                    family=patch.get("family", "?"),
                    transform=patch.get("transform", "unknown"),
                    speedup=speedup,
                    status=status,
                    original_sql=original_sql,
                    optimized_sql=patch.get("output_sql", ""),
                    original_ms=patch.get("original_ms", 0.0) or 0.0,
                    patch_ms=patch.get("patch_ms", 0.0) or 0.0,
                    explain_before=explain_before,
                    explain_after=explain_after,
                    hypothesis=hypothesis,
                    semantic_passed=semantic_passed,
                    error=error,
                    engine=engine,
                    benchmark=benchmark,
                    session_dir=str(session_path),
                )

                # Classify
                if speedup >= min_speedup and semantic_passed and error is None:
                    wins.append(candidate)
                elif (
                    speedup < max_regression
                    and status == "REGRESSION"
                    and error is None
                    and semantic_passed
                ):
                    regressions.append(candidate)

    # Sort: wins by speedup descending, regressions by speedup ascending
    wins.sort(key=lambda c: c.speedup, reverse=True)
    regressions.sort(key=lambda c: c.speedup)

    logger.info(
        "Scanned %d sessions: %d wins (>= %.1fx), %d regressions (< %.2fx)",
        sessions_scanned,
        len(wins),
        min_speedup,
        len(regressions),
        max_regression,
    )

    return {"wins": wins, "regressions": regressions}


def deduplicate_candidates(
    candidates: List[PromotionCandidate],
) -> List[PromotionCandidate]:
    """Keep only the best candidate per (query_id, transform) pair.

    When the same query+transform appears across multiple iterations or sessions,
    keep the one with highest speedup (for wins) or lowest speedup (for regressions).
    """
    best: Dict[str, PromotionCandidate] = {}

    for c in candidates:
        key = f"{c.query_id}::{c.transform}"
        existing = best.get(key)
        if existing is None:
            best[key] = c
        elif c.speedup >= 1.0 and c.speedup > existing.speedup:
            best[key] = c  # Better win
        elif c.speedup < 1.0 and c.speedup < existing.speedup:
            best[key] = c  # More severe regression

    return sorted(best.values(), key=lambda c: c.speedup, reverse=True)


def candidates_to_json(candidates: List[PromotionCandidate]) -> List[Dict[str, Any]]:
    """Serialize candidates to JSON-safe dicts."""
    return [
        {
            "query_id": c.query_id,
            "patch_id": c.patch_id,
            "iteration": c.iteration,
            "family": c.family,
            "transform": c.transform,
            "speedup": round(c.speedup, 2),
            "status": c.status,
            "original_sql": c.original_sql,
            "optimized_sql": c.optimized_sql,
            "original_ms": round(c.original_ms, 1),
            "patch_ms": round(c.patch_ms, 1),
            "explain_before": c.explain_before,
            "explain_after": c.explain_after,
            "hypothesis": c.hypothesis,
            "semantic_passed": c.semantic_passed,
            "error": c.error,
            "engine": c.engine,
            "benchmark": c.benchmark,
            "session_dir": c.session_dir,
        }
        for c in candidates
    ]


def candidates_from_json(data: List[Dict[str, Any]]) -> List[PromotionCandidate]:
    """Deserialize candidates from JSON dicts."""
    return [
        PromotionCandidate(
            query_id=d["query_id"],
            patch_id=d["patch_id"],
            iteration=d.get("iteration", 0),
            family=d.get("family", "?"),
            transform=d["transform"],
            speedup=d["speedup"],
            status=d.get("status", ""),
            original_sql=d.get("original_sql", ""),
            optimized_sql=d.get("optimized_sql", ""),
            original_ms=d.get("original_ms", 0.0),
            patch_ms=d.get("patch_ms", 0.0),
            explain_before=d.get("explain_before", ""),
            explain_after=d.get("explain_after", ""),
            hypothesis=d.get("hypothesis", ""),
            semantic_passed=d.get("semantic_passed", True),
            error=d.get("error"),
            engine=d.get("engine", "unknown"),
            benchmark=d.get("benchmark", "unknown"),
            session_dir=d.get("session_dir", ""),
        )
        for d in data
    ]


def _speedup_stars(speedup: float) -> str:
    """Return star rating for speedup magnitude."""
    if speedup >= 5.0:
        return " *****"
    elif speedup >= 3.0:
        return " ****"
    elif speedup >= 2.0:
        return " ***"
    elif speedup >= 1.5:
        return " **"
    elif speedup >= 1.1:
        return " *"
    return ""


def _regression_severity(speedup: float) -> str:
    """Return severity indicator for regressions."""
    if speedup < 0.20:
        return " !!!"
    elif speedup < 0.50:
        return " !!"
    return " !"


def format_candidates_report(
    results: Dict[str, List[PromotionCandidate]],
    benchmark_name: str = "",
    sessions_dir: str = "",
) -> str:
    """Format candidates into a human-readable report."""
    lines: List[str] = []

    lines.append("=" * 60)
    lines.append(f"Beam Session Analysis: {benchmark_name}")
    if sessions_dir:
        lines.append(f"Sessions: {sessions_dir}")
    lines.append("=" * 60)
    lines.append("")

    # Wins
    wins = results.get("wins", [])
    lines.append(f"WINS (>= 2.0x speedup): {len(wins)}")
    lines.append("")

    for i, c in enumerate(wins, 1):
        stars = _speedup_stars(c.speedup)
        lines.append(f"  [{i:2d}] {c.query_id} ({c.patch_id}) — {c.speedup:.2f}x{stars}")
        lines.append(f"       Transform: {c.transform} (Family {c.family})")
        lines.append(f"       Original: {c.original_ms:.1f}ms -> Optimized: {c.patch_ms:.1f}ms")
        if c.hypothesis:
            hyp = c.hypothesis[:120] + "..." if len(c.hypothesis) > 120 else c.hypothesis
            lines.append(f"       Hypothesis: {hyp}")
        lines.append("")

    # Regressions
    regressions = results.get("regressions", [])
    lines.append(f"REGRESSIONS (< 0.90x, correct SQL): {len(regressions)}")
    lines.append("")

    for i, c in enumerate(regressions, 1):
        severity = _regression_severity(c.speedup)
        lines.append(f"  [{i:2d}] {c.query_id} ({c.patch_id}) — {c.speedup:.2f}x{severity}")
        lines.append(f"       Transform: {c.transform} (Family {c.family})")
        lines.append(f"       Original: {c.original_ms:.1f}ms -> Optimized: {c.patch_ms:.1f}ms")
        if c.hypothesis:
            hyp = c.hypothesis[:120] + "..." if len(c.hypothesis) > 120 else c.hypothesis
            lines.append(f"       Hypothesis: {hyp}")
        lines.append("")

    # Summary
    lines.append("=" * 60)
    lines.append("Summary:")
    lines.append(f"  Wins >= 2.0x:                {len(wins)}")
    lines.append(f"  Regressions < 0.90x (correct): {len(regressions)}")
    lines.append(f"  Total promotion candidates:    {len(wins) + len(regressions)}")
    lines.append("=" * 60)

    return "\n".join(lines)
