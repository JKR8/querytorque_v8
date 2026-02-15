"""Forensic intelligence collector — builds WorkloadProfile from benchmark data.

Single entry point: collect_workload_profile(benchmark_dir, engine) -> WorkloadProfile
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    CostEntry,
    EngineGap,
    EngineProfile,
    EngineStrength,
    ExecutionSummary,
    ForensicQuery,
    ForensicSummary,
    ForensicTransformMatch,
    ImpactSummary,
    PatternCoverage,
    PatternStat,
    QErrorEntry,
    QueryResult,
    ResourceImpact,
    ResourceProfile,
    RunSummary,
    WorkloadProfile,
)

logger = logging.getLogger(__name__)

# Triage constants (mirror fleet/orchestrator.py)
_RUNTIME_THRESHOLDS = [
    (100, "SKIP"), (1_000, "LOW"), (10_000, "MEDIUM"), (float("inf"), "HIGH"),
]
_RUNTIME_WEIGHTS = {"SKIP": 0, "LOW": 1, "MEDIUM": 3, "HIGH": 5}


# ---------------------------------------------------------------------------
# Query ID normalization
# ---------------------------------------------------------------------------

def normalize_qid(raw: str) -> str:
    """Normalize query ID to canonical q{N} format, preserving variant suffixes.

    Handles:
      q88              -> q88
      query_1          -> q1
      query001         -> q1
      query_88         -> q88
      query013_spj_i1  -> q13_spj_i1   (suffix preserved — distinct variant)
      query013_spj_i2  -> q13_spj_i2   (suffix preserved — distinct variant)
    """
    raw = raw.strip()
    if re.match(r'^q\d+', raw):
        return raw
    m = re.match(r'^query[_-]?0*(\d+)(.*)', raw, re.IGNORECASE)
    if m:
        return f"q{int(m.group(1))}{m.group(2)}"
    return raw


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def collect_workload_profile(
    benchmark_dir: Path,
    engine: str,
) -> WorkloadProfile:
    """Build a complete WorkloadProfile from benchmark directory."""
    forensic = _build_forensic(benchmark_dir, engine)
    execution = _build_execution(benchmark_dir)
    impact = _build_impact(forensic, execution, engine)

    return WorkloadProfile(
        benchmark_name=benchmark_dir.name,
        engine=engine,
        collected_at=datetime.now(timezone.utc).isoformat(),
        forensic=forensic,
        execution=execution,
        impact=impact,
    )


# ---------------------------------------------------------------------------
# Forensic (pre-execution intelligence)
# ---------------------------------------------------------------------------

def _build_forensic(benchmark_dir: Path, engine: str) -> ForensicSummary:
    queries_dir = benchmark_dir / "queries"
    if not queries_dir.exists():
        return ForensicSummary(total_queries=0, total_runtime_ms=0.0)

    query_files = sorted(queries_dir.glob("*.sql"))
    if not query_files:
        return ForensicSummary(total_queries=0, total_runtime_ms=0.0)

    # Load AST detection catalog
    transforms_catalog = None
    catalog_by_id: Dict[str, Any] = {}
    try:
        from ..detection import detect_transforms, load_transforms
        transforms_catalog = load_transforms()
        catalog_by_id = {t["id"]: t for t in transforms_catalog}
    except Exception as e:
        logger.warning(f"Detection unavailable: {e}")

    engine_name = {"postgresql": "postgresql", "postgres": "postgresql"}.get(
        engine, engine)
    dialect = "postgres" if engine in ("postgresql", "postgres") else engine

    # Load q-error data (keyed by normalized query ID)
    qerror_map = _load_qerror_data(benchmark_dir)

    # Build per-query ForensicQuery objects
    forensic_queries: List[ForensicQuery] = []
    pattern_entries: List[Tuple[str, float, str, Dict[str, float]]] = []

    for sql_path in query_files:
        raw_qid = sql_path.stem
        qid = normalize_qid(raw_qid)
        sql = sql_path.read_text().strip()
        runtime_ms = load_explain_timing(benchmark_dir, raw_qid, engine)
        bucket = _bucket_runtime(runtime_ms)

        # AST detection
        pattern_overlaps: Dict[str, float] = {}
        matched_transforms: List[ForensicTransformMatch] = []
        tractability = 0
        top_overlap = 0.0
        top_transform = ""
        n_matches = 0

        if transforms_catalog and sql:
            try:
                matched = detect_transforms(
                    sql, transforms_catalog,
                    engine=engine_name, dialect=dialect,
                )
                for m in matched:
                    if m.overlap_ratio >= 0.6:
                        n_matches += 1
                        tractability += 1
                        pattern_overlaps[m.id] = m.overlap_ratio
                        t_meta = catalog_by_id.get(m.id, {})
                        matched_transforms.append(ForensicTransformMatch(
                            id=m.id,
                            overlap=round(m.overlap_ratio, 3),
                            gap=m.gap,
                            family=t_meta.get("family", ""),
                        ))
                if matched:
                    top_overlap = matched[0].overlap_ratio
                    top_transform = matched[0].id
                tractability = min(tractability, 4)
            except Exception as e:
                logger.debug(f"[{qid}] Detection error: {e}")

        # Priority score (PLAN.md §4.3)
        weight = _RUNTIME_WEIGHTS.get(bucket, 0)
        priority_score = weight * (1.0 + tractability + top_overlap)

        # Q-error (join by normalized ID)
        qerror = qerror_map.get(qid)

        # Structural flags from q-error
        structural_flags: List[str] = []
        if qerror and qerror.structural_flags:
            structural_flags = [f for f in qerror.structural_flags.split("|") if f]

        # EXPLAIN text for drawer
        has_explain, explain_text = _load_explain_text(benchmark_dir, raw_qid)

        fq = ForensicQuery(
            query_id=qid,
            runtime_ms=runtime_ms,
            bucket=bucket,
            top_overlap=round(top_overlap, 3),
            tractability=tractability,
            n_matches=n_matches,
            top_transform=top_transform,
            priority_score=round(priority_score, 1),
            matched_transforms=matched_transforms,
            qerror=qerror,
            structural_flags=structural_flags,
            has_explain=has_explain,
            explain_text=explain_text,
        )
        forensic_queries.append(fq)
        pattern_entries.append((qid, runtime_ms, bucket, pattern_overlaps))

    # Sort by runtime descending, compute cost context
    total_runtime = sum(q.runtime_ms for q in forensic_queries if q.runtime_ms > 0)
    by_runtime = sorted(forensic_queries, key=lambda q: -max(q.runtime_ms, 0))
    cumulative = 0.0
    for rank, fq in enumerate(by_runtime, 1):
        pct = (fq.runtime_ms / total_runtime) if total_runtime > 0 and fq.runtime_ms > 0 else 0.0
        cumulative += pct
        fq.pct_of_total = round(pct, 4)
        fq.cumulative_pct = round(cumulative, 4)
        fq.cost_rank = rank

    # Legacy cost_concentration (for existing frontend)
    cost_concentration: List[CostEntry] = [
        CostEntry(
            query_id=fq.query_id,
            runtime_ms=fq.runtime_ms,
            pct_of_total=fq.pct_of_total,
            cumulative_pct=fq.cumulative_pct,
            bucket=fq.bucket,
            detected_patterns=[m.id for m in fq.matched_transforms],
        )
        for fq in by_runtime
    ]

    # Bucket distribution
    bucket_dist: Dict[str, int] = defaultdict(int)
    for fq in forensic_queries:
        bucket_dist[fq.bucket] += 1

    # Pattern coverage
    with_detection = sum(1 for fq in forensic_queries if fq.matched_transforms)
    without_detection = len(forensic_queries) - with_detection
    pattern_stats = _compute_pattern_stats(pattern_entries, catalog_by_id)

    # Engine profile + compute per-gap query matches
    engine_profile = _load_engine_profile(engine)
    if engine_profile:
        _compute_gap_matches(engine_profile, forensic_queries)

    # Dominant pathology
    dominant_pathology = _compute_dominant_pathology(forensic_queries)

    # Estimated opportunity (HIGH + MEDIUM bucket queries)
    estimated_opportunity = sum(
        fq.runtime_ms for fq in forensic_queries
        if fq.bucket in ("HIGH", "MEDIUM") and fq.runtime_ms > 0
    )

    # Resource profile (PG only)
    resource_profile = _load_resource_profile(benchmark_dir, engine)

    return ForensicSummary(
        total_queries=len(forensic_queries),
        total_runtime_ms=round(total_runtime, 1),
        queries=by_runtime,
        cost_concentration=cost_concentration,
        bucket_distribution=dict(bucket_dist),
        pattern_coverage=PatternCoverage(
            queries_with_detection=with_detection,
            queries_without_detection=without_detection,
            top_patterns=pattern_stats,
        ),
        engine_profile=engine_profile,
        resource_profile=resource_profile,
        dominant_pathology=dominant_pathology,
        estimated_opportunity_ms=round(estimated_opportunity, 1),
    )


# ---------------------------------------------------------------------------
# Forensic data loaders
# ---------------------------------------------------------------------------

def _load_qerror_data(benchmark_dir: Path) -> Dict[str, QErrorEntry]:
    """Load q-error analysis, return dict keyed by normalized query ID."""
    path = benchmark_dir / "qerror_analysis.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        result: Dict[str, QErrorEntry] = {}
        for entry in data:
            raw_qid = entry.get("query_id", "")
            qid = normalize_qid(raw_qid)
            result[qid] = QErrorEntry(
                severity=entry.get("severity", ""),
                direction=entry.get("direction", ""),
                worst_node=entry.get("worst_node", ""),
                worst_est=entry.get("worst_est", 0),
                worst_act=entry.get("worst_act", 0),
                max_q_error=entry.get("max_q_error", 0.0),
                locus=entry.get("locus", ""),
                pathology_routing=entry.get("pathology_routing", ""),
                structural_flags=entry.get("structural_flags", ""),
                n_signals=entry.get("n_signals", 0),
            )
        return result
    except Exception as e:
        logger.debug(f"Q-error load error: {e}")
        return {}


def _load_engine_profile(engine: str) -> Optional[EngineProfile]:
    """Load engine profile from constraints directory."""
    engine_key = {"postgres": "postgresql", "postgresql": "postgresql"}.get(
        engine, engine)
    constraints_dir = Path(__file__).resolve().parent.parent / "constraints"
    path = constraints_dir / f"engine_profile_{engine_key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        strengths = [
            EngineStrength(
                id=s.get("id", ""),
                summary=s.get("summary", ""),
                implication=s.get("implication", ""),
            )
            for s in data.get("strengths", [])
        ]
        gaps = [
            EngineGap(
                id=g.get("id", ""),
                priority=g.get("priority", ""),
                what=g.get("what", ""),
                why=g.get("why", ""),
                opportunity=g.get("opportunity", ""),
                what_worked=g.get("what_worked", ""),
            )
            for g in data.get("gaps", [])
        ]
        return EngineProfile(
            engine=data.get("engine", engine),
            version_tested=data.get("version_tested", ""),
            briefing_note=data.get("briefing_note", ""),
            strengths=strengths,
            gaps=gaps,
        )
    except Exception as e:
        logger.debug(f"Engine profile load error: {e}")
        return None


def _compute_gap_matches(
    profile: EngineProfile,
    queries: List[ForensicQuery],
) -> None:
    """Populate n_queries_matched and matched_query_ids on each engine gap.

    Cross-references gap IDs against each query's matched transforms.
    A query "matches" a gap if any of its transforms target that gap.
    """
    for gap in profile.gaps:
        matched_ids = [
            fq.query_id for fq in queries
            if any(m.gap == gap.id for m in fq.matched_transforms)
        ]
        gap.n_queries_matched = len(matched_ids)
        gap.matched_query_ids = matched_ids


def _load_explain_text(benchmark_dir: Path, query_id: str) -> Tuple[bool, str]:
    """Load EXPLAIN plan text for a query. Returns (has_explain, truncated_text)."""
    search_paths = [
        benchmark_dir / "explains" / f"{query_id}.json",
        benchmark_dir / "explains" / "sf10" / f"{query_id}.json",
        benchmark_dir / "explains" / "sf5" / f"{query_id}.json",
    ]
    for path in search_paths:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
            text = data.get("plan_text", "")
            if not text:
                # PG format — render plan tree from plan_json
                plan_json = data.get("plan_json")
                if isinstance(plan_json, list) and plan_json:
                    text = _render_pg_plan(plan_json[0].get("Plan", {}))
            if text:
                # Truncate to 80 lines
                lines = text.splitlines()
                if len(lines) > 80:
                    text = "\n".join(lines[:80]) + f"\n... ({len(lines) - 80} more lines)"
                return True, text
        except Exception:
            pass
    return False, ""


def _render_pg_plan(node: dict, depth: int = 0) -> str:
    """Render a PG EXPLAIN JSON plan node into human-readable indented text."""
    if not node:
        return ""
    indent = "  " * depth
    node_type = node.get("Node Type", "?")
    actual_time = node.get("Actual Total Time")
    actual_rows = node.get("Actual Rows")
    plan_rows = node.get("Plan Rows")

    line = f"{indent}-> {node_type}"
    extras = []
    if actual_time is not None:
        extras.append(f"time={actual_time:.1f}ms")
    if actual_rows is not None and plan_rows is not None:
        extras.append(f"rows={actual_rows} (est {plan_rows})")
    elif actual_rows is not None:
        extras.append(f"rows={actual_rows}")
    if node.get("Filter"):
        extras.append(f"filter: {node['Filter']}")
    if node.get("Join Type"):
        extras.append(f"join: {node['Join Type']}")
    if node.get("Relation Name"):
        extras.append(f"on: {node['Relation Name']}")
    if extras:
        line += f"  ({', '.join(extras)})"

    lines = [line]
    for child in node.get("Plans", []):
        lines.append(_render_pg_plan(child, depth + 1))
    return "\n".join(lines)


def _compute_dominant_pathology(queries: List[ForensicQuery]) -> str:
    """Find most frequent pathology code across queries with q-error data."""
    counts: Dict[str, int] = defaultdict(int)
    for fq in queries:
        if fq.qerror and fq.qerror.pathology_routing:
            for p in fq.qerror.pathology_routing.split(","):
                p = p.strip()
                if p:
                    counts[p] += 1
    if not counts:
        return ""
    return max(counts, key=counts.get)


def _compute_pattern_stats(
    entries: List[Tuple[str, float, str, Dict[str, float]]],
    catalog_by_id: Optional[Dict[str, Any]] = None,
) -> List[PatternStat]:
    """Aggregate pattern frequency across all queries."""
    pattern_counts: Dict[str, int] = defaultdict(int)
    pattern_overlaps: Dict[str, List[float]] = defaultdict(list)

    for _, _, _, poverlaps in entries:
        for pid, overlap in poverlaps.items():
            pattern_counts[pid] += 1
            pattern_overlaps[pid].append(overlap)

    stats = []
    for pid, count in sorted(pattern_counts.items(), key=lambda x: -x[1]):
        overlaps = pattern_overlaps[pid]
        avg = sum(overlaps) / len(overlaps) if overlaps else 0.0
        gap = ""
        if catalog_by_id and pid in catalog_by_id:
            gap = catalog_by_id[pid].get("gap", "")
        stats.append(PatternStat(
            pattern_id=pid,
            pattern_name=pid.replace("_", " ").title(),
            query_count=count,
            avg_overlap=round(avg, 3),
            target_gap=gap,
        ))

    return stats[:15]  # Top 15


def _load_resource_profile(
    benchmark_dir: Path, engine: str,
) -> Optional[ResourceProfile]:
    """Load cached PG system profile if available."""
    if engine not in ("postgresql", "postgres"):
        return None

    profile_path = benchmark_dir / "pg_system_profile.json"
    if not profile_path.exists():
        return None

    try:
        data = json.loads(profile_path.read_text())
        settings = {s["name"]: s["setting"]
                    for s in data.get("settings", [])}

        # Detect storage type heuristic
        random_cost = float(settings.get("random_page_cost", "4"))
        storage = "SSD" if random_cost <= 1.5 else "HDD"

        return ResourceProfile(
            shared_buffers=settings.get("shared_buffers", "?"),
            work_mem_default=settings.get("work_mem", "?"),
            max_parallel_workers=int(
                settings.get("max_parallel_workers_per_gather", "0")),
            effective_cache_size=settings.get("effective_cache_size", "?"),
            storage_type=storage,
        )
    except Exception as e:
        logger.debug(f"Resource profile load error: {e}")
        return None


# ---------------------------------------------------------------------------
# Execution (fleet run results)
# ---------------------------------------------------------------------------

def _build_execution(benchmark_dir: Path) -> ExecutionSummary:
    runs_dir = benchmark_dir / "runs"
    if not runs_dir.exists():
        return ExecutionSummary()

    runs: List[RunSummary] = []
    latest_results: Dict[str, QueryResult] = {}

    for run_dir in sorted(runs_dir.iterdir()):
        if not run_dir.is_dir():
            continue

        summary = _load_json(run_dir / "summary.json")
        if not summary:
            continue

        run_id = run_dir.name
        raw_results = summary.get("results", [])

        # Parse timestamp from dir name
        timestamp = ""
        ts_match = re.search(r"(\d{8}_\d{6})", run_id)
        if ts_match:
            try:
                dt = datetime.strptime(ts_match.group(1), "%Y%m%d_%H%M%S")
                timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                timestamp = ts_match.group(1)

        # Count statuses
        status_counts: Dict[str, int] = defaultdict(int)
        for r in raw_results:
            st = r.get("status", "UNKNOWN")
            status_counts[st] += 1

        runs.append(RunSummary(
            run_id=run_id,
            timestamp=timestamp,
            mode=summary.get("mode", "unknown"),
            total_queries=summary.get("total", len(raw_results)),
            completed=summary.get("completed", 0),
            status_counts=dict(status_counts),
            elapsed_seconds=summary.get("elapsed_seconds", 0),
        ))

        # Also try loading per-query result.json for richer data
        for r in raw_results:
            qid = r.get("query_id", "")
            if not qid:
                continue

            nqid = normalize_qid(qid)

            # Try per-query result.json for detailed data
            qr_path = run_dir / qid / "result.json"
            qr_data = _load_json(qr_path) if qr_path.exists() else None

            baseline_ms = 0.0
            optimized_ms = 0.0
            transform_used = ""
            set_local_cmds: List[str] = []
            worker_id = None
            speedup = r.get("speedup") or 0.0

            if qr_data:
                baseline_ms = qr_data.get("baseline_ms", 0.0) or 0.0
                if speedup and baseline_ms > 0:
                    optimized_ms = baseline_ms / speedup if speedup > 0 else 0
                transforms = qr_data.get("best_transforms", [])
                transform_used = transforms[0] if transforms else ""
                worker_id = qr_data.get("best_worker_id")
                set_local_cmds = qr_data.get("set_local_commands", [])

            latest_results[nqid] = QueryResult(
                query_id=nqid,
                status=r.get("status", "UNKNOWN"),
                speedup=speedup,
                baseline_ms=baseline_ms,
                optimized_ms=optimized_ms,
                transform_used=transform_used,
                set_local_commands=set_local_cmds,
                worker_id=worker_id,
            )

    # Newest first
    runs.reverse()

    return ExecutionSummary(runs=runs, latest_results=latest_results)


# ---------------------------------------------------------------------------
# Impact (post-execution analysis)
# ---------------------------------------------------------------------------

def _build_impact(
    forensic: ForensicSummary,
    execution: ExecutionSummary,
    engine: str,
) -> ImpactSummary:
    results = execution.latest_results
    if not results:
        return ImpactSummary()

    # Build cost map from forensic queries
    cost_map = {q.query_id: q.runtime_ms for q in forensic.queries}

    total_baseline = 0.0
    total_optimized = 0.0
    status_counts: Dict[str, int] = defaultdict(int)
    regressions: List[QueryResult] = []

    for qid, qr in results.items():
        status_counts[qr.status] += 1

        baseline = qr.baseline_ms or cost_map.get(qid, 0.0)
        if baseline <= 0:
            continue

        optimized = baseline
        if qr.speedup and qr.speedup > 0:
            optimized = baseline / qr.speedup

        total_baseline += baseline
        total_optimized += optimized

        if qr.status == "REGRESSION":
            regressions.append(qr)

    savings_ms = total_baseline - total_optimized
    savings_pct = (savings_ms / total_baseline * 100) if total_baseline > 0 else 0

    # Resource impact
    resource_impact = _compute_resource_impact(results, engine)

    return ImpactSummary(
        total_baseline_ms=round(total_baseline, 1),
        total_optimized_ms=round(total_optimized, 1),
        total_savings_ms=round(savings_ms, 1),
        total_savings_pct=round(savings_pct, 1),
        status_counts=dict(status_counts),
        regressions=regressions,
        resource_impact=resource_impact,
    )


# Sorted longest-suffix-first so "MB" matches before "B", etc.
_PG_SIZE_UNITS = [("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2), ("kB", 1024), ("B", 1)]


def _parse_pg_size(val: str) -> int:
    """Parse a PostgreSQL size string like '256MB' into bytes."""
    val = val.strip()
    for unit, mult in _PG_SIZE_UNITS:
        if val.endswith(unit):
            try:
                return int(float(val[:-len(unit)].strip()) * mult)
            except ValueError:
                return 0
    try:
        return int(val)
    except ValueError:
        return 0


def _format_bytes(n: int) -> str:
    if n >= 1024**3:
        return f"{n / 1024**3:.1f}GB"
    if n >= 1024**2:
        return f"{n / 1024**2:.0f}MB"
    if n >= 1024:
        return f"{n / 1024:.0f}kB"
    return f"{n}B"


def _compute_resource_impact(
    results: Dict[str, QueryResult],
    engine: str,
) -> Optional[ResourceImpact]:
    if engine not in ("postgresql", "postgres"):
        return None

    work_mem_bytes = 0
    parallel_total = 0
    queries_with_sl = 0
    conflicts: List[str] = []
    warnings: List[str] = []
    parallel_queries: List[str] = []

    default_work_mem = _parse_pg_size("4MB")

    for qid, r in results.items():
        if r.status not in ("WIN", "IMPROVED"):
            continue
        if not r.set_local_commands:
            continue

        queries_with_sl += 1
        for cmd in r.set_local_commands:
            cmd_lower = cmd.lower()
            if "work_mem" in cmd_lower:
                # Extract value: SET LOCAL work_mem = '256MB'
                m = re.search(r"=\s*'?([^';\s]+)", cmd)
                if m:
                    work_mem_bytes += _parse_pg_size(m.group(1))
            if "max_parallel_workers_per_gather" in cmd_lower:
                m = re.search(r"=\s*'?(\d+)", cmd)
                if m:
                    val = int(m.group(1))
                    parallel_total += val
                    if val >= 4:
                        parallel_queries.append(qid)

    # Detect conflicts
    if len(parallel_queries) >= 2:
        conflicts.append(
            f"{', '.join(parallel_queries)} all request high parallel workers")

    # Peak factor
    peak_factor = (work_mem_bytes / default_work_mem) if default_work_mem > 0 else 0

    # Warnings
    if work_mem_bytes > 512 * 1024**2:
        warnings.append(
            f"Peak work_mem ({_format_bytes(work_mem_bytes)}) exceeds 512MB — "
            "review if running concurrently")
    if parallel_total > 16:
        warnings.append(
            f"Total parallel workers ({parallel_total}) exceeds typical core count")

    return ResourceImpact(
        queries_with_set_local=queries_with_sl,
        work_mem_total=_format_bytes(work_mem_bytes),
        work_mem_peak_factor=round(peak_factor, 1),
        parallel_workers_total=parallel_total,
        conflicts=conflicts,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _bucket_runtime(runtime_ms: float) -> str:
    if runtime_ms < 0:
        return "MEDIUM"
    for threshold, bucket in _RUNTIME_THRESHOLDS:
        if runtime_ms < threshold:
            return bucket
    return "HIGH"


def load_explain_timing(benchmark_dir: Path, query_id: str, engine: str) -> float:
    """Extract execution time from cached EXPLAIN JSON."""
    search_paths = [
        benchmark_dir / "explains" / f"{query_id}.json",
        benchmark_dir / "explains" / "sf10" / f"{query_id}.json",
        benchmark_dir / "explains" / "sf5" / f"{query_id}.json",
    ]

    for path in search_paths:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())

            # DuckDB format
            if "execution_time_ms" in data:
                val = data["execution_time_ms"]
                if val and val > 0:
                    return float(val)

            # PostgreSQL format
            plan_json = data.get("plan_json")
            if isinstance(plan_json, list) and plan_json:
                exec_time = plan_json[0].get("Execution Time")
                if exec_time is not None:
                    return float(exec_time)

            # DuckDB plan_json dict
            if isinstance(plan_json, dict):
                latency = plan_json.get("latency")
                if latency and latency > 0:
                    return float(latency) * 1000

            # Snowflake: executionTime field
            exec_time = data.get("executionTime")
            if exec_time is not None:
                return float(exec_time)

        except Exception:
            pass

    return -1.0
