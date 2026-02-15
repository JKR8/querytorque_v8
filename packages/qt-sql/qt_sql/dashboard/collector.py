"""Forensic intelligence collector — builds WorkloadProfile from benchmark data.

Single entry point: collect_workload_profile(benchmark_dir, engine) → WorkloadProfile
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
    ExecutionSummary,
    ForensicSummary,
    ImpactSummary,
    PatternCoverage,
    PatternStat,
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
    try:
        from ..detection import detect_transforms, load_transforms
        transforms_catalog = load_transforms()
    except Exception as e:
        logger.warning(f"Detection unavailable: {e}")

    engine_name = {"postgresql": "postgresql", "postgres": "postgresql"}.get(
        engine, engine)
    dialect = "postgres" if engine in ("postgresql", "postgres") else engine

    # Build per-query data
    entries: List[Tuple[str, float, str, List[str], float]] = []
    # (query_id, runtime_ms, bucket, pattern_ids, top_overlap)

    for sql_path in query_files:
        qid = sql_path.stem
        sql = sql_path.read_text().strip()
        runtime_ms = load_explain_timing(benchmark_dir, qid, engine)
        bucket = _bucket_runtime(runtime_ms)

        pattern_ids: List[str] = []
        top_overlap = 0.0

        if transforms_catalog and sql:
            try:
                matched = detect_transforms(
                    sql, transforms_catalog,
                    engine=engine_name, dialect=dialect,
                )
                pattern_ids = [m.id for m in matched if m.overlap_ratio >= 0.25]
                if matched:
                    top_overlap = matched[0].overlap_ratio
            except Exception as e:
                logger.debug(f"[{qid}] Detection error: {e}")

        entries.append((qid, runtime_ms, bucket, pattern_ids, top_overlap))

    # Total runtime (exclude unknowns)
    total_runtime = sum(rt for _, rt, _, _, _ in entries if rt > 0)

    # Cost concentration (sorted by runtime desc)
    by_runtime = sorted(entries, key=lambda e: -max(e[1], 0))
    cost_concentration: List[CostEntry] = []
    cumulative = 0.0
    for qid, rt, bucket, pids, _ in by_runtime:
        pct = (rt / total_runtime) if total_runtime > 0 and rt > 0 else 0.0
        cumulative += pct
        cost_concentration.append(CostEntry(
            query_id=qid,
            runtime_ms=rt,
            pct_of_total=round(pct, 4),
            cumulative_pct=round(cumulative, 4),
            bucket=bucket,
            detected_patterns=pids,
        ))

    # Bucket distribution
    bucket_dist: Dict[str, int] = defaultdict(int)
    for _, _, bucket, _, _ in entries:
        bucket_dist[bucket] += 1

    # Pattern coverage
    with_detection = sum(1 for _, _, _, pids, _ in entries if pids)
    without_detection = len(entries) - with_detection

    # Top patterns across workload
    pattern_stats = _compute_pattern_stats(entries)

    # Resource profile (PG only)
    resource_profile = _load_resource_profile(benchmark_dir, engine)

    return ForensicSummary(
        total_queries=len(entries),
        total_runtime_ms=round(total_runtime, 1),
        cost_concentration=cost_concentration,
        bucket_distribution=dict(bucket_dist),
        pattern_coverage=PatternCoverage(
            queries_with_detection=with_detection,
            queries_without_detection=without_detection,
            top_patterns=pattern_stats,
        ),
        resource_profile=resource_profile,
    )


def _compute_pattern_stats(
    entries: List[Tuple[str, float, str, List[str], float]],
) -> List[PatternStat]:
    """Aggregate pattern frequency across all queries."""
    pattern_counts: Dict[str, int] = defaultdict(int)
    pattern_overlaps: Dict[str, List[float]] = defaultdict(list)

    for _, _, _, pids, top_overlap in entries:
        for pid in pids:
            pattern_counts[pid] += 1
            pattern_overlaps[pid].append(top_overlap)

    stats = []
    for pid, count in sorted(pattern_counts.items(), key=lambda x: -x[1]):
        overlaps = pattern_overlaps[pid]
        avg = sum(overlaps) / len(overlaps) if overlaps else 0.0
        stats.append(PatternStat(
            pattern_id=pid,
            pattern_name=pid.replace("_", " ").title(),
            query_count=count,
            avg_overlap=round(avg, 3),
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

            latest_results[qid] = QueryResult(
                query_id=qid,
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

    # Build cost map from forensic data
    cost_map = {c.query_id: c.runtime_ms for c in forensic.cost_concentration}

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


_PG_SIZE_UNITS = {"B": 1, "kB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}


def _parse_pg_size(val: str) -> int:
    """Parse a PostgreSQL size string like '256MB' into bytes."""
    val = val.strip()
    for unit, mult in _PG_SIZE_UNITS.items():
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
