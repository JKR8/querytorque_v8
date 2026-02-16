"""Config Coach: iterative SET LOCAL + pg_hint_plan tuning with LLM reflection.

Complements the SQL rewrite pipeline by searching the config/hint space
orthogonally. The coach proposes 4-8 config+hint candidates per iteration,
all get mechanically applied and benchmarked, then results are fed back
for up to 3 iterations.

No SQL rewriting — hints and configs are small, mechanical to apply.

Usage:
    from qt_sql.config_coach import coach_query, coach_benchmark, CoachConfig

    result = coach_query(sql, dsn, "query001")
    results = coach_benchmark(bench_dir, dsn)
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────────

@dataclass
class CoachConfig:
    """Configuration for a coach session."""
    max_iterations: int = 3
    max_candidates: int = 8
    min_candidates: int = 4
    min_speedup: float = 1.05        # Target: 5% improvement
    benchmark_timeout_ms: int = 300_000
    collect_explain_top_n: int = 3    # Collect EXPLAIN for top N candidates
    dry_run: bool = False             # Skip benchmarks, just parse LLM output
    model: Optional[str] = None       # LLM model override


@dataclass
class CoachResult:
    """Result of coaching a single query."""
    query_id: str
    input_sql: str
    baseline_ms: float
    iterations: List[Dict[str, Any]] = field(default_factory=list)
    best_candidate: Optional[Dict[str, Any]] = None
    best_speedup: float = 0.0
    best_config_commands: List[str] = field(default_factory=list)
    best_hint_comment: str = ""
    total_candidates_tested: int = 0
    total_api_calls: int = 0
    duration_seconds: float = 0.0
    status: str = "PENDING"           # PENDING, WIN, NO_GAIN, ERROR


# ── Main entry points ────────────────────────────────────────────────────

def coach_query(
    query_sql: str,
    dsn: str,
    query_id: str = "unknown",
    config: Optional[CoachConfig] = None,
    existing_explain: Optional[str] = None,
) -> CoachResult:
    """Run the coach iteration loop on a single query.

    Args:
        query_sql: The SQL query to optimize (FIXED — not rewritten).
        dsn: PostgreSQL DSN string.
        query_id: Identifier for this query.
        config: Coach configuration (defaults to CoachConfig()).
        existing_explain: Pre-collected EXPLAIN text (avoids re-running).

    Returns:
        CoachResult with best candidate and full iteration history.
    """
    from .execution.factory import PostgresConfig
    from .hint_plan import (
        apply_hints_to_sql,
        build_hint_comment,
        check_hint_plan_available,
        parse_hint_string,
    )
    from .pg_tuning import (
        build_resource_envelope,
        build_set_local_sql,
        load_or_collect_profile,
    )
    from .prompts.config_coach_prompt import (
        CandidateResult,
        IterationResult,
        build_base_prefix,
        build_iteration_prompt,
        parse_coach_response,
    )

    if config is None:
        config = CoachConfig()

    start_time = time.time()
    result = CoachResult(
        query_id=query_id,
        input_sql=query_sql,
        baseline_ms=0.0,
    )

    try:
        # ── Collect context ──────────────────────────────────────────
        ctx = _collect_context(query_sql, dsn, existing_explain)
        result.baseline_ms = ctx["baseline_ms"]
        baseline_row_count = ctx["baseline_row_count"]

        if result.baseline_ms <= 0:
            result.status = "ERROR"
            result.duration_seconds = time.time() - start_time
            return result

        # Check hint plan availability
        hint_available = check_hint_plan_available(dsn)
        if not hint_available:
            logger.info("pg_hint_plan not available — config-only mode")

        # ── Build cache-stable prefix ────────────────────────────────
        base_prefix = build_base_prefix(
            query_sql=query_sql,
            explain_plan=ctx["explain_text"],
            engine_profile=ctx["engine_profile"],
            resource_envelope=ctx["resource_envelope"],
            current_settings=ctx["current_settings"],
            baseline_ms=result.baseline_ms,
            hint_plan_available=hint_available,
        )

        # ── Create LLM client ───────────────────────────────────────
        from qt_shared.llm import create_llm_client
        llm = create_llm_client(model=config.model)
        if llm is None:
            result.status = "ERROR"
            logger.error("Failed to create LLM client")
            result.duration_seconds = time.time() - start_time
            return result

        # ── Iteration loop ───────────────────────────────────────────
        previous_iterations: List[IterationResult] = []
        best_speedup = 0.0
        best_cr: Optional[CandidateResult] = None

        pg_config = PostgresConfig.from_dsn(dsn)

        for iteration_num in range(1, config.max_iterations + 1):
            logger.info(
                f"[{query_id}] Iteration {iteration_num}/{config.max_iterations}"
            )

            # Build prompt
            prompt = build_iteration_prompt(
                base_prefix=base_prefix,
                previous_iterations=previous_iterations,
                iteration_num=iteration_num,
                max_candidates=config.max_candidates,
                min_candidates=config.min_candidates,
            )

            # Call LLM
            logger.info(f"[{query_id}] Calling LLM (iteration {iteration_num})...")
            try:
                response = llm.analyze(prompt)
                result.total_api_calls += 1
            except Exception as e:
                logger.error(f"[{query_id}] LLM call failed: {e}")
                break

            # Log usage if available
            if hasattr(llm, "last_usage") and llm.last_usage:
                usage = llm.last_usage
                cache_hit = usage.get("prompt_cache_hit_tokens", 0)
                total_prompt = usage.get("prompt_tokens", 0)
                if total_prompt > 0:
                    pct = (cache_hit / total_prompt) * 100
                    logger.info(
                        f"[{query_id}] Cache hit: {cache_hit}/{total_prompt} "
                        f"({pct:.0f}%)"
                    )

            # Parse candidates, enforce max_candidates cap
            candidates = parse_coach_response(response)
            if len(candidates) > config.max_candidates:
                logger.info(
                    f"[{query_id}] LLM returned {len(candidates)} candidates, "
                    f"capping to {config.max_candidates}"
                )
                candidates = candidates[:config.max_candidates]
            if not candidates:
                logger.warning(
                    f"[{query_id}] No valid candidates from iteration "
                    f"{iteration_num}"
                )
                # Record empty iteration
                iter_result = IterationResult(iteration=iteration_num)
                previous_iterations.append(iter_result)
                result.iterations.append(_serialize_iteration(iter_result))
                continue

            # Strip hints if pg_hint_plan not available, then drop
            # candidates that became empty (hint-only with no set_local)
            if not hint_available:
                for c in candidates:
                    c.hints = ""
                candidates = [
                    c for c in candidates if c.set_local
                ]
                if not candidates:
                    logger.warning(
                        f"[{query_id}] All candidates were hint-only "
                        f"and pg_hint_plan is unavailable"
                    )
                    iter_result = IterationResult(iteration=iteration_num)
                    previous_iterations.append(iter_result)
                    result.iterations.append(
                        _serialize_iteration(iter_result)
                    )
                    continue

            logger.info(
                f"[{query_id}] Got {len(candidates)} candidates, benchmarking..."
            )

            # Benchmark (unless dry run)
            if config.dry_run:
                candidate_results = [
                    CandidateResult(candidate=c) for c in candidates
                ]
            else:
                candidate_results = _benchmark_candidates_interleaved(
                    dsn=dsn,
                    query_sql=query_sql,
                    candidates=candidates,
                    baseline_ms=result.baseline_ms,
                    baseline_row_count=baseline_row_count,
                    timeout_ms=config.benchmark_timeout_ms,
                )

            result.total_candidates_tested += len(candidate_results)

            # Collect EXPLAIN for top candidates
            if not config.dry_run and config.collect_explain_top_n > 0:
                _collect_explains_for_top(
                    dsn=dsn,
                    query_sql=query_sql,
                    candidate_results=candidate_results,
                    top_n=config.collect_explain_top_n,
                )

            # Find best in this iteration
            iter_best_id = ""
            iter_best_speedup = 0.0
            for cr in candidate_results:
                if cr.error or not cr.rows_match:
                    continue
                if cr.speedup > iter_best_speedup:
                    iter_best_speedup = cr.speedup
                    iter_best_id = cr.candidate.id

            iter_result = IterationResult(
                iteration=iteration_num,
                candidate_results=candidate_results,
                best_candidate_id=iter_best_id,
                best_speedup=iter_best_speedup,
            )
            previous_iterations.append(iter_result)
            result.iterations.append(_serialize_iteration(iter_result))

            # Track global best
            for cr in candidate_results:
                if cr.error or not cr.rows_match:
                    continue
                if cr.speedup > best_speedup:
                    best_speedup = cr.speedup
                    best_cr = cr

            # Early exit if target met
            if best_speedup >= config.min_speedup:
                logger.info(
                    f"[{query_id}] Target met: {best_speedup:.2f}x "
                    f"(target: {config.min_speedup}x)"
                )
                break

            # Log iteration summary
            logger.info(
                f"[{query_id}] Iteration {iteration_num}: "
                f"best={iter_best_speedup:.2f}x "
                f"(global best={best_speedup:.2f}x)"
            )

        # ── Finalize result ──────────────────────────────────────────
        if best_cr is not None and best_speedup >= config.min_speedup:
            result.status = "WIN"
            result.best_speedup = best_speedup
            result.best_candidate = _serialize_candidate(best_cr)
            result.best_config_commands = build_set_local_sql(
                best_cr.candidate.set_local
            )
            if best_cr.candidate.hints:
                hint_directives = parse_hint_string(best_cr.candidate.hints)
                result.best_hint_comment = build_hint_comment(hint_directives)
        elif config.dry_run:
            result.status = "DRY_RUN"
        else:
            result.status = "NO_GAIN"
            if best_cr is not None:
                result.best_speedup = best_speedup
                result.best_candidate = _serialize_candidate(best_cr)

    except Exception as e:
        logger.error(f"[{query_id}] Coach error: {e}", exc_info=True)
        result.status = "ERROR"

    result.duration_seconds = time.time() - start_time
    return result


def coach_benchmark(
    benchmark_dir: Path,
    dsn: str,
    config: Optional[CoachConfig] = None,
    query_ids: Optional[List[str]] = None,
) -> List[CoachResult]:
    """Run the coach on a benchmark suite.

    Args:
        benchmark_dir: Path to benchmark directory with queries/*.sql.
        dsn: PostgreSQL DSN.
        config: Coach configuration.
        query_ids: Optional list of query IDs to process (default: all).

    Returns:
        List of CoachResult, one per query.
    """
    if config is None:
        config = CoachConfig()

    queries_dir = benchmark_dir / "queries"
    if not queries_dir.exists():
        logger.error(f"No queries directory: {queries_dir}")
        return []

    # Resolve query list
    if query_ids:
        sql_files = []
        for qid in query_ids:
            sql_path = queries_dir / f"{qid}.sql"
            if sql_path.exists():
                sql_files.append(sql_path)
            else:
                logger.warning(f"Query file not found: {sql_path}")
    else:
        sql_files = sorted(queries_dir.glob("*.sql"))

    # Load existing explains if available
    explains_dir = benchmark_dir / "explains"
    explains: Dict[str, str] = {}
    if explains_dir.exists():
        for epath in explains_dir.glob("*.json"):
            try:
                edata = json.loads(epath.read_text())
                plan_text = edata.get("plan_text", "")
                if plan_text:
                    explains[epath.stem] = plan_text
            except Exception:
                pass

    results: List[CoachResult] = []
    for sql_path in sql_files:
        qid = sql_path.stem
        sql = sql_path.read_text().strip()
        existing_explain = explains.get(qid)

        logger.info(f"Coaching {qid}...")
        r = coach_query(
            query_sql=sql,
            dsn=dsn,
            query_id=qid,
            config=config,
            existing_explain=existing_explain,
        )
        results.append(r)
        logger.info(f"  {qid}: {r.status} ({r.best_speedup:.2f}x)")

    return results


# ── Context collection ────────────────────────────────────────────────────

def _collect_context(
    query_sql: str,
    dsn: str,
    existing_explain: Optional[str] = None,
) -> Dict[str, Any]:
    """Collect all context needed for the coach prompt.

    Returns dict with: explain_text, engine_profile, resource_envelope,
    current_settings, baseline_ms, baseline_row_count.
    """
    from .execution.factory import PostgresConfig
    from .pg_tuning import build_resource_envelope, load_or_collect_profile

    pg_config = PostgresConfig.from_dsn(dsn)
    executor = pg_config.get_executor()

    ctx: Dict[str, Any] = {
        "explain_text": "",
        "engine_profile": {},
        "resource_envelope": "",
        "current_settings": {},
        "baseline_ms": 0.0,
        "baseline_row_count": 0,
    }

    with executor:
        # ── Baseline timing (3 runs, discard 1st, average last 2) ────
        timings = []
        row_count = -1  # sentinel: not yet captured
        for i in range(3):
            t0 = time.time()
            try:
                rows = executor.execute(query_sql, timeout_ms=300_000)
                elapsed = (time.time() - t0) * 1000
                timings.append(elapsed)
                # Capture row count from first *successful* run
                if row_count < 0:
                    row_count = len(rows)
            except Exception as e:
                logger.error(f"Baseline run {i} failed: {e}")
                timings.append(0)
        if row_count < 0:
            row_count = 0  # all runs failed

        if len(timings) >= 3 and timings[1] > 0 and timings[2] > 0:
            ctx["baseline_ms"] = (timings[1] + timings[2]) / 2
        elif timings:
            valid = [t for t in timings if t > 0]
            ctx["baseline_ms"] = sum(valid) / len(valid) if valid else 0
        ctx["baseline_row_count"] = row_count

        # ── EXPLAIN ANALYZE ──────────────────────────────────────────
        if existing_explain:
            ctx["explain_text"] = existing_explain
        else:
            try:
                explain_result = executor.explain(query_sql, analyze=True)
                if explain_result and "Plan" in explain_result:
                    from .execution.database_utils import _plan_to_text
                    ctx["explain_text"] = _plan_to_text(explain_result["Plan"])
                elif explain_result:
                    ctx["explain_text"] = str(explain_result)
            except Exception as e:
                logger.warning(f"EXPLAIN ANALYZE failed: {e}")

        # ── Current settings ─────────────────────────────────────────
        ctx["current_settings"] = executor.get_settings()

    # ── System profile + resource envelope ───────────────────────────
    try:
        # Use a temp dir for caching (or benchmark dir if available)
        import tempfile
        cache_dir = Path(tempfile.gettempdir()) / "qt_coach_cache"
        profile = load_or_collect_profile(dsn, cache_dir)
        ctx["resource_envelope"] = build_resource_envelope(profile)
    except Exception as e:
        logger.warning(f"Failed to collect system profile: {e}")
        ctx["resource_envelope"] = "System profile unavailable."

    # ── Engine profile ───────────────────────────────────────────────
    try:
        from .prompter import _load_engine_profile
        profile = _load_engine_profile("postgresql")
        if profile:
            ctx["engine_profile"] = profile
    except Exception as e:
        logger.warning(f"Failed to load engine profile: {e}")

    # ── Dialect tuning config (post-optimization, not rewrite prompt) ──
    try:
        from .knowledge.configuration import load_dialect_config
        dialect_config = load_dialect_config("postgresql")
        if dialect_config:
            ctx["dialect_config"] = dialect_config
    except Exception as e:
        logger.warning(f"Failed to load dialect config: {e}")

    return ctx


# ── Interleaved benchmarking ──────────────────────────────────────────────

def _benchmark_candidates_interleaved(
    dsn: str,
    query_sql: str,
    candidates: List,  # List[CoachCandidate]
    baseline_ms: float,
    baseline_row_count: int,
    timeout_ms: int = 300_000,
) -> List:  # List[CandidateResult]
    """Benchmark candidates with interleaved measurement to control drift.

    Pattern: warmup all -> measure round 1 (C1..CN) -> measure round 2 (C1..CN)
    -> average per candidate.
    """
    from .execution.factory import PostgresConfig
    from .hint_plan import apply_hints_to_sql, build_hint_comment, parse_hint_string
    from .pg_tuning import build_set_local_sql
    from .prompts.config_coach_prompt import CandidateResult

    pg_config = PostgresConfig.from_dsn(dsn)
    results: List[CandidateResult] = []

    # Prepare each candidate's SQL + config
    prepared = []
    for c in candidates:
        modified_sql, config_commands = _apply_candidate(query_sql, c)
        prepared.append((c, modified_sql, config_commands))

    executor = pg_config.get_executor()
    try:
        executor.connect()

        # ── Warmup all candidates (1 run each) ──────────────────────
        for c, mod_sql, config_cmds in prepared:
            try:
                if config_cmds:
                    executor.execute_with_config(
                        mod_sql, config_cmds, timeout_ms=timeout_ms
                    )
                else:
                    executor.execute(mod_sql, timeout_ms=timeout_ms)
            except Exception:
                pass  # Warmup errors are handled in measurement

        # ── Measurement rounds ───────────────────────────────────────
        timings: Dict[str, List[float]] = {c.id: [] for c, _, _ in prepared}
        row_counts: Dict[str, int] = {}
        errors: Dict[str, str] = {}

        for round_num in range(2):
            for c, mod_sql, config_cmds in prepared:
                if c.id in errors:
                    continue  # Skip already-failed

                try:
                    t0 = time.time()
                    if config_cmds:
                        rows = executor.execute_with_config(
                            mod_sql, config_cmds, timeout_ms=timeout_ms
                        )
                    else:
                        rows = executor.execute(mod_sql, timeout_ms=timeout_ms)
                    elapsed = (time.time() - t0) * 1000
                    timings[c.id].append(elapsed)

                    if round_num == 0:
                        row_counts[c.id] = len(rows)
                except Exception as e:
                    errors[c.id] = str(e)
                    logger.warning(f"Candidate {c.id} failed: {e}")

        # ── Build results ────────────────────────────────────────────
        for c, _, _ in prepared:
            cr = CandidateResult(candidate=c)

            if c.id in errors:
                cr.error = errors[c.id]
            elif timings[c.id]:
                cr.elapsed_ms = sum(timings[c.id]) / len(timings[c.id])
                cr.speedup = baseline_ms / cr.elapsed_ms if cr.elapsed_ms > 0 else 0
                cr.row_count = row_counts.get(c.id, 0)
                cr.rows_match = (cr.row_count == baseline_row_count)
            else:
                cr.error = "No timing data"

            results.append(cr)

    finally:
        executor.close()

    return results


def _apply_candidate(
    query_sql: str,
    candidate,  # CoachCandidate
) -> tuple:
    """Apply a candidate's hints and config to the query.

    Returns (modified_sql, config_commands) where:
    - modified_sql has hints prepended
    - config_commands is a list of SET LOCAL statements
    """
    from .hint_plan import apply_hints_to_sql, build_hint_comment, parse_hint_string
    from .pg_tuning import build_set_local_sql

    # Build SET LOCAL commands
    config_commands = build_set_local_sql(candidate.set_local)

    # Build and apply hints
    modified_sql = query_sql
    if candidate.hints:
        directives = parse_hint_string(candidate.hints)
        if directives:
            hint_comment = build_hint_comment(directives)
            modified_sql = apply_hints_to_sql(query_sql, hint_comment)
            # Enable pg_hint_plan if hints are present
            config_commands.insert(0, "SET LOCAL pg_hint_plan.enable_hint = 'on'")

    return modified_sql, config_commands


# ── EXPLAIN collection ────────────────────────────────────────────────────

def _collect_explains_for_top(
    dsn: str,
    query_sql: str,
    candidate_results: List,  # List[CandidateResult]
    top_n: int = 3,
) -> None:
    """Collect EXPLAIN ANALYZE for the top N non-error candidates.

    Modifies candidate_results in place (sets explain_text).
    """
    from .execution.factory import PostgresConfig

    # Sort by speedup, pick top N
    valid = [
        cr for cr in candidate_results
        if not cr.error and cr.rows_match
    ]
    valid.sort(key=lambda cr: cr.speedup, reverse=True)
    top = valid[:top_n]

    if not top:
        return

    pg_config = PostgresConfig.from_dsn(dsn)
    executor = pg_config.get_executor()

    try:
        executor.connect()

        for cr in top:
            modified_sql, config_commands = _apply_candidate(
                query_sql, cr.candidate
            )
            try:
                # Run EXPLAIN ANALYZE within the same config context
                explain_sql = (
                    f"EXPLAIN (ANALYZE, FORMAT TEXT) {modified_sql}"
                )
                if config_commands:
                    rows = executor.execute_with_config(
                        explain_sql, config_commands, timeout_ms=300_000
                    )
                else:
                    rows = executor.execute(explain_sql, timeout_ms=300_000)

                if rows:
                    cr.explain_text = "\n".join(
                        str(r.get("QUERY PLAN", r))
                        for r in rows
                    )
            except Exception as e:
                logger.debug(f"EXPLAIN for {cr.candidate.id} failed: {e}")

    finally:
        executor.close()


# ── Serialization helpers ─────────────────────────────────────────────────

def _serialize_candidate(cr) -> Dict[str, Any]:
    """Serialize a CandidateResult to a JSON-safe dict."""
    c = cr.candidate
    return {
        "id": c.id,
        "hypothesis": c.hypothesis,
        "predicted_speedup": c.predicted_speedup,
        "set_local": c.set_local,
        "hints": c.hints,
        "reasoning": c.reasoning,
        "elapsed_ms": round(cr.elapsed_ms, 1),
        "speedup": round(cr.speedup, 3),
        "row_count": cr.row_count,
        "rows_match": cr.rows_match,
        "error": cr.error,
    }


def _serialize_iteration(iter_result) -> Dict[str, Any]:
    """Serialize an IterationResult to a JSON-safe dict."""
    return {
        "iteration": iter_result.iteration,
        "candidates": [
            _serialize_candidate(cr)
            for cr in iter_result.candidate_results
        ],
        "best_candidate_id": iter_result.best_candidate_id,
        "best_speedup": round(iter_result.best_speedup, 3),
    }
