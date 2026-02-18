"""Unified benchmark module — ONE connection per query, fail-fast correctness.

This replaces the duplicated _sequential_benchmark / _parallel_benchmark /
race_candidates code paths with a single function that:

1. Opens exactly ONE database connection per query lifecycle
2. Runs baseline + all candidates sequentially on that connection
3. Fail-fast: if run 1 correctness fails, skip remaining runs
4. Winner confirmation on same connection
5. EXPLAIN collection on same connection (no run_explain_analyze!)
6. Connection closed at end

Critical invariant: at most db_slots connections open at any time.
wave_runner controls parallelism; this module is connection-unaware.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..execution.factory import create_executor_from_dsn
from .equivalence_checker import EquivalenceChecker

logger = logging.getLogger(__name__)


@dataclass
class CandidateResult:
    """Result for a single candidate benchmark."""

    patch_idx: int
    passed: bool
    speedup: float = 0.0
    avg_ms: float = 0.0
    row_count: int = 0
    checksum: Optional[str] = None
    correctness_verified: bool = False
    error: Optional[str] = None
    all_times: List[float] = field(default_factory=list)
    explain_text: Optional[str] = None


@dataclass
class BenchmarkSummary:
    """Summary of a full query benchmark run."""

    baseline_ms: float
    baseline_rows: int
    baseline_checksum: Optional[str]
    n_benchmarked: int
    n_passed: int
    best_speedup: float
    best_patch_idx: Optional[int]
    candidate_results: List[CandidateResult] = field(default_factory=list)


def _timed_runs(
    executor: Any,
    sql: str,
    runs: int = 3,
    capture_rows: bool = False,
    timeout_ms: int = 300_000,
):
    """Execute SQL with proper run pattern.

    Run patterns:
      runs=1: one measured run
      runs=2: two measured runs, average both
      runs=3: warmup + 2 measured runs, average measured runs
      runs=4: warmup + 3 measured runs, average measured runs
      runs>=5: N measured runs, drop min/max, average middle (trimmed mean)

    Returns:
        (avg_ms, rows_or_None, all_times) tuple.
    """
    if runs >= 5:
        times = []
        rows = None
        for i in range(runs):
            start = time.perf_counter()
            result = executor.execute(sql, timeout_ms=timeout_ms)
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)
            if i == 0 and capture_rows:
                rows = result
            logger.debug(f"  run {i+1}/{runs}: {elapsed:.1f}ms")
        times_sorted = sorted(times)
        trimmed = times_sorted[1:-1]
        avg_ms = sum(trimmed) / len(trimmed)
        logger.info(
            f"  trimmed mean ({runs} runs, drop min/max): {avg_ms:.1f}ms "
            f"[{', '.join(f'{t:.0f}' for t in times)}]"
        )
        return avg_ms, rows, times

    runs = max(1, int(runs))
    measured_times: List[float] = []
    captured_rows = None

    if runs >= 3:
        executor.execute(sql, timeout_ms=timeout_ms)  # warmup

    measured_count = runs if runs <= 2 else (runs - 1)
    for i in range(measured_count):
        start = time.perf_counter()
        rows = executor.execute(sql, timeout_ms=timeout_ms)
        elapsed = (time.perf_counter() - start) * 1000
        measured_times.append(elapsed)
        if i == 0 and capture_rows:
            captured_rows = rows

    avg_ms = sum(measured_times) / len(measured_times)
    logger.debug(
        f"  {runs}-run: "
        f"[{', '.join(f'{t:.1f}ms' for t in measured_times)}] "
        f"-> avg {avg_ms:.1f}ms"
    )
    return avg_ms, captured_rows, measured_times


def benchmark_query_patches(
    patches: List[Any],
    original_sql: str,
    db_path: str,
    query_id: str,
    *,
    baseline_runs: int = 3,
    candidate_runs: int = 3,
    winner_runs: int = 3,
    known_timeout: bool = False,
    timeout_seconds: int = 300,
    timeout_ms: int = 300_000,
    collect_explain: bool = True,
    classify_speedup_fn: Optional[Callable[[float], str]] = None,
    sample_db_path: Optional[str] = None,
    collect_explain_fn: Optional[Callable] = None,
    dialect: str = "duckdb",
) -> BenchmarkSummary:
    """Benchmark all candidate patches for a single query.

    Opens exactly ONE database connection, reuses it for:
    - Baseline measurement
    - All candidate measurements (sequential, fail-fast)
    - Winner confirmation
    - EXPLAIN collection

    Args:
        patches: List of AppliedPatch objects (modified in-place).
        original_sql: The original SQL query.
        db_path: Database path or DSN.
        query_id: Query identifier for logging.
        baseline_runs: Number of baseline runs (3 = warmup + 2 measured).
        candidate_runs: Number of candidate runs per patch.
        winner_runs: Number of confirmation runs for best candidate.
        known_timeout: If True, skip baseline execution (use timeout_seconds as baseline).
        timeout_seconds: Timeout value used as baseline when known_timeout=True.
        timeout_ms: Per-execution timeout in milliseconds.
        collect_explain: Whether to collect EXPLAIN ANALYZE for passing candidates.
        classify_speedup_fn: Function to classify speedup into status string.
        sample_db_path: Optional DuckDB path for TABLESAMPLE equivalence check.
        collect_explain_fn: Optional callback(executor, sql) -> str that collects
            EXPLAIN on the open executor and returns compact formatted text.
            If None, falls back to raw EXPLAIN ANALYZE text.
        dialect: Database dialect (duckdb/postgres/snowflake).

    Returns:
        BenchmarkSummary with all results.
    """
    checker = EquivalenceChecker()

    def classify(speedup: float) -> str:
        if classify_speedup_fn:
            return classify_speedup_fn(speedup)
        if speedup >= 1.10:
            return "WIN"
        elif speedup >= 1.05:
            return "IMPROVED"
        elif speedup >= 0.95:
            return "NEUTRAL"
        else:
            return "REGRESSION"

    logger.info(
        f"[{query_id}] Unified benchmark: {len(patches)} patches, "
        f"baseline={baseline_runs}x, candidate={candidate_runs}x, "
        f"winner={winner_runs}x"
    )

    # ONE connection for the entire query lifecycle
    with create_executor_from_dsn(db_path) as executor:
        # ── Step 1: Baseline ──────────────────────────────────────────
        if known_timeout:
            orig_ms = float(timeout_seconds * 1000)
            orig_count = None
            orig_checksum = None
            logger.info(
                f"[{query_id}] Baseline: SKIPPED "
                f"(known timeout, using {orig_ms:.0f}ms)"
            )
        else:
            logger.info(f"[{query_id}] Baseline: {baseline_runs}x...")
            try:
                orig_ms, orig_rows, orig_times = _timed_runs(
                    executor, original_sql,
                    runs=baseline_runs, capture_rows=True, timeout_ms=timeout_ms,
                )
            except Exception as e:
                logger.error(f"[{query_id}] Baseline execution FAILED: {e}")
                # Mark all patches as ERROR and return early
                for patch in patches:
                    patch.speedup = 0.0
                    patch.status = "ERROR"
                    patch.apply_error = f"Baseline failed: {e}"
                return BenchmarkSummary(
                    baseline_ms=0.0, baseline_rows=0,
                    baseline_checksum=None,
                    n_benchmarked=len(patches), n_passed=0,
                    best_speedup=0.0, best_patch_idx=None,
                )
            orig_count = len(orig_rows) if orig_rows else 0
            if orig_count == 0:
                logger.warning(
                    f"[{query_id}] Baseline returned 0 rows — "
                    f"correctness checks will be unreliable"
                )
            orig_checksum = None
            if orig_rows:
                try:
                    orig_checksum = checker.compute_checksum(orig_rows)
                except Exception as e:
                    logger.warning(
                        f"[{query_id}] Baseline checksum compute failed "
                        f"(non-blocking): {e}"
                    )
            logger.info(
                f"[{query_id}] Baseline: {orig_ms:.1f}ms "
                f"({orig_count} rows, checksum={orig_checksum}) "
                f"[{', '.join(f'{t:.0f}' for t in orig_times)}]"
            )

        # ── Step 2: Per-candidate benchmark (fail-fast) ───────────────
        candidate_results: List[CandidateResult] = []
        current_best_speedup = float("-inf")

        for idx, patch in enumerate(patches):
            if not patch.output_sql:
                candidate_results.append(CandidateResult(
                    patch_idx=idx, passed=False,
                    error="No output SQL",
                ))
                continue

            logger.info(
                f"[{query_id}] Benchmark {idx + 1}/{len(patches)}: "
                f"{patch.patch_id} ({getattr(patch, 'family', '?')}/"
                f"{getattr(patch, 'transform', '?')})"
            )

            try:
                result = _benchmark_single_candidate(
                    executor=executor,
                    patch=patch,
                    patch_idx=idx,
                    orig_ms=orig_ms,
                    orig_count=orig_count,
                    orig_checksum=orig_checksum,
                    checker=checker,
                    candidate_runs=candidate_runs,
                    timeout_ms=timeout_ms,
                    query_id=query_id,
                    original_sql=original_sql,
                    sample_db_path=sample_db_path,
                )
                candidate_results.append(result)

                if result.passed:
                    patch.original_ms = orig_ms
                    patch.patch_ms = result.avg_ms
                    patch.speedup = result.speedup
                    patch.status = classify(result.speedup)
                    if orig_count is not None:
                        patch.correctness_verified = True

                    logger.info(
                        f"[{query_id}]   result: orig={orig_ms:.1f}ms, "
                        f"patch={result.avg_ms:.1f}ms, "
                        f"speedup={result.speedup:.2f}x "
                        f"({patch.status}, {result.row_count} rows) "
                        f"[{', '.join(f'{t:.0f}' for t in result.all_times)}]"
                    )

                    # Winner confirmation: fuller timing for current best
                    if (
                        winner_runs > candidate_runs
                        and result.speedup > current_best_speedup
                    ):
                        logger.info(
                            f"[{query_id}]   winner confirm: {patch.patch_id} "
                            f"{candidate_runs}x->{winner_runs}x"
                        )
                        try:
                            win_ms, win_rows, win_times = _timed_runs(
                                executor, patch.output_sql,
                                runs=winner_runs, capture_rows=True,
                                timeout_ms=timeout_ms,
                            )
                        except Exception as e:
                            logger.warning(
                                f"[{query_id}]   winner confirm FAILED: {e}"
                            )
                            # Keep candidate result from initial runs
                            if result.passed and result.speedup > current_best_speedup:
                                current_best_speedup = result.speedup
                            continue
                        win_count = len(win_rows) if win_rows else 0

                        # Re-verify correctness after winner runs
                        win_ok = True
                        if orig_count is not None:
                            if win_count != orig_count:
                                patch.speedup = 0.0
                                patch.status = "FAIL"
                                patch.correctness_verified = False
                                patch.apply_error = (
                                    f"Row count mismatch on confirm: "
                                    f"original={orig_count}, patch={win_count}"
                                )
                                result.passed = False
                                win_ok = False
                            elif orig_checksum and win_rows:
                                try:
                                    win_cksum = checker.compute_checksum(win_rows)
                                    if win_cksum != orig_checksum:
                                        patch.speedup = 0.0
                                        patch.status = "FAIL"
                                        patch.correctness_verified = False
                                        patch.apply_error = (
                                            f"Checksum mismatch on confirm: "
                                            f"original={orig_checksum}, "
                                            f"patch={win_cksum}"
                                        )
                                        result.passed = False
                                        win_ok = False
                                except Exception as e:
                                    logger.warning(
                                        f"[{query_id}]   winner checksum "
                                        f"compute failed (non-blocking): {e}"
                                    )

                        if win_ok:
                            patch.patch_ms = win_ms
                            patch.speedup = orig_ms / win_ms if win_ms > 0 else 1.0
                            patch.status = classify(patch.speedup)
                            result.speedup = patch.speedup
                            result.avg_ms = win_ms
                            logger.info(
                                f"[{query_id}]   winner confirmed: "
                                f"patch={win_ms:.1f}ms, "
                                f"speedup={patch.speedup:.2f}x "
                                f"({patch.status}, {win_count} rows) "
                                f"[{', '.join(f'{t:.0f}' for t in win_times)}]"
                            )

                    if result.passed and result.speedup > current_best_speedup:
                        current_best_speedup = result.speedup
                else:
                    # Failed correctness or execution
                    patch.speedup = 0.0
                    patch.status = result.error.split(":")[0] if result.error else "FAIL"
                    if result.error:
                        patch.apply_error = result.error
                    logger.warning(
                        f"[{query_id}]   FAIL: {patch.patch_id}: {result.error}"
                    )

            except Exception as e:
                patch.speedup = 0.0
                patch.status = "ERROR"
                patch.apply_error = str(e)
                candidate_results.append(CandidateResult(
                    patch_idx=idx, passed=False, error=str(e),
                ))
                logger.warning(
                    f"[{query_id}]   ERROR: {patch.patch_id}: {e}"
                )

        # ── Step 3: EXPLAIN collection (same connection) ──────────────
        if collect_explain:
            for idx, patch in enumerate(patches):
                if not patch.output_sql:
                    continue
                cr = candidate_results[idx] if idx < len(candidate_results) else None
                if cr and not cr.passed:
                    continue
                try:
                    if collect_explain_fn:
                        # Caller controls EXPLAIN format + rendering
                        patch.explain_text = collect_explain_fn(
                            executor, patch.output_sql,
                        )
                    else:
                        # Fallback: raw EXPLAIN ANALYZE text
                        explain_rows = executor.execute(
                            f"EXPLAIN ANALYZE {patch.output_sql}",
                            timeout_ms=timeout_ms,
                        )
                        if explain_rows and isinstance(explain_rows, list):
                            if isinstance(explain_rows[0], dict):
                                lines = []
                                for row in explain_rows:
                                    for v in row.values():
                                        lines.append(str(v))
                                explain_text = "\n".join(lines)
                            else:
                                explain_text = "\n".join(
                                    str(r) for r in explain_rows
                                )
                        elif isinstance(explain_rows, str):
                            explain_text = explain_rows
                        else:
                            explain_text = str(explain_rows) if explain_rows else ""

                        explain_lines = explain_text.split("\n")
                        if len(explain_lines) > 80:
                            explain_text = (
                                "\n".join(explain_lines[:80])
                                + "\n... (truncated)"
                            )
                        patch.explain_text = explain_text

                except Exception as e:
                    logger.warning(f"EXPLAIN failed for {patch.patch_id}: {e}")

    # Connection closed here. No other connections opened.

    # ── Build summary ─────────────────────────────────────────────
    n_passed = sum(1 for cr in candidate_results if cr.passed)
    best_idx = None
    best_speedup = 0.0
    for cr in candidate_results:
        if cr.passed and cr.speedup > best_speedup:
            best_speedup = cr.speedup
            best_idx = cr.patch_idx

    return BenchmarkSummary(
        baseline_ms=orig_ms,
        baseline_rows=orig_count if orig_count is not None else 0,
        baseline_checksum=orig_checksum if not known_timeout else None,
        n_benchmarked=len(candidate_results),
        n_passed=n_passed,
        best_speedup=best_speedup,
        best_patch_idx=best_idx,
        candidate_results=candidate_results,
    )


def _benchmark_single_candidate(
    executor: Any,
    patch: Any,
    patch_idx: int,
    orig_ms: float,
    orig_count: Optional[int],
    orig_checksum: Optional[str],
    checker: EquivalenceChecker,
    candidate_runs: int,
    timeout_ms: int,
    query_id: str,
    original_sql: str = "",
    sample_db_path: Optional[str] = None,
) -> CandidateResult:
    """Benchmark a single candidate with fail-fast correctness.

    Run 1: execute + capture rows + check correctness immediately.
    If correctness fails, skip remaining runs.
    Runs 2+: execute + time only.
    """
    all_times: List[float] = []
    captured_rows = None
    sample_equivalent = False  # True if 0-row but passed sample check

    # Warmup for runs >= 3
    if candidate_runs >= 3:
        executor.execute(patch.output_sql, timeout_ms=timeout_ms)

    measured_count = candidate_runs if candidate_runs <= 2 else (candidate_runs - 1)

    for i in range(measured_count):
        start = time.perf_counter()
        rows = executor.execute(patch.output_sql, timeout_ms=timeout_ms)
        elapsed = (time.perf_counter() - start) * 1000
        all_times.append(elapsed)

        if i == 0:
            captured_rows = rows
            row_count = len(rows) if rows else 0

            # ── Fail-fast correctness on run 1 ──────────────────
            if orig_count is not None:
                # Row count mismatch
                if row_count != orig_count:
                    # Check sample DB for timeout recovery (0-row = likely timeout)
                    if row_count == 0 and sample_db_path:
                        from .sample_checker import SampleChecker
                        sc = SampleChecker(sample_db_path)
                        sr = sc.check_semantic_equivalence(
                            original_sql,
                            patch.output_sql,
                        )
                        if sr.equivalent:
                            logger.info(
                                f"[{query_id}]   0-row but sample-equivalent "
                                f"({sr.original_sample_rows} rows) — "
                                f"continuing with timing only"
                            )
                            sample_equivalent = True
                            # Don't return — continue timing runs
                        else:
                            return CandidateResult(
                                patch_idx=patch_idx,
                                passed=False,
                                row_count=row_count,
                                error=(
                                    f"Row count mismatch: "
                                    f"original={orig_count}, patch={row_count} "
                                    f"(sample check also failed)"
                                ),
                                all_times=all_times,
                            )
                    else:
                        return CandidateResult(
                            patch_idx=patch_idx,
                            passed=False,
                            row_count=row_count,
                            error=(
                                f"Row count mismatch: "
                                f"original={orig_count}, patch={row_count}"
                            ),
                            all_times=all_times,
                        )

                # Checksum mismatch (skip if sample-equivalent — no rows to check)
                if not sample_equivalent and orig_checksum and captured_rows:
                    try:
                        patch_checksum = checker.compute_checksum(captured_rows)
                        if patch_checksum != orig_checksum:
                            return CandidateResult(
                                patch_idx=patch_idx,
                                passed=False,
                                row_count=row_count,
                                checksum=patch_checksum,
                                error=(
                                    f"Checksum mismatch: "
                                    f"original={orig_checksum}, "
                                    f"patch={patch_checksum}"
                                ),
                                all_times=all_times,
                            )
                    except Exception as e:
                        logger.warning(
                            f"[{query_id}]   checksum compute failed "
                            f"(non-blocking): {e}"
                        )

    # All runs complete — compute timing
    row_count = len(captured_rows) if captured_rows else 0
    avg_ms = sum(all_times) / len(all_times) if all_times else 0.0
    speedup = orig_ms / avg_ms if avg_ms > 0 else 1.0

    return CandidateResult(
        patch_idx=patch_idx,
        passed=True,
        speedup=speedup,
        avg_ms=avg_ms,
        row_count=row_count,
        # Correctness is NOT verified if we only passed sample check
        correctness_verified=orig_count is not None and not sample_equivalent,
        all_times=all_times,
    )
