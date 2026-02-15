"""Oneshot Patch optimization session — iterative patch plan loop.

Flow per iteration:
1. Build oneshot patch prompt → LLM call → 4 patch plans
2. Retry loop for malformed/un-parseable patches (before validation)
3. Apply patches → semantic validation → race → EXPLAIN collection
4. Multi-handling: keep good patches, retry only failed ones, merge results
5. Build snipe prompt (original + summary history + latest results) → LLM → new patches
6. Iterate until target speedup or max_iterations
"""

from __future__ import annotations

import copy
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .base_session import OptimizationSession
from ..schemas import SessionResult

if TYPE_CHECKING:
    from ..pipeline import Pipeline

logger = logging.getLogger(__name__)

# LLM call timeout (seconds)
LLM_TIMEOUT_SECONDS = 600
# Minimum number of patches we expect from the LLM
MIN_PATCHES_EXPECTED = 3


# ── Per-Iteration Data Classes ──────────────────────────────────────────────


@dataclass
class AppliedPatch:
    """Result from applying a single patch plan to IR."""

    patch_id: str
    family: str
    transform: str
    relevance_score: float
    output_sql: Optional[str] = None
    apply_error: Optional[str] = None
    semantic_passed: bool = False
    speedup: Optional[float] = None
    status: str = "PENDING"
    explain_text: Optional[str] = None
    original_ms: Optional[float] = None
    patch_ms: Optional[float] = None
    raw_plan: Optional[dict] = None  # preserve original LLM JSON for retry/snipe


@dataclass
class PatchIterationResult:
    """Complete result from one iteration of the session."""

    iteration: int
    prompt: str
    response: str
    n_api_calls: int
    patches: List[AppliedPatch] = field(default_factory=list)
    race_result: Optional[Any] = None
    explains: Dict[str, str] = field(default_factory=dict)
    best_speedup: float = 0.0
    best_patch_id: Optional[str] = None
    best_sql: Optional[str] = None


# ── Session Class ───────────────────────────────────────────────────────────


class OneshotPatchSession(OptimizationSession):
    """Iterative patch-plan optimization: prompt → 4 patches → validate → snipe."""

    def run(self) -> SessionResult:
        """Execute the oneshot patch optimization loop.

        Dispatches to tiered mode if config.tiered_patch_enabled is True.
        """
        if getattr(self.pipeline.config, "tiered_patch_enabled", False):
            return self._run_tiered()
        return self._run_single_tier()

    def _run_single_tier(self) -> SessionResult:
        """Original single-tier patch loop (one LLM does everything)."""
        from ..ir import build_script_ir, render_ir_node_map, Dialect
        from ..patches.oneshot_patch_prompt_builder import (
            build_oneshot_patch_prompt,
            build_oneshot_patch_snipe_prompt,
            load_gold_examples,
        )
        from ..generate import CandidateGenerator
        from ..execution.database_utils import run_explain_analyze, run_explain_text

        logger.info(
            f"[{self.query_id}] OneshotPatchSession: "
            f"max {self.max_iterations} iterations, "
            f"target {self.target_speedup:.1f}x"
        )

        # ── Setup ──────────────────────────────────────────────────────
        db_path = self.pipeline.config.benchmark_dsn or self.pipeline.config.db_path_or_dsn
        dialect_upper = self.dialect.upper()
        dialect_enum = (
            Dialect[dialect_upper]
            if dialect_upper in Dialect.__members__
            else Dialect.POSTGRES
        )

        # Session directory for disk persistence
        session_dir = self._create_session_dir()

        # Build IR and node map (once — reused across iterations)
        script_ir = build_script_ir(self.original_sql, dialect_enum)
        ir_node_map = render_ir_node_map(script_ir)

        # Get EXPLAIN for the original query
        original_explain = ""
        explain_result = run_explain_analyze(db_path, self.original_sql)
        if explain_result:
            original_explain = explain_result.get("plan_text", "")
        if not original_explain:
            original_explain = run_explain_text(db_path, self.original_sql) or "(EXPLAIN unavailable)"

        # Load gold examples for families
        gold_examples = load_gold_examples(self.dialect)

        # LLM generator
        generator = CandidateGenerator(
            provider=self.pipeline.provider,
            model=self.pipeline.model,
            analyze_fn=self.pipeline.analyze_fn,
        )

        # ── Iteration State ────────────────────────────────────────────
        best_speedup = 0.0
        best_sql = self.original_sql
        best_transforms: List[str] = []
        best_status = "NEUTRAL"
        iterations_data: List[PatchIterationResult] = []
        total_api_calls = 0

        for iteration in range(self.max_iterations):
            logger.info(
                f"[{self.query_id}] Patch iteration {iteration + 1}/{self.max_iterations}"
            )
            iter_api_calls = 0

            # ── Phase 1: Build prompt ────────────────────────────────
            if iteration == 0:
                prompt = build_oneshot_patch_prompt(
                    query_id=self.query_id,
                    original_sql=self.original_sql,
                    explain_text=original_explain,
                    ir_node_map=ir_node_map,
                    all_5_examples=gold_examples,
                    dialect=self.dialect,
                )
            else:
                prev = iterations_data[-1]
                # Build history from ALL prior iterations for summary table
                all_prior = [it.patches for it in iterations_data]
                prompt = build_oneshot_patch_snipe_prompt(
                    original_prompt=iterations_data[0].prompt,
                    iteration=iteration - 1,
                    patches=prev.patches,
                    original_explain=original_explain,
                    explains=prev.explains,
                    race_result=prev.race_result,
                    all_prior_iterations=all_prior,
                )

            # ── Phase 1.5: LLM call + retry ───────────────────────────
            logger.info(
                f"[{self.query_id}] Calling LLM ({self.pipeline.model}, "
                f"prompt={len(prompt)} chars)..."
            )
            response = self._call_llm_with_timeout(generator, prompt)
            iter_api_calls += 1
            logger.info(
                f"[{self.query_id}] LLM response: {len(response)} chars"
            )

            # Persist prompt + response to disk
            self._save_to_disk(session_dir, iteration, "prompt", prompt)
            self._save_to_disk(session_dir, iteration, "response", response)

            patches, errors = self._parse_and_apply_all(
                response, script_ir, dialect_enum
            )

            # Check minimum patch count — warn + retry if < 3
            if len(patches) < MIN_PATCHES_EXPECTED and not errors:
                logger.warning(
                    f"[{self.query_id}] LLM returned only {len(patches)} patches "
                    f"(expected >= {MIN_PATCHES_EXPECTED}), requesting more"
                )
                errors.append(("_count", f"Only {len(patches)} patches returned, expected at least {MIN_PATCHES_EXPECTED}"))

            # Retry loop: up to 2 retries for malformed patches
            retry_count = 0
            while errors and retry_count < 2:
                logger.info(
                    f"[{self.query_id}] Retrying {len(errors)} issues "
                    f"(attempt {retry_count + 1}/2)"
                )
                from ..patches.oneshot_patch_prompt_builder import build_oneshot_patch_retry_prompt
                retry_prompt = build_oneshot_patch_retry_prompt(
                    original_prompt=prompt,
                    previous_response=response,
                    all_patches=patches,
                    errors=errors,
                )
                response = self._call_llm_with_timeout(generator, retry_prompt)
                iter_api_calls += 1
                retry_count += 1

                # Persist retry response
                self._save_to_disk(session_dir, iteration, f"retry_{retry_count}_response", response)

                patches, errors = self._parse_and_apply_all(
                    response, script_ir, dialect_enum
                )

            # Filter to successfully applied patches
            applied = [p for p in patches if p.output_sql]
            logger.info(
                f"[{self.query_id}] {len(applied)}/{len(patches)} patches applied"
            )

            if not applied:
                iterations_data.append(PatchIterationResult(
                    iteration=iteration,
                    prompt=prompt,
                    response=response,
                    n_api_calls=iter_api_calls,
                    patches=patches,
                ))
                total_api_calls += iter_api_calls
                continue

            # ── Phase 2: Validate ──────────────────────────────────────
            iter_result = self._validate_patches(applied, db_path)
            iter_result.iteration = iteration
            iter_result.prompt = prompt
            iter_result.response = response
            iter_result.n_api_calls = iter_api_calls

            # Include all patches (applied + failed) for snipe diagnosis
            all_patches = []
            applied_ids = {p.patch_id for p in applied}
            for p in patches:
                if p.patch_id in applied_ids:
                    # Find the validated version
                    validated = next((a for a in applied if a.patch_id == p.patch_id), p)
                    all_patches.append(validated)
                else:
                    all_patches.append(p)
            iter_result.patches = all_patches

            # ── Phase 3: Multi-handling retry ─────────────────────────
            # Only retry patches that actually errored at runtime.
            # Keep everything else — wins, neutrals, AND regressions
            # are all useful signal for the snipe prompt.
            runtime_errored = [
                p for p in all_patches
                if p.status == "ERROR" and p.output_sql
            ]
            runtime_kept = [
                p for p in all_patches
                if p.status != "ERROR" and p.status != "PENDING"
            ]

            if runtime_errored and runtime_kept:
                logger.info(
                    f"[{self.query_id}] Multi-handling: "
                    f"{len(runtime_kept)} kept, {len(runtime_errored)} errored "
                    f"— retrying errored patches only"
                )
                from ..patches.oneshot_patch_prompt_builder import (
                    build_runtime_error_retry_prompt,
                )

                retry_prompt = build_runtime_error_retry_prompt(
                    original_prompt=prompt,
                    good_patches=runtime_kept,
                    failed_patches=runtime_errored,
                )
                logger.info(
                    f"[{self.query_id}] Calling LLM for {len(runtime_errored)} "
                    f"replacement patches ({len(retry_prompt)} chars)..."
                )
                retry_response = self._call_llm_with_timeout(generator, retry_prompt)
                iter_api_calls += 1
                logger.info(
                    f"[{self.query_id}] Multi-handling LLM response: "
                    f"{len(retry_response)} chars"
                )

                self._save_to_disk(
                    session_dir, iteration, "multi_retry_prompt", retry_prompt
                )
                self._save_to_disk(
                    session_dir, iteration, "multi_retry_response", retry_response
                )

                new_patches, _ = self._parse_and_apply_all(
                    retry_response, script_ir, dialect_enum
                )
                new_applied = [p for p in new_patches if p.output_sql]
                logger.info(
                    f"[{self.query_id}] Multi-handling: "
                    f"{len(new_applied)}/{len(new_patches)} replacement patches applied"
                )

                if new_applied:
                    # Validate only the new patches
                    new_result = self._validate_patches(new_applied, db_path)

                    # Merge: replace errored patches with new results
                    failed_ids = {p.patch_id for p in runtime_errored}
                    merged = [p for p in all_patches if p.patch_id not in failed_ids]
                    merged.extend(new_applied)
                    iter_result.patches = merged

                    # Merge explains
                    iter_result.explains.update(new_result.explains)

                    # Recalculate best from merged set
                    merged_with_speedup = [
                        p for p in merged
                        if p.speedup is not None and p.speedup >= 1.0
                    ]
                    if merged_with_speedup:
                        best_merged = max(
                            merged_with_speedup, key=lambda p: p.speedup
                        )
                        iter_result.best_speedup = best_merged.speedup or 0.0
                        iter_result.best_patch_id = best_merged.patch_id
                        iter_result.best_sql = best_merged.output_sql

                    logger.info(
                        f"[{self.query_id}] Multi-handling merged: "
                        f"{len(merged)} patches total, "
                        f"best={iter_result.best_speedup:.2f}x"
                    )

            iterations_data.append(iter_result)
            total_api_calls += iter_api_calls

            # Persist iteration results
            self._save_to_disk(
                session_dir, iteration, "result",
                json.dumps(self._serialize_iteration(iter_result), indent=2, default=str)
            )

            # ── Track best (only >= 1.0 counts) ──────────────────────
            if iter_result.best_speedup >= 1.0 and iter_result.best_speedup > best_speedup:
                best_speedup = iter_result.best_speedup
                best_sql = iter_result.best_sql or self.original_sql
                best_patch = next(
                    (p for p in all_patches if p.patch_id == iter_result.best_patch_id),
                    None,
                )
                if best_patch:
                    best_transforms = [best_patch.transform]
                best_status = self._classify_speedup(best_speedup)

            logger.info(
                f"[{self.query_id}] Iteration {iteration + 1}: "
                f"best={best_speedup:.2f}x ({best_status})"
            )

            # ── Early exit if target met ───────────────────────────────
            if best_speedup >= self.target_speedup:
                logger.info(
                    f"[{self.query_id}] Target {self.target_speedup:.1f}x reached "
                    f"({best_speedup:.2f}x) — stopping"
                )
                break

        # ── Build SessionResult ────────────────────────────────────────
        return SessionResult(
            query_id=self.query_id,
            mode="oneshot_patch",
            best_speedup=best_speedup,
            best_sql=best_sql,
            original_sql=self.original_sql,
            best_transforms=best_transforms,
            status=best_status,
            iterations=[self._serialize_iteration(it) for it in iterations_data],
            n_iterations=len(iterations_data),
            n_api_calls=total_api_calls,
        )

    # ── Tiered Mode ─────────────────────────────────────────────────────────

    def _run_tiered(self) -> SessionResult:
        """2-tier patch mode: analyst → workers → semantic retry → benchmark → snipe → iterate.

        Full pipeline per iteration:
        1. Analyst call (or snipe on iteration 2+)
        2. Workers (parallel, role-routed)
        3. Apply-error retry (existing)
        4. Semantic validation + worker retry for semantic failures
        5. Benchmark semantically-valid patches (5x trimmed mean)
        6. EXPLAIN collection
        7. Track best, stop if >= target_speedup
        8. Next iteration uses snipe prompt with all results
        """
        from ..ir import build_script_ir, render_ir_node_map, Dialect
        from ..patches.oneshot_patch_prompt_builder import load_gold_examples
        from ..patches.tiered_orchestrator import TieredOrchestrator
        from ..execution.database_utils import run_explain_analyze, run_explain_text

        # Use session-level target if set, fall back to config
        target_speedup = self.target_speedup or getattr(self.pipeline.config, "target_speedup", 10.0)

        logger.info(
            f"[{self.query_id}] OneshotPatchSession TIERED: "
            f"max {self.max_iterations} iterations, "
            f"target {target_speedup:.1f}x"
        )

        # ── Setup ──────────────────────────────────────────────────────
        db_path = self.pipeline.config.benchmark_dsn or self.pipeline.config.db_path_or_dsn
        dialect_upper = self.dialect.upper()
        dialect_enum = (
            Dialect[dialect_upper]
            if dialect_upper in Dialect.__members__
            else Dialect.POSTGRES
        )

        session_dir = self._create_session_dir()
        script_ir = build_script_ir(self.original_sql, dialect_enum)
        ir_node_map = render_ir_node_map(script_ir)

        # Get EXPLAIN
        original_explain = ""
        explain_result = run_explain_analyze(db_path, self.original_sql)
        if explain_result:
            original_explain = explain_result.get("plan_text", "")
        if not original_explain:
            original_explain = run_explain_text(db_path, self.original_sql) or "(EXPLAIN unavailable)"

        gold_examples = load_gold_examples(self.dialect)

        # ── Build LLM call functions ──────────────────────────────────
        analyst_call_fn = self._make_llm_call_fn(
            getattr(self.pipeline.config, "analyst_model", None)
        )
        worker_call_fn = self._make_llm_call_fn(
            getattr(self.pipeline.config, "worker_model", None)
        )

        orchestrator = TieredOrchestrator(
            analyst_call_fn=analyst_call_fn,
            worker_call_fn=worker_call_fn,
            gold_examples=gold_examples,
            dialect=self.dialect,
        )

        # ── Iteration State ────────────────────────────────────────────
        best_speedup = 0.0
        best_sql = self.original_sql
        best_transforms: List[str] = []
        best_status = "NEUTRAL"
        iterations_data: List[PatchIterationResult] = []
        total_api_calls = 0
        # Track all patches across iterations for snipe context
        prev_all_patches: Optional[List[AppliedPatch]] = None
        prev_explains: Dict[str, str] = {}

        for iteration in range(self.max_iterations):
            logger.info(
                f"[{self.query_id}] Tiered iteration {iteration + 1}/{self.max_iterations}"
            )
            iter_api_calls = 0

            # ── Phase 1: Analyst call (or snipe on iteration 2+) ──────
            if self.on_phase_change:
                self.on_phase_change(
                    phase="analyst" if iteration == 0 else "snipe",
                    iteration=iteration,
                )
            if iteration == 0:
                targets, analyst_prompt, analyst_response = orchestrator.run_analyst(
                    query_id=self.query_id,
                    original_sql=self.original_sql,
                    explain_text=original_explain,
                    ir_node_map=ir_node_map,
                )
            else:
                # Snipe round: analyst sees all prior results
                targets, analyst_prompt, analyst_response = orchestrator.run_snipe(
                    query_id=self.query_id,
                    original_sql=self.original_sql,
                    explain_text=original_explain,
                    ir_node_map=ir_node_map,
                    patches=prev_all_patches or [],
                    patch_explains=prev_explains,
                )
            iter_api_calls += 1

            self._save_to_disk(session_dir, iteration, "analyst_prompt", analyst_prompt)
            self._save_to_disk(session_dir, iteration, "analyst_response", analyst_response)

            if not targets:
                logger.warning(f"[{self.query_id}] No analyst targets parsed")
                iterations_data.append(PatchIterationResult(
                    iteration=iteration,
                    prompt=analyst_prompt,
                    response=analyst_response,
                    n_api_calls=iter_api_calls,
                ))
                total_api_calls += iter_api_calls
                continue

            # Save target summaries
            self._save_to_disk(
                session_dir, iteration, "targets",
                json.dumps([t.to_dict() for t in targets], indent=2)
            )

            # ── Phase 2: Worker calls (parallel, role-routed) ────────
            if self.on_phase_change:
                self.on_phase_change(phase="workers", iteration=iteration)
            logger.info(
                f"[{self.query_id}] Phase 2: dispatching {len(targets)} workers"
            )
            patches, worker_api_calls = orchestrator.run_workers(
                original_sql=self.original_sql,
                ir_node_map=ir_node_map,
                targets=targets,
                script_ir=script_ir,
                dialect_enum=dialect_enum,
            )
            iter_api_calls += worker_api_calls

            applied = [p for p in patches if p.output_sql]
            logger.info(
                f"[{self.query_id}] Phase 2 done: "
                f"{len(applied)}/{len(patches)} patches applied"
            )

            # Save worker outputs
            for p in patches:
                status = "OK" if p.output_sql else f"FAIL: {p.apply_error}"
                self._save_to_disk(
                    session_dir, iteration, f"worker_{p.patch_id}",
                    f"Family: {p.family}\nTransform: {p.transform}\n"
                    f"Status: {status}\n\n"
                    f"{'--- OUTPUT SQL ---' if p.output_sql else '--- NO OUTPUT ---'}\n"
                    f"{p.output_sql or p.apply_error or 'N/A'}"
                )

            # ── Phase 3: Apply-error retry for failed patches ────────
            logger.info(f"[{self.query_id}] Phase 3: apply-error retries")
            failed = [p for p in patches if not p.output_sql and p.apply_error]
            for p in failed:
                target_match = next(
                    (t for t in targets if t.target_id == p.patch_id), None
                )
                if not target_match:
                    continue

                for retry_num in range(2):
                    logger.info(
                        f"[{self.query_id}] Worker retry {retry_num + 1}/2 "
                        f"for {p.patch_id}: {p.apply_error[:60]}"
                    )
                    retried = orchestrator.retry_worker(
                        original_sql=self.original_sql,
                        ir_node_map=ir_node_map,
                        target=target_match,
                        error=p.apply_error,
                        script_ir=script_ir,
                        dialect_enum=dialect_enum,
                    )
                    iter_api_calls += 1

                    if retried and retried.output_sql:
                        idx = patches.index(p)
                        patches[idx] = retried
                        applied.append(retried)
                        logger.info(
                            f"[{self.query_id}] Worker retry SUCCESS for {p.patch_id}"
                        )
                        break

            if not applied:
                iterations_data.append(PatchIterationResult(
                    iteration=iteration,
                    prompt=analyst_prompt,
                    response=analyst_response,
                    n_api_calls=iter_api_calls,
                    patches=patches,
                ))
                total_api_calls += iter_api_calls
                continue

            # ── Phase 4: Semantic validation + retry ──────────────────
            if self.on_phase_change:
                self.on_phase_change(phase="semantic", iteration=iteration)
            logger.info(
                f"[{self.query_id}] Phase 4: semantic validation "
                f"({len(applied)} patches)"
            )
            applied, sem_api_calls = self._semantic_validate_and_retry(
                applied=applied,
                targets=targets,
                orchestrator=orchestrator,
                ir_node_map=ir_node_map,
                db_path=db_path,
                script_ir=script_ir,
                dialect_enum=dialect_enum,
            )
            iter_api_calls += sem_api_calls

            sem_passed = [p for p in applied if p.semantic_passed]
            if not sem_passed:
                logger.info(
                    f"[{self.query_id}] No patches passed semantic validation"
                )
                # Build all_patches for snipe context
                all_patches = self._merge_all_patches(patches, applied)
                prev_all_patches = all_patches
                prev_explains = {}
                iterations_data.append(PatchIterationResult(
                    iteration=iteration,
                    prompt=analyst_prompt,
                    response=analyst_response,
                    n_api_calls=iter_api_calls,
                    patches=all_patches,
                ))
                total_api_calls += iter_api_calls
                continue

            # ── Phase 5: Benchmark semantically-valid patches ─────────
            # Acquire benchmark_lock if shared across concurrent sessions
            # to prevent timing pollution from concurrent DB load
            if self.on_phase_change:
                self.on_phase_change(phase="benchmark", iteration=iteration)
            logger.info(
                f"[{self.query_id}] Phase 5: benchmark "
                f"{len(sem_passed)} sem-passed patches (3x warmup+avg2)"
            )
            from contextlib import nullcontext
            bench_ctx = self.benchmark_lock if self.benchmark_lock else nullcontext()
            with bench_ctx:
                logger.info(f"[{self.query_id}] Benchmark lock acquired")
                self._sequential_benchmark(sem_passed, db_path)
                logger.info(f"[{self.query_id}] Benchmark lock released")

            # Save benchmark results
            for p in sem_passed:
                speedup_str = f"{p.speedup:.2f}x" if p.speedup else "N/A"
                self._save_to_disk(
                    session_dir, iteration, f"bench_{p.patch_id}",
                    f"Speedup: {speedup_str}\nStatus: {p.status}\n"
                    f"Original ms: {p.original_ms}\nPatch ms: {p.patch_ms}\n"
                    f"Error: {p.apply_error or 'none'}"
                )

            # ── Phase 6: EXPLAIN collection ───────────────────────────
            if self.on_phase_change:
                self.on_phase_change(phase="explain", iteration=iteration)
            logger.info(
                f"[{self.query_id}] Phase 6: EXPLAIN collection"
            )
            iter_explains: Dict[str, str] = {}
            for p in sem_passed:
                if p.output_sql:
                    try:
                        explain_data = run_explain_analyze(db_path, p.output_sql)
                        if explain_data:
                            p.explain_text = explain_data.get("plan_text", "")
                            iter_explains[p.patch_id] = p.explain_text
                        else:
                            text = run_explain_text(db_path, p.output_sql)
                            if text:
                                p.explain_text = text
                                iter_explains[p.patch_id] = text
                    except Exception as e:
                        logger.warning(f"EXPLAIN failed for {p.patch_id}: {e}")

            # Build complete all_patches list
            all_patches = self._merge_all_patches(patches, applied)

            # Build iteration result
            iter_result = PatchIterationResult(
                iteration=iteration,
                prompt=analyst_prompt,
                response=analyst_response,
                n_api_calls=iter_api_calls,
                patches=all_patches,
                explains=iter_explains,
            )

            # Find best from benchmarked patches
            candidates_with_speedup = [
                p for p in sem_passed
                if p.speedup is not None and p.speedup >= 1.0
            ]
            if candidates_with_speedup:
                best_patch = max(candidates_with_speedup, key=lambda p: p.speedup)
                iter_result.best_speedup = best_patch.speedup or 0.0
                iter_result.best_patch_id = best_patch.patch_id
                iter_result.best_sql = best_patch.output_sql

            # Store for next iteration's snipe prompt
            prev_all_patches = all_patches
            prev_explains = iter_explains

            iterations_data.append(iter_result)
            total_api_calls += iter_api_calls

            # Persist
            self._save_to_disk(
                session_dir, iteration, "result",
                json.dumps(self._serialize_iteration(iter_result), indent=2, default=str)
            )

            # ── Track best ──────────────────────────────────────────────
            if iter_result.best_speedup >= 1.0 and iter_result.best_speedup > best_speedup:
                best_speedup = iter_result.best_speedup
                best_sql = iter_result.best_sql or self.original_sql
                bp = next(
                    (p for p in all_patches if p.patch_id == iter_result.best_patch_id),
                    None,
                )
                if bp:
                    best_transforms = [bp.transform]
                best_status = self._classify_speedup(best_speedup)

            logger.info(
                f"[{self.query_id}] Tiered iteration {iteration + 1}: "
                f"best={best_speedup:.2f}x ({best_status})"
            )

            if best_speedup >= target_speedup:
                logger.info(
                    f"[{self.query_id}] Target {target_speedup:.1f}x reached "
                    f"({best_speedup:.2f}x) — stopping"
                )
                break

        return SessionResult(
            query_id=self.query_id,
            mode="oneshot_patch",
            best_speedup=best_speedup,
            best_sql=best_sql,
            original_sql=self.original_sql,
            best_transforms=best_transforms,
            status=best_status,
            iterations=[self._serialize_iteration(it) for it in iterations_data],
            n_iterations=len(iterations_data),
            n_api_calls=total_api_calls,
        )

    @staticmethod
    def _merge_all_patches(
        patches: List[AppliedPatch],
        applied: List[AppliedPatch],
    ) -> List[AppliedPatch]:
        """Merge applied patches (potentially updated by retries) back into full list."""
        applied_ids = {p.patch_id for p in applied}
        all_patches = []
        for p in patches:
            if p.patch_id in applied_ids:
                validated = next(
                    (a for a in applied if a.patch_id == p.patch_id), p
                )
                all_patches.append(validated)
            else:
                all_patches.append(p)
        return all_patches

    def _semantic_validate_and_retry(
        self,
        applied: List[AppliedPatch],
        targets: list,
        orchestrator,
        ir_node_map: str,
        db_path: str,
        script_ir,
        dialect_enum,
    ) -> tuple[List[AppliedPatch], int]:
        """Run semantic validation on applied patches, retry failures.

        For each applied patch:
        1. Run MiniValidator
        2. If fail → retry worker with semantic error context (up to 2 retries)
        3. Re-validate retry results

        Returns:
            (updated_patches, api_call_count)
        """
        from ..validation.mini_validator import MiniValidator

        api_calls = 0

        if not self.pipeline.config.semantic_validation_enabled:
            for p in applied:
                p.semantic_passed = True
            return applied, 0

        try:
            validator = MiniValidator(
                db_path=db_path,
                sample_pct=self.pipeline.config.semantic_sample_pct,
                timeout_ms=self.pipeline.config.semantic_timeout_ms,
                dialect=self.dialect,
            )
        except Exception as e:
            logger.warning(f"Semantic validator init failed: {e}")
            for p in applied:
                p.semantic_passed = True
            return applied, 0

        for p in applied:
            if not p.output_sql:
                continue

            # Find matching target for this patch
            target_match = next(
                (t for t in targets if t.target_id == p.patch_id), None
            )

            # Validate
            try:
                sem_result = validator.validate_rewrite(
                    self.original_sql, p.output_sql, worker_id=0
                )
            except Exception as e:
                logger.warning(f"Semantic validation error for {p.patch_id}: {e}")
                p.semantic_passed = True  # Don't block on validator errors
                continue

            if sem_result.passed:
                p.semantic_passed = True
                continue

            # Failed — retry with semantic error context
            p.semantic_passed = False
            logger.info(
                f"[{self.query_id}] Semantic FAIL for {p.patch_id}: "
                f"{'; '.join(sem_result.errors[:2])}"
            )

            if not target_match:
                p.status = "FAIL"
                p.apply_error = f"Semantic: {'; '.join(sem_result.errors[:2])}"
                continue

            # Up to 2 semantic retries
            retried_ok = False
            for retry_num in range(2):
                logger.info(
                    f"[{self.query_id}] Semantic retry {retry_num + 1}/2 "
                    f"for {p.patch_id}"
                )
                retried = orchestrator.retry_worker_semantic(
                    original_sql=self.original_sql,
                    ir_node_map=ir_node_map,
                    target=target_match,
                    sem_result=sem_result,
                    rewrite_sql=p.output_sql,
                    script_ir=script_ir,
                    dialect_enum=dialect_enum,
                )
                api_calls += 1

                if not retried or not retried.output_sql:
                    continue

                # Re-validate the retry
                try:
                    retry_sem = validator.validate_rewrite(
                        self.original_sql, retried.output_sql, worker_id=0
                    )
                except Exception as e:
                    logger.warning(f"Semantic re-validation error: {e}")
                    retry_sem = None

                if retry_sem and retry_sem.passed:
                    # Success — replace the patch
                    retried.semantic_passed = True
                    idx = applied.index(p)
                    applied[idx] = retried
                    logger.info(
                        f"[{self.query_id}] Semantic retry SUCCESS for {p.patch_id}"
                    )
                    retried_ok = True
                    break
                else:
                    # Update sem_result for next retry with latest errors
                    if retry_sem:
                        sem_result = retry_sem
                    # Update rewrite_sql to the latest attempt
                    p.output_sql = retried.output_sql

            if not retried_ok:
                p.status = "FAIL"
                p.apply_error = f"Semantic (after retries): {'; '.join(sem_result.errors[:2])}"

        sem_passed_count = sum(1 for p in applied if p.semantic_passed)
        logger.info(
            f"[{self.query_id}] Semantic validation: "
            f"{sem_passed_count}/{len(applied)} passed"
        )

        return applied, api_calls

    def _make_llm_call_fn(self, model_spec: Optional[str] = None) -> callable:
        """Create an LLM call function for a specific model.

        Args:
            model_spec: Model identifier string (e.g., "deepseek/deepseek-r1")
                or None to use the pipeline's default.
                The model spec is passed as the model name to the pipeline's
                provider (typically OpenRouter, which accepts "vendor/model").

        Returns:
            Callable that takes prompt string, returns response string.
        """
        from ..generate import CandidateGenerator

        if not model_spec:
            generator = CandidateGenerator(
                provider=self.pipeline.provider,
                model=self.pipeline.model,
                analyze_fn=self.pipeline.analyze_fn,
            )
        else:
            # Pass model_spec as-is to the pipeline's provider (e.g. OpenRouter)
            # OpenRouter accepts "vendor/model" format natively
            generator = CandidateGenerator(
                provider=self.pipeline.provider,
                model=model_spec,
                analyze_fn=self.pipeline.analyze_fn,
            )

        return lambda prompt: self._call_llm_with_timeout(generator, prompt)

    # ── Internal Methods ────────────────────────────────────────────────────

    def _create_session_dir(self) -> Path:
        """Create a session directory for disk persistence."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        session_dir = Path("test_patch_logs") / f"session_{self.query_id}_{timestamp}"
        session_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"[{self.query_id}] Session dir: {session_dir}")
        return session_dir

    def _save_to_disk(self, session_dir: Path, iteration: int, label: str, content: str) -> None:
        """Save content to disk for debugging/audit."""
        filename = f"iter{iteration}_{label}.txt"
        filepath = session_dir / filename
        try:
            filepath.write_text(content, encoding="utf-8")
            logger.debug(f"Saved {filepath} ({len(content)} chars)")
        except Exception as e:
            logger.warning(f"Failed to save {filepath}: {e}")

    def _call_llm_with_timeout(self, generator, prompt: str) -> str:
        """Call LLM with timeout protection."""
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(generator._analyze, prompt)
            try:
                return future.result(timeout=LLM_TIMEOUT_SECONDS)
            except TimeoutError:
                logger.error(
                    f"[{self.query_id}] LLM call timed out after {LLM_TIMEOUT_SECONDS}s"
                )
                return '[]'  # empty JSON array — will trigger retry or skip
            except Exception as e:
                logger.error(f"[{self.query_id}] LLM call failed: {e}")
                return '[]'

    def _parse_and_apply_all(
        self,
        response: str,
        script_ir,
        dialect_enum,
    ) -> tuple[List[AppliedPatch], List[tuple[str, str]]]:
        """Parse LLM response and apply all patch plans.

        Returns:
            (patches, errors) where errors is [(patch_id, error_msg)]
            for patches that failed to parse or apply.
        """
        from ..ir import dict_to_plan, apply_patch_plan
        from ..patches.oneshot_patch_validator import _extract_json_array

        patches_data = _extract_json_array(response)
        if patches_data is None:
            logger.warning("Failed to extract JSON array from LLM response")
            return [], [("all", "Failed to extract JSON array from response")]

        patches: List[AppliedPatch] = []
        errors: List[tuple[str, str]] = []

        for i, patch_data in enumerate(patches_data[:4]):
            if not isinstance(patch_data, dict):
                errors.append((f"patch_{i}", f"Expected dict, got {type(patch_data).__name__}"))
                continue

            patch_id = patch_data.get("plan_id", f"t{i + 1}")
            family = patch_data.get("family", "?")
            transform = patch_data.get("transform", "unknown")
            try:
                relevance = float(patch_data.get("relevance_score", 0.0))
            except (TypeError, ValueError):
                relevance = 0.0

            patch = AppliedPatch(
                patch_id=patch_id,
                family=family,
                transform=transform,
                relevance_score=relevance,
                raw_plan=patch_data,  # preserve for retry context
            )

            try:
                ir_copy = copy.deepcopy(script_ir)
                plan = dict_to_plan(patch_data)
                result = apply_patch_plan(ir_copy, plan)

                if result.success and result.output_sql:
                    patch.output_sql = result.output_sql
                else:
                    error_msg = (
                        "; ".join(result.errors[:2])
                        if result.errors
                        else "Unknown apply error"
                    )
                    patch.apply_error = error_msg
                    patch.status = "FAIL"
                    errors.append((patch_id, error_msg))
            except Exception as e:
                patch.apply_error = str(e)
                patch.status = "FAIL"
                errors.append((patch_id, str(e)))

            patches.append(patch)

        return patches, errors

    def _validate_patches(
        self,
        applied: List[AppliedPatch],
        db_path: str,
    ) -> PatchIterationResult:
        """Validate applied patches: semantic → race → explain."""
        from ..validate import race_candidates
        from ..validation.mini_validator import MiniValidator
        from ..execution.database_utils import run_explain_analyze, run_explain_text

        result = PatchIterationResult(
            iteration=0,
            prompt="",
            response="",
            n_api_calls=0,
            patches=applied,
        )

        # ── Semantic pre-validation (parallel) ───────────────────────
        if self.pipeline.config.semantic_validation_enabled:
            try:
                validator = MiniValidator(
                    db_path=db_path,
                    sample_pct=self.pipeline.config.semantic_sample_pct,
                    timeout_ms=self.pipeline.config.semantic_timeout_ms,
                    dialect=self.dialect,
                )

                def validate_one(patch: AppliedPatch, idx: int) -> bool:
                    try:
                        sem_result = validator.validate_rewrite(
                            self.original_sql, patch.output_sql, worker_id=idx
                        )
                        patch.semantic_passed = sem_result.passed
                        if not sem_result.passed:
                            patch.status = "FAIL"
                            patch.apply_error = "; ".join(sem_result.errors[:2])
                        return sem_result.passed
                    except Exception as e:
                        logger.warning(
                            f"Semantic validation failed for {patch.patch_id}: {e}"
                        )
                        # Don't block on validator errors
                        patch.semantic_passed = True
                        return True

                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = {
                        pool.submit(validate_one, p, i): p
                        for i, p in enumerate(applied)
                    }
                    for future in as_completed(futures):
                        future.result()

            except Exception as e:
                logger.warning(f"Semantic validation setup failed: {e}")
                for p in applied:
                    p.semantic_passed = True
        else:
            # Semantic validation disabled — mark all as passed
            for p in applied:
                p.semantic_passed = True

        sem_passed = [p for p in applied if p.semantic_passed]
        logger.info(
            f"[{self.query_id}] Semantic: {len(sem_passed)}/{len(applied)} passed"
        )

        if not sem_passed:
            return result

        # ── Race validation ──────────────────────────────────────────
        candidate_sqls = [p.output_sql for p in sem_passed]
        worker_ids = list(range(1, len(sem_passed) + 1))

        race_result = race_candidates(
            db_path=db_path,
            original_sql=self.original_sql,
            candidate_sqls=candidate_sqls,
            worker_ids=worker_ids,
            min_runtime_ms=self.pipeline.config.race_min_runtime_ms,
            min_margin=self.pipeline.config.race_min_margin,
        )

        if race_result:
            result.race_result = race_result
            orig_ms = race_result.original.elapsed_ms

            for i, (status, speedup, errs, _cat) in enumerate(race_result.verdicts):
                if i < len(sem_passed):
                    patch = sem_passed[i]
                    patch.speedup = speedup
                    patch.status = self._classify_speedup(speedup)
                    patch.original_ms = orig_ms
                    # Get patch timing from candidate lane
                    if i < len(race_result.candidates):
                        patch.patch_ms = race_result.candidates[i].elapsed_ms
                    if errs:
                        patch.apply_error = "; ".join(errs[:2])
        else:
            # Race returned None (original too fast) — interleaved sequential benchmark
            logger.info(
                f"[{self.query_id}] Race skipped (original too fast), "
                f"using interleaved sequential benchmark"
            )
            self._sequential_benchmark(sem_passed, db_path)

        # ── Find best (only >= 1.0 counts) ────────────────────────────
        candidates_with_speedup = [
            p for p in sem_passed if p.speedup is not None and p.speedup >= 1.0
        ]
        if candidates_with_speedup:
            best_patch = max(candidates_with_speedup, key=lambda p: p.speedup)
            result.best_speedup = best_patch.speedup or 0.0
            result.best_patch_id = best_patch.patch_id
            result.best_sql = best_patch.output_sql

        # ── EXPLAIN collection — ALL patches with output_sql ──────────
        explain_count = sum(1 for p in sem_passed if p.output_sql)
        logger.info(
            f"[{self.query_id}] Collecting EXPLAIN for {explain_count} patches"
        )
        for idx, p in enumerate(sem_passed):
            if p.output_sql:
                logger.info(
                    f"[{self.query_id}]   EXPLAIN {idx + 1}/{explain_count}: {p.patch_id}"
                )
                try:
                    explain_data = run_explain_analyze(db_path, p.output_sql)
                    if explain_data:
                        p.explain_text = explain_data.get("plan_text", "")
                        result.explains[p.patch_id] = p.explain_text
                        logger.info(
                            f"[{self.query_id}]   {p.patch_id}: "
                            f"EXPLAIN ANALYZE OK ({len(p.explain_text)} chars)"
                        )
                    else:
                        text = run_explain_text(db_path, p.output_sql)
                        if text:
                            p.explain_text = text
                            result.explains[p.patch_id] = text
                            logger.info(
                                f"[{self.query_id}]   {p.patch_id}: "
                                f"EXPLAIN TEXT OK ({len(text)} chars)"
                            )
                        else:
                            logger.info(
                                f"[{self.query_id}]   {p.patch_id}: no EXPLAIN available"
                            )
                except Exception as e:
                    logger.warning(f"EXPLAIN failed for {p.patch_id}: {e}")

        return result

    def _sequential_benchmark(
        self, patches: List[AppliedPatch], db_path: str
    ) -> None:
        """5x trimmed-mean sequential benchmark when race is skipped.

        Each query (original + each patch) is executed 5 times.
        Min and max are dropped, middle 3 are averaged.
        Correctness gate: row count + checksum must match original.
        """
        from ..execution.factory import create_executor_from_dsn
        from ..validate import _timed_runs_pg
        from ..validation.equivalence_checker import EquivalenceChecker

        BENCH_RUNS = 3
        checker = EquivalenceChecker()

        logger.info(
            f"[{self.query_id}] Sequential benchmark: "
            f"{len(patches)} patches, {BENCH_RUNS}x (warmup + avg last 2)"
        )

        try:
            with create_executor_from_dsn(db_path) as executor:
                # Measure original (5x trimmed mean) + capture rows for correctness
                logger.info(
                    f"[{self.query_id}] Baseline: {BENCH_RUNS}x trimmed mean..."
                )
                orig_ms, orig_rows, orig_times = _timed_runs_pg(
                    executor, self.original_sql, runs=BENCH_RUNS,
                    capture_rows=True,
                )
                orig_count = len(orig_rows) if orig_rows else 0
                orig_checksum = None
                if orig_rows:
                    try:
                        orig_checksum = checker.compute_checksum(orig_rows)
                    except Exception:
                        pass
                logger.info(
                    f"[{self.query_id}] Baseline: {orig_ms:.1f}ms "
                    f"({orig_count} rows, checksum={orig_checksum}) "
                    f"[{', '.join(f'{t:.0f}' for t in orig_times)}]"
                )

                for idx, p in enumerate(patches):
                    logger.info(
                        f"[{self.query_id}] Benchmark {idx + 1}/{len(patches)}: "
                        f"{p.patch_id} ({p.family}/{p.transform})"
                    )
                    try:
                        patch_ms, patch_rows, patch_times = _timed_runs_pg(
                            executor, p.output_sql, runs=BENCH_RUNS,
                            capture_rows=True,
                        )
                        patch_count = len(patch_rows) if patch_rows else 0

                        # ── Correctness gate: row count + checksum ──
                        if patch_count != orig_count:
                            p.speedup = 0.0
                            p.status = "FAIL"
                            p.apply_error = (
                                f"Row count mismatch: original={orig_count}, "
                                f"patch={patch_count}"
                            )
                            logger.warning(
                                f"[{self.query_id}]   FAIL: {p.patch_id}: "
                                f"{p.apply_error}"
                            )
                            continue

                        if orig_checksum and patch_rows:
                            try:
                                patch_checksum = checker.compute_checksum(patch_rows)
                                if patch_checksum != orig_checksum:
                                    p.speedup = 0.0
                                    p.status = "FAIL"
                                    p.apply_error = (
                                        f"Checksum mismatch: original={orig_checksum}, "
                                        f"patch={patch_checksum}"
                                    )
                                    logger.warning(
                                        f"[{self.query_id}]   FAIL: {p.patch_id}: "
                                        f"{p.apply_error}"
                                    )
                                    continue
                            except Exception:
                                pass  # checksum compute failed — don't block

                        p.original_ms = orig_ms
                        p.patch_ms = patch_ms
                        p.speedup = orig_ms / patch_ms if patch_ms > 0 else 1.0
                        p.status = self._classify_speedup(p.speedup)

                        logger.info(
                            f"[{self.query_id}]   result: orig={orig_ms:.1f}ms, "
                            f"patch={patch_ms:.1f}ms, speedup={p.speedup:.2f}x "
                            f"({p.status}, {patch_count} rows) "
                            f"[{', '.join(f'{t:.0f}' for t in patch_times)}]"
                        )
                    except Exception as e:
                        p.speedup = 0.0
                        p.status = "ERROR"
                        p.apply_error = str(e)
                        logger.warning(
                            f"[{self.query_id}]   ERROR: {p.patch_id}: {e}"
                        )
        except Exception as e:
            logger.warning(f"Sequential benchmark failed: {e}")

    def _serialize_iteration(self, it: PatchIterationResult) -> dict:
        """Serialize iteration result for SessionResult.iterations."""
        return {
            "iteration": it.iteration,
            "n_api_calls": it.n_api_calls,
            "best_speedup": round(it.best_speedup, 2),
            "best_patch_id": it.best_patch_id,
            "patches": [
                {
                    "patch_id": p.patch_id,
                    "family": p.family,
                    "transform": p.transform,
                    "relevance_score": p.relevance_score,
                    "status": p.status,
                    "speedup": round(p.speedup, 2) if p.speedup is not None else None,
                    "semantic_passed": p.semantic_passed,
                    "error": p.apply_error,
                    "original_ms": round(p.original_ms, 1) if p.original_ms is not None else None,
                    "patch_ms": round(p.patch_ms, 1) if p.patch_ms is not None else None,
                }
                for p in it.patches
            ],
        }
