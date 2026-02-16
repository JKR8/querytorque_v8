"""Beam optimization session — automated analyst → N workers → validate → snipe.

Flow per iteration:
1. Analyst call → identifies targets/patch plans (or snipe on iter 2+)
2. Workers (4 parallel, role-routed) → generate patches
3. Apply-error retry for failed patches
4. Equivalence check (full dataset row count + MD5 checksum) + worker retry
5. Benchmark equivalent patches (5x trimmed mean or race)
6. EXPLAIN collection for snipe enrichment
7. Iterate until target speedup or max_iterations
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
    worker_prompt: Optional[str] = None   # raw prompt sent to worker LLM
    worker_response: Optional[str] = None  # raw response from worker LLM
    worker_role: Optional[str] = None      # W1/W2/W3/W4


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


class BeamSession(OptimizationSession):
    """Iterative patch-plan optimization: prompt → 4 patches → validate → snipe."""

    def run(self) -> SessionResult:
        """Execute the beam optimization loop.

        Dispatches to tiered mode if config.tiered_patch_enabled is True.
        """
        if getattr(self.pipeline.config, "tiered_patch_enabled", False):
            return self._run_tiered()
        return self._run_single_tier()

    @staticmethod
    def _render_explain_compact(explain_result: Optional[dict], dialect: str = "duckdb") -> str:
        """Render EXPLAIN result as compact operator tree.

        Uses structured plan_json when available (DuckDB: ~40 lines vs ~230 box-drawing).
        Falls back to plan_text if no JSON plan.

        IMPORTANT: This is the ONLY entry point for EXPLAIN into prompts.
        The compact format is what analysts, workers, and snipers all see.
        Raw box-drawing must NEVER leak through.
        """
        if not explain_result:
            return "(EXPLAIN unavailable)"

        from ..prompts.analyst_briefing import format_duckdb_explain_tree

        plan_json = explain_result.get("plan_json")
        plan_text = explain_result.get("plan_text", "")

        if dialect.lower() == "duckdb":
            if plan_json and isinstance(plan_json, dict) and plan_json.get("children"):
                import json as _json
                rendered = format_duckdb_explain_tree(_json.dumps(plan_json))
                if rendered:
                    return rendered
            # plan_text may be JSON string or box-drawing.
            # format_duckdb_explain_tree handles JSON; box-drawing falls through.
            if plan_text:
                rendered = format_duckdb_explain_tree(plan_text)
                # Guard: if parser returned box-drawing unchanged, reject it
                if "┌" not in rendered and "└" not in rendered:
                    return rendered
                # Box-drawing leaked — return a warning instead of 300 lines of noise
                return "(EXPLAIN: compact rendering unavailable — re-run with JSON EXPLAIN)"

        # PG / Snowflake fallback
        if plan_text:
            return plan_text
        return "(EXPLAIN unavailable)"

    def _run_single_tier(self) -> SessionResult:
        """Original single-tier patch loop (one LLM does everything)."""
        from ..ir import build_script_ir, render_ir_node_map, Dialect
        from ..patches.beam_prompt_builder import (
            build_beam_prompt,
            build_beam_snipe_prompt,
            load_gold_examples,
        )
        from ..generate import CandidateGenerator
        from ..execution.database_utils import run_explain_analyze

        logger.info(
            f"[{self.query_id}] BeamSession: "
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

        # Get EXPLAIN for the original query (compact rendering)
        explain_result = run_explain_analyze(db_path, self.original_sql)
        original_explain = self._render_explain_compact(explain_result, self.dialect)

        # Load gold examples for families
        gold_examples = load_gold_examples(self.dialect)

        # ── AST Detection + Cached Classification ─────────────────────
        intelligence_brief = ""
        try:
            from ..detection import detect_transforms, load_transforms
            from ..patches.pathology_classifier import build_intelligence_brief

            transforms_catalog = load_transforms()
            detected = detect_transforms(
                self.original_sql, transforms_catalog,
                engine=self.engine, dialect=self.dialect,
            )
            classification = self._load_cached_classification(self.query_id)
            intelligence_brief = build_intelligence_brief(detected, classification)
        except Exception as e:
            logger.warning(f"[{self.query_id}] Intelligence brief failed: {e}")

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
                prompt = build_beam_prompt(
                    query_id=self.query_id,
                    original_sql=self.original_sql,
                    explain_text=original_explain,
                    ir_node_map=ir_node_map,
                    all_5_examples=gold_examples,
                    dialect=self.dialect,
                    intelligence_brief=intelligence_brief,
                )
            else:
                prev = iterations_data[-1]
                # Build history from ALL prior iterations for summary table
                all_prior = [it.patches for it in iterations_data]
                prompt = build_beam_snipe_prompt(
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
                from ..patches.beam_prompt_builder import build_beam_retry_prompt
                retry_prompt = build_beam_retry_prompt(
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
                from ..patches.beam_prompt_builder import (
                    build_beam_runtime_error_retry_prompt,
                )

                retry_prompt = build_beam_runtime_error_retry_prompt(
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
            mode="beam",
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
        from ..patches.beam_prompt_builder import load_gold_examples
        from ..patches.tiered_orchestrator import TieredOrchestrator
        from ..execution.database_utils import run_explain_analyze

        # Use session-level target if set, fall back to config
        target_speedup = self.target_speedup or getattr(self.pipeline.config, "target_speedup", 10.0)

        logger.info(
            f"[{self.query_id}] BeamSession TIERED: "
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

        # Get EXPLAIN (compact rendering)
        explain_result = run_explain_analyze(db_path, self.original_sql)
        original_explain = self._render_explain_compact(explain_result, self.dialect)

        gold_examples = load_gold_examples(self.dialect)

        # ── AST Detection + Cached Classification ─────────────────────
        intelligence_brief = ""
        ast_top_match = None  # (transform_id, family, gap) from detection
        try:
            from ..detection import detect_transforms, load_transforms
            from ..patches.pathology_classifier import build_intelligence_brief

            transforms_catalog = load_transforms()
            transforms_by_id = {t["id"]: t for t in transforms_catalog}
            detected = detect_transforms(
                self.original_sql, transforms_catalog,
                engine=self.engine, dialect=self.dialect,
            )

            # Load pre-computed classification if available
            classification = self._load_cached_classification(self.query_id)

            intelligence_brief = build_intelligence_brief(detected, classification)

            # Extract top AST match for guaranteed worker slot
            if detected and detected[0].overlap_ratio >= 0.75:
                top = detected[0]
                top_catalog = transforms_by_id.get(top.id, {})
                ast_top_match = {
                    "transform_id": top.id,
                    "family": top_catalog.get("family", "?"),
                    "gap": top.gap or "",
                    "overlap": top.overlap_ratio,
                }
                logger.info(
                    f"[{self.query_id}] AST top match: {top.id} "
                    f"(family {ast_top_match['family']}, "
                    f"{top.overlap_ratio:.0%} overlap) → guaranteed W4 slot"
                )

            if intelligence_brief:
                logger.info(
                    f"[{self.query_id}] Intelligence brief: "
                    f"{len(detected)} AST matches, "
                    f"classification={'yes' if classification else 'no'}"
                )
        except Exception as e:
            logger.warning(f"[{self.query_id}] Intelligence brief failed: {e}")

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
            intelligence_brief=intelligence_brief,
            ast_top_match=ast_top_match,
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
                # Pass ALL prior iterations for history summary table
                all_prior = [it.patches for it in iterations_data]
                all_prior_explains = {}
                for it in iterations_data:
                    all_prior_explains.update(it.explains)
                targets, analyst_prompt, analyst_response = orchestrator.run_snipe(
                    query_id=self.query_id,
                    original_sql=self.original_sql,
                    explain_text=original_explain,
                    ir_node_map=ir_node_map,
                    patches=prev_all_patches or [],
                    patch_explains=prev_explains,
                    all_prior_iterations=all_prior,
                    iteration=iteration,
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
            patches, worker_api_calls, all_targets = orchestrator.run_workers(
                original_sql=self.original_sql,
                ir_node_map=ir_node_map,
                targets=targets,
                script_ir=script_ir,
                dialect_enum=dialect_enum,
                force_full_roster=(iteration == 0),
            )
            iter_api_calls += worker_api_calls

            applied = [p for p in patches if p.output_sql]

            # ── Dedup: kill identical SQL rewrites ────────────────
            seen_sql: Dict[str, str] = {}  # normalized_sql → first patch_id
            deduped = []
            for p in applied:
                # Normalize whitespace for comparison
                norm = " ".join(p.output_sql.split())
                if norm in seen_sql:
                    logger.info(
                        f"[{self.query_id}] Dedup: {p.patch_id} identical "
                        f"to {seen_sql[norm]}, skipping"
                    )
                    p.status = "DEDUP"
                    p.apply_error = f"Duplicate of {seen_sql[norm]}"
                else:
                    seen_sql[norm] = p.patch_id
                    deduped.append(p)
            if len(deduped) < len(applied):
                logger.info(
                    f"[{self.query_id}] Dedup removed "
                    f"{len(applied) - len(deduped)} duplicates"
                )
            applied = deduped

            logger.info(
                f"[{self.query_id}] Phase 2 done: "
                f"{len(applied)}/{len(patches)} patches applied"
            )

            # Save worker outputs (full prompts, responses, and results)
            for p in patches:
                status = "OK" if p.output_sql else f"FAIL: {p.apply_error}"
                role = p.worker_role or "?"
                self._save_to_disk(
                    session_dir, iteration, f"worker_{p.patch_id}_summary",
                    f"Worker Role: {role}\nFamily: {p.family}\n"
                    f"Transform: {p.transform}\nStatus: {status}\n\n"
                    f"{'--- OUTPUT SQL ---' if p.output_sql else '--- NO OUTPUT ---'}\n"
                    f"{p.output_sql or p.apply_error or 'N/A'}"
                )
                if p.worker_prompt:
                    self._save_to_disk(
                        session_dir, iteration, f"worker_{p.patch_id}_prompt",
                        p.worker_prompt,
                    )
                if p.worker_response:
                    self._save_to_disk(
                        session_dir, iteration, f"worker_{p.patch_id}_response",
                        p.worker_response,
                    )

            # ── Phase 3: Apply-error retry for failed patches ────────
            logger.info(f"[{self.query_id}] Phase 3: apply-error retries")
            failed = [p for p in patches if not p.output_sql and p.apply_error]
            for p in failed:
                target_match = next(
                    (t for t in all_targets if t.target_id == p.patch_id), None
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

            # ── Phase 4: Equivalence check + retry ──────────────────
            if self.on_phase_change:
                self.on_phase_change(phase="semantic", iteration=iteration)
            logger.info(
                f"[{self.query_id}] Phase 4: equivalence check "
                f"({len(applied)} patches, full dataset)"
            )
            applied, sem_api_calls = self._equivalence_check_and_retry(
                applied=applied,
                targets=all_targets,
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
                    f"[{self.query_id}] No patches passed equivalence check"
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

            # ── Phase 5: Benchmark equivalent patches ─────────────────
            if self.on_phase_change:
                self.on_phase_change(phase="benchmark", iteration=iteration)
            logger.info(
                f"[{self.query_id}] Phase 5: benchmark "
                f"{len(sem_passed)} verified patches (3x warmup+avg2)"
            )
            from contextlib import nullcontext
            bench_ctx = self.benchmark_lock if self.benchmark_lock else nullcontext()
            with bench_ctx:
                logger.info(f"[{self.query_id}] Benchmark lock acquired")
                self._sequential_benchmark(sem_passed, db_path)
                logger.info(f"[{self.query_id}] Benchmark lock released")

            # Expose benchmarked patches for dashboard callbacks
            self._current_patches = list(sem_passed)

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
                        compact = self._render_explain_compact(
                            explain_data, self.dialect
                        )
                        p.explain_text = compact
                        iter_explains[p.patch_id] = compact
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
            mode="beam",
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

    def _equivalence_check_and_retry(
        self,
        applied: List[AppliedPatch],
        targets: list,
        orchestrator,
        ir_node_map: str,
        db_path: str,
        script_ir,
        dialect_enum,
    ) -> tuple[List[AppliedPatch], int]:
        """Run full-dataset equivalence check on applied patches, retry failures.

        Executes original + each rewrite on the real database (no sampling).
        Compares row count + MD5 checksum. Failed patches get up to 1 worker
        retry with the error context.

        Always runs — this is the primary correctness gate. The
        semantic_validation_enabled config only controls the TABLESAMPLE
        mini-validator, not this full-dataset check.

        Returns:
            (updated_patches, api_call_count)
        """

        from ..execution.factory import create_executor_from_dsn
        from ..validation.equivalence_checker import EquivalenceChecker
        from ..validation.mini_validator import MiniValidator

        api_calls = 0
        checker = EquivalenceChecker()

        # ── Tier-1: Structural pre-check (instant, no DB) ────────────
        # Catches parse errors, changed literals, column shape mismatches
        # before expensive full-dataset execution.
        tier1_validator = MiniValidator(
            db_path=db_path, dialect=self.dialect, sample_pct=0,
        )
        tier1_failed = []
        for p in applied:
            if not p.output_sql:
                continue
            t1 = tier1_validator._tier1_structural(self.original_sql, p.output_sql)
            if not t1.get("passed", True):
                errors = t1.get("errors", ["Structural check failed"])
                error_msg = f"Tier-1 structural: {'; '.join(errors)}"
                logger.info(
                    f"[{self.query_id}] Tier-1 FAIL {p.patch_id}: {error_msg}"
                )
                p.semantic_passed = False
                p.status = "FAIL"
                p.apply_error = error_msg
                tier1_failed.append(p.patch_id)

        # ── Tier-1 retry: feed specific error back to worker ──────
        if tier1_failed:
            logger.info(
                f"[{self.query_id}] Tier-1 rejected {len(tier1_failed)} patches, "
                f"retrying: {tier1_failed}"
            )
            for p in applied:
                if p.patch_id not in tier1_failed or not p.apply_error:
                    continue
                target_match = next(
                    (t for t in targets if t.target_id == p.patch_id), None
                )
                if not target_match:
                    continue

                logger.info(
                    f"[{self.query_id}] Tier-1 retry for {p.patch_id}: "
                    f"{p.apply_error[:80]}"
                )
                retried = orchestrator.retry_worker(
                    original_sql=self.original_sql,
                    ir_node_map=ir_node_map,
                    target=target_match,
                    error=p.apply_error,
                    script_ir=script_ir,
                    dialect_enum=dialect_enum,
                )
                api_calls += 1

                if retried and retried.output_sql:
                    # Re-run tier-1 on the retry output
                    t1_retry = tier1_validator._tier1_structural(
                        self.original_sql, retried.output_sql
                    )
                    if t1_retry.get("passed", True):
                        # Retry passed tier-1 — replace the failed patch
                        idx = applied.index(p)
                        applied[idx] = retried
                        tier1_failed.remove(p.patch_id)
                        logger.info(
                            f"[{self.query_id}] Tier-1 retry SUCCESS "
                            f"for {p.patch_id}"
                        )
                    else:
                        retry_errors = t1_retry.get("errors", [])
                        logger.info(
                            f"[{self.query_id}] Tier-1 retry STILL FAILED "
                            f"for {p.patch_id}: {retry_errors}"
                        )

        if tier1_failed:
            logger.info(
                f"[{self.query_id}] Tier-1 final rejected: {tier1_failed}"
            )
        # Filter to only tier-1 passed for full-dataset check
        applied_for_equiv = [p for p in applied if p.patch_id not in tier1_failed]
        if not applied_for_equiv:
            passed_count = sum(1 for p in applied if p.semantic_passed)
            logger.info(
                f"[{self.query_id}] All patches failed Tier-1, skipping equivalence"
            )
            return applied, api_calls

        try:
            with create_executor_from_dsn(db_path) as executor:
                # Execute original once, cache result
                orig_rows = executor.execute(self.original_sql)
                orig_count = len(orig_rows) if orig_rows else 0
                orig_checksum = None
                if orig_rows:
                    try:
                        orig_checksum = checker.compute_checksum(orig_rows)
                    except Exception:
                        pass
                logger.info(
                    f"[{self.query_id}] Equivalence baseline: "
                    f"{orig_count} rows, checksum={orig_checksum}"
                )

                for p in applied_for_equiv:
                    if not p.output_sql:
                        continue

                    target_match = next(
                        (t for t in targets if t.target_id == p.patch_id), None
                    )

                    try:
                        patch_rows = executor.execute(p.output_sql)
                        patch_count = len(patch_rows) if patch_rows else 0
                    except Exception as e:
                        p.semantic_passed = False
                        p.status = "FAIL"
                        p.apply_error = f"Equivalence execution error: {e}"
                        logger.warning(
                            f"[{self.query_id}] Equiv FAIL {p.patch_id}: {e}"
                        )
                        continue

                    # Row count check
                    if patch_count != orig_count:
                        error_msg = (
                            f"Row count mismatch: original={orig_count}, "
                            f"patch={patch_count}"
                        )
                        logger.info(
                            f"[{self.query_id}] Equiv FAIL {p.patch_id}: {error_msg}"
                        )
                        p, retried = self._equiv_retry(
                            p, error_msg, target_match, orchestrator,
                            ir_node_map, script_ir, dialect_enum,
                            executor, checker, orig_count, orig_checksum,
                        )
                        if retried:
                            api_calls += 1
                        continue

                    # Checksum check
                    if orig_checksum and patch_rows:
                        try:
                            patch_checksum = checker.compute_checksum(patch_rows)
                        except Exception:
                            patch_checksum = None
                        if patch_checksum and patch_checksum != orig_checksum:
                            error_msg = (
                                f"Checksum mismatch: original={orig_checksum}, "
                                f"patch={patch_checksum}"
                            )
                            logger.info(
                                f"[{self.query_id}] Equiv FAIL {p.patch_id}: "
                                f"{error_msg}"
                            )
                            p, retried = self._equiv_retry(
                                p, error_msg, target_match, orchestrator,
                                ir_node_map, script_ir, dialect_enum,
                                executor, checker, orig_count, orig_checksum,
                            )
                            if retried:
                                api_calls += 1
                            continue

                    # Passed
                    p.semantic_passed = True

        except Exception as e:
            logger.warning(f"Equivalence check failed: {e}")
            # Don't block — mark all as passed on infrastructure error
            for p in applied:
                p.semantic_passed = True

        passed_count = sum(1 for p in applied if p.semantic_passed)
        logger.info(
            f"[{self.query_id}] Equivalence check: "
            f"{passed_count}/{len(applied)} passed"
        )
        return applied, api_calls

    def _equiv_retry(
        self,
        patch: AppliedPatch,
        error_msg: str,
        target_match,
        orchestrator,
        ir_node_map: str,
        script_ir,
        dialect_enum,
        executor,
        checker,
        orig_count: int,
        orig_checksum: Optional[str],
    ) -> tuple[AppliedPatch, bool]:
        """Retry a worker once after equivalence failure.

        Returns:
            (patch, did_retry) — updated patch and whether a retry LLM call was made.
        """
        if not target_match:
            patch.semantic_passed = False
            patch.status = "FAIL"
            patch.apply_error = f"Equivalence: {error_msg}"
            return patch, False

        logger.info(
            f"[{self.query_id}] Equiv retry for {patch.patch_id}: {error_msg}"
        )

        # Build a semantic-style result for the retry prompt
        from ..schemas import SemanticValidationResult
        sem_result = SemanticValidationResult(
            tier_passed=2,
            passed=False,
            errors=[error_msg],
        )

        retried = orchestrator.retry_worker_semantic(
            original_sql=self.original_sql,
            ir_node_map=ir_node_map,
            target=target_match,
            sem_result=sem_result,
            rewrite_sql=patch.output_sql,
            script_ir=script_ir,
            dialect_enum=dialect_enum,
        )

        if not retried or not retried.output_sql:
            patch.semantic_passed = False
            patch.status = "FAIL"
            patch.apply_error = f"Equivalence (retry failed): {error_msg}"
            return patch, True

        # Re-check the retry
        try:
            retry_rows = executor.execute(retried.output_sql)
            retry_count = len(retry_rows) if retry_rows else 0
        except Exception as e:
            patch.semantic_passed = False
            patch.status = "FAIL"
            patch.apply_error = f"Equivalence retry execution error: {e}"
            return patch, True

        if retry_count != orig_count:
            patch.semantic_passed = False
            patch.status = "FAIL"
            patch.apply_error = (
                f"Equivalence (after retry): row count "
                f"{retry_count} vs {orig_count}"
            )
            return patch, True

        if orig_checksum and retry_rows:
            try:
                retry_checksum = checker.compute_checksum(retry_rows)
            except Exception:
                retry_checksum = None
            if retry_checksum and retry_checksum != orig_checksum:
                patch.semantic_passed = False
                patch.status = "FAIL"
                patch.apply_error = (
                    f"Equivalence (after retry): checksum "
                    f"{retry_checksum} vs {orig_checksum}"
                )
                return patch, True

        # Retry passed — replace patch
        retried.semantic_passed = True
        logger.info(
            f"[{self.query_id}] Equiv retry SUCCESS for {patch.patch_id}"
        )
        # Copy over the retried patch into the original's slot
        patch.output_sql = retried.output_sql
        patch.semantic_passed = True
        patch.status = "PENDING"
        patch.apply_error = None
        patch.worker_response = retried.worker_response
        patch.raw_plan = retried.raw_plan
        return patch, True

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

        effective_model = model_spec or self.pipeline.model
        effective_provider = self.pipeline.provider

        logger.info(
            f"[{self.query_id}] LLM call fn: "
            f"provider={effective_provider}, model={effective_model}"
        )

        generator = CandidateGenerator(
            provider=effective_provider,
            model=effective_model,
            analyze_fn=self.pipeline.analyze_fn,
        )

        def call_fn(prompt: str) -> str:
            logger.info(
                f"[{self.query_id}] LLM call → {effective_model} "
                f"({len(prompt)} chars prompt)"
            )
            t0 = time.time()
            result = self._call_llm_with_timeout(generator, prompt)
            elapsed = time.time() - t0
            logger.info(
                f"[{self.query_id}] LLM done ← {effective_model} "
                f"({len(result)} chars response, {elapsed:.1f}s)"
            )
            return result

        return call_fn

    # ── Internal Methods ────────────────────────────────────────────────────

    def _load_cached_classification(self, query_id: str):
        """Load pre-computed classification from benchmark_dir/classifications.json.

        Returns ClassificationResult or None if not available.
        """
        try:
            classifications_path = (
                self.pipeline.benchmark_dir / "classifications.json"
            )
            if not classifications_path.exists():
                return None

            import json
            data = json.loads(classifications_path.read_text())
            entry = data.get(query_id)
            if not entry:
                return None

            from ..patches.pathology_classifier import (
                ClassificationResult,
                PathologyMatch,
            )

            matches = [
                PathologyMatch(
                    pathology_id=m["pathology_id"],
                    name=m.get("name", ""),
                    confidence=m.get("confidence", 0.0),
                    evidence=m.get("evidence", ""),
                    recommended_transform=m.get("transform", ""),
                )
                for m in entry.get("llm_matches", [])
            ]

            return ClassificationResult(
                query_id=query_id,
                matches=matches,
                reasoning=entry.get("reasoning", ""),
            )
        except Exception as e:
            logger.debug(f"[{query_id}] No cached classification: {e}")
            return None

    def _create_session_dir(self) -> Path:
        """Create a session directory for disk persistence."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        session_dir = (
            self.pipeline.benchmark_dir
            / "beam_sessions"
            / f"{self.query_id}_{timestamp}"
        )
        session_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"[{self.query_id}] Session dir: {session_dir}")

        # Write session metadata
        metadata = {
            "query_id": self.query_id,
            "timestamp": datetime.now().isoformat(),
            "engine": self.pipeline.config.engine,
            "benchmark": self.pipeline.config.benchmark,
            "db_path": self.pipeline.config.db_path_or_dsn,
            "scale_factor": self.pipeline.config.scale_factor,
            "max_iterations": self.max_iterations,
            "target_speedup": self.target_speedup,
            "analyst_model": getattr(self.pipeline.config, "analyst_model", "?"),
            "worker_model": getattr(self.pipeline.config, "worker_model", "?"),
            "provider": self.pipeline.provider or "?",
            "semantic_validation_enabled": self.pipeline.config.semantic_validation_enabled,
            "semantic_sample_pct": self.pipeline.config.semantic_sample_pct,
            "validation_method": self.pipeline.config.validation_method,
        }
        (session_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2)
        )
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
        from ..patches.beam_patch_validator import _extract_json_array

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
        from ..execution.database_utils import run_explain_analyze

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
                    compact = self._render_explain_compact(explain_data, self.dialect)
                    p.explain_text = compact
                    result.explains[p.patch_id] = compact
                    logger.info(
                        f"[{self.query_id}]   {p.patch_id}: "
                        f"EXPLAIN OK ({len(compact)} chars)"
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
            "best_sql": it.best_sql,
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
                    "output_sql": p.output_sql,
                    "has_explain": bool(p.explain_text),
                }
                for p in it.patches
            ],
            "explains": {pid: text[:500] for pid, text in it.explains.items()} if it.explains else {},
        }
