"""Tiered orchestrator: Analyst (DeepSeek) → Worker (qwen) patch pipeline.

DeepSeek outputs structural targets (IR node maps).
qwen converts each target into an executable PatchPlan JSON.
Patch engine applies plans to IR. Unchanged validation downstream.
"""

from __future__ import annotations

import copy
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── Worker Roles & Routing ────────────────────────────────────────────────

WORKER_ROLES: Dict[str, Dict[str, Any]] = {
    "W1": {
        "key": "W1",
        "name": "Reducer",
        "families": ["A", "D"],
        "focus": "Cardinality reduction — WHERE filters, set operations, early pruning",
        "description": (
            "Reduce row counts early: push predicates into CTEs, convert set "
            "operations to EXISTS/NOT EXISTS, apply early filtering before "
            "expensive joins."
        ),
    },
    "W2": {
        "key": "W2",
        "name": "Unnester",
        "families": ["B", "C"],
        "focus": "Logic simplification — decorrelation, aggregation pushdown",
        "description": (
            "Eliminate per-row re-execution: convert correlated subqueries to "
            "GROUP BY CTEs, push aggregation before joins when GROUP BY keys "
            "⊇ join keys."
        ),
    },
    "W3": {
        "key": "W3",
        "name": "Builder",
        "families": ["F", "E"],
        "focus": "Structural optimization — join restructuring, materialization",
        "description": (
            "Restructure join topology and materialize repeated work: convert "
            "comma joins to explicit INNER JOIN, extract shared scans into "
            "CTEs, prefetch dimension tables."
        ),
    },
    "W4": {
        "key": "W4",
        "name": "Wildcard",
        "families": [],  # Dynamic — assigned the #1 priority target
        "focus": "Deep specialist for the #1 identified problem",
        "description": (
            "Focus entirely on the highest-priority optimization target. May "
            "combine strategies from multiple families or try novel approaches "
            "not covered by other workers."
        ),
    },
}

FAMILY_TO_WORKER: Dict[str, str] = {
    "A": "W1",
    "D": "W1",
    "B": "W2",
    "C": "W2",
    "F": "W3",
    "E": "W3",
}


@dataclass
class AnalystTarget:
    """A structural optimization target from the analyst (DeepSeek)."""

    target_id: str
    family: str
    transform: str
    relevance_score: float
    hypothesis: str
    target_ir: str
    recommended_examples: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target_id": self.target_id,
            "family": self.family,
            "transform": self.transform,
            "relevance_score": self.relevance_score,
            "hypothesis": self.hypothesis,
            "target_ir": self.target_ir,
            "recommended_examples": self.recommended_examples,
        }


class TieredOrchestrator:
    """Coordinate analyst → worker → patch engine pipeline.

    Args:
        analyst_call_fn: Callable that takes a prompt string, returns LLM response string.
        worker_call_fn: Callable that takes a prompt string, returns LLM response string.
        gold_examples: Dict mapping family ID (A-F) to gold example JSON.
        dialect: SQL dialect string (duckdb, postgres, snowflake).
    """

    def __init__(
        self,
        analyst_call_fn: Callable[[str], str],
        worker_call_fn: Callable[[str], str],
        gold_examples: Dict[str, Dict[str, Any]],
        dialect: str,
        intelligence_brief: str = "",
        ast_top_match: Optional[Dict[str, Any]] = None,
    ):
        self.analyst_call_fn = analyst_call_fn
        self.worker_call_fn = worker_call_fn
        self.gold_examples = gold_examples
        self.dialect = dialect
        self.intelligence_brief = intelligence_brief
        self.ast_top_match = ast_top_match  # {transform_id, family, gap, overlap}

        # Build lookup: example_id → example JSON (for recommended_examples matching)
        self._example_by_id: Dict[str, Dict[str, Any]] = {}
        for ex in gold_examples.values():
            eid = ex.get("id", "")
            if eid:
                self._example_by_id[eid] = ex

    def run_analyst(
        self,
        query_id: str,
        original_sql: str,
        explain_text: str,
        ir_node_map: str,
    ) -> Tuple[List[AnalystTarget], str, str]:
        """Build tiered analyst prompt, call DeepSeek, parse response into targets.

        Returns:
            (targets, prompt, response) — parsed targets, raw prompt, raw response.
        """
        from .beam_prompt_builder import build_beam_prompt_tiered

        prompt = build_beam_prompt_tiered(
            query_id=query_id,
            original_sql=original_sql,
            explain_text=explain_text,
            ir_node_map=ir_node_map,
            all_5_examples=self.gold_examples,
            dialect=self.dialect,
            intelligence_brief=self.intelligence_brief,
        )

        logger.info(
            f"[{query_id}] Tiered analyst prompt: {len(prompt)} chars"
        )

        response = self.analyst_call_fn(prompt)
        logger.info(
            f"[{query_id}] Tiered analyst response: {len(response)} chars"
        )

        targets = self._parse_analyst_response(response)
        logger.info(
            f"[{query_id}] Parsed {len(targets)} analyst targets"
        )

        return targets, prompt, response

    def run_workers(
        self,
        original_sql: str,
        ir_node_map: str,
        targets: List[AnalystTarget],
        script_ir: Any,
        dialect_enum: Any,
        force_full_roster: bool = False,
    ) -> Tuple[List["AppliedPatch"], int, List[AnalystTarget]]:
        """Call workers in parallel (one per target). Apply each patch.

        Workers are routed to specialized roles based on target family:
        - W4 "Wildcard": always gets the #1 target (highest relevance)
        - W1 "Reducer" (A/D), W2 "Unnester" (B/C), W3 "Builder" (F/E)

        Args:
            force_full_roster: If True, create synthetic targets to fill
                all 4 worker slots when analyst produces fewer than 4.

        Returns:
            (patches, n_api_calls, all_targets) — applied patches, LLM call
            count, and all assigned targets (including synthetic/AST).
        """
        from .beam_prompt_builder import build_worker_patch_prompt
        from ..sessions.beam_session import AppliedPatch
        import threading

        results: List[AppliedPatch] = []
        api_call_count = [0]  # mutable for thread-safe increment
        api_lock = threading.Lock()

        assignments = self._assign_workers(targets, force_full_roster=force_full_roster)

        def process_assignment(
            target: AnalystTarget, worker_role: Dict[str, Any]
        ) -> AppliedPatch:
            patch = AppliedPatch(
                patch_id=target.target_id,
                family=target.family,
                transform=target.transform,
                relevance_score=target.relevance_score,
            )

            # Find gold patch plan(s) from recommended examples
            gold_patches = self._find_all_gold_patches(target)
            if not gold_patches:
                patch.apply_error = (
                    f"No gold patch plan found for examples: "
                    f"{target.recommended_examples}"
                )
                patch.status = "FAIL"
                return patch

            # Build worker prompt with role context
            # Primary gold plan for the template
            gold_patch = gold_patches[0]
            worker_prompt = build_worker_patch_prompt(
                original_sql=original_sql,
                ir_node_map=ir_node_map,
                target=target.to_dict(),
                gold_patch_plan=gold_patch,
                dialect=self.dialect,
                worker_role=worker_role,
            )

            # For compound families, append additional gold examples
            if len(gold_patches) > 1:
                extra_lines = [
                    "\n## Additional Gold Examples (for compound strategy)\n"
                ]
                for i, extra_plan in enumerate(gold_patches[1:], 2):
                    extra_lines.append(f"**Gold Example {i}:**")
                    extra_lines.append("```json")
                    extra_lines.append(json.dumps(extra_plan, indent=2))
                    extra_lines.append("```\n")
                extra_lines.append(
                    "Combine techniques from ALL gold examples above "
                    "into a single unified patch plan."
                )
                worker_prompt += "\n".join(extra_lines)

            # Capture worker role for logging
            patch.worker_role = worker_role.get("key", "?")
            patch.worker_prompt = worker_prompt

            # Call worker LLM
            try:
                worker_response = self.worker_call_fn(worker_prompt)
                patch.worker_response = worker_response
                with api_lock:
                    api_call_count[0] += 1
            except Exception as e:
                with api_lock:
                    api_call_count[0] += 1  # attempted call still counts
                patch.apply_error = f"Worker LLM call failed: {e}"
                patch.status = "FAIL"
                return patch

            # Parse and apply the patch plan
            return self._apply_worker_response(
                patch, worker_response, worker_prompt,
                script_ir, dialect_enum,
            )

        # Run workers in parallel
        with ThreadPoolExecutor(max_workers=len(assignments) or 1) as pool:
            futures = {
                pool.submit(process_assignment, target, role): target
                for target, role in assignments
            }
            for future in as_completed(futures):
                target = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(
                        f"Worker {target.target_id} crashed: {e}"
                    )
                    results.append(AppliedPatch(
                        patch_id=target.target_id,
                        family=target.family,
                        transform=target.transform,
                        relevance_score=target.relevance_score,
                        apply_error=str(e),
                        status="FAIL",
                    ))

        # Sort by original target order
        all_targets = [t for t, _role in assignments]
        target_order = {t.target_id: i for i, t in enumerate(all_targets)}
        results.sort(key=lambda p: target_order.get(p.patch_id, 99))

        return results, api_call_count[0], all_targets

    def retry_worker(
        self,
        original_sql: str,
        ir_node_map: str,
        target: AnalystTarget,
        error: str,
        script_ir: Any,
        dialect_enum: Any,
    ) -> Optional["AppliedPatch"]:
        """Retry a single failed worker with error context."""
        from .beam_prompt_builder import (
            build_worker_patch_prompt,
            build_worker_retry_prompt,
        )
        from ..sessions.beam_session import AppliedPatch

        gold_patch = self._find_gold_patch(target)
        if not gold_patch:
            return None

        # Derive worker role from family
        primary_family = target.family.split("+")[0].strip()
        worker_key = FAMILY_TO_WORKER.get(primary_family, "W3")
        worker_role = WORKER_ROLES.get(worker_key)

        worker_prompt = build_worker_patch_prompt(
            original_sql=original_sql,
            ir_node_map=ir_node_map,
            target=target.to_dict(),
            gold_patch_plan=gold_patch,
            dialect=self.dialect,
            worker_role=worker_role,
        )

        retry_prompt = build_worker_retry_prompt(worker_prompt, error)

        try:
            response = self.worker_call_fn(retry_prompt)
        except Exception as e:
            logger.warning(f"Worker retry LLM call failed: {e}")
            return None

        patch = AppliedPatch(
            patch_id=target.target_id,
            family=target.family,
            transform=target.transform,
            relevance_score=target.relevance_score,
        )

        return self._apply_worker_response(
            patch, response, retry_prompt, script_ir, dialect_enum,
        )

    def retry_worker_semantic(
        self,
        original_sql: str,
        ir_node_map: str,
        target: AnalystTarget,
        sem_result,
        rewrite_sql: str,
        script_ir: Any,
        dialect_enum: Any,
    ) -> Optional["AppliedPatch"]:
        """Retry a worker whose patch failed semantic validation.

        Uses build_worker_semantic_retry_prompt() with row count diffs,
        value diffs, and SQL diff as error context.
        """
        from .beam_prompt_builder import (
            build_worker_patch_prompt,
            build_worker_semantic_retry_prompt,
        )
        from ..sessions.beam_session import AppliedPatch

        gold_patch = self._find_gold_patch(target)
        if not gold_patch:
            return None

        primary_family = target.family.split("+")[0].strip()
        worker_key = FAMILY_TO_WORKER.get(primary_family, "W3")
        worker_role = WORKER_ROLES.get(worker_key)

        worker_prompt = build_worker_patch_prompt(
            original_sql=original_sql,
            ir_node_map=ir_node_map,
            target=target.to_dict(),
            gold_patch_plan=gold_patch,
            dialect=self.dialect,
            worker_role=worker_role,
        )

        retry_prompt = build_worker_semantic_retry_prompt(
            worker_prompt, sem_result, original_sql, rewrite_sql,
        )

        try:
            response = self.worker_call_fn(retry_prompt)
        except Exception as e:
            logger.warning(f"Worker semantic retry LLM call failed: {e}")
            return None

        patch = AppliedPatch(
            patch_id=target.target_id,
            family=target.family,
            transform=target.transform,
            relevance_score=target.relevance_score,
        )

        return self._apply_worker_response(
            patch, response, retry_prompt, script_ir, dialect_enum,
        )

    def run_snipe(
        self,
        query_id: str,
        original_sql: str,
        explain_text: str,
        ir_node_map: str,
        patches: List[Any],
        patch_explains: Dict[str, str],
    ) -> Tuple[List[AnalystTarget], str, str]:
        """Build snipe prompt from benchmark results, call analyst, parse targets.

        Returns:
            (targets, prompt, response) — parsed targets, raw prompt, raw response.
        """
        from .beam_prompt_builder import build_beam_tiered_snipe_prompt

        prompt = build_beam_tiered_snipe_prompt(
            query_id=query_id,
            original_sql=original_sql,
            explain_text=explain_text,
            ir_node_map=ir_node_map,
            all_5_examples=self.gold_examples,
            dialect=self.dialect,
            patches=patches,
            patch_explains=patch_explains,
        )

        logger.info(
            f"[{query_id}] Snipe prompt: {len(prompt)} chars"
        )

        response = self.analyst_call_fn(prompt)
        logger.info(
            f"[{query_id}] Snipe response: {len(response)} chars"
        )

        targets = self._parse_analyst_response(response)
        logger.info(
            f"[{query_id}] Parsed {len(targets)} snipe targets"
        )

        return targets, prompt, response

    # ── Internal helpers ──────────────────────────────────────────────────

    # Default family + transform for each worker when creating synthetic targets
    _WORKER_DEFAULTS: Dict[str, Tuple[str, str]] = {
        "W1": ("A", "early_filter"),
        "W2": ("B", "decorrelate"),
        "W3": ("E", "materialize_cte"),
    }

    def _assign_workers(
        self,
        targets: List[AnalystTarget],
        force_full_roster: bool = False,
    ) -> List[Tuple[AnalystTarget, Dict[str, Any]]]:
        """Assign targets to specialized workers via AST-anchored draft.

        Draft algorithm:
        1. W4 "Wildcard" gets the **AST top match** (guaranteed slot from
           detection). If no AST match, falls back to analyst's #1 target.
        2. ALL analyst targets route to family workers W1-W3:
           - W1 "Reducer" (A, D)
           - W2 "Unnester" (B, C)
           - W3 "Builder" (F, E)
        3. If a worker has multiple targets (e.g. both E and F → W3),
           merge them into a compound target.
        4. If force_full_roster is True and fewer than 4 workers assigned,
           create synthetic targets for unfilled slots.

        Returns:
            List of (target, worker_role) tuples (max 4).
        """
        if not targets and not self.ast_top_match:
            return []

        sorted_targets = sorted(
            targets, key=lambda t: t.relevance_score, reverse=True
        )

        assignments: List[Tuple[AnalystTarget, Dict[str, Any]]] = []

        # W4 gets the AST top match (guaranteed detection-driven slot)
        if self.ast_top_match:
            ast_family = self.ast_top_match["family"]
            ast_transform = self.ast_top_match["transform_id"]
            ast_gap = self.ast_top_match.get("gap", "")

            # Find gold example for this transform/family
            family_ex = self.gold_examples.get(ast_family)
            example_id = family_ex.get("id", ast_transform) if family_ex else ast_transform

            # Check if analyst already produced a target for this exact transform
            analyst_match = next(
                (t for t in sorted_targets if t.transform == ast_transform),
                None,
            )
            if analyst_match:
                # Analyst already covers it — use the analyst's richer target
                w4_target = analyst_match
                logger.info(
                    f"W4 Wildcard ← {w4_target.target_id} "
                    f"(AST+analyst match: {ast_transform}, "
                    f"family {ast_family})"
                )
                # Exclude matched target from family routing (W4 has it)
                remaining = [t for t in sorted_targets if t is not analyst_match]
            else:
                # Create target from AST detection
                ref_ir = sorted_targets[0].target_ir if sorted_targets else ""
                w4_target = AnalystTarget(
                    target_id="ast_w4",
                    family=ast_family,
                    transform=ast_transform,
                    relevance_score=1.0,
                    hypothesis=(
                        f"AST detection: {ast_transform} "
                        f"({self.ast_top_match['overlap']:.0%} overlap). "
                        f"Engine gap: {ast_gap}."
                    ),
                    target_ir=ref_ir,
                    recommended_examples=[example_id],
                )
                logger.info(
                    f"W4 Wildcard ← ast_w4 (AST-driven: {ast_transform}, "
                    f"family {ast_family}, "
                    f"{self.ast_top_match['overlap']:.0%} overlap)"
                )
                # ALL analyst targets go to family workers
                remaining = sorted_targets

            assignments.append((w4_target, WORKER_ROLES["W4"]))
        elif sorted_targets:
            # No AST match — fall back to analyst's #1 target for W4
            assignments.append((sorted_targets[0], WORKER_ROLES["W4"]))
            logger.info(
                f"W4 Wildcard ← {sorted_targets[0].target_id} "
                f"(analyst #1, family {sorted_targets[0].family}, "
                f"relevance {sorted_targets[0].relevance_score:.2f})"
            )
            remaining = sorted_targets[1:]
        else:
            remaining = []

        # Group analyst targets by super-family worker
        worker_groups: Dict[str, List[AnalystTarget]] = {}
        for target in remaining:
            primary_family = target.family.split("+")[0].strip()
            worker_key = FAMILY_TO_WORKER.get(primary_family, "W3")
            worker_groups.setdefault(worker_key, []).append(target)

        # Create one assignment per worker group (W1, W2, W3 order)
        assigned_workers: set = set()
        for worker_key in ["W1", "W2", "W3"]:
            group = worker_groups.get(worker_key)
            if not group:
                continue

            if len(group) == 1:
                merged = group[0]
            else:
                merged = self._merge_targets(group)

            assignments.append((merged, WORKER_ROLES[worker_key]))
            assigned_workers.add(worker_key)
            logger.info(
                f"{worker_key} {WORKER_ROLES[worker_key]['name']} ← "
                f"{merged.target_id} (family {merged.family}, "
                f"{len(group)} target(s))"
            )

        # Fill empty slots with synthetic targets on first iteration
        if force_full_roster and len(assignments) < 4:
            for worker_key in ["W1", "W2", "W3"]:
                if worker_key in assigned_workers:
                    continue
                if len(assignments) >= 4:
                    break
                ref = sorted_targets[0] if sorted_targets else w4_target
                synthetic = self._create_synthetic_target(worker_key, ref)
                assignments.append((synthetic, WORKER_ROLES[worker_key]))
                logger.info(
                    f"{worker_key} {WORKER_ROLES[worker_key]['name']} ← "
                    f"{synthetic.target_id} (SYNTHETIC, family {synthetic.family})"
                )

        return assignments[:4]

    def _create_synthetic_target(
        self, worker_key: str, reference_target: AnalystTarget,
    ) -> AnalystTarget:
        """Create a fallback target for an unfilled worker slot.

        Uses the worker's default family/transform and borrows IR context
        from the top analyst target so the worker has structural context.
        """
        family, transform = self._WORKER_DEFAULTS.get(worker_key, ("A", "early_filter"))
        role = WORKER_ROLES[worker_key]

        # Find a gold example ID for this family
        family_ex = self.gold_examples.get(family)
        example_id = family_ex.get("id", transform) if family_ex else transform

        return AnalystTarget(
            target_id=f"syn_{worker_key.lower()}",
            family=family,
            transform=transform,
            relevance_score=0.3,
            hypothesis=(
                f"Synthetic target for {role['name']}: {role['focus']}. "
                f"Apply {transform} patterns to the query."
            ),
            target_ir=reference_target.target_ir,
            recommended_examples=[example_id],
        )

    @staticmethod
    def _merge_targets(targets: List[AnalystTarget]) -> AnalystTarget:
        """Merge multiple targets for the same super-family worker into one compound target.

        The compound target has:
        - family: unique families joined by "+" (e.g. "E+F", not "E+E")
        - hypothesis: all hypotheses joined by " | "
        - target_ir: all IRs joined by separator
        - recommended_examples: deduplicated union
        - relevance_score: max across targets
        """
        # Deduplicate families (e.g. two "B" targets → just "B", not "B+B")
        seen_families: list = []
        for t in targets:
            for f in t.family.split("+"):
                f = f.strip()
                if f not in seen_families:
                    seen_families.append(f)
        family = "+".join(seen_families)

        transforms = "+".join(t.transform for t in targets)
        hypotheses = " | ".join(t.hypothesis for t in targets if t.hypothesis)
        target_irs = "\n---\n".join(t.target_ir for t in targets if t.target_ir)

        # Deduplicate examples preserving order
        examples: list = []
        for t in targets:
            for ex in t.recommended_examples:
                if ex not in examples:
                    examples.append(ex)

        return AnalystTarget(
            target_id=targets[0].target_id,
            family=family,
            transform=transforms,
            relevance_score=max(t.relevance_score for t in targets),
            hypothesis=hypotheses,
            target_ir=target_irs,
            recommended_examples=examples,
        )

    def _parse_analyst_response(self, response: str) -> List[AnalystTarget]:
        """Parse analyst JSON array (or individual objects) into AnalystTarget objects."""
        from ..patches.beam_patch_validator import _extract_json_array

        targets_data = _extract_json_array(response)
        # Validate: targets must be dicts (not strings like recommended_examples)
        if targets_data and not isinstance(targets_data[0], dict):
            targets_data = None
        if targets_data is None:
            # Fallback: analyst may emit individual JSON objects in markdown blocks
            targets_data = self._extract_individual_json_objects(response)
        if not targets_data:
            logger.warning("Failed to extract targets from analyst response")
            return []

        targets = []
        for i, td in enumerate(targets_data[:4]):
            if not isinstance(td, dict):
                continue
            try:
                targets.append(AnalystTarget(
                    target_id=td.get("target_id", f"t{i + 1}"),
                    family=td.get("family", "?"),
                    transform=td.get("transform", "unknown"),
                    relevance_score=float(td.get("relevance_score", 0.0)),
                    hypothesis=td.get("hypothesis", ""),
                    target_ir=td.get("target_ir", ""),
                    recommended_examples=td.get("recommended_examples", []),
                ))
            except (TypeError, ValueError) as e:
                logger.warning(f"Failed to parse analyst target {i}: {e}")

        return targets

    def _find_all_gold_patches(self, target: AnalystTarget) -> List[Dict[str, Any]]:
        """Find ALL matching gold patch plans for a target.

        For compound families (e.g. "A+B"), returns one plan per family letter.
        Used to give workers multiple reference patterns for compound strategies.
        """
        plans: List[Dict[str, Any]] = []
        seen_ids: set = set()

        # Try recommended examples first
        for ex_id in target.recommended_examples:
            ex = self._example_by_id.get(ex_id)
            if ex and ex.get("patch_plan") and ex_id not in seen_ids:
                plans.append(ex["patch_plan"])
                seen_ids.add(ex_id)

        # Try each family letter
        family_letters = [f.strip() for f in target.family.split("+")]
        for fam in family_letters:
            family_ex = self.gold_examples.get(fam)
            if family_ex and family_ex.get("patch_plan"):
                ex_id = family_ex.get("id", fam)
                if ex_id not in seen_ids:
                    plans.append(family_ex["patch_plan"])
                    seen_ids.add(ex_id)

        return plans

    def _find_gold_patch(self, target: AnalystTarget) -> Optional[Dict[str, Any]]:
        """Find the best matching gold patch plan for a target.

        Handles compound families (e.g. "A+B") by trying recommended examples
        first, then each family letter in order.
        """
        # Try recommended examples first
        for ex_id in target.recommended_examples:
            ex = self._example_by_id.get(ex_id)
            if ex and ex.get("patch_plan"):
                return ex["patch_plan"]

        # Handle compound families (e.g. "A+B") — try each family letter
        family_letters = [f.strip() for f in target.family.split("+")]
        for fam in family_letters:
            family_ex = self.gold_examples.get(fam)
            if family_ex and family_ex.get("patch_plan"):
                return family_ex["patch_plan"]

        return None

    def _apply_worker_response(
        self,
        patch: "AppliedPatch",
        response: str,
        prompt: str,
        script_ir: Any,
        dialect_enum: Any,
    ) -> "AppliedPatch":
        """Parse worker response and apply patch plan to IR."""
        from ..ir import dict_to_plan, apply_patch_plan

        # Extract JSON object from response
        patch_data = self._extract_json_object(response)
        if patch_data is None:
            patch.apply_error = "Failed to extract JSON from worker response"
            patch.status = "FAIL"
            return patch

        # Ensure plan_id and dialect are set
        if "plan_id" not in patch_data:
            patch_data["plan_id"] = patch.patch_id
        if "dialect" not in patch_data:
            patch_data["dialect"] = self.dialect

        patch.raw_plan = patch_data

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
        except Exception as e:
            patch.apply_error = str(e)
            patch.status = "FAIL"

        return patch

    @staticmethod
    def _extract_individual_json_objects(text: str) -> Optional[List[Dict[str, Any]]]:
        """Extract multiple individual JSON objects from text (e.g. each in its own code block).

        Handles the case where the analyst outputs separate ```json blocks
        instead of a single JSON array.
        """
        import re
        objects = []

        # Find all JSON code blocks
        for match in re.finditer(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL):
            try:
                obj = json.loads(match.group(1))
                if isinstance(obj, dict) and ("family" in obj or "target_id" in obj):
                    objects.append(obj)
            except json.JSONDecodeError:
                continue

        if objects:
            logger.info(
                f"Extracted {len(objects)} individual JSON objects from response"
            )
            return objects

        return None

    @staticmethod
    def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
        """Extract a single JSON object from text (worker outputs one object, not array)."""
        # Try direct parse first
        text = text.strip()
        if text.startswith("{"):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                pass

        # Try extracting from code block
        import re
        match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # Try finding first { ... } pair
        start = text.find("{")
        if start >= 0:
            depth = 0
            for i in range(start, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(text[start : i + 1])
                        except json.JSONDecodeError:
                            break

        return None
