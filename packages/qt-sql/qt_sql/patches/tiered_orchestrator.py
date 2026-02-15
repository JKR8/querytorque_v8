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
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


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
        gold_examples: Dict mapping family ID (A-E) to gold example JSON.
        dialect: SQL dialect string (duckdb, postgres, snowflake).
    """

    def __init__(
        self,
        analyst_call_fn: Callable[[str], str],
        worker_call_fn: Callable[[str], str],
        gold_examples: Dict[str, Dict[str, Any]],
        dialect: str,
    ):
        self.analyst_call_fn = analyst_call_fn
        self.worker_call_fn = worker_call_fn
        self.gold_examples = gold_examples
        self.dialect = dialect

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
    ) -> List[AnalystTarget]:
        """Build tiered analyst prompt, call DeepSeek, parse response into targets."""
        from .oneshot_patch_prompt_builder import build_oneshot_patch_prompt_tiered

        prompt = build_oneshot_patch_prompt_tiered(
            query_id=query_id,
            original_sql=original_sql,
            explain_text=explain_text,
            ir_node_map=ir_node_map,
            all_5_examples=self.gold_examples,
            dialect=self.dialect,
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
    ) -> List["AppliedPatch"]:
        """Call qwen in parallel (one per target). Apply each patch.

        Returns list of AppliedPatch objects (same format as single-tier).
        """
        from .oneshot_patch_prompt_builder import build_worker_patch_prompt
        from ..sessions.oneshot_patch_session import AppliedPatch

        results: List[AppliedPatch] = []

        def process_target(target: AnalystTarget) -> AppliedPatch:
            patch = AppliedPatch(
                patch_id=target.target_id,
                family=target.family,
                transform=target.transform,
                relevance_score=target.relevance_score,
            )

            # Find gold patch plan from recommended examples
            gold_patch = self._find_gold_patch(target)
            if not gold_patch:
                patch.apply_error = (
                    f"No gold patch plan found for examples: "
                    f"{target.recommended_examples}"
                )
                patch.status = "FAIL"
                return patch

            # Build worker prompt
            worker_prompt = build_worker_patch_prompt(
                original_sql=original_sql,
                ir_node_map=ir_node_map,
                target=target.to_dict(),
                gold_patch_plan=gold_patch,
                dialect=self.dialect,
            )

            # Call worker LLM
            try:
                worker_response = self.worker_call_fn(worker_prompt)
            except Exception as e:
                patch.apply_error = f"Worker LLM call failed: {e}"
                patch.status = "FAIL"
                return patch

            # Parse and apply the patch plan
            return self._apply_worker_response(
                patch, worker_response, worker_prompt,
                script_ir, dialect_enum,
            )

        # Run workers in parallel (4 threads)
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {
                pool.submit(process_target, t): t
                for t in targets[:4]
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
        target_order = {t.target_id: i for i, t in enumerate(targets)}
        results.sort(key=lambda p: target_order.get(p.patch_id, 99))

        return results

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
        from .oneshot_patch_prompt_builder import (
            build_worker_patch_prompt,
            build_worker_retry_prompt,
        )
        from ..sessions.oneshot_patch_session import AppliedPatch

        gold_patch = self._find_gold_patch(target)
        if not gold_patch:
            return None

        worker_prompt = build_worker_patch_prompt(
            original_sql=original_sql,
            ir_node_map=ir_node_map,
            target=target.to_dict(),
            gold_patch_plan=gold_patch,
            dialect=self.dialect,
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

    # ── Internal helpers ──────────────────────────────────────────────────

    def _parse_analyst_response(self, response: str) -> List[AnalystTarget]:
        """Parse analyst JSON array into AnalystTarget objects."""
        from ..patches.oneshot_patch_validator import _extract_json_array

        targets_data = _extract_json_array(response)
        if targets_data is None:
            logger.warning("Failed to extract JSON array from analyst response")
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

    def _find_gold_patch(self, target: AnalystTarget) -> Optional[Dict[str, Any]]:
        """Find the best matching gold patch plan for a target."""
        # Try recommended examples first
        for ex_id in target.recommended_examples:
            ex = self._example_by_id.get(ex_id)
            if ex and ex.get("patch_plan"):
                return ex["patch_plan"]

        # Fallback: use the family's default gold example
        family_ex = self.gold_examples.get(target.family)
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
