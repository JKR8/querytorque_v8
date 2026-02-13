"""DAX LLM optimizer — call + parse + retry.

Follows qt_sql/generate.py pattern: LLM call, JSON extraction from
fenced code blocks, retry loop with error feedback.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional, Protocol

from .prompter import DAXPrompter, PromptInputs

logger = logging.getLogger(__name__)


class LLMClient(Protocol):
    """Matches qt_shared.llm.protocol.LLMClient."""

    def analyze(self, prompt: str) -> str: ...


@dataclass
class DAXOptimizationResult:
    """Result of a DAX optimization attempt."""

    optimized_dax: str = ""
    transforms_applied: list[str] = field(default_factory=list)
    rationale: str = ""
    attempts: int = 0
    status: str = "error"  # "pass" | "no_improvement" | "error"
    error: str = ""
    warnings: list[str] = field(default_factory=list)
    prompt: str = ""  # populated in dry-run mode


class DAXOptimizer:
    """LLM-powered DAX measure optimiser with retry loop."""

    def __init__(self, llm_client: Optional[LLMClient] = None) -> None:
        self.llm = llm_client
        self.prompter = DAXPrompter()

    def optimize_measure(
        self,
        inputs: PromptInputs,
        *,
        max_attempts: int = 3,
        dry_run: bool = False,
    ) -> DAXOptimizationResult:
        """Optimise a single DAX measure.

        Args:
            inputs: All data needed to build the prompt.
            max_attempts: Maximum LLM calls (including retries with error feedback).
            dry_run: If True, build prompt only — do not call LLM.

        Returns:
            DAXOptimizationResult with optimised DAX (or error details).
        """
        result = DAXOptimizationResult()

        # Build initial prompt
        prompt = self.prompter.build_prompt(inputs)

        if dry_run:
            result.prompt = prompt
            result.status = "dry_run"
            return result

        if self.llm is None:
            result.error = "No LLM client configured"
            return result

        prev_error: Optional[str] = None
        prev_attempt: Optional[str] = None

        for attempt in range(1, max_attempts + 1):
            result.attempts = attempt

            # On retry, rebuild prompt with error context
            if prev_error and prev_attempt:
                retry_inputs = PromptInputs(
                    measure_name=inputs.measure_name,
                    measure_table=inputs.measure_table,
                    measure_dax=inputs.measure_dax,
                    dependency_chain=inputs.dependency_chain,
                    model_schema=inputs.model_schema,
                    detected_issues=inputs.detected_issues,
                    previous_attempt=prev_attempt,
                    previous_error=prev_error,
                )
                prompt = self.prompter.build_prompt(retry_inputs)

            # Call LLM
            try:
                response = self.llm.analyze(prompt)
            except Exception as exc:
                logger.warning("LLM call failed (attempt %d): %s", attempt, exc)
                result.error = str(exc)
                result.status = "error"
                continue

            # Parse JSON from response
            parsed = _parse_json_response(response)
            if parsed is None:
                prev_error = "Could not parse JSON from LLM response"
                prev_attempt = response[:2000]
                result.error = prev_error
                result.status = "error"
                logger.warning("JSON parse failed (attempt %d)", attempt)
                continue

            optimized_dax = parsed.get("optimized_dax", "").strip()
            transforms = parsed.get("transforms_applied", [])
            rationale = parsed.get("rationale", "")

            result.optimized_dax = optimized_dax
            result.transforms_applied = transforms if isinstance(transforms, list) else []
            result.rationale = rationale

            # Empty optimized_dax means "no improvement possible"
            if not optimized_dax:
                result.status = "no_improvement"
                return result

            # Success
            result.status = "pass"
            return result

        # Exhausted all attempts
        if not result.error:
            result.error = f"Failed after {max_attempts} attempts"
        return result


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

_JSON_BLOCK_RE = re.compile(r"```json\s*\n(.*?)\n\s*```", re.DOTALL)


def _parse_json_response(response: str) -> Optional[dict]:
    """Extract the first ```json ... ``` block and parse it.

    Falls back to scanning the whole response for a JSON object if
    no fenced block is found.
    """
    # Try fenced json block first
    m = _JSON_BLOCK_RE.search(response)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # Fallback: find first { ... } in response
    start = response.find("{")
    if start == -1:
        return None

    # Walk forward to find matching closing brace
    depth = 0
    for i in range(start, len(response)):
        if response[i] == "{":
            depth += 1
        elif response[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(response[start : i + 1])
                except json.JSONDecodeError:
                    return None

    return None
