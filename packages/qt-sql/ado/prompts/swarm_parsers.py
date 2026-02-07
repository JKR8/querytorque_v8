"""Parsers for swarm mode analyst responses.

Parses fan-out (4 worker assignments) and snipe (refined strategy) responses.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class WorkerAssignment:
    """Parsed worker assignment from fan-out response."""
    worker_id: int
    strategy: str
    examples: List[str]
    hint: str


@dataclass
class SnipeAnalysis:
    """Parsed snipe analysis from snipe response."""
    failure_analysis: str
    unexplored: str
    refined_strategy: str
    examples: List[str]
    hint: str


def parse_fan_out_response(response: str) -> List[WorkerAssignment]:
    """Parse analyst fan-out response into 4 worker assignments.

    Expected format:
        WORKER_1:
        STRATEGY: <name>
        EXAMPLES: <ex1>, <ex2>, <ex3>
        HINT: <guidance>

        WORKER_2:
        ...

    Returns:
        List of 4 WorkerAssignment objects.

    Raises:
        ValueError: If response cannot be parsed into valid assignments.
    """
    assignments = []

    # Split by WORKER_N: headers
    worker_blocks = re.split(r'WORKER_(\d+)\s*:', response, flags=re.IGNORECASE)

    # worker_blocks alternates: [preamble, "1", block1, "2", block2, ...]
    for i in range(1, len(worker_blocks) - 1, 2):
        worker_id = int(worker_blocks[i])
        block = worker_blocks[i + 1]

        strategy = _extract_field(block, "STRATEGY")
        examples_raw = _extract_field(block, "EXAMPLES")
        hint = _extract_field(block, "HINT")

        examples = [ex.strip() for ex in examples_raw.split(",") if ex.strip()]

        assignments.append(WorkerAssignment(
            worker_id=worker_id,
            strategy=strategy,
            examples=examples,
            hint=hint,
        ))

    # Validate we got exactly 4
    if len(assignments) < 4:
        logger.warning(
            f"Expected 4 worker assignments, got {len(assignments)}. "
            f"Padding with defaults."
        )
        # Pad with defaults using any remaining examples
        used = {ex for a in assignments for ex in a.examples}
        while len(assignments) < 4:
            wid = len(assignments) + 1
            assignments.append(WorkerAssignment(
                worker_id=wid,
                strategy=f"fallback_{wid}",
                examples=[],
                hint="Apply any relevant optimization pattern.",
            ))

    # Warn on duplicates but don't fail (LLM may not follow strictly)
    all_examples = [ex for a in assignments for ex in a.examples]
    unique_examples = set(all_examples)
    if len(all_examples) != len(unique_examples):
        dupes = [ex for ex in unique_examples if all_examples.count(ex) > 1]
        logger.warning(f"Duplicate examples across workers: {dupes}")

    return assignments[:4]


def parse_snipe_response(response: str) -> SnipeAnalysis:
    """Parse analyst snipe response into structured analysis.

    Expected format:
        FAILURE_ANALYSIS:
        <text>

        UNEXPLORED_OPPORTUNITIES:
        <text>

        REFINED_STRATEGY:
        <text>

        EXAMPLES: <ex1>, <ex2>, <ex3>
        HINT: <guidance>

    Returns:
        SnipeAnalysis object.
    """
    failure = _extract_section(response, "FAILURE_ANALYSIS")
    unexplored = _extract_section(response, "UNEXPLORED_OPPORTUNITIES")
    strategy = _extract_section(response, "REFINED_STRATEGY")

    examples_raw = _extract_field(response, "EXAMPLES")
    examples = [ex.strip() for ex in examples_raw.split(",") if ex.strip()]

    hint = _extract_field(response, "HINT")

    # Fallback: if nothing parsed, use the whole response
    if not failure and not strategy:
        logger.warning("Could not parse snipe response sections, using raw response")
        failure = response.strip()

    return SnipeAnalysis(
        failure_analysis=failure,
        unexplored=unexplored,
        refined_strategy=strategy,
        examples=examples,
        hint=hint,
    )


def _extract_field(text: str, field_name: str) -> str:
    """Extract a single-line field value: FIELD_NAME: <value>."""
    pattern = rf'{field_name}\s*:\s*(.+?)(?:\n|$)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""


def _extract_section(text: str, section_name: str) -> str:
    """Extract a multi-line section value.

    Captures everything after SECTION_NAME: until the next section header
    or end of text.
    """
    # Match section header, then capture until next known header or end
    headers = [
        "FAILURE_ANALYSIS", "UNEXPLORED_OPPORTUNITIES", "REFINED_STRATEGY",
        "EXAMPLES", "HINT", "WORKER_",
    ]
    next_headers = "|".join(h for h in headers if h != section_name)
    pattern = rf'{section_name}\s*:\s*\n?(.*?)(?=(?:{next_headers})\s*:|$)'
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""
