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

    Notes:
        This parser is fault-tolerant: it normalizes worker IDs to 1..4 and
        synthesizes fallback assignments for missing/invalid sections.
    """
    assignments: List[WorkerAssignment] = []
    assignments_by_worker: dict[int, WorkerAssignment] = {}

    # Split by WORKER_N: headers
    worker_blocks = re.split(r'WORKER_(\d+)\s*:', response, flags=re.IGNORECASE)

    # worker_blocks alternates: [preamble, "1", block1, "2", block2, ...]
    for i in range(1, len(worker_blocks) - 1, 2):
        worker_id = int(worker_blocks[i])
        if worker_id < 1 or worker_id > 4:
            logger.warning(f"Ignoring invalid worker id WORKER_{worker_id}")
            continue
        block = worker_blocks[i + 1]

        strategy = _extract_field(block, "STRATEGY")
        examples_raw = _extract_field(block, "EXAMPLES")
        hint = _extract_field(block, "HINT")

        examples = [ex.strip() for ex in examples_raw.split(",") if ex.strip()]
        if len(examples) > 3:
            logger.warning(
                f"WORKER_{worker_id} had {len(examples)} examples; trimming to 3"
            )
            examples = examples[:3]

        if worker_id in assignments_by_worker:
            logger.warning(f"Duplicate WORKER_{worker_id} block found; keeping first")
            continue

        assignments_by_worker[worker_id] = WorkerAssignment(
            worker_id=worker_id,
            strategy=strategy or f"fallback_{worker_id}",
            examples=examples,
            hint=hint or "Apply any relevant optimization pattern.",
        )

    # Build normalized 1..4 assignments so downstream always gets stable IDs.
    for worker_id in range(1, 5):
        assignment = assignments_by_worker.get(worker_id)
        if assignment is None:
            logger.warning(
                f"Missing WORKER_{worker_id} assignment, creating fallback entry"
            )
            assignment = WorkerAssignment(
                worker_id=worker_id,
                strategy=f"fallback_{worker_id}",
                examples=[],
                hint="Apply any relevant optimization pattern.",
            )
        assignments.append(assignment)

    # Warn on duplicates but don't fail (LLM may not follow strictly)
    all_examples = [ex for a in assignments for ex in a.examples]
    unique_examples = set(all_examples)
    if len(all_examples) != len(unique_examples):
        dupes = [ex for ex in unique_examples if all_examples.count(ex) > 1]
        logger.warning(f"Duplicate examples across workers: {dupes}")

    return assignments


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
