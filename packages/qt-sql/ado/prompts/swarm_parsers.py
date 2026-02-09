"""Parsers for swarm mode analyst responses.

Parses fan-out (4 worker assignments), snipe (refined strategy),
and V2 briefing (structured analyst interpretation) responses.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

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


# =========================================================================
# V2 Briefing parser — structured analyst interpretation
# =========================================================================


@dataclass
class BriefingShared:
    """Shared briefing sections (all workers receive these)."""
    semantic_contract: str = ""
    bottleneck_diagnosis: str = ""
    active_constraints: str = ""
    regression_warnings: str = ""
    resource_envelope: str = ""  # PG only: system resource bounds (input-only, not parsed from LLM)


@dataclass
class BriefingWorker:
    """Per-worker briefing from the analyst."""
    worker_id: int = 0
    strategy: str = ""
    target_dag: str = ""         # DAG topology + node contracts
    examples: List[str] = field(default_factory=list)
    example_reasoning: str = ""
    hazard_flags: str = ""


@dataclass
class ParsedBriefing:
    """Complete parsed V2 analyst briefing."""
    shared: BriefingShared = field(default_factory=BriefingShared)
    workers: List[BriefingWorker] = field(default_factory=list)
    raw: str = ""                # full response for audit


def parse_briefing_response(response: str) -> ParsedBriefing:
    """Parse V2 analyst briefing response into structured sections.

    Fault-tolerant: returns empty sections rather than raising.
    If no workers extracted, caller falls back to V1.

    Expected format:
        <reasoning>...</reasoning>

        === SHARED BRIEFING ===
        SEMANTIC_CONTRACT: ...
        BOTTLENECK_DIAGNOSIS: ...
        ACTIVE_CONSTRAINTS: ...
        REGRESSION_WARNINGS: ...

        === WORKER 1 BRIEFING ===
        STRATEGY: ...
        TARGET_DAG: ...
        NODE_CONTRACTS: ...
        EXAMPLES: ...
        EXAMPLE_REASONING: ...
        HAZARD_FLAGS: ...

        === WORKER 2 BRIEFING ===
        ...
    """
    result = ParsedBriefing(raw=response)

    # Strip <reasoning>...</reasoning> block
    stripped = re.sub(
        r'<reasoning>.*?</reasoning>',
        '', response, flags=re.DOTALL,
    ).strip()

    # Parse shared briefing
    result.shared = _parse_shared_briefing(stripped)

    # Parse worker briefings
    workers = _parse_worker_briefings(stripped)
    if workers:
        result.workers = workers
    else:
        # Pad with 4 empty workers
        result.workers = [BriefingWorker(worker_id=i) for i in range(1, 5)]

    # Ensure exactly 4 workers (pad if needed)
    existing_ids = {w.worker_id for w in result.workers}
    for wid in range(1, 5):
        if wid not in existing_ids:
            result.workers.append(BriefingWorker(worker_id=wid))
    result.workers.sort(key=lambda w: w.worker_id)
    result.workers = result.workers[:4]

    return result


def _parse_shared_briefing(text: str) -> BriefingShared:
    """Extract shared briefing sections from analyst response."""
    shared = BriefingShared()

    # Find the shared briefing section
    shared_match = re.search(
        r'===\s*SHARED\s+BRIEFING\s*===\s*\n(.*?)(?====\s*WORKER|$)',
        text, re.DOTALL | re.IGNORECASE,
    )
    if not shared_match:
        # Try without the === delimiters
        shared_text = text
    else:
        shared_text = shared_match.group(1)

    # Extract each section
    shared.semantic_contract = _extract_briefing_section(
        shared_text, "SEMANTIC_CONTRACT",
        ["BOTTLENECK_DIAGNOSIS", "ACTIVE_CONSTRAINTS", "REGRESSION_WARNINGS",
         "WORKER", "==="],
    )
    shared.bottleneck_diagnosis = _extract_briefing_section(
        shared_text, "BOTTLENECK_DIAGNOSIS",
        ["ACTIVE_CONSTRAINTS", "REGRESSION_WARNINGS", "WORKER", "==="],
    )
    shared.active_constraints = _extract_briefing_section(
        shared_text, "ACTIVE_CONSTRAINTS",
        ["REGRESSION_WARNINGS", "WORKER", "==="],
    )
    shared.regression_warnings = _extract_briefing_section(
        shared_text, "REGRESSION_WARNINGS",
        ["WORKER", "==="],
    )

    return shared


def _parse_worker_briefings(text: str) -> List[BriefingWorker]:
    """Extract per-worker briefing sections."""
    workers: List[BriefingWorker] = []

    # Split by === WORKER N BRIEFING === headers
    worker_blocks = re.split(
        r'===\s*WORKER\s+(\d+)\s+BRIEFING\s*===',
        text, flags=re.IGNORECASE,
    )

    # worker_blocks: [preamble, "1", block1, "2", block2, ...]
    for i in range(1, len(worker_blocks) - 1, 2):
        try:
            worker_id = int(worker_blocks[i])
        except ValueError:
            continue
        if worker_id < 1 or worker_id > 4:
            continue

        block = worker_blocks[i + 1]
        worker = _parse_single_worker(block, worker_id)
        workers.append(worker)

    return workers


def _parse_single_worker(block: str, worker_id: int) -> BriefingWorker:
    """Parse a single worker's briefing block."""
    w = BriefingWorker(worker_id=worker_id)

    # STRATEGY (single line)
    w.strategy = _extract_field(block, "STRATEGY") or f"strategy_{worker_id}"

    # EXAMPLES (comma-separated IDs)
    examples_raw = _extract_field(block, "EXAMPLES")
    w.examples = [ex.strip() for ex in examples_raw.split(",") if ex.strip()]
    if len(w.examples) > 3:
        w.examples = w.examples[:3]

    # TARGET_DAG + NODE_CONTRACTS (multi-line, combined)
    # These are conceptually one unit — the CTE blueprint
    target_dag = _extract_briefing_section(
        block, "TARGET_DAG",
        ["EXAMPLES", "EXAMPLE_REASONING", "HAZARD_FLAGS", "==="],
    )
    node_contracts = _extract_briefing_section(
        block, "NODE_CONTRACTS",
        ["EXAMPLES", "EXAMPLE_REASONING", "HAZARD_FLAGS", "==="],
    )
    # Combine target_dag and node_contracts
    parts = []
    if target_dag:
        parts.append(f"TARGET_DAG:\n{target_dag}")
    if node_contracts:
        parts.append(f"NODE_CONTRACTS:\n{node_contracts}")
    w.target_dag = "\n\n".join(parts)

    # EXAMPLE_REASONING (multi-line)
    w.example_reasoning = _extract_briefing_section(
        block, "EXAMPLE_REASONING",
        ["HAZARD_FLAGS", "===", "WORKER"],
    )

    # HAZARD_FLAGS (multi-line)
    w.hazard_flags = _extract_briefing_section(
        block, "HAZARD_FLAGS",
        ["===", "WORKER"],
    )

    return w


def _extract_briefing_section(
    text: str,
    section_name: str,
    stop_markers: List[str],
) -> str:
    """Extract a multi-line section from a briefing block.

    Captures everything after SECTION_NAME: until the next stop marker.
    """
    # Build stop pattern
    stop_patterns = []
    for marker in stop_markers:
        if marker == "===":
            stop_patterns.append(r'===')
        elif marker == "WORKER":
            stop_patterns.append(r'WORKER\s+\d+')
        else:
            stop_patterns.append(re.escape(marker))
    stop_re = "|".join(stop_patterns) if stop_patterns else "$"

    pattern = rf'{re.escape(section_name)}\s*:\s*\n?(.*?)(?={stop_re}|$)'
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""
