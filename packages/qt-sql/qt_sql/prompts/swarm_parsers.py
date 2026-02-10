"""Parsers for swarm mode analyst responses.

Parses fan-out (4 worker assignments), snipe (diagnosis-then-synthesis),
and briefing (structured analyst interpretation) responses.
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
    target_logical_tree: str = ""  # Logical tree topology + node contracts
    examples: List[str] = field(default_factory=list)
    example_reasoning: str = ""
    hazard_flags: str = ""


@dataclass
class ParsedBriefing:
    """Complete parsed analyst briefing."""
    shared: BriefingShared = field(default_factory=BriefingShared)
    workers: List[BriefingWorker] = field(default_factory=list)
    raw: str = ""                # full response for audit


def parse_briefing_response(response: str) -> ParsedBriefing:
    """Parse analyst briefing response into structured sections.

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
        TARGET_LOGICAL_TREE: ...
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

    # TARGET_LOGICAL_TREE + NODE_CONTRACTS (multi-line, combined)
    # These are conceptually one unit — the CTE blueprint
    target_logical_tree = _extract_briefing_section(
        block, "TARGET_LOGICAL_TREE",
        ["EXAMPLES", "EXAMPLE_REASONING", "HAZARD_FLAGS", "==="],
    )
    node_contracts = _extract_briefing_section(
        block, "NODE_CONTRACTS",
        ["EXAMPLES", "EXAMPLE_REASONING", "HAZARD_FLAGS", "==="],
    )
    # Combine target logical tree and node contracts
    parts = []
    if target_logical_tree:
        parts.append(f"TARGET_LOGICAL_TREE:\n{target_logical_tree}")
    if node_contracts:
        parts.append(f"NODE_CONTRACTS:\n{node_contracts}")
    w.target_logical_tree = "\n\n".join(parts)

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


@dataclass
class SnipeAnalysis:
    """Parsed snipe analyst analysis — diagnosis-then-synthesis output."""
    failure_synthesis: str = ""     # WHY best won, WHY others didn't
    best_foundation: str = ""       # What to build on (if any > 1.0x)
    unexplored_angles: str = ""     # What couldn't be designed without results
    strategy_guidance: str = ""     # Synthesized approach for sniper
    examples: List[str] = field(default_factory=list)
    example_adaptation: str = ""    # APPLY/IGNORE per example
    hazard_flags: str = ""          # Risks based on observed failures
    retry_worthiness: str = ""      # "high" or "low" + reason
    retry_digest: str = ""          # 5-10 line compact failure guide for sniper2
    raw: str = ""


def parse_snipe_response(response: str) -> SnipeAnalysis:
    """Parse snipe analyst response into structured analysis.

    Fault-tolerant: returns empty fields rather than raising exceptions.

    Expected format:
        === SNIPE BRIEFING ===

        FAILURE_SYNTHESIS:
        <text>

        BEST_FOUNDATION:
        <text>

        UNEXPLORED_ANGLES:
        <text>

        STRATEGY_GUIDANCE:
        <text>

        EXAMPLES: <ex1>, <ex2>, <ex3>

        EXAMPLE_ADAPTATION:
        <text>

        HAZARD_FLAGS:
        <text>

        RETRY_WORTHINESS: high|low — <reason>

        RETRY_DIGEST:
        <text>
    """
    result = SnipeAnalysis(raw=response)

    # Strip <reasoning>...</reasoning> block if present
    stripped = re.sub(
        r'<reasoning>.*?</reasoning>',
        '', response, flags=re.DOTALL,
    ).strip()

    # All section names in order (used for stop markers)
    _SNIPE_SECTIONS = [
        "FAILURE_SYNTHESIS", "BEST_FOUNDATION", "UNEXPLORED_ANGLES",
        "STRATEGY_GUIDANCE", "EXAMPLES", "EXAMPLE_ADAPTATION",
        "HAZARD_FLAGS", "RETRY_WORTHINESS", "RETRY_DIGEST",
    ]

    def _extract_snipe_section(text: str, name: str) -> str:
        """Extract a multi-line section, stopping at next known section."""
        others = [s for s in _SNIPE_SECTIONS if s != name]
        stop_re = "|".join(re.escape(s) for s in others)
        pattern = rf'{re.escape(name)}\s*:\s*\n?(.*?)(?=(?:{stop_re})\s*:|$)'
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return ""

    result.failure_synthesis = _extract_snipe_section(stripped, "FAILURE_SYNTHESIS")
    result.best_foundation = _extract_snipe_section(stripped, "BEST_FOUNDATION")
    result.unexplored_angles = _extract_snipe_section(stripped, "UNEXPLORED_ANGLES")
    result.strategy_guidance = _extract_snipe_section(stripped, "STRATEGY_GUIDANCE")
    result.example_adaptation = _extract_snipe_section(stripped, "EXAMPLE_ADAPTATION")
    result.hazard_flags = _extract_snipe_section(stripped, "HAZARD_FLAGS")
    result.retry_worthiness = _extract_snipe_section(stripped, "RETRY_WORTHINESS")
    result.retry_digest = _extract_snipe_section(stripped, "RETRY_DIGEST")

    # EXAMPLES is single-line comma-separated
    examples_raw = _extract_field(stripped, "EXAMPLES")
    result.examples = [ex.strip() for ex in examples_raw.split(",") if ex.strip()]

    # Fallback: if nothing parsed, use the whole response
    if not result.failure_synthesis and not result.strategy_guidance:
        logger.warning("Could not parse snipe analyst response, using raw response")
        result.failure_synthesis = stripped

    return result


@dataclass
class OneshotResult:
    """Parsed oneshot response — analyst produces SQL directly."""
    strategy: str = ""
    transforms: List[str] = field(default_factory=list)
    optimized_sql: str = ""
    shared: BriefingShared = field(default_factory=BriefingShared)
    raw: str = ""


def parse_oneshot_response(response: str) -> OneshotResult:
    """Parse oneshot analyst response into structured result.

    Fault-tolerant: returns empty fields rather than raising.

    Expected format:
        <reasoning>...</reasoning>

        === SHARED BRIEFING ===
        SEMANTIC_CONTRACT: ...
        BOTTLENECK_DIAGNOSIS: ...
        ACTIVE_CONSTRAINTS: ...
        REGRESSION_WARNINGS: ...

        === OPTIMIZED SQL ===

        STRATEGY: strategy_name
        TRANSFORM: transform_names

        ```sql
        SELECT ...
        ```
    """
    result = OneshotResult(raw=response)

    # Strip <reasoning>...</reasoning> block
    stripped = re.sub(
        r'<reasoning>.*?</reasoning>',
        '', response, flags=re.DOTALL,
    ).strip()

    # Parse shared briefing (reuse existing parser)
    result.shared = _parse_shared_briefing(stripped)

    # Find the === OPTIMIZED SQL === section
    sql_section_match = re.search(
        r'===\s*OPTIMIZED\s+SQL\s*===\s*\n(.*)',
        stripped, re.DOTALL | re.IGNORECASE,
    )
    if sql_section_match:
        sql_section = sql_section_match.group(1)
    else:
        # Fallback: use everything after shared briefing
        sql_section = stripped

    # Extract STRATEGY (single line)
    result.strategy = _extract_field(sql_section, "STRATEGY")

    # Extract TRANSFORM (comma-separated or single)
    transform_raw = _extract_field(sql_section, "TRANSFORM")
    if transform_raw:
        result.transforms = [t.strip() for t in transform_raw.split(",") if t.strip()]

    # Extract SQL from code block
    sql_match = re.search(
        r'```sql\s*\n(.*?)```',
        sql_section, re.DOTALL | re.IGNORECASE,
    )
    if sql_match:
        result.optimized_sql = sql_match.group(1).strip()
    else:
        # Try bare code block
        bare_match = re.search(r'```\s*\n(.*?)```', sql_section, re.DOTALL)
        if bare_match:
            result.optimized_sql = bare_match.group(1).strip()

    # Fallback: if nothing parsed, log warning
    if not result.optimized_sql:
        logger.warning("Could not parse optimized SQL from oneshot response")

    return result


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
