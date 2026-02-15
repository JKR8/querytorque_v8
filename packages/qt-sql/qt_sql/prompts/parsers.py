"""Parsers for analyst briefing responses.

Parses the §VI output format: SHARED BRIEFING (with OPTIMAL_PATH,
CURRENT_PLAN_GAP, DIVERSITY_MAP) + per-WORKER BRIEFINGs (with APPROACH,
TARGET_QUERY_MAP, NODE_CONTRACTS).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class BriefingShared:
    """Shared briefing sections — all workers receive these."""
    semantic_contract: str = ""
    optimal_path: str = ""
    current_plan_gap: str = ""
    active_constraints: str = ""
    regression_warnings: str = ""
    diversity_map: str = ""
    # Backwards-compatible aliases (used by pipeline analysis formatter)
    bottleneck_diagnosis: str = ""
    goal_violations: str = ""
    resource_envelope: str = ""  # PG only (input-only, not parsed from LLM)


@dataclass
class BriefingWorker:
    """Per-worker briefing from the analyst."""
    worker_id: int = 0
    strategy: str = ""
    role: str = ""  # proven_compound | structural_alt | aggressive_compound | novel_orthogonal
    primary_family: str = ""  # A-F — family code
    approach: str = ""
    target_query_map: str = ""
    node_contracts: str = ""
    examples: List[str] = field(default_factory=list)
    example_adaptation: str = ""
    hazard_flags: str = ""
    # Worker 4 / exploration fields
    exploration_type: str = ""
    hypothesis_tag: str = ""
    uncovered_family: str = ""  # Which family W1-W3 missed
    # Backwards-compatible aliases
    cost_region: str = ""
    risk_level: str = ""
    goal_addressed: str = ""
    target_logical_tree: str = ""
    constraint_override: str = ""
    override_reasoning: str = ""


@dataclass
class ParsedBriefing:
    """Complete parsed analyst briefing."""
    shared: BriefingShared = field(default_factory=BriefingShared)
    workers: List[BriefingWorker] = field(default_factory=list)
    raw: str = ""


# ── Helpers ─────────────────────────────────────────────────────────────

def _strip_markdown(text: str) -> str:
    """Normalize markdown formatting for reliable parsing."""
    text = re.sub(r'\*\*([A-Z_]+)\*\*', r'\1', text)
    text = re.sub(r'`([A-Z][A-Z0-9_]+)`', r'\1', text)
    text = re.sub(
        r'^#{1,4}\s+(SHARED\s+BRIEFING)\s*(?:\(.*?\))?\s*$',
        r'=== \1 ===',
        text, flags=re.MULTILINE | re.IGNORECASE,
    )
    text = re.sub(
        r'^#{1,4}\s+(WORKER\s+\d+\s+BRIEFING)\s*(?:\(.*?\))?\s*$',
        r'=== \1 ===',
        text, flags=re.MULTILINE | re.IGNORECASE,
    )
    text = re.sub(r'^---+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^```\w*\s*$', '', text, flags=re.MULTILINE)
    return text


def _extract_field(text: str, field_name: str) -> str:
    """Extract a single-line field value: FIELD_NAME: <value>.

    Tolerates missing colon (common LLM formatting: **FIELD** text).
    """
    # Try with colon first (more specific)
    pattern = rf'{field_name}\s*:\s*(.+?)(?:\n|$)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # Fallback: no colon (LLM used bold header without colon)
    pattern = rf'{field_name}\s+(.+?)(?:\n|$)'
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _extract_section(
    text: str,
    section_name: str,
    stop_markers: List[str],
) -> str:
    """Extract a multi-line section, stopping at the next known marker.

    Tolerates missing colon (common LLM formatting: **FIELD** on its own line).
    """
    stop_patterns = []
    for marker in stop_markers:
        if marker == "===":
            stop_patterns.append(r'===')
        elif marker == "WORKER":
            stop_patterns.append(r'WORKER\s+\d+')
        else:
            stop_patterns.append(re.escape(marker))
    stop_re = "|".join(stop_patterns) if stop_patterns else "$"
    # Try with colon first (more specific)
    pattern = rf'{re.escape(section_name)}\s*:\s*\n?(.*?)(?={stop_re}|$)'
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match and match.group(1).strip():
        return match.group(1).strip()
    # Fallback: no colon (LLM used bold header without colon)
    pattern = rf'{re.escape(section_name)}\s*\n(.*?)(?={stop_re}|$)'
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else ""


# ── Main parser ─────────────────────────────────────────────────────────

def parse_briefing_response(response: str) -> ParsedBriefing:
    """Parse analyst briefing response into structured sections.

    Fault-tolerant: returns empty sections rather than raising.

    Expected format:
        <reasoning>...</reasoning>

        === SHARED BRIEFING ===
        SEMANTIC_CONTRACT: ...
        OPTIMAL_PATH: ...
        CURRENT_PLAN_GAP: ...
        ACTIVE_CONSTRAINTS: ...
        REGRESSION_WARNINGS: ...
        DIVERSITY_MAP: ...

        === WORKER 1 BRIEFING ===
        STRATEGY: ...
        APPROACH: ...
        TARGET_QUERY_MAP: ...
        NODE_CONTRACTS: ...
        EXAMPLES: ...
        EXAMPLE_ADAPTATION: ...
        HAZARD_FLAGS: ...
    """
    result = ParsedBriefing(raw=response)

    # Strip <reasoning> block
    stripped = re.sub(
        r'<reasoning>.*?</reasoning>',
        '', response, flags=re.DOTALL,
    ).strip()
    stripped = _strip_markdown(stripped)

    # Parse shared
    result.shared = _parse_shared(stripped)

    # Parse workers
    workers = _parse_workers(stripped)
    if workers:
        result.workers = workers
    else:
        result.workers = [BriefingWorker(worker_id=i) for i in range(1, 5)]

    # Ensure exactly 4 workers
    existing_ids = {w.worker_id for w in result.workers}
    for wid in range(1, 5):
        if wid not in existing_ids:
            result.workers.append(BriefingWorker(worker_id=wid))
    result.workers.sort(key=lambda w: w.worker_id)
    result.workers = result.workers[:4]

    return result


def _parse_shared(text: str) -> BriefingShared:
    """Extract shared briefing sections."""
    shared = BriefingShared()

    shared_match = re.search(
        r'===\s*SHARED\s+BRIEFING\s*===\s*\n(.*?)(?====\s*WORKER|$)',
        text, re.DOTALL | re.IGNORECASE,
    )
    shared_text = shared_match.group(1) if shared_match else text

    _SHARED_FIELDS = [
        "SEMANTIC_CONTRACT", "OPTIMAL_PATH", "CURRENT_PLAN_GAP",
        "ACTIVE_CONSTRAINTS", "REGRESSION_WARNINGS", "DIVERSITY_MAP",
        # Backwards-compat: also stop at old field names if present
        "BOTTLENECK_DIAGNOSIS", "GOAL_VIOLATIONS",
    ]

    def _get(field_name: str) -> str:
        others = [f for f in _SHARED_FIELDS if f != field_name]
        others.extend(["WORKER", "==="])
        return _extract_section(shared_text, field_name, others)

    shared.semantic_contract = _get("SEMANTIC_CONTRACT")
    shared.optimal_path = _get("OPTIMAL_PATH")
    shared.current_plan_gap = _get("CURRENT_PLAN_GAP")
    shared.active_constraints = _get("ACTIVE_CONSTRAINTS")
    shared.regression_warnings = _get("REGRESSION_WARNINGS")
    shared.diversity_map = _get("DIVERSITY_MAP")

    # Backwards-compat: populate old field aliases for pipeline code
    # that reads .bottleneck_diagnosis and .goal_violations
    shared.bottleneck_diagnosis = shared.optimal_path or _get("BOTTLENECK_DIAGNOSIS")
    shared.goal_violations = shared.current_plan_gap or _get("GOAL_VIOLATIONS")

    return shared


def _parse_workers(text: str) -> List[BriefingWorker]:
    """Extract per-worker briefing sections."""
    workers: List[BriefingWorker] = []

    worker_blocks = re.split(
        r'===\s*WORKER\s+(\d+)\s+BRIEFING\s*(?:\(.*?\))?\s*===',
        text, flags=re.IGNORECASE,
    )

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

    _WORKER_FIELDS = [
        "STRATEGY", "ROLE", "PRIMARY_FAMILY", "APPROACH",
        "TARGET_QUERY_MAP", "NODE_CONTRACTS",
        "EXAMPLES", "EXAMPLE_ADAPTATION", "HAZARD_FLAGS",
        "EXPLORATION_TYPE", "HYPOTHESIS_TAG", "UNCOVERED_FAMILY",
        # Backwards-compat old field names
        "COST_REGION", "RISK_LEVEL", "GOAL_ADDRESSED",
        "TARGET_LOGICAL_TREE", "CONSTRAINT_OVERRIDE", "OVERRIDE_REASONING",
    ]

    def _stops_for(field_name: str) -> List[str]:
        others = [f for f in _WORKER_FIELDS if f != field_name]
        others.extend(["===", "WORKER"])
        return others

    # Single-line fields
    w.strategy = _extract_field(block, "STRATEGY") or f"strategy_{worker_id}"
    w.role = _extract_field(block, "ROLE")
    w.primary_family = _extract_field(block, "PRIMARY_FAMILY")
    w.approach = _extract_field(block, "APPROACH") or _extract_section(block, "APPROACH", _stops_for("APPROACH"))
    w.exploration_type = _extract_field(block, "EXPLORATION_TYPE")
    w.hypothesis_tag = _extract_field(block, "HYPOTHESIS_TAG")
    w.uncovered_family = _extract_field(block, "UNCOVERED_FAMILY")

    # Backwards-compat single-line fields
    w.cost_region = _extract_field(block, "COST_REGION")
    w.risk_level = _extract_field(block, "RISK_LEVEL")
    w.goal_addressed = _extract_field(block, "GOAL_ADDRESSED")
    w.constraint_override = _extract_field(block, "CONSTRAINT_OVERRIDE")
    w.override_reasoning = _extract_field(block, "OVERRIDE_REASONING")

    # Examples (comma-separated)
    examples_raw = _extract_field(block, "EXAMPLES")
    w.examples = [ex.strip() for ex in examples_raw.split(",") if ex.strip()]
    if len(w.examples) > 3:
        w.examples = w.examples[:3]

    # Multi-line fields
    w.target_query_map = _extract_section(block, "TARGET_QUERY_MAP", _stops_for("TARGET_QUERY_MAP"))
    w.node_contracts = _extract_section(block, "NODE_CONTRACTS", _stops_for("NODE_CONTRACTS"))

    # Compose target_logical_tree from new fields (backwards compat for worker prompt)
    parts = []
    if w.target_query_map:
        parts.append(f"TARGET_QUERY_MAP:\n{w.target_query_map}")
    if w.node_contracts:
        parts.append(f"NODE_CONTRACTS:\n{w.node_contracts}")
    w.target_logical_tree = "\n\n".join(parts)

    # Also try old-style TARGET_LOGICAL_TREE if new fields empty
    if not w.target_logical_tree:
        old_tree = _extract_section(block, "TARGET_LOGICAL_TREE", _stops_for("TARGET_LOGICAL_TREE"))
        old_contracts = _extract_section(block, "NODE_CONTRACTS", _stops_for("NODE_CONTRACTS"))
        parts = []
        if old_tree:
            parts.append(f"TARGET_LOGICAL_TREE:\n{old_tree}")
        if old_contracts:
            parts.append(f"NODE_CONTRACTS:\n{old_contracts}")
        w.target_logical_tree = "\n\n".join(parts)

    w.example_adaptation = _extract_section(block, "EXAMPLE_ADAPTATION", _stops_for("EXAMPLE_ADAPTATION"))
    w.hazard_flags = _extract_section(block, "HAZARD_FLAGS", _stops_for("HAZARD_FLAGS"))

    return w
