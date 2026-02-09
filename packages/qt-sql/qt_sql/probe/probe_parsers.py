"""Probe response parsers — extract ATTACK blocks and DISCOVERY_SUMMARY.

Follows the same fault-tolerant parsing pattern as swarm_parsers.py:
- Strip <reasoning>...</reasoning> blocks (deepseek-reasoner)
- Split on ATTACK_N: headers
- Extract fields via regex
- Extract SQL from ```sql``` code blocks
"""

from __future__ import annotations

import logging
import re
from typing import List, Optional, Tuple

from .schemas import AttackResult, DiscoverySummary

logger = logging.getLogger(__name__)


def parse_probe_response(
    response: str,
) -> Tuple[List[AttackResult], Optional[DiscoverySummary]]:
    """Parse ATTACK_N blocks and DISCOVERY_SUMMARY from probe response.

    Args:
        response: Raw LLM response text.

    Returns:
        Tuple of (list of AttackResults, DiscoverySummary or None).
    """
    # Strip <reasoning>...</reasoning> blocks (deepseek-reasoner)
    stripped = re.sub(
        r"<reasoning>.*?</reasoning>", "", response, flags=re.DOTALL
    ).strip()

    # Parse attack blocks
    attacks = _parse_attack_blocks(stripped)

    # Parse discovery summary
    discovery = _parse_discovery_summary(stripped)

    return attacks, discovery


def _parse_attack_blocks(text: str) -> List[AttackResult]:
    """Split text on ATTACK_N: headers and parse each block."""
    attacks: List[AttackResult] = []

    # Split by ATTACK_N: headers (with optional whitespace)
    blocks = re.split(r"ATTACK_(\d+)\s*:", text, flags=re.IGNORECASE)

    # blocks alternates: [preamble, "1", block1, "2", block2, ...]
    for i in range(1, len(blocks) - 1, 2):
        try:
            attack_id = int(blocks[i])
        except ValueError:
            continue

        block = blocks[i + 1]
        attack = _parse_attack_block(block, attack_id)
        if attack is not None:
            attacks.append(attack)

    return attacks


def _parse_attack_block(block: str, attack_id: int) -> Optional[AttackResult]:
    """Extract fields from one ATTACK block.

    Expected fields:
        TARGET_NODE: ...
        GAP_HYPOTHESIS: ...
        STRUCTURAL_PRECONDITIONS: ...
        MECHANISM: ...
        EXPECTED_PLAN_CHANGE: ...
        SEMANTIC_RISK: ...
        SQL: ```sql ... ```
    """
    target_node = _extract_field(block, "TARGET_NODE")
    gap_hypothesis = _extract_field(block, "GAP_HYPOTHESIS")
    structural_preconditions = _extract_multiline_field(
        block, "STRUCTURAL_PRECONDITIONS",
        ["MECHANISM", "EXPECTED_PLAN_CHANGE", "SEMANTIC_RISK", "SQL", "ATTACK_"],
    )
    mechanism = _extract_multiline_field(
        block, "MECHANISM",
        ["EXPECTED_PLAN_CHANGE", "SEMANTIC_RISK", "SQL", "ATTACK_"],
    )
    expected_plan_change = _extract_multiline_field(
        block, "EXPECTED_PLAN_CHANGE",
        ["SEMANTIC_RISK", "SQL", "ATTACK_"],
    )
    semantic_risk = _extract_multiline_field(
        block, "SEMANTIC_RISK",
        ["SQL", "ATTACK_", "DISCOVERY_SUMMARY"],
    )
    optimized_sql = _extract_attack_sql(block)

    if not optimized_sql:
        logger.warning(f"ATTACK_{attack_id}: no SQL found, skipping")
        return None

    return AttackResult(
        attack_id=attack_id,
        target_node=target_node or "",
        gap_hypothesis=gap_hypothesis or "",
        structural_preconditions=structural_preconditions or "",
        mechanism=mechanism or "",
        expected_plan_change=expected_plan_change or "",
        semantic_risk=semantic_risk or "",
        optimized_sql=optimized_sql,
    )


def _extract_attack_sql(block: str) -> str:
    """Extract SQL from ```sql``` code block within an ATTACK.

    Looks for the LAST ```sql ... ``` block in the attack (the LLM
    sometimes includes before/after examples, we want the rewrite).
    """
    # Find all ```sql ... ``` blocks
    sql_blocks = re.findall(
        r"```sql\s*\n(.*?)```", block, re.DOTALL | re.IGNORECASE
    )
    if sql_blocks:
        # Use the last SQL block (most likely the rewrite)
        return sql_blocks[-1].strip()

    # Fallback: look for SQL: field followed by code block
    sql_match = re.search(
        r"SQL\s*:\s*```\s*\n?(.*?)```", block, re.DOTALL | re.IGNORECASE
    )
    if sql_match:
        return sql_match.group(1).strip()

    # Final fallback: any ``` ... ``` block
    any_block = re.findall(r"```\s*\n(.*?)```", block, re.DOTALL)
    if any_block:
        return any_block[-1].strip()

    return ""


def _extract_field(text: str, field_name: str) -> str:
    """Extract a single-line field value: FIELD_NAME: <value>."""
    pattern = rf"{field_name}\s*:\s*(.+?)(?:\n|$)"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip().strip("[]")
    return ""


def _extract_multiline_field(
    text: str,
    field_name: str,
    stop_markers: List[str],
) -> str:
    """Extract a multi-line field, stopping at next known field."""
    stop_patterns = []
    for marker in stop_markers:
        stop_patterns.append(re.escape(marker))
    stop_re = "|".join(stop_patterns) if stop_patterns else "$"

    pattern = rf"{re.escape(field_name)}\s*:\s*\n?(.*?)(?=(?:{stop_re})\s*:|$)"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip().strip("[]")
    return ""


def _parse_discovery_summary(text: str) -> Optional[DiscoverySummary]:
    """Parse DISCOVERY_SUMMARY section from probe response.

    Expected format:
        DISCOVERY_SUMMARY:
          NEW_GAPS: [gap1, gap2]
          EXTENDED_GAPS: [gap1]
          NEGATIVE_RESULTS: [result1, result2]
    """
    # Find the DISCOVERY_SUMMARY section
    ds_match = re.search(
        r"DISCOVERY_SUMMARY\s*:\s*\n?(.*?)$",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if not ds_match:
        return None

    ds_text = ds_match.group(1)

    new_gaps = _parse_list_field(ds_text, "NEW_GAPS")
    extended_gaps = _parse_list_field(ds_text, "EXTENDED_GAPS")
    negative_results = _parse_list_field(ds_text, "NEGATIVE_RESULTS")

    return DiscoverySummary(
        new_gaps=new_gaps,
        extended_gaps=extended_gaps,
        negative_results=negative_results,
    )


def _parse_list_field(text: str, field_name: str) -> List[str]:
    """Parse a field that contains a bracketed list or comma-separated values.

    Handles formats:
        FIELD: [item1, item2, item3]
        FIELD: item1
        FIELD:
          - item1
          - item2
    """
    # Try bracketed list first
    match = re.search(
        rf"{field_name}\s*:\s*\[(.*?)\]",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if match:
        items = match.group(1).split(",")
        return [item.strip().strip("'\"") for item in items if item.strip()]

    # Try bullet list
    match = re.search(
        rf"{field_name}\s*:\s*\n((?:\s*-\s*.+\n?)+)",
        text,
        re.IGNORECASE,
    )
    if match:
        lines = match.group(1).strip().split("\n")
        items = []
        for line in lines:
            line = line.strip()
            if line.startswith("-"):
                items.append(line[1:].strip())
        return items

    # Try single-line value
    match = re.search(
        rf"{field_name}\s*:\s*(.+?)(?:\n|$)",
        text,
        re.IGNORECASE,
    )
    if match:
        val = match.group(1).strip()
        if val and val.lower() not in ("none", "n/a", "[]"):
            return [val]

    return []
