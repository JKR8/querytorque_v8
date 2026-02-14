"""Scenario Cards — resource envelopes and failure definitions.

Scenario cards define the target environment constraints.
They are engine-agnostic and reusable across different engines.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

_CARDS_DIR = Path(__file__).parent
_card_cache: Dict[str, Dict[str, Any]] = {}


def load_scenario_card(name: str) -> Optional[Dict[str, Any]]:
    """Load a scenario card by name.

    Args:
        name: Card name (e.g., 'xsmall_survival', 'postgres_small_instance').
              Can include or omit .yaml extension.

    Returns:
        Parsed YAML dict, or None if not found. Cached after first load.
    """
    if name in _card_cache:
        return _card_cache[name]

    filename = name if name.endswith(".yaml") else f"{name}.yaml"
    card_path = _CARDS_DIR / filename
    if not card_path.exists():
        logger.warning(f"Scenario card not found: {card_path}")
        return None

    card = yaml.safe_load(card_path.read_text())
    _card_cache[name] = card
    return card


def list_scenario_cards() -> List[str]:
    """List all available scenario card names."""
    return [
        p.stem for p in _CARDS_DIR.glob("*.yaml")
        if not p.name.startswith("_")
    ]


def detect_scenario_from_profile(
    engine: str,
    resource_envelope: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Auto-detect the best matching scenario card for the given profile.

    Args:
        engine: Engine name (postgres, duckdb, snowflake)
        resource_envelope: Optional resource info (memory, cpu, etc.)

    Returns:
        Scenario card name, or None if no match.
    """
    if engine in ("postgres", "postgresql"):
        return "postgres_small_instance"
    elif engine == "duckdb":
        return "duckdb_embedded"
    elif engine == "snowflake":
        return "xsmall_survival"
    return None


def render_scenario_for_prompt(card: Dict[str, Any]) -> str:
    """Render scenario card as text for LLM prompt injection.

    Args:
        card: Parsed scenario card dict.

    Returns:
        Formatted text block.
    """
    lines = [f"[SCENARIO: {card.get('name', 'unknown')}]"]
    lines.append(card.get("description", "").strip())
    lines.append("")

    # Resource envelope
    envelope = card.get("resource_envelope", {})
    if envelope:
        lines.append("RESOURCE ENVELOPE:")
        for key, val in envelope.items():
            lines.append(f"  {key}: {val}")
        lines.append("")

    # Failure definitions
    failures = card.get("failure_definitions", [])
    if failures:
        lines.append("FAILURE DEFINITIONS:")
        for f in failures:
            severity = f.get("severity", "warning").upper()
            lines.append(
                f"  [{severity}] {f.get('metric', '?')} {f.get('threshold', '?')}"
            )
        lines.append("")

    # Strategy priorities
    priorities = card.get("strategy_priorities", [])
    if priorities:
        lines.append("STRATEGY PRIORITIES (ordered):")
        for i, p in enumerate(priorities, 1):
            lines.append(f"  {i}. {p}")
        lines.append("")

    # Strategy avoid
    avoid = card.get("strategy_avoid", [])
    if avoid:
        lines.append("AVOID (will fail on this envelope):")
        for a in avoid:
            lines.append(f"  - {a}")

    return "\n".join(lines)
