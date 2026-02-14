"""Universal Doctrine — engine-agnostic optimization philosophy.

Provides shared vocabulary (bottleneck taxonomy), safety rules
(hallucination prevention, correctness constraints), and worker
diversity enforcement that apply to ALL engines.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml

logger = logging.getLogger(__name__)

_DOCTRINE_PATH = Path(__file__).parent / "universal_doctrine.yaml"
_cached_doctrine: Dict[str, Any] | None = None


def load_doctrine() -> Dict[str, Any]:
    """Load the universal doctrine YAML. Cached after first call."""
    global _cached_doctrine
    if _cached_doctrine is not None:
        return _cached_doctrine
    _cached_doctrine = yaml.safe_load(_DOCTRINE_PATH.read_text())
    return _cached_doctrine


def get_bottleneck_taxonomy() -> Dict[str, str]:
    """Return the 9 universal bottleneck labels with descriptions."""
    doctrine = load_doctrine()
    return doctrine.get("bottleneck_taxonomy", {})


def get_hallucination_rules() -> Dict[str, str]:
    """Return hallucination prevention rules."""
    doctrine = load_doctrine()
    return doctrine.get("hallucination_rules", {})


def get_correctness_constraints() -> Dict[str, str]:
    """Return the 4 correctness constraints."""
    doctrine = load_doctrine()
    return doctrine.get("correctness_constraints", {})


def get_worker_diversity() -> Dict[str, Any]:
    """Return worker diversity rules and family definitions."""
    doctrine = load_doctrine()
    return doctrine.get("worker_diversity", {})


def get_global_guards() -> List[str]:
    """Return the list of global guard rules (hard-stop regression prevention)."""
    doctrine = load_doctrine()
    return doctrine.get("global_guards", [])


def get_principles() -> Dict[str, str]:
    """Return the 4 optimization principles."""
    doctrine = load_doctrine()
    return doctrine.get("principles", {})


def get_engine_signal_mapping(engine: str, bottleneck: str) -> str | None:
    """Look up which engine-specific signal maps to a universal bottleneck.

    Args:
        engine: Engine name (postgres, duckdb, snowflake)
        bottleneck: Universal bottleneck label (spill, bad_pruning, etc.)

    Returns:
        Engine-specific signal description, or None if no mapping.
    """
    doctrine = load_doctrine()
    mappings = doctrine.get("engine_signal_mappings", {})
    bottleneck_map = mappings.get(bottleneck, {})
    return bottleneck_map.get(engine)


def render_doctrine_for_prompt() -> str:
    """Render doctrine as text suitable for LLM prompt injection.

    Returns a compact text block with principles, taxonomy, and guards.
    """
    doctrine = load_doctrine()
    lines = ["[UNIVERSAL DOCTRINE]", ""]

    # Mission
    lines.append(f"MISSION: {doctrine.get('mission', '').strip()}")
    lines.append("")

    # Principles
    lines.append("OPTIMIZATION PRINCIPLES:")
    for name, text in doctrine.get("principles", {}).items():
        lines.append(f"  {name}: {text.strip()}")
    lines.append("")

    # Bottleneck taxonomy
    lines.append("BOTTLENECK TAXONOMY (universal labels):")
    for label, desc in doctrine.get("bottleneck_taxonomy", {}).items():
        lines.append(f"  {label}: {desc}")
    lines.append("")

    # Global guards
    guards = doctrine.get("global_guards", [])
    if guards:
        lines.append("GLOBAL GUARDS (hard-stop rules):")
        for g in guards:
            lines.append(f"  - {g}")
        lines.append("")

    # Correctness constraints
    lines.append("CORRECTNESS CONSTRAINTS:")
    for name, text in doctrine.get("correctness_constraints", {}).items():
        label = name.upper()
        lines.append(f"  {label}: {text.strip()}")

    return "\n".join(lines)
