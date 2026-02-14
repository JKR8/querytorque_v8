"""Engine Pack loader -- declarative YAML knowledge bases per engine.

Engine packs complement (not replace) the existing:
- engine_profile_*.json files (detailed what_worked/what_didnt_work evidence)
- knowledge/{dialect}.md playbooks (LLM-optimized prompt content)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

_PACKS_DIR = Path(__file__).parent
_pack_cache: Dict[str, Dict[str, Any]] = {}

# Map engine names to pack files
_PACK_FILES = {
    "postgres": "postgres_17.yaml",
    "postgresql": "postgres_17.yaml",
    "duckdb": "duckdb_1_2.yaml",
    "snowflake": "snowflake_2025.yaml",
}


def load_engine_pack(engine: str) -> Optional[Dict[str, Any]]:
    """Load engine pack YAML for the given engine.

    Returns None if no pack exists for the engine.
    Cached after first load.
    """
    engine_key = engine.lower()
    if engine_key in _pack_cache:
        return _pack_cache[engine_key]

    filename = _PACK_FILES.get(engine_key)
    if not filename:
        logger.warning(f"No engine pack for engine: {engine}")
        return None

    pack_path = _PACKS_DIR / filename
    if not pack_path.exists():
        logger.warning(f"Engine pack file not found: {pack_path}")
        return None

    pack = yaml.safe_load(pack_path.read_text())
    _pack_cache[engine_key] = pack
    return pack


def get_capabilities(engine: str) -> Dict[str, Any]:
    """Return the capabilities section of the engine pack."""
    pack = load_engine_pack(engine)
    return pack.get("capabilities", {}) if pack else {}


def get_optimizer_profile(engine: str) -> Dict[str, Any]:
    """Return the optimizer_profile section of the engine pack."""
    pack = load_engine_pack(engine)
    return pack.get("optimizer_profile", {}) if pack else {}


def get_profile_signals(engine: str) -> Dict[str, Any]:
    """Return the profile_signals section of the engine pack."""
    pack = load_engine_pack(engine)
    return pack.get("profile_signals", {}) if pack else {}


def get_rewrite_playbook(engine: str) -> List[Dict[str, str]]:
    """Return the rewrite_playbook section of the engine pack."""
    pack = load_engine_pack(engine)
    return pack.get("rewrite_playbook", []) if pack else []


def get_config_boost_rules(engine: str) -> List[Dict[str, str]]:
    """Return the config_boost_rules section of the engine pack."""
    pack = load_engine_pack(engine)
    return pack.get("config_boost_rules", []) if pack else []


def get_validation_probes(engine: str) -> List[str]:
    """Return the validation section of the engine pack."""
    pack = load_engine_pack(engine)
    return pack.get("validation", []) if pack else []


def render_capabilities_for_prompt(engine: str) -> str:
    """Render engine capabilities as text for prompt injection."""
    caps = get_capabilities(engine)
    if not caps:
        return ""

    lines = ["[ENGINE CAPABILITIES]"]

    # Services
    services = caps.get("services", {})
    if services:
        lines.append("\nAVAILABLE SERVICES:")
        for name, info in services.items():
            if isinstance(info, dict) and info.get("exists"):
                lines.append(f"  {name}: {info.get('description', '')}")
                triggers = info.get("triggers", [])
                if triggers:
                    lines.append(f"    Triggers: {'; '.join(triggers[:3])}")

    # Hints
    hints = caps.get("hints", {})
    if hints:
        lines.append("\nOPTIMIZER HINTS:")
        for category, hint_list in hints.items():
            if isinstance(hint_list, list):
                lines.append(f"  {category}: {', '.join(hint_list[:5])}")

    return "\n".join(lines)


def render_optimizer_profile_for_prompt(engine: str) -> str:
    """Render optimizer profile as text for prompt injection."""
    profile = get_optimizer_profile(engine)
    if not profile:
        return ""

    lines = ["[OPTIMIZER PROFILE]"]

    handles_well = profile.get("handles_well", [])
    if handles_well:
        lines.append("\nHANDLES WELL (do NOT rewrite these patterns):")
        for item in handles_well:
            pattern = item.get("pattern", "")
            implication = item.get("implication", "")
            lines.append(f"  - {pattern}: {implication}")

    blind_spots = profile.get("blind_spots", [])
    if blind_spots:
        lines.append("\nBLIND SPOTS (optimizer misses these -- rewrite opportunities):")
        for item in blind_spots:
            pattern = item.get("pattern", "")
            opportunity = item.get("opportunity", "")
            lines.append(f"  - {pattern}: {opportunity}")

    return "\n".join(lines)
