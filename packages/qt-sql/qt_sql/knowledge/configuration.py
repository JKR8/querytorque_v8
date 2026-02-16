"""Dialect-specific post-optimization configuration profiles."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from .normalization import normalize_dialect

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent / "config"


def get_config_path(dialect: str) -> Path:
    """Return config profile path for a dialect."""
    return CONFIG_DIR / f"{normalize_dialect(dialect)}.json"


def load_dialect_config(dialect: str) -> Optional[Dict[str, Any]]:
    """Load a dialect config profile from knowledge/config/{dialect}.json."""
    path = get_config_path(dialect)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except Exception as e:
        logger.warning("Failed to load dialect config %s: %s", path, e)
        return None

    data["dialect"] = normalize_dialect(data.get("dialect") or dialect)
    return data


def get_threshold(
    config: Optional[Dict[str, Any]],
    key: str,
    default: int,
) -> int:
    """Read an integer threshold from config.thresholds with a default."""
    if not isinstance(config, dict):
        return default
    thresholds = config.get("thresholds")
    if not isinstance(thresholds, dict):
        return default
    value = thresholds.get(key, default)
    try:
        return int(value)
    except Exception:
        return default
