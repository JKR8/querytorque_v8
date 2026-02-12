"""Transform detection system.

Given a SQL query, extracts precondition features and matches them against
the transform catalog (transforms.json) to rank candidate transforms by
feature overlap.

Usage:
    from qt_sql.detection import detect_transforms, load_transforms

    transforms = load_transforms()
    matches = detect_transforms(sql, transforms, engine="duckdb")
    for m in matches[:3]:
        print(f"{m.id}: {m.overlap_ratio:.0%} overlap")
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from qt_sql.tag_index import extract_precondition_features

logger = logging.getLogger(__name__)

# Default path to transforms catalog (master copy in knowledge/)
_DEFAULT_TRANSFORMS_PATH = (
    Path(__file__).resolve().parent / "knowledge" / "transforms.json"
)


@dataclass
class TransformMatch:
    """Result of matching a SQL query against a transform's preconditions."""

    id: str
    overlap_ratio: float  # len(matched) / len(required)
    matched_features: List[str]
    missing_features: List[str]
    total_required: int
    gap: Optional[str]
    engines: List[str]
    contraindications: List[Dict[str, Any]] = field(default_factory=list)


def load_transforms(path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Load the transform catalog from transforms.json.

    Args:
        path: Path to transforms.json. Uses default if None.

    Returns:
        List of transform dicts with precondition_features, engines, etc.
    """
    p = path or _DEFAULT_TRANSFORMS_PATH
    with open(p) as f:
        return json.load(f)


def detect_transforms(
    sql: str,
    transforms: List[Dict[str, Any]],
    engine: Optional[str] = None,
    dialect: str = "duckdb",
) -> List[TransformMatch]:
    """Detect applicable transforms for a SQL query.

    Extracts precondition features from the SQL, then scores each transform
    by the ratio of matched features to required features.

    Args:
        sql: Original SQL query text
        transforms: Transform catalog from load_transforms()
        engine: Filter to transforms supporting this engine (e.g. "duckdb").
                None means no engine filter.
        dialect: SQL dialect for parsing ("duckdb" or "postgres")

    Returns:
        List of TransformMatch sorted by descending overlap_ratio
    """
    features = extract_precondition_features(sql, dialect=dialect)

    matches: List[TransformMatch] = []

    for t in transforms:
        # Engine filter
        t_engines = t.get("engines", [])
        if engine and t_engines and engine not in t_engines:
            continue

        required = set(t.get("precondition_features", []))
        if not required:
            continue

        matched = features & required
        missing = required - features

        overlap = len(matched) / len(required)

        matches.append(
            TransformMatch(
                id=t["id"],
                overlap_ratio=overlap,
                matched_features=sorted(matched),
                missing_features=sorted(missing),
                total_required=len(required),
                gap=t.get("gap"),
                engines=t_engines,
                contraindications=t.get("contraindications", []),
            )
        )

    # Sort by overlap ratio descending, then by more required features
    # (more specific match wins ties), then fewer missing features
    matches.sort(key=lambda m: (-m.overlap_ratio, -m.total_required, len(m.missing_features)))

    return matches
