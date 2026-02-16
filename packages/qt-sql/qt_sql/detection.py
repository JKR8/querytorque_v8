"""Transform detection system.

Given a SQL query, extracts precondition features and matches them against
the transform catalog (transforms.json) to rank candidate transforms by
feature overlap.

Usage:
    from qt_sql.detection import detect_transforms, load_transforms

    transforms = load_transforms()
    matches = detect_transforms(sql, transforms, dialect_filter="duckdb")
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
from qt_sql.knowledge.normalization import (
    normalize_dialect,
    normalize_gap_id,
    normalize_transform_catalog,
)

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
        data = json.load(f)
    return normalize_transform_catalog(data)


def detect_transforms(
    sql: str,
    transforms: List[Dict[str, Any]],
    dialect_filter: Optional[str] = None,
    engine: Optional[str] = None,
    dialect: str = "duckdb",
) -> List[TransformMatch]:
    """Detect applicable transforms for a SQL query.

    Extracts precondition features from the SQL, then scores each transform
    by the ratio of matched features to required features.

    Args:
        sql: Original SQL query text
        transforms: Transform catalog from load_transforms()
        dialect_filter: Canonical dialect filter for transform support
                        (e.g. "duckdb", "postgresql"). None means no filter.
        engine: Deprecated alias for ``dialect_filter`` (kept for compatibility).
        dialect: SQL dialect for parsing ("duckdb" or "postgres")

    Returns:
        List of TransformMatch sorted by descending overlap_ratio
    """
    norm_parse_dialect = normalize_dialect(dialect)
    parse_dialect = "postgres" if norm_parse_dialect == "postgresql" else norm_parse_dialect

    features = extract_precondition_features(sql, dialect=parse_dialect)

    matches: List[TransformMatch] = []

    filter_value = dialect_filter if dialect_filter is not None else engine
    norm_filter = normalize_dialect(filter_value) if filter_value else None

    for t in transforms:
        # Dialect filter
        t_engines = t.get("engines", [])
        if norm_filter and t_engines and norm_filter not in t_engines:
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
                gap=normalize_gap_id(t.get("gap")),
                engines=t_engines,
                contraindications=t.get("contraindications", []),
            )
        )

    # Sort by overlap ratio descending, then by more required features
    # (more specific match wins ties), then fewer missing features
    matches.sort(key=lambda m: (-m.overlap_ratio, -m.total_required, len(m.missing_features)))

    return matches
