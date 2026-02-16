"""Integrity checks for dialect-first knowledge artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Set

from qt_sql.knowledge.normalization import normalize_dialect


QT_SQL_DIR = Path(__file__).resolve().parent.parent / "qt_sql"
TRANSFORMS_PATH = QT_SQL_DIR / "knowledge" / "transforms.json"
EXAMPLES_DIR = QT_SQL_DIR / "examples"
CONSTRAINTS_DIR = QT_SQL_DIR / "constraints"


def _load_transform_index() -> Dict[str, Dict]:
    payload = json.loads(TRANSFORMS_PATH.read_text())
    return {t["id"]: t for t in payload if isinstance(t, dict) and t.get("id")}


def _example_transforms(record: Dict) -> Set[str]:
    refs: Set[str] = set()
    for key in ("transforms", "transforms_applied"):
        values = record.get(key)
        if isinstance(values, list):
            refs.update(v for v in values if isinstance(v, str) and v)

    if isinstance(record.get("transform_attempted"), str):
        refs.add(record["transform_attempted"])

    example = record.get("example")
    if isinstance(example, dict):
        values = example.get("transforms")
        if isinstance(values, list):
            refs.update(v for v in values if isinstance(v, str) and v)
        output = example.get("output")
        if isinstance(output, dict):
            rewrite_sets = output.get("rewrite_sets")
            if isinstance(rewrite_sets, list):
                for rs in rewrite_sets:
                    if isinstance(rs, dict) and isinstance(rs.get("transform"), str):
                        refs.add(rs["transform"])
    return refs


def test_engine_profiles_are_dialect_normalized() -> None:
    for path in sorted(CONSTRAINTS_DIR.glob("engine_profile_*.json")):
        payload = json.loads(path.read_text())
        expected = path.stem.replace("engine_profile_", "")
        assert payload.get("profile_type") == "engine_profile"
        assert payload.get("dialect") == expected
        assert "engine" not in payload


def test_constraints_use_dialect_not_engine() -> None:
    for path in sorted(CONSTRAINTS_DIR.glob("*.json")):
        payload = json.loads(path.read_text())
        assert "engine" not in payload
        if "dialect" in payload:
            assert payload["dialect"] == normalize_dialect(payload["dialect"])


def test_examples_reference_canonical_transforms_gaps_and_families() -> None:
    transform_index = _load_transform_index()
    all_transform_ids = set(transform_index)
    known_gap_ids = {t["gap"] for t in transform_index.values() if t.get("gap")}

    for path in sorted(EXAMPLES_DIR.glob("**/*.json")):
        payload = json.loads(path.read_text())
        assert "engine" not in payload

        rel = path.relative_to(EXAMPLES_DIR)
        dialect_dir = rel.parts[0]
        expected_dialect = normalize_dialect(
            "postgresql" if dialect_dir == "postgres" else dialect_dir
        )
        assert payload.get("dialect") == expected_dialect

        transforms = _example_transforms(payload)
        assert transforms, f"{path} has no transform references"
        assert transforms <= all_transform_ids, (
            f"{path} references unknown transforms: {sorted(transforms - all_transform_ids)}"
        )

        expected_families = {
            transform_index[t]["family"]
            for t in transforms
            if transform_index.get(t, {}).get("family")
        }
        if "regressions" not in path.parts:
            assert payload.get("family") in {"A", "B", "C", "D", "E", "F"}
            families = set(payload.get("families", []))
            assert payload.get("family") in families
            assert expected_families <= families

        expected_gaps = {
            transform_index[t]["gap"]
            for t in transforms
            if transform_index.get(t, {}).get("gap")
        }
        gap_ids = set(payload.get("gap_ids", []))
        if expected_gaps:
            assert expected_gaps <= gap_ids
        assert gap_ids <= known_gap_ids
