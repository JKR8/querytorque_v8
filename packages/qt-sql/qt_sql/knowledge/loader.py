"""
Loader for consolidated engine dossiers.

Usage:
    from qt_sql.knowledge.loader import load_engine_dossier
    dossier = load_engine_dossier("duckdb")
"""
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

KNOWLEDGE_DIR = Path(__file__).resolve().parent

DOSSIER_FILES = {
    "duckdb": KNOWLEDGE_DIR / "duckdb_dossier.json",
    "postgresql": KNOWLEDGE_DIR / "postgresql_dossier.json",
}

REQUIRED_TOP_LEVEL = {"schema_version", "engine", "strengths", "gaps", "global_guard_rails", "transform_catalog"}
REQUIRED_GAP_FIELDS = {"id", "priority", "what", "why", "opportunity", "gold_examples", "regressions", "guard_rails"}


class DossierValidationError(Exception):
    pass


def _validate(dossier: dict[str, Any]) -> list[str]:
    """Validate dossier against schema v3.0. Returns list of warnings (empty = valid)."""
    warnings = []

    # Top-level fields
    missing = REQUIRED_TOP_LEVEL - set(dossier.keys())
    if missing:
        raise DossierValidationError(f"Missing top-level fields: {missing}")

    if dossier.get("schema_version") != "3.0":
        warnings.append(f"Expected schema_version 3.0, got {dossier.get('schema_version')}")

    # Gaps
    for gap in dossier.get("gaps", []):
        gap_missing = REQUIRED_GAP_FIELDS - set(gap.keys())
        if gap_missing:
            warnings.append(f"Gap {gap.get('id', '?')}: missing fields {gap_missing}")

        for ex in gap.get("gold_examples", []):
            if "id" not in ex or "original_sql" not in ex:
                warnings.append(f"Gap {gap['id']}: gold example missing id or original_sql")

        for reg in gap.get("regressions", []):
            if "id" not in reg or "regression_mechanism" not in reg:
                warnings.append(f"Gap {gap['id']}: regression missing id or regression_mechanism")

        for rail in gap.get("guard_rails", []):
            if "id" not in rail or "instruction" not in rail:
                warnings.append(f"Gap {gap['id']}: guard_rail missing id or instruction")

    # Global guard rails
    for rail in dossier.get("global_guard_rails", []):
        if "id" not in rail or "instruction" not in rail:
            warnings.append(f"Global guard_rail missing id or instruction")

    return warnings


def _normalize_dialect(dialect: str) -> str:
    dialect = dialect.lower()
    if dialect in ("postgres", "pg"):
        return "postgresql"
    return dialect


@lru_cache(maxsize=2)
def _load_dossier(dialect: str) -> dict[str, Any]:
    """Internal cached loader (dialect must be normalized)."""
    path = DOSSIER_FILES.get(dialect)
    if not path or not path.exists():
        raise FileNotFoundError(f"No dossier for dialect '{dialect}'. Available: {list(DOSSIER_FILES.keys())}")

    with open(path) as f:
        dossier = json.load(f)

    warnings = _validate(dossier)
    if warnings:
        import logging
        logger = logging.getLogger(__name__)
        for w in warnings:
            logger.warning(f"Dossier validation: {w}")

    return dossier


def load_engine_dossier(dialect: str) -> dict[str, Any]:
    """Load and validate an engine dossier.

    Args:
        dialect: "duckdb", "postgresql", "postgres", or "pg"

    Returns:
        The full dossier dict.

    Raises:
        FileNotFoundError: If dossier file doesn't exist.
        DossierValidationError: If required fields are missing.
    """
    return _load_dossier(_normalize_dialect(dialect))


def get_gap_by_id(dossier: dict, gap_id: str) -> dict | None:
    """Look up a specific gap by ID."""
    for gap in dossier.get("gaps", []):
        if gap["id"] == gap_id:
            return gap
    return None


def get_transform_info(dossier: dict, transform: str) -> dict | None:
    """Look up transform catalog entry."""
    return dossier.get("transform_catalog", {}).get(transform)
