"""Canonical normalization helpers for knowledge artifacts.

This module defines the runtime canonical vocabulary for:
- dialect identifiers
- gap identifiers
- transform identifiers
- family identifiers (A-F)

Loaders should normalize incoming data through these helpers so legacy
artifacts remain readable while new writes converge on one vocabulary.
"""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Set

# Canonical dialect names used by runtime knowledge paths.
_DIALECT_ALIASES: Dict[str, str] = {
    "duckdb": "duckdb",
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "pg": "postgresql",
    "snowflake": "snowflake",
    "databricks": "databricks",
}

# Gap alias mapping (legacy -> canonical).
_GAP_ID_ALIASES: Dict[str, str] = {
    "INTERSECT_MATERIALIZE_BOTH": "INTERSECT_MATERIALIZATION",
    "CORRELATED_SCALAR_AGGREGATION_FAILURE": "CORRELATED_SUBQUERY_PARALYSIS",
}

# Transform alias mapping (legacy -> canonical).
_TRANSFORM_ID_ALIASES: Dict[str, str] = {
    "explicit_join_materialized": "date_cte_explicit_join",
    "pg_date_cte_explicit_join": "date_cte_explicit_join",
    "pg_dimension_prefetch_star": "dimension_prefetch_star",
    "pg_materialized_dimension_fact_prefilter": "materialized_dimension_fact_prefilter",
    "predicate_transitivity": "sf_sk_pushdown_union_all",
    "sk_range_pushdown": "sf_sk_pushdown_union_all",
    "pushdown": "early_filter",
    "semantic_rewrite": "materialize_cte",
    "join_rewrite": "decorrelate",
}

# Some legacy artifacts use "postgres" directory naming for examples.
_EXAMPLE_DIR_BY_DIALECT: Dict[str, str] = {
    "postgresql": "postgres",
}


def normalize_dialect(value: Optional[str], default: str = "duckdb") -> str:
    """Return canonical dialect name."""
    if not value:
        return default
    key = str(value).strip().lower()
    return _DIALECT_ALIASES.get(key, key)


def dialect_example_dir(value: Optional[str], default: str = "duckdb") -> str:
    """Return directory name used under qt_sql/examples for a dialect."""
    dialect = normalize_dialect(value, default=default)
    return _EXAMPLE_DIR_BY_DIALECT.get(dialect, dialect)


def normalize_gap_id(gap_id: Optional[str]) -> Optional[str]:
    """Normalize a gap identifier to canonical ID."""
    if not gap_id:
        return gap_id
    key = str(gap_id).strip()
    return _GAP_ID_ALIASES.get(key, key)


def normalize_transform_id(transform_id: Optional[str]) -> Optional[str]:
    """Normalize a transform identifier to canonical ID."""
    if not transform_id:
        return transform_id
    key = str(transform_id).strip()
    return _TRANSFORM_ID_ALIASES.get(key, key)


def _expand_transform_tokens(value: Optional[str]) -> List[str]:
    """Expand free-form transform text into canonical transform IDs.

    Handles:
    - "decorrelate: explanation text" -> ["decorrelate"]
    - "date_cte_isolate + early_filter" -> ["date_cte_isolate", "early_filter"]
    """
    if not value:
        return []
    text = str(value).strip()
    base = text.split(":", 1)[0].strip()
    if not base:
        return []
    parts = [p.strip() for p in re.split(r"\s*\+\s*", base) if p.strip()]
    return [normalize_transform_id(p) for p in parts if normalize_transform_id(p)]


def _normalize_transform_list(values: Optional[List[Any]]) -> List[str]:
    """Normalize and deduplicate transform IDs while preserving order."""
    if not isinstance(values, list):
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for v in values:
        for t in _expand_transform_tokens(str(v) if v is not None else None):
            if t not in seen:
                seen.add(t)
                out.append(t)
    return out


def normalize_family_id(value: Optional[str]) -> Optional[str]:
    """Normalize family to canonical A-F ID when possible."""
    if value is None:
        return None
    family = str(value).strip().upper()
    if family in {"A", "B", "C", "D", "E", "F"}:
        return family
    return None


def normalize_engine_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize engine profile payload to canonical keys and IDs.

    Keeps backward-compat aliases but adds canonical ``dialect`` key.
    """
    out = deepcopy(profile)

    # Canonical dialect key.
    dialect = normalize_dialect(out.get("dialect") or out.get("engine"))
    out["dialect"] = dialect
    out.pop("engine", None)

    gaps = out.get("gaps")
    if isinstance(gaps, list):
        for gap in gaps:
            if not isinstance(gap, dict):
                continue
            gap_id = normalize_gap_id(gap.get("id"))
            if gap_id:
                gap["id"] = gap_id

    return out


def normalize_constraint_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize one constraint JSON payload."""
    out = deepcopy(record)
    if out.get("dialect") or out.get("engine"):
        out["dialect"] = normalize_dialect(out.get("dialect") or out.get("engine"))
    out.pop("engine", None)
    return out


def normalize_transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize one transform catalog entry."""
    out = deepcopy(record)

    transform_id = normalize_transform_id(out.get("id"))
    if transform_id:
        out["id"] = transform_id

    gap_id = normalize_gap_id(out.get("gap"))
    if gap_id:
        out["gap"] = gap_id

    engines = out.get("engines")
    if isinstance(engines, list):
        out["engines"] = [normalize_dialect(e) for e in engines if e]

    family = normalize_family_id(out.get("family"))
    if family:
        out["family"] = family

    return out


def normalize_transform_catalog(
    transforms: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Normalize a transforms catalog list."""
    return [normalize_transform_record(t) for t in transforms if isinstance(t, dict)]


def build_transform_index(
    transforms: Iterable[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Build normalized transform lookup keyed by canonical transform ID."""
    index: Dict[str, Dict[str, Any]] = {}
    for t in normalize_transform_catalog(transforms):
        tid = t.get("id")
        if isinstance(tid, str) and tid:
            index[tid] = t
    return index


def normalize_example_record(
    record: Dict[str, Any],
    *,
    default_dialect: str = "duckdb",
    transform_index: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Normalize one example/regression JSON payload to canonical references."""
    out = deepcopy(record)

    out["dialect"] = normalize_dialect(
        out.get("dialect") or out.get("engine"), default=default_dialect
    )
    out.pop("engine", None)

    if isinstance(out.get("transforms"), list):
        out["transforms"] = _normalize_transform_list(out.get("transforms"))

    if isinstance(out.get("transforms_applied"), list):
        out["transforms_applied"] = _normalize_transform_list(out.get("transforms_applied"))

    transform_attempted = out.get("transform_attempted")
    if isinstance(transform_attempted, str):
        attempted = _expand_transform_tokens(transform_attempted)
        if attempted:
            out["transform_attempted"] = attempted[0]

    example = out.get("example")
    if isinstance(example, dict):
        if isinstance(example.get("transforms"), list):
            example["transforms"] = _normalize_transform_list(example.get("transforms"))
        output = example.get("output")
        if isinstance(output, dict):
            rewrite_sets = output.get("rewrite_sets")
            if isinstance(rewrite_sets, list):
                for rs in rewrite_sets:
                    if isinstance(rs, dict) and isinstance(rs.get("transform"), str):
                        tokens = _expand_transform_tokens(rs["transform"])
                        if tokens:
                            rs["transform"] = tokens[0]

    patch_plan = out.get("patch_plan")
    if isinstance(patch_plan, dict):
        if patch_plan.get("dialect"):
            patch_plan["dialect"] = normalize_dialect(patch_plan.get("dialect"))

    dag_example = out.get("dag_example")
    if isinstance(dag_example, dict):
        if dag_example.get("dialect"):
            dag_example["dialect"] = normalize_dialect(dag_example.get("dialect"))

    family = normalize_family_id(out.get("family"))
    if family:
        out["family"] = family

    if isinstance(out.get("families"), list):
        families = sorted(
            {f for f in (normalize_family_id(v) for v in out["families"]) if f}
        )
        if families:
            out["families"] = families

    transform_ids: Set[str] = set()
    for key in ("transforms", "transforms_applied"):
        values = out.get(key)
        if isinstance(values, list):
            for t in values:
                if isinstance(t, str) and t:
                    transform_ids.add(t)

    if isinstance(example, dict):
        values = example.get("transforms")
        if isinstance(values, list):
            for t in values:
                if isinstance(t, str) and t:
                    transform_ids.add(t)
        output = example.get("output")
        if isinstance(output, dict):
            rewrite_sets = output.get("rewrite_sets")
            if isinstance(rewrite_sets, list):
                for rs in rewrite_sets:
                    if isinstance(rs, dict) and isinstance(rs.get("transform"), str):
                        transform_ids.add(rs["transform"])

    if isinstance(transform_attempted, str) and out.get("transform_attempted"):
        transform_ids.add(out["transform_attempted"])

    if transform_ids and (not isinstance(out.get("transforms"), list)):
        out["transforms"] = sorted(transform_ids)

    if transform_index:
        families: Set[str] = set()
        gap_ids: Set[str] = set()
        for t_id in sorted(transform_ids):
            t = transform_index.get(t_id)
            if not t:
                continue
            fam = normalize_family_id(t.get("family"))
            if fam:
                families.add(fam)
            gap_id = normalize_gap_id(t.get("gap"))
            if gap_id:
                gap_ids.add(gap_id)

        if families:
            out["families"] = sorted(families)
            out["family"] = sorted(families)[0]
        if gap_ids:
            out["gap_ids"] = sorted(gap_ids)

    return out


def normalize_tag_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize one similarity-tags index entry."""
    out = deepcopy(entry)
    meta = out.get("metadata")
    if not isinstance(meta, dict):
        meta = {}
        out["metadata"] = meta

    dialect = normalize_dialect(
        out.get("dialect") or out.get("engine") or meta.get("dialect") or meta.get("engine")
    )
    out["dialect"] = dialect
    out.pop("engine", None)

    transforms = meta.get("transforms")
    if isinstance(transforms, list):
        meta["transforms"] = [normalize_transform_id(t) for t in transforms if t]
        if meta["transforms"]:
            meta["winning_transform"] = meta["transforms"][0]

    meta["dialect"] = dialect
    meta.pop("engine", None)
    return out
