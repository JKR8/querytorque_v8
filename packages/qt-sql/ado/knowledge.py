"""Knowledge retrieval for ADO.

This module retrieves gold examples and constraints for SQL optimization.
Uses tag-based similarity matching from the tag index, with fallback
to loading all local examples.

Local directories:
- ado/examples/*.json - Gold optimization examples
- ado/constraints/*.json - Safety constraints
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import yaml

logger = logging.getLogger(__name__)

# Directory paths
BASE_DIR = Path(__file__).resolve().parent
EXAMPLES_DIR = BASE_DIR / "examples"
CONSTRAINTS_DIR = BASE_DIR / "constraints"
MODELS_DIR = BASE_DIR / "models"
DSB_RULES_FILE = BASE_DIR / "knowledge_dsb.yaml"
DSB_QUERY_MAPPING_FILE = BASE_DIR / "dsb_query_rule_mapping.json"


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class GoldExample:
    """A verified gold example for optimization."""
    id: str
    name: str
    description: str
    verified_speedup: str
    example: dict[str, Any]


@dataclass
class Constraint:
    """A constraint learned from benchmark failures."""
    id: str
    severity: str
    description: str
    prompt_instruction: str
    observed_failures: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class RetrievalResult:
    """Result from knowledge retrieval."""
    gold_examples: list[GoldExample]
    constraints: list[Constraint]
    rationale: dict[str, Any]


@dataclass
class DSBRule:
    """A DSB-specific rule."""
    id: str
    description: str


# =============================================================================
# Loading Functions
# =============================================================================

def _load_examples() -> list[GoldExample]:
    """Load all examples from ado/examples/ and subdirectories."""
    if not EXAMPLES_DIR.exists():
        return []

    examples: list[GoldExample] = []
    for path in sorted(EXAMPLES_DIR.glob("**/*.json")):
        try:
            data = json.loads(path.read_text())
            examples.append(GoldExample(
                id=data.get("id", path.stem),
                name=data.get("name", data.get("id", path.stem)),
                description=data.get("description", ""),
                verified_speedup=data.get("verified_speedup", ""),
                example=data.get("example", {}),
            ))
        except Exception as e:
            logger.warning(f"Failed to load example {path}: {e}")

    return examples


def _load_constraints() -> list[Constraint]:
    """Load all constraints from ado/constraints/ directory."""
    if not CONSTRAINTS_DIR.exists():
        return []

    constraints: list[Constraint] = []
    for path in sorted(CONSTRAINTS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            constraints.append(Constraint(
                id=data.get("id", path.stem),
                severity=data.get("severity", "MEDIUM"),
                description=data.get("description", ""),
                prompt_instruction=data.get("prompt_instruction", ""),
                observed_failures=data.get("observed_failures", []),
            ))
        except Exception as e:
            logger.warning(f"Failed to load constraint {path}: {e}")

    # Sort by severity: CRITICAL > HIGH > MEDIUM > LOW
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    constraints.sort(key=lambda c: order.get(c.severity, 9))

    return constraints


def _load_dsb_rules() -> list[DSBRule]:
    """Load DSB-specific rules from knowledge_dsb.yaml."""
    if not DSB_RULES_FILE.exists():
        return []

    try:
        data = yaml.safe_load(DSB_RULES_FILE.read_text()) or {}
        rules = []
        for item in data.get("rules", []):
            rules.append(DSBRule(
                id=item.get("id", ""),
                description=item.get("description", ""),
            ))
        return rules
    except Exception as e:
        logger.warning(f"Failed to load DSB rules: {e}")
        return []


# DSB query mapping cache
_DSB_QUERY_MAPPING: dict[str, list[str]] | None = None


def _load_dsb_query_mapping() -> dict[str, list[str]]:
    """Load DSB query to rule mapping from dsb_query_rule_mapping.json."""
    global _DSB_QUERY_MAPPING
    if _DSB_QUERY_MAPPING is not None:
        return _DSB_QUERY_MAPPING

    if not DSB_QUERY_MAPPING_FILE.exists():
        logger.debug(f"DSB query mapping not found at {DSB_QUERY_MAPPING_FILE}")
        _DSB_QUERY_MAPPING = {}
        return _DSB_QUERY_MAPPING

    try:
        _DSB_QUERY_MAPPING = json.loads(DSB_QUERY_MAPPING_FILE.read_text())
        logger.info(f"Loaded DSB query mapping with {len(_DSB_QUERY_MAPPING)} queries")
        return _DSB_QUERY_MAPPING
    except Exception as e:
        logger.warning(f"Failed to load DSB query mapping: {e}")
        _DSB_QUERY_MAPPING = {}
        return _DSB_QUERY_MAPPING


def _get_rules_for_dsb_query(query_id: str) -> list[str]:
    """Get applicable rules for a DSB query by ID.

    Normalizes query_id to match mapping keys (e.g., 'q13' -> 'query013_agg').

    Args:
        query_id: Query identifier (e.g., 'q13', 'query013', 'query013_agg')

    Returns:
        List of applicable rule IDs, or empty list if not found
    """
    import re

    mapping = _load_dsb_query_mapping()
    if not mapping:
        return []

    # Try exact match first
    if query_id in mapping:
        return mapping[query_id]

    # Normalize query_id to find matches
    # Extract query number
    match = re.search(r'(\d+)', query_id)
    if not match:
        return []

    qnum = int(match.group(1))
    qnum_padded = f"{qnum:03d}"  # e.g., "013"

    # Determine variant (agg, spj, multi)
    variant = None
    if 'spj' in query_id.lower():
        variant = 'spj_spj'
    elif 'agg' in query_id.lower():
        variant = 'agg'
    elif 'multi' in query_id.lower():
        variant = 'multi'

    # Try to find matching keys
    candidates = []
    for key in mapping.keys():
        if f"query{qnum_padded}" in key or f"query{qnum}" in key:
            if variant:
                if variant in key:
                    return mapping[key]
            candidates.append(key)

    # Return first matching candidate
    if candidates:
        return mapping[candidates[0]]

    return []


# =============================================================================
# Tag-Based Similarity Matching (replaces FAISS)
# =============================================================================

class ADOFAISSRecommender:
    """Tag-based similarity recommender for ADO examples.

    Uses ado/models/similarity_tags.json for tag overlap matching.
    Class name kept as ADOFAISSRecommender for backward compatibility
    with pipeline.py and other callers.
    """

    def __init__(self):
        self.tag_entries: list[dict] = []
        self.tag_metadata: dict | None = None
        self._initialized = False
        self._load_index()

    def _load_index(self):
        """Load tag index from ado/models/."""
        tags_file = MODELS_DIR / "similarity_tags.json"
        metadata_file = MODELS_DIR / "similarity_metadata.json"

        if not tags_file.exists():
            logger.debug(f"ADO tag index not found at {tags_file}")
            return

        try:
            data = json.loads(tags_file.read_text())
            self.tag_entries = data.get("examples", [])

            if metadata_file.exists():
                self.tag_metadata = json.loads(metadata_file.read_text())

            self._initialized = bool(self.tag_entries)
            logger.info(f"Loaded ADO tag index with {len(self.tag_entries)} examples")

        except Exception as e:
            logger.warning(f"Failed to load ADO tag index: {e}")

    def find_similar_examples(
        self, sql: str, k: int = 5, dialect: str = "duckdb",
    ) -> list[tuple[str, float, dict]]:
        """Find similar gold examples using tag overlap matching.

        Returns gold examples for the target engine only. Regressions
        and cross-engine examples are excluded.

        Args:
            sql: SQL query to find similar examples for
            k: Number of results to return
            dialect: SQL dialect — "duckdb" or "postgres"

        Returns:
            List of (example_id, similarity_score, metadata) tuples
        """
        if not self._initialized or not self.tag_entries:
            return []

        engine = "postgres" if dialect == "postgres" else "duckdb"

        try:
            from .faiss_builder import extract_tags, classify_category

            query_tags = extract_tags(sql, dialect=dialect)
            query_category = classify_category(query_tags)

            scored = []
            for ex in self.tag_entries:
                ex_engine = ex.get("engine")
                # Match engine-specific + seed (universal) examples
                if ex_engine != engine and ex_engine != "seed":
                    continue
                if ex.get("type") == "regression":
                    continue
                ex_tags = set(ex.get("tags", []))
                overlap = len(query_tags & ex_tags)
                category_bonus = 1 if ex.get("category") == query_category else 0
                if overlap > 0:
                    scored.append((ex, overlap, category_bonus))

            scored.sort(key=lambda x: (x[1], x[2]), reverse=True)

            results = []
            for ex, overlap, _ in scored[:k]:
                meta = ex.get("metadata", {})
                max_possible = max(len(query_tags), 1)
                similarity = overlap / max_possible
                results.append((ex["id"], similarity, meta))

            return results

        except Exception as e:
            logger.warning(f"Tag search failed: {e}")
            return []

    def find_relevant_regressions(
        self, sql: str, k: int = 5, dialect: str = "duckdb",
    ) -> list[tuple[str, float, dict]]:
        """Find relevant regression examples (anti-patterns) for a query.

        Same engine-filtered tag overlap, but returns only regressions.
        Use these as warnings — transforms that hurt similar queries.

        Args:
            sql: SQL query to check against
            k: Number of results to return
            dialect: SQL dialect — "duckdb" or "postgres"

        Returns:
            List of (example_id, similarity_score, metadata) tuples
        """
        if not self._initialized or not self.tag_entries:
            return []

        engine = "postgres" if dialect == "postgres" else "duckdb"

        try:
            from .faiss_builder import extract_tags, classify_category

            query_tags = extract_tags(sql, dialect=dialect)
            query_category = classify_category(query_tags)

            scored = []
            for ex in self.tag_entries:
                ex_engine = ex.get("engine")
                if ex_engine != engine and ex_engine != "seed":
                    continue
                if ex.get("type") != "regression":
                    continue
                ex_tags = set(ex.get("tags", []))
                overlap = len(query_tags & ex_tags)
                category_bonus = 1 if ex.get("category") == query_category else 0
                if overlap > 0:
                    scored.append((ex, overlap, category_bonus))

            scored.sort(key=lambda x: (x[1], x[2]), reverse=True)

            results = []
            for ex, overlap, _ in scored[:k]:
                meta = ex.get("metadata", {})
                max_possible = max(len(query_tags), 1)
                similarity = overlap / max_possible
                results.append((ex["id"], similarity, meta))

            return results

        except Exception as e:
            logger.warning(f"Regression search failed: {e}")
            return []


# Global singleton
_ADO_RECOMMENDER: ADOFAISSRecommender | None = None


def _get_ado_recommender() -> ADOFAISSRecommender | None:
    """Get or create the ADO tag-based recommender."""
    global _ADO_RECOMMENDER
    if _ADO_RECOMMENDER is None:
        _ADO_RECOMMENDER = ADOFAISSRecommender()
    return _ADO_RECOMMENDER


def _get_faiss_recommendations(sql: str, k: int = 5) -> list[str]:
    """Get example recommendations using ADO's tag index.

    Uses ado/models/ tag index for similarity matching.

    Args:
        sql: The SQL query text
        k: Number of recommendations to return

    Returns:
        List of example IDs ordered by similarity
    """
    recommender = _get_ado_recommender()
    if not recommender or not recommender._initialized:
        logger.debug("ADO tag recommender not initialized")
        return []

    similar = recommender.find_similar_examples(sql, k=k)
    return [example_id for example_id, _, _ in similar]


# =============================================================================
# Main Retriever
# =============================================================================

class KnowledgeRetriever:
    """Retrieve examples + constraints for ADO optimization.

    Uses FAISS-based retrieval when available, with fallback to
    returning all local examples.
    """

    def __init__(self):
        """Initialize retriever."""
        self._examples_cache: Optional[list[GoldExample]] = None
        self._constraints_cache: Optional[list[Constraint]] = None

    @property
    def examples(self) -> list[GoldExample]:
        """Get cached examples."""
        if self._examples_cache is None:
            self._examples_cache = _load_examples()
        return self._examples_cache

    @property
    def constraints(self) -> list[Constraint]:
        """Get cached constraints."""
        if self._constraints_cache is None:
            self._constraints_cache = _load_constraints()
        return self._constraints_cache

    def retrieve(
        self, sql: str, k_examples: int = 3, query_id: str | None = None
    ) -> RetrievalResult:
        """Retrieve relevant examples and constraints for a SQL query.

        Uses explicit DSB query mapping when query_id is provided and matches
        a known DSB query. Falls back to FAISS similarity search otherwise.

        Args:
            sql: The SQL query to optimize
            k_examples: Maximum number of examples to return
            query_id: Optional query identifier (e.g., 'q13', 'query013_agg')

        Returns:
            RetrievalResult with examples and constraints
        """
        examples_by_id = {ex.id: ex for ex in self.examples}
        selected: list[GoldExample] = []
        retrieval_method = "fallback"
        rule_ids: list[str] = []

        # 1. Try explicit DSB query mapping first (if query_id provided)
        if query_id:
            rule_ids = _get_rules_for_dsb_query(query_id)
            if rule_ids:
                retrieval_method = "dsb_mapping"
                logger.info(f"Using DSB mapping for {query_id}: {len(rule_ids)} rules")

                for rule_id in rule_ids[:k_examples]:
                    if rule_id in examples_by_id:
                        selected.append(examples_by_id[rule_id])
                    else:
                        # Try case-insensitive match
                        for ex_id, ex in examples_by_id.items():
                            if ex_id.upper() == rule_id.upper():
                                if ex not in selected:
                                    selected.append(ex)
                                    break

        # 2. Fall back to FAISS similarity if no DSB mapping match
        faiss_recs: list[str] = []
        if not selected:
            faiss_recs = _get_faiss_recommendations(sql, k=max(5, k_examples))
            if faiss_recs:
                retrieval_method = "faiss"

                for rec_id in faiss_recs:
                    if rec_id in examples_by_id:
                        selected.append(examples_by_id[rec_id])
                    else:
                        # Try partial match
                        for ex_id, ex in examples_by_id.items():
                            if rec_id.upper() in ex_id.upper() or ex_id.upper() in rec_id.upper():
                                if ex not in selected:
                                    selected.append(ex)
                                    break

                    if len(selected) >= k_examples:
                        break

        # 3. Final fallback - return first k examples
        if not selected:
            selected = self.examples[:k_examples]
            retrieval_method = "fallback"

        # Load DSB rules for additional context
        dsb_rules = _load_dsb_rules()

        return RetrievalResult(
            gold_examples=selected,
            constraints=self.constraints,
            rationale={
                "retrieval_method": retrieval_method,
                "query_id": query_id,
                "dsb_rule_ids": rule_ids[:k_examples] if rule_ids else [],
                "faiss_recommendations": faiss_recs,
                "selected_example_ids": [e.id for e in selected],
                "dsb_rules": [r.id for r in dsb_rules],
                "total_examples_available": len(self.examples),
                "total_constraints": len(self.constraints),
            },
        )

    def get_example_by_id(self, example_id: str) -> Optional[GoldExample]:
        """Get a specific example by ID.

        Args:
            example_id: The example ID

        Returns:
            GoldExample or None if not found
        """
        for ex in self.examples:
            if ex.id == example_id:
                return ex
        return None

    def reload(self) -> None:
        """Reload examples and constraints from disk."""
        self._examples_cache = None
        self._constraints_cache = None
