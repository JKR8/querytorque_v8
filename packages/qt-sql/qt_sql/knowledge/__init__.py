"""Knowledge retrieval for qt_sql.

This module retrieves gold examples and constraints for SQL optimization.
Uses tag-based similarity matching from the tag index, with fallback
to loading all local examples.

Local directories:
- qt_sql/examples/*.json - Gold optimization examples
- qt_sql/constraints/*.json - Safety constraints
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from .normalization import normalize_dialect, normalize_tag_entry

logger = logging.getLogger(__name__)

# Directory paths — resolve relative to the qt_sql package root (parent of knowledge/)
BASE_DIR = Path(__file__).resolve().parent.parent
MODELS_DIR = BASE_DIR / "models"


# =============================================================================
# Tag-Based Similarity Matching (replaces FAISS)
# =============================================================================

class TagRecommender:
    """Tag-based similarity recommender for qt_sql examples.

    Uses qt_sql/models/similarity_tags.json for tag overlap matching.
    """

    def __init__(self):
        self.tag_entries: list[dict] = []
        self.tag_metadata: dict | None = None
        self._initialized = False
        self._load_index()

    def _load_index(self):
        """Load tag index from qt_sql/models/."""
        tags_file = MODELS_DIR / "similarity_tags.json"
        metadata_file = MODELS_DIR / "similarity_metadata.json"

        if not tags_file.exists():
            logger.debug(f"Tag index not found at {tags_file}")
            return

        try:
            data = json.loads(tags_file.read_text())
            raw_entries = data.get("examples", [])
            self.tag_entries = [
                normalize_tag_entry(e) for e in raw_entries if isinstance(e, dict)
            ]

            if metadata_file.exists():
                self.tag_metadata = json.loads(metadata_file.read_text())

            self._initialized = bool(self.tag_entries)
            logger.info(f"Loaded tag index with {len(self.tag_entries)} examples")

        except Exception as e:
            logger.warning(f"Failed to load tag index: {e}")

    def find_similar_examples(
        self, sql: str, k: int = 5, dialect: str = "duckdb",
    ) -> list[tuple[str, float, dict]]:
        """Find similar gold examples using tag overlap matching.

        Returns gold examples for the target dialect only. Regressions
        and cross-dialect examples are excluded.

        Args:
            sql: SQL query to find similar examples for
            k: Number of results to return
            dialect: SQL dialect (canonicalized internally)

        Returns:
            List of (example_id, similarity_score, metadata) tuples
        """
        if not self._initialized or not self.tag_entries:
            return []

        canonical_dialect = normalize_dialect(dialect)
        parser_dialect = (
            "postgres" if canonical_dialect == "postgresql" else canonical_dialect
        )

        try:
            from qt_sql.tag_index import extract_tags, classify_category

            query_tags = extract_tags(sql, dialect=parser_dialect)
            query_category = classify_category(query_tags)

            scored = []
            for ex in self.tag_entries:
                ex_dialect = normalize_dialect(ex.get("dialect") or ex.get("engine"))
                # Match dialect-specific + seed (universal) examples
                if ex_dialect not in {canonical_dialect, "seed"}:
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

        canonical_dialect = normalize_dialect(dialect)
        parser_dialect = (
            "postgres" if canonical_dialect == "postgresql" else canonical_dialect
        )

        try:
            from qt_sql.tag_index import extract_tags, classify_category

            query_tags = extract_tags(sql, dialect=parser_dialect)
            query_category = classify_category(query_tags)

            scored = []
            for ex in self.tag_entries:
                ex_dialect = normalize_dialect(ex.get("dialect") or ex.get("engine"))
                if ex_dialect not in {canonical_dialect, "seed"}:
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
