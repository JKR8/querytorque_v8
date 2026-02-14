"""Pattern Library -- cross-engine index of anti-patterns and canonical rewrites.

Provides scored example selection (Patch 3 from architecture spec):
  1. Filter patterns by engine tag
  2. Score each pattern:
     - Bottleneck match (evidence bundle -> pattern.anti_pattern): +3
     - Signal match (runtime profile -> pattern.detection): +2
     - Scenario match (scenario card -> pattern.scenario_tags): +1
  3. Select top 5 by score
  4. Fallback: if <3 matches, include 3 most common patterns for engine
"""

from __future__ import annotations

import functools
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LIBRARY_PATH = Path(__file__).parent / "library.yaml"

# Patterns that are near-universally applicable, used as fallback when
# scoring produces fewer than 3 matches.  Ordered by observed win rate.
_COMMON_PATTERNS_BY_ENGINE: Dict[str, List[str]] = {
    "duckdb": [
        "decorrelate_subquery",
        "date_cte_isolate",
        "single_pass_aggregation",
    ],
    "postgresql": [
        "decorrelate_subquery",
        "explicit_join_conversion",
        "date_cte_isolate",
    ],
    "snowflake": [
        "date_cte_isolate",
        "explicit_join_conversion",
        "decorrelate_subquery",
    ],
}

# Default fallback for unknown engines
_DEFAULT_COMMON = [
    "decorrelate_subquery",
    "date_cte_isolate",
    "single_pass_aggregation",
]


# ---------------------------------------------------------------------------
# Loader (cached)
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def load_pattern_library() -> Dict[str, Any]:
    """Load and cache the pattern library YAML.

    Returns the parsed YAML as a dict with a top-level ``patterns`` key
    containing the list of pattern definitions.

    Raises:
        FileNotFoundError: If library.yaml is missing.
        yaml.YAMLError: If the YAML is malformed.
    """
    if not _LIBRARY_PATH.exists():
        raise FileNotFoundError(
            f"Pattern library not found at {_LIBRARY_PATH}. "
            "Run build_library.py or create library.yaml manually."
        )
    with open(_LIBRARY_PATH, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict) or "patterns" not in data:
        raise ValueError(
            "library.yaml must have a top-level 'patterns' key with a list of patterns."
        )
    return data


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def get_patterns_for_engine(engine: str) -> List[Dict[str, Any]]:
    """Return patterns whose ``engine_tags`` include *engine* or ``"all"``.

    Parameters
    ----------
    engine : str
        Target engine name (e.g., ``"duckdb"``, ``"postgresql"``, ``"snowflake"``).
        Compared case-insensitively.

    Returns
    -------
    list[dict]
        Filtered pattern dicts.
    """
    engine_lower = engine.lower()
    library = load_pattern_library()
    results: List[Dict[str, Any]] = []
    for pat in library["patterns"]:
        tags = [t.lower() for t in pat.get("engine_tags", [])]
        if "all" in tags or engine_lower in tags:
            results.append(pat)
    return results


# ---------------------------------------------------------------------------
# Scoring (Patch 3 rubric)
# ---------------------------------------------------------------------------


def _keyword_overlap(text: str, reference: str) -> bool:
    """Return True if any significant word (len>=4) from *reference* appears
    in *text*.  Both are lowered before comparison."""
    if not text or not reference:
        return False
    ref_words = {
        w for w in reference.lower().split() if len(w) >= 4
    }
    text_lower = text.lower()
    return any(w in text_lower for w in ref_words)


def _tag_overlap_count(tags: List[str], scenario_tags: List[str]) -> int:
    """Count how many scenario_tags appear in *tags*."""
    tag_set = {t.upper() for t in tags}
    return sum(1 for st in scenario_tags if st.upper() in tag_set)


def score_patterns(
    patterns: List[Dict[str, Any]],
    evidence_bundle: Optional[str] = None,
    scenario_card: Optional[Dict[str, Any]] = None,
    bottleneck: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Score and rank patterns using the Patch 3 rubric.

    Scoring rubric:
      - **Bottleneck match** (+3): the *bottleneck* description has keyword
        overlap with the pattern's ``anti_pattern`` field.
      - **Signal match** (+2): the *evidence_bundle* has keyword overlap with
        any ``motivation_variants[].detection`` field.
      - **Scenario match** (+1 per tag): tags from *scenario_card* that appear
        in the pattern's ``scenario_tags``.

    Parameters
    ----------
    patterns : list[dict]
        Patterns to score (typically from :func:`get_patterns_for_engine`).
    evidence_bundle : str, optional
        Free-text evidence string (e.g., EXPLAIN output, runtime profile).
    scenario_card : dict, optional
        Dict with a ``"tags"`` key containing a list of structural tag strings.
    bottleneck : str, optional
        Free-text description of the bottleneck hypothesis.

    Returns
    -------
    list[dict]
        Each input pattern dict augmented with a ``_score`` key, sorted
        descending by score.
    """
    scored: List[Dict[str, Any]] = []
    scenario_tags = []
    if scenario_card and isinstance(scenario_card.get("tags"), list):
        scenario_tags = scenario_card["tags"]

    for pat in patterns:
        score = 0

        # +3: Bottleneck match
        if bottleneck and _keyword_overlap(bottleneck, pat.get("anti_pattern", "")):
            score += 3

        # +2: Signal match (check all motivation_variants.detection)
        if evidence_bundle:
            for mv in pat.get("motivation_variants", []):
                detection = mv.get("detection", "")
                if _keyword_overlap(evidence_bundle, detection):
                    score += 2
                    break  # only award once per pattern

        # +1 per tag: Scenario match
        pat_scenario_tags = pat.get("scenario_tags", [])
        score += _tag_overlap_count(scenario_tags, pat_scenario_tags)

        scored_pat = dict(pat)
        scored_pat["_score"] = score
        scored.append(scored_pat)

    scored.sort(key=lambda p: p["_score"], reverse=True)
    return scored


# ---------------------------------------------------------------------------
# Selection pipeline
# ---------------------------------------------------------------------------


def select_gold_examples(
    engine: str,
    evidence_bundle: Optional[str] = None,
    scenario_card: Optional[Dict[str, Any]] = None,
    bottleneck: Optional[str] = None,
    k: int = 5,
) -> List[Dict[str, Any]]:
    """Full selection pipeline: filter, score, pick top-k, with fallback.

    Steps:
      1. Filter patterns by engine tag.
      2. Score using the Patch 3 rubric.
      3. Take top *k* patterns with score > 0.
      4. Fallback: if fewer than 3 scored matches, pad with the most common
         patterns for the engine (up to 3).

    Each returned dict contains the full pattern definition plus:
      - ``_score``: the computed relevance score.
      - ``_gold_examples``: list of gold example refs for this engine only.

    Parameters
    ----------
    engine : str
        Target engine.
    evidence_bundle : str, optional
        Free-text evidence (EXPLAIN, profile, etc.).
    scenario_card : dict, optional
        Scenario card with ``tags`` list.
    bottleneck : str, optional
        Bottleneck hypothesis text.
    k : int
        Maximum number of patterns to return (default 5).

    Returns
    -------
    list[dict]
        Selected patterns, sorted by score descending.
    """
    engine_lower = engine.lower()

    # Step 1: filter
    engine_patterns = get_patterns_for_engine(engine_lower)

    # Step 2: score
    scored = score_patterns(
        engine_patterns,
        evidence_bundle=evidence_bundle,
        scenario_card=scenario_card,
        bottleneck=bottleneck,
    )

    # Step 3: take top-k with score > 0
    selected = [p for p in scored if p.get("_score", 0) > 0][:k]

    # Step 4: fallback -- if <3 scored matches, pad with common patterns
    if len(selected) < 3:
        selected_names = {p["name"] for p in selected}
        common_names = _COMMON_PATTERNS_BY_ENGINE.get(
            engine_lower, _DEFAULT_COMMON
        )
        # Build a name->pattern lookup from the full engine set
        pattern_by_name = {p["name"]: p for p in scored}
        for cname in common_names:
            if cname not in selected_names and cname in pattern_by_name:
                fallback_pat = dict(pattern_by_name[cname])
                fallback_pat["_score"] = fallback_pat.get("_score", 0)
                selected.append(fallback_pat)
                selected_names.add(cname)
            if len(selected) >= 3:
                break

    # Enrich with engine-specific gold examples only
    for pat in selected:
        engine_examples = [
            ex
            for ex in pat.get("gold_examples", [])
            if ex.get("engine", "").lower() == engine_lower
        ]
        pat["_gold_examples"] = engine_examples

    return selected


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------


def get_pattern_names() -> List[str]:
    """Return all pattern names in the library."""
    library = load_pattern_library()
    return [p["name"] for p in library["patterns"]]


def get_pattern_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Look up a single pattern by name. Returns None if not found."""
    library = load_pattern_library()
    for p in library["patterns"]:
        if p["name"] == name:
            return p
    return None
