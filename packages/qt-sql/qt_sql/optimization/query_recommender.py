"""Query-specific gold example recommender based on ML predictions."""

import re
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class QueryRecommendation:
    """A single recommendation for a query."""
    transform: str
    confidence_pct: int
    estimated_speedup: float
    is_match: bool = False  # If this was the actual winning transform


def parse_recommendations_report(report_path: str) -> Dict[str, List[QueryRecommendation]]:
    """Parse query_recommendations_report.md into per-query recommendations.

    Args:
        report_path: Path to query_recommendations_report.md

    Returns:
        Dict mapping query_id (e.g., 'q1') to list of recommendations
    """
    with open(report_path) as f:
        content = f.read()

    recommendations: Dict[str, List[QueryRecommendation]] = {}

    # Split into query sections
    query_sections = re.split(r'^#### (Q\d+)', content, flags=re.MULTILINE)

    # Process each query (skip first element which is header)
    for i in range(1, len(query_sections), 2):
        query_id = query_sections[i].lower()  # q1, q2, etc.
        section_content = query_sections[i + 1]

        # Extract top 3 recommendations
        recs: List[QueryRecommendation] = []

        # Pattern: "1. **transform_name** ✓ **MATCH**" or "1. **transform_name**"
        rec_pattern = r'^\d+\.\s+\*\*(\w+)\*\*(\s+✓\s+\*\*MATCH\*\*)?\s*$'
        conf_pattern = r'Combined confidence:\s+(\d+)%'
        speedup_pattern = r'Estimated speedup:\s+([\d.]+)x'

        lines = section_content.split('\n')
        current_transform = None
        current_is_match = False
        current_conf = 0
        current_speedup = 0.0

        for line in lines:
            # Check for recommendation header
            match = re.match(rec_pattern, line.strip())
            if match:
                # Save previous recommendation if exists
                if current_transform:
                    recs.append(QueryRecommendation(
                        transform=current_transform,
                        confidence_pct=current_conf,
                        estimated_speedup=current_speedup,
                        is_match=current_is_match
                    ))
                    if len(recs) >= 3:
                        break

                # Start new recommendation
                current_transform = match.group(1)
                current_is_match = bool(match.group(2))
                current_conf = 0
                current_speedup = 0.0
                continue

            # Extract confidence
            if current_transform:
                conf_match = re.search(conf_pattern, line)
                if conf_match:
                    current_conf = int(conf_match.group(1))

                speedup_match = re.search(speedup_pattern, line)
                if speedup_match:
                    current_speedup = float(speedup_match.group(1))

        # Don't forget last recommendation
        if current_transform:
            recs.append(QueryRecommendation(
                transform=current_transform,
                confidence_pct=current_conf,
                estimated_speedup=current_speedup,
                is_match=current_is_match
            ))

        if recs:
            recommendations[query_id] = recs

    return recommendations


def get_recommended_examples(
    query_id: str,
    recommendations_map: Dict[str, List[QueryRecommendation]],
    top_n: int = 3
) -> List[str]:
    """Get recommended gold example IDs for a specific query.

    Args:
        query_id: Query ID (e.g., 'q1', 'q15')
        recommendations_map: Output from parse_recommendations_report()
        top_n: Number of top recommendations to return

    Returns:
        List of gold example IDs (e.g., ['decorrelate', 'early_filter', 'or_to_union'])
    """
    if query_id not in recommendations_map:
        return []

    recs = recommendations_map[query_id][:top_n]
    return [rec.transform for rec in recs]


# Default path to recommendations report
DEFAULT_REPORT_PATH = Path(__file__).parent.parent.parent.parent.parent / "research" / "ml_pipeline" / "recommendations" / "query_recommendations_report.md"

# Cache for parsed recommendations
_RECOMMENDATIONS_CACHE: Optional[Dict[str, List[QueryRecommendation]]] = None


def get_query_recommendations(query_id: str, top_n: int = 3) -> List[str]:
    """Get recommended gold examples for a query (with caching).

    Args:
        query_id: Query ID (e.g., 'q1', 'q15')
        top_n: Number of recommendations to return

    Returns:
        List of gold example IDs

    Example:
        >>> get_query_recommendations('q1', top_n=3)
        ['decorrelate', 'early_filter']
    """
    global _RECOMMENDATIONS_CACHE

    # Load and cache on first use
    if _RECOMMENDATIONS_CACHE is None:
        if DEFAULT_REPORT_PATH.exists():
            _RECOMMENDATIONS_CACHE = parse_recommendations_report(str(DEFAULT_REPORT_PATH))
        else:
            _RECOMMENDATIONS_CACHE = {}

    return get_recommended_examples(query_id, _RECOMMENDATIONS_CACHE, top_n)
