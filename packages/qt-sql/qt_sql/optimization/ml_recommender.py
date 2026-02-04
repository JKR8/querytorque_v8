"""ML-based optimization recommender combining pattern detection and similarity search.

Hybrid approach:
1. AST pattern detection → gold pattern weights → transform recommendations
2. Vector similarity → find similar historical queries with speedups
"""

import json
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

# Lazy imports for optional dependencies
faiss = None
ASTVectorizer = None


@dataclass
class TransformRecommendation:
    """A recommended transformation with confidence and evidence."""
    transform_name: str
    confidence: float
    avg_speedup: float
    max_speedup: float
    evidence_type: str  # "pattern" or "similarity"
    evidence_details: dict


@dataclass
class SimilarQuery:
    """A similar historical query."""
    query_id: str
    distance: float
    speedup: float
    winning_transform: str
    similarity_score: float  # 0-1, higher is more similar


class MLRecommender:
    """Hybrid ML recommender combining patterns and similarity."""

    def __init__(self, models_dir: Optional[Path] = None):
        """Initialize recommender with pre-trained models.

        Args:
            models_dir: Path to models directory. If None, uses default location.
        """
        if models_dir is None:
            # Default to research/ml_pipeline/models
            base = Path(__file__).parent.parent.parent.parent.parent
            models_dir = base / "research" / "ml_pipeline" / "models"

        self.models_dir = models_dir
        self.pattern_weights = None
        self.faiss_index = None
        self.faiss_metadata = None
        self.vectorizer = None

        # Try to load models (lazy loading)
        self._load_pattern_weights()
        self._load_faiss_index()

    def _load_pattern_weights(self):
        """Load pattern weight matrix."""
        weights_file = self.models_dir / "pattern_weights.json"
        if not weights_file.exists():
            return

        try:
            with open(weights_file) as f:
                self.pattern_weights = json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load pattern weights: {e}")

    def _load_faiss_index(self):
        """Load FAISS similarity index."""
        global faiss, ASTVectorizer

        index_file = self.models_dir / "similarity_index.faiss"
        metadata_file = self.models_dir / "similarity_metadata.json"

        if not index_file.exists() or not metadata_file.exists():
            return

        try:
            # Lazy import
            if faiss is None:
                import faiss as faiss_module
                faiss = faiss_module

            # Load index
            self.faiss_index = faiss.read_index(str(index_file))

            # Load metadata
            with open(metadata_file) as f:
                self.faiss_metadata = json.load(f)

            # Load vectorizer
            if ASTVectorizer is None:
                import sys
                scripts_dir = Path(__file__).parent.parent.parent.parent.parent / "scripts"
                sys.path.insert(0, str(scripts_dir))
                from vectorize_queries import ASTVectorizer as ASTVectorizerClass
                ASTVectorizer = ASTVectorizerClass

            self.vectorizer = ASTVectorizer()

        except Exception as e:
            print(f"Warning: Failed to load FAISS index: {e}")
            self.faiss_index = None

    def recommend(self, sql: str, gold_detections: List[str],
                  top_k: int = 3) -> Dict[str, any]:
        """Generate optimization recommendations for a query.

        Args:
            sql: SQL query text
            gold_detections: List of detected gold pattern IDs (e.g., ["GLD-003", "GLD-001"])
            top_k: Number of top recommendations to return

        Returns:
            Dictionary with:
            - pattern_recommendations: List[TransformRecommendation]
            - similar_queries: List[SimilarQuery]
            - combined_recommendations: Merged and ranked recommendations
        """
        results = {
            "pattern_recommendations": [],
            "similar_queries": [],
            "combined_recommendations": [],
        }

        # 1. Pattern-based recommendations
        if self.pattern_weights and gold_detections:
            results["pattern_recommendations"] = self._recommend_from_patterns(
                gold_detections, top_k
            )

        # 2. Similarity-based recommendations
        if self.faiss_index and self.vectorizer:
            results["similar_queries"] = self._find_similar_queries(sql, k=5)

        # 3. Combine and rank
        results["combined_recommendations"] = self._combine_recommendations(
            results["pattern_recommendations"],
            results["similar_queries"],
            top_k
        )

        return results

    def _recommend_from_patterns(self, gold_detections: List[str],
                                 top_k: int) -> List[TransformRecommendation]:
        """Recommend transforms based on detected gold patterns."""
        if not self.pattern_weights:
            return []

        recommendations = []

        # Check single patterns
        for gold_id in gold_detections:
            if gold_id not in self.pattern_weights["single_patterns"]:
                continue

            transforms = self.pattern_weights["single_patterns"][gold_id]
            for transform_name, stats in transforms.items():
                recommendations.append(TransformRecommendation(
                    transform_name=transform_name,
                    confidence=stats["confidence"],
                    avg_speedup=stats["avg_speedup"],
                    max_speedup=stats["max_speedup"],
                    evidence_type="pattern",
                    evidence_details={
                        "pattern": gold_id,
                        "count": stats["count"],
                    }
                ))

        # Check pattern combinations
        if len(gold_detections) > 1:
            combo_key = "+".join(sorted(gold_detections))
            if combo_key in self.pattern_weights["pattern_combinations"]:
                transforms = self.pattern_weights["pattern_combinations"][combo_key]
                for transform_name, stats in transforms.items():
                    # Boost confidence for exact combo match
                    boosted_confidence = min(1.0, stats["confidence"] * 1.2)
                    recommendations.append(TransformRecommendation(
                        transform_name=transform_name,
                        confidence=boosted_confidence,
                        avg_speedup=stats["avg_speedup"],
                        max_speedup=stats["max_speedup"],
                        evidence_type="pattern_combo",
                        evidence_details={
                            "patterns": gold_detections,
                            "count": stats["count"],
                        }
                    ))

        # Sort by confidence * avg_speedup
        recommendations.sort(
            key=lambda r: r.confidence * r.avg_speedup,
            reverse=True
        )

        return recommendations[:top_k]

    def _find_similar_queries(self, sql: str, k: int = 5) -> List[SimilarQuery]:
        """Find similar historical queries using FAISS."""
        if not self.faiss_index or not self.vectorizer:
            return []

        try:
            # Vectorize query
            query_vector = self.vectorizer.vectorize(sql, dialect="duckdb")
            query_vector = query_vector.reshape(1, -1).astype('float32')

            # Normalize for cosine similarity
            faiss.normalize_L2(query_vector)

            # Search
            distances, indices = self.faiss_index.search(query_vector, k)

            # Build results
            similar_queries = []
            query_ids = list(self.faiss_metadata["query_metadata"].keys())

            for dist, idx in zip(distances[0], indices[0]):
                qid = query_ids[idx]
                meta = self.faiss_metadata["query_metadata"][qid]

                # Convert L2 distance to similarity score (0-1)
                # For normalized vectors: similarity = 1 - (distance^2 / 2)
                similarity = max(0, 1 - (dist ** 2) / 2)

                # Only include queries with wins
                if meta.get("has_win", False):
                    similar_queries.append(SimilarQuery(
                        query_id=qid,
                        distance=float(dist),
                        speedup=meta["speedup"],
                        winning_transform=meta["winning_transform"],
                        similarity_score=similarity
                    ))

            return similar_queries

        except Exception as e:
            print(f"Warning: Similarity search failed: {e}")
            return []

    def _combine_recommendations(self,
                                pattern_recs: List[TransformRecommendation],
                                similar_queries: List[SimilarQuery],
                                top_k: int) -> List[Dict]:
        """Combine pattern and similarity recommendations."""

        # Aggregate by transform name
        combined = {}

        # Add pattern-based recommendations
        for rec in pattern_recs:
            if rec.transform_name not in combined:
                combined[rec.transform_name] = {
                    "transform": rec.transform_name,
                    "pattern_confidence": rec.confidence,
                    "pattern_avg_speedup": rec.avg_speedup,
                    "pattern_max_speedup": rec.max_speedup,
                    "pattern_evidence": rec.evidence_details,
                    "similar_query_count": 0,
                    "similar_query_avg_speedup": 0.0,
                    "similar_queries": [],
                }

        # Add similarity-based evidence
        for sim_query in similar_queries:
            transform = sim_query.winning_transform
            if not transform:
                continue

            if transform not in combined:
                combined[transform] = {
                    "transform": transform,
                    "pattern_confidence": 0.0,
                    "pattern_avg_speedup": 0.0,
                    "pattern_max_speedup": 0.0,
                    "pattern_evidence": None,
                    "similar_query_count": 0,
                    "similar_query_avg_speedup": 0.0,
                    "similar_queries": [],
                }

            rec = combined[transform]
            rec["similar_query_count"] += 1
            rec["similar_queries"].append({
                "query_id": sim_query.query_id,
                "similarity": sim_query.similarity_score,
                "speedup": sim_query.speedup,
            })

        # Calculate final scores
        for transform, rec in combined.items():
            # Calculate similarity-based average speedup
            if rec["similar_queries"]:
                rec["similar_query_avg_speedup"] = sum(
                    q["speedup"] for q in rec["similar_queries"]
                ) / len(rec["similar_queries"])

            # Combined confidence score
            # = 0.7 * pattern_confidence + 0.3 * (similar_query_count / 5)
            pattern_score = rec["pattern_confidence"]
            similarity_score = min(1.0, rec["similar_query_count"] / 5)
            rec["combined_confidence"] = 0.7 * pattern_score + 0.3 * similarity_score

            # Combined speedup estimate
            # Weighted average of pattern and similarity speedups
            if pattern_score > 0 and similarity_score > 0:
                rec["estimated_speedup"] = (
                    0.7 * rec["pattern_avg_speedup"] +
                    0.3 * rec["similar_query_avg_speedup"]
                )
            elif pattern_score > 0:
                rec["estimated_speedup"] = rec["pattern_avg_speedup"]
            else:
                rec["estimated_speedup"] = rec["similar_query_avg_speedup"]

        # Sort by combined_confidence * estimated_speedup
        ranked = sorted(
            combined.values(),
            key=lambda r: r["combined_confidence"] * r["estimated_speedup"],
            reverse=True
        )

        return ranked[:top_k]

    def format_for_prompt(self, recommendations: Dict, max_similar: int = 2) -> str:
        """Format recommendations for inclusion in optimization prompt.

        Args:
            recommendations: Output from recommend()
            max_similar: Max similar queries to show per transform

        Returns:
            Formatted text block for prompt
        """
        if not recommendations["combined_recommendations"]:
            return ""

        lines = ["", "## 🎯 ML-Recommended Transformations", ""]
        lines.append("Based on detected patterns and similar historical queries:")
        lines.append("")

        for i, rec in enumerate(recommendations["combined_recommendations"], 1):
            # Header
            transform = rec["transform"]
            confidence = rec["combined_confidence"]
            speedup = rec["estimated_speedup"]

            lines.append(f"### {i}. **{transform}** "
                        f"(confidence: {confidence:.0%}, est. speedup: {speedup:.2f}x)")
            lines.append("")

            # Pattern evidence
            if rec["pattern_confidence"] > 0 and rec["pattern_evidence"]:
                evidence = rec["pattern_evidence"]
                if isinstance(evidence.get("patterns"), list):
                    pattern_str = " + ".join(evidence["patterns"])
                else:
                    pattern_str = evidence.get("pattern", "")

                lines.append(f"   - **Pattern detected**: {pattern_str}")
                lines.append(f"   - Historical speedup: {rec['pattern_avg_speedup']:.2f}x avg, "
                            f"{rec['pattern_max_speedup']:.2f}x max ({evidence.get('count', 0)} cases)")

            # Similar query evidence
            if rec["similar_queries"]:
                lines.append(f"   - **Similar queries**: {rec['similar_query_count']} found")
                for sim_q in rec["similar_queries"][:max_similar]:
                    lines.append(f"      - {sim_q['query_id']}: "
                                f"{sim_q['speedup']:.2f}x speedup "
                                f"(similarity: {sim_q['similarity']:.0%})")

            lines.append("")

        return "\n".join(lines)


def load_recommender() -> Optional[MLRecommender]:
    """Load ML recommender with error handling.

    Returns:
        MLRecommender instance if models exist, None otherwise
    """
    try:
        recommender = MLRecommender()
        if recommender.pattern_weights or recommender.faiss_index:
            return recommender
        return None
    except Exception as e:
        print(f"Warning: Failed to load ML recommender: {e}")
        return None
