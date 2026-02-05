"""Test ML recommender functionality."""

import pytest
from pathlib import Path

from qt_sql.optimization.ml_recommender import MLRecommender, load_recommender


def test_recommender_loads_models():
    """Test that recommender can load models if they exist."""
    recommender = load_recommender()

    # May be None if models don't exist (not an error)
    if recommender:
        assert isinstance(recommender, MLRecommender)
        # Should have at least one model loaded
        assert recommender.pattern_weights or recommender.faiss_index


def test_pattern_recommendations():
    """Test pattern-based recommendations."""
    recommender = load_recommender()

    if not recommender or not recommender.pattern_weights:
        pytest.skip("Pattern weights not available")

    # Test with known gold patterns
    sql = "SELECT * FROM store_sales JOIN date_dim WHERE d_year = 2001"
    gold_detections = ["GLD-003"]  # Early filter pushdown

    recommendations = recommender.recommend(sql, gold_detections, top_k=3)

    # Should have pattern recommendations
    assert len(recommendations["pattern_recommendations"]) > 0

    # First recommendation should be early_filter (high confidence for GLD-003)
    top_rec = recommendations["pattern_recommendations"][0]
    assert top_rec.transform_name == "early_filter"
    assert top_rec.confidence >= 0.5  # Allow boundary value
    assert top_rec.avg_speedup > 1.0


def test_similarity_search():
    """Test similarity-based recommendations."""
    recommender = load_recommender()

    if not recommender or not recommender.faiss_index:
        pytest.skip("FAISS index not available")

    # Test with Q1-like query (decorrelate pattern)
    sql = """
    SELECT c_customer_id
    FROM customer
    WHERE c_birth_year > (
        SELECT AVG(c_birth_year)
        FROM customer c2
        WHERE c2.c_customer_sk = customer.c_customer_sk
    )
    """

    gold_detections = ["GLD-001", "GLD-005"]

    recommendations = recommender.recommend(sql, gold_detections, top_k=3)

    # Should find similar queries
    assert len(recommendations["similar_queries"]) > 0

    # Similar queries should have speedups
    for sim_q in recommendations["similar_queries"]:
        assert sim_q.speedup >= 1.0
        assert 0 <= sim_q.similarity_score <= 1.0


def test_combined_recommendations():
    """Test combined pattern + similarity recommendations."""
    recommender = load_recommender()

    if not recommender or not (recommender.pattern_weights and recommender.faiss_index):
        pytest.skip("Both models not available")

    sql = """
    SELECT ss_item_sk, SUM(ss_sales_price)
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 2001
    GROUP BY ss_item_sk
    """

    gold_detections = ["GLD-003"]

    recommendations = recommender.recommend(sql, gold_detections, top_k=3)

    # Should have combined recommendations
    assert len(recommendations["combined_recommendations"]) > 0

    # Combined should include confidence and speedup estimates
    for rec in recommendations["combined_recommendations"]:
        assert rec["transform"] is not None
        assert 0 <= rec["combined_confidence"] <= 1.0
        assert rec["estimated_speedup"] >= 1.0


def test_format_for_prompt():
    """Test prompt formatting."""
    recommender = load_recommender()

    if not recommender:
        pytest.skip("Recommender not available")

    sql = "SELECT * FROM store_sales JOIN date_dim WHERE d_year = 2001"
    gold_detections = ["GLD-003"]

    recommendations = recommender.recommend(sql, gold_detections, top_k=2)
    formatted = recommender.format_for_prompt(recommendations, max_similar=2)

    # Should be non-empty if we have recommendations
    if recommendations["combined_recommendations"]:
        assert len(formatted) > 0
        assert "ML-Recommended Transformations" in formatted
        assert "confidence:" in formatted.lower()


def test_no_recommendations_for_no_patterns():
    """Test that no patterns = no recommendations."""
    recommender = load_recommender()

    if not recommender:
        pytest.skip("Recommender not available")

    sql = "SELECT 1"
    gold_detections = []

    recommendations = recommender.recommend(sql, gold_detections)

    # May have similar queries but no pattern recommendations
    assert len(recommendations["pattern_recommendations"]) == 0


def test_multi_pattern_combination():
    """Test recommendations for multiple pattern combinations."""
    recommender = load_recommender()

    if not recommender or not recommender.pattern_weights:
        pytest.skip("Pattern weights not available")

    sql = "SELECT * FROM store_sales"
    gold_detections = ["GLD-001", "GLD-003", "GLD-004"]

    recommendations = recommender.recommend(sql, gold_detections, top_k=5)

    # Should handle multiple patterns
    assert len(recommendations["pattern_recommendations"]) > 0

    # Check that pattern evidence is captured
    for rec in recommendations["pattern_recommendations"]:
        assert rec.evidence_details is not None


# ==============================================================================
# Demo / Manual Testing
# ==============================================================================

def demo_recommender():
    """Demo script showing recommender usage."""
    print("=" * 80)
    print("ML RECOMMENDER DEMO")
    print("=" * 80)

    recommender = load_recommender()

    if not recommender:
        print("❌ Models not found. Run: bash scripts/run_ml_training.sh")
        return

    print("✓ Models loaded")
    print(f"  - Pattern weights: {recommender.pattern_weights is not None}")
    print(f"  - FAISS index: {recommender.faiss_index is not None}")
    print()

    # Test queries
    test_cases = [
        {
            "name": "Early Filter Pattern (Q93-like)",
            "sql": """
                SELECT COUNT(*)
                FROM store_sales ss
                JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
                WHERE d.d_year = 2001
            """,
            "gold_detections": ["GLD-003"],
        },
        {
            "name": "Decorrelate Pattern (Q1-like)",
            "sql": """
                SELECT c_customer_id
                FROM customer
                WHERE c_birth_year > (
                    SELECT AVG(c2.c_birth_year)
                    FROM customer c2
                    WHERE c2.c_customer_sk = customer.c_customer_sk
                )
            """,
            "gold_detections": ["GLD-001", "GLD-005"],
        },
        {
            "name": "Projection Pruning Pattern",
            "sql": """
                WITH cte AS (
                    SELECT col1, col2, col3, col4, col5
                    FROM large_table
                )
                SELECT col1, col2
                FROM cte
            """,
            "gold_detections": ["GLD-004"],
        },
    ]

    for i, test in enumerate(test_cases, 1):
        print(f"\n{'=' * 80}")
        print(f"Test {i}: {test['name']}")
        print(f"{'=' * 80}")
        print(f"Gold detections: {test['gold_detections']}")
        print()

        recommendations = recommender.recommend(
            test["sql"],
            test["gold_detections"],
            top_k=3
        )

        # Print combined recommendations
        if recommendations["combined_recommendations"]:
            print("Combined Recommendations:")
            print("-" * 80)
            for j, rec in enumerate(recommendations["combined_recommendations"], 1):
                print(f"{j}. {rec['transform']}")
                print(f"   Confidence: {rec['combined_confidence']:.0%}")
                print(f"   Est. speedup: {rec['estimated_speedup']:.2f}x")

                if rec['pattern_evidence']:
                    print(f"   Pattern: {rec['pattern_evidence']}")

                if rec['similar_queries']:
                    print(f"   Similar queries: {rec['similar_query_count']}")
                    for sim in rec['similar_queries'][:2]:
                        print(f"      - {sim['query_id']}: {sim['speedup']:.2f}x")
                print()

        # Print formatted for prompt
        print("\nFormatted for prompt:")
        print("-" * 80)
        formatted = recommender.format_for_prompt(recommendations, max_similar=2)
        print(formatted)

    print("\n" + "=" * 80)
    print("✅ Demo complete!")
    print("=" * 80)


if __name__ == "__main__":
    demo_recommender()
