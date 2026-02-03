"""Tests for MCTS PUCT prior computation.

Tests the priors.py and llm_ranker.py modules for:
- Uniform prior computation from KB weights
- Contextual priors with opportunity detection
- LLM ranking parsing and prior conversion
- PUCT score computation in MCTSNode
"""

import pytest
from unittest.mock import Mock, patch
import math

from qt_sql.optimization.mcts.priors import (
    PriorConfig,
    TransformPrior,
    compute_uniform_priors,
    compute_contextual_priors,
    get_priors_for_node,
)
from qt_sql.optimization.mcts.llm_ranker import (
    ranking_to_priors,
    _parse_ranking_response,
    should_use_llm_ranking,
)
from qt_sql.optimization.mcts.node import MCTSNode


# =============================================================================
# PRIORS TESTS
# =============================================================================

class TestUniformPriors:
    """Tests for compute_uniform_priors()."""

    def test_uniform_priors_sum_to_one(self):
        """Priors should sum to 1.0."""
        transform_ids = ["push_pred", "reorder_join", "or_to_union", "correlated_to_cte"]
        priors = compute_uniform_priors(transform_ids)

        total = sum(p.prior for p in priors.values())
        assert abs(total - 1.0) < 1e-6, f"Priors sum to {total}, expected 1.0"

    def test_uniform_priors_all_candidates_present(self):
        """All candidates should have priors."""
        transform_ids = ["push_pred", "reorder_join", "flatten_subq"]
        priors = compute_uniform_priors(transform_ids)

        assert set(priors.keys()) == set(transform_ids)

    def test_uniform_priors_high_weight_higher_prior(self):
        """High-weight transforms should have higher priors."""
        transform_ids = [
            "correlated_to_cte",  # weight 9 (high value)
            "remove_redundant",   # weight 2 (low)
        ]
        priors = compute_uniform_priors(transform_ids)

        assert priors["correlated_to_cte"].prior > priors["remove_redundant"].prior

    def test_uniform_priors_empty_input(self):
        """Empty input should return empty dict."""
        priors = compute_uniform_priors([])
        assert priors == {}

    def test_uniform_priors_source_is_kb_weight(self):
        """Priors should have source='kb_weight'."""
        priors = compute_uniform_priors(["push_pred", "reorder_join"])

        for prior in priors.values():
            assert prior.source == "kb_weight"


class TestContextualPriors:
    """Tests for compute_contextual_priors()."""

    def test_contextual_priors_sum_to_one(self):
        """Contextual priors should also sum to 1.0."""
        sql = "SELECT * FROM users WHERE id > 10 OR name = 'test'"
        transform_ids = ["push_pred", "or_to_union", "remove_redundant"]
        config = PriorConfig()

        priors = compute_contextual_priors(sql, transform_ids, [], config)

        total = sum(p.prior for p in priors.values())
        assert abs(total - 1.0) < 1e-6

    def test_opportunity_boost_applied(self):
        """Transforms matching opportunities should get boosted."""
        # SQL with OR condition -> should boost or_to_union
        sql = "SELECT * FROM users WHERE id = 1 OR name = 'test'"
        transform_ids = ["push_pred", "or_to_union", "remove_redundant"]
        config = PriorConfig(use_opportunity_detection=True, opportunity_boost=2.0)

        priors = compute_contextual_priors(sql, transform_ids, [], config)

        # or_to_union should have higher prior due to opportunity boost
        # (after normalization, it should still be notably higher)
        assert priors["or_to_union"].prior > priors["remove_redundant"].prior

    def test_correlated_subquery_opportunity(self):
        """Correlated subquery pattern should boost correlated_to_cte."""
        sql = """
        SELECT * FROM orders o
        WHERE o.total > (
            SELECT AVG(total) FROM orders o2 WHERE o2.store_id = o.store_id
        )
        """
        transform_ids = ["push_pred", "correlated_to_cte", "remove_redundant"]
        config = PriorConfig(use_opportunity_detection=True)

        priors = compute_contextual_priors(sql, transform_ids, [], config)

        # correlated_to_cte should be boosted
        assert priors["correlated_to_cte"].boost_reason is not None
        assert "opportunity" in priors["correlated_to_cte"].boost_reason

    def test_applied_transform_penalty(self):
        """Already-applied transforms should get penalized."""
        sql = "SELECT * FROM users"
        transform_ids = ["push_pred", "reorder_join", "remove_redundant"]
        config = PriorConfig(diminishing_returns_penalty=0.5)

        # First call: nothing applied
        priors_before = compute_contextual_priors(sql, transform_ids, [], config)

        # Second call: push_pred already applied
        priors_after = compute_contextual_priors(
            sql, transform_ids, ["push_pred"], config
        )

        # push_pred's relative prior should be lower when already applied
        before_ratio = priors_before["push_pred"].prior / priors_before["reorder_join"].prior
        after_ratio = priors_after["push_pred"].prior / priors_after["reorder_join"].prior

        assert after_ratio < before_ratio

    def test_high_value_boost(self):
        """High-value category transforms should get boosted."""
        sql = "SELECT * FROM users"
        transform_ids = ["or_to_union", "remove_redundant"]  # high_value vs standard
        config = PriorConfig(
            use_kb_weights=False,  # Disable KB weights to isolate high_value boost
            use_opportunity_detection=False,
            high_value_boost=1.5,
        )

        priors = compute_contextual_priors(sql, transform_ids, [], config)

        # or_to_union (high_value) should have higher prior
        assert priors["or_to_union"].prior > priors["remove_redundant"].prior

    def test_disabled_opportunity_detection(self):
        """Can disable opportunity detection."""
        sql = "SELECT * FROM users WHERE id = 1 OR name = 'test'"
        transform_ids = ["or_to_union", "remove_redundant"]
        config = PriorConfig(use_opportunity_detection=False, use_kb_weights=False)

        priors = compute_contextual_priors(sql, transform_ids, [], config)

        # Without opportunity detection and KB weights, should be more uniform
        # (only high_value boost applies)
        for prior in priors.values():
            assert prior.boost_reason is None or "opportunity" not in prior.boost_reason


# =============================================================================
# LLM RANKER TESTS
# =============================================================================

class TestRankingToProiors:
    """Tests for ranking_to_priors()."""

    def test_ranking_to_priors_sum_to_one(self):
        """Converted priors should sum to 1.0."""
        ranking = ["push_pred", "or_to_union", "correlated_to_cte"]
        candidates = ranking.copy()

        priors = ranking_to_priors(ranking, candidates)

        total = sum(priors.values())
        assert abs(total - 1.0) < 1e-6

    def test_ranking_first_highest_prior(self):
        """First-ranked transform should have highest prior."""
        ranking = ["push_pred", "or_to_union", "correlated_to_cte"]
        candidates = ranking.copy()

        priors = ranking_to_priors(ranking, candidates)

        assert priors["push_pred"] > priors["or_to_union"]
        assert priors["or_to_union"] > priors["correlated_to_cte"]

    def test_ranking_with_missing_candidates(self):
        """Missing candidates should get small non-zero prior."""
        ranking = ["push_pred", "or_to_union"]
        candidates = ["push_pred", "or_to_union", "remove_redundant"]

        priors = ranking_to_priors(ranking, candidates)

        assert priors["remove_redundant"] > 0
        assert priors["remove_redundant"] < priors["or_to_union"]

    def test_empty_ranking_uniform_fallback(self):
        """Empty ranking should give uniform priors."""
        ranking = []
        candidates = ["push_pred", "or_to_union"]

        priors = ranking_to_priors(ranking, candidates)

        assert abs(priors["push_pred"] - priors["or_to_union"]) < 1e-6


class TestParseRankingResponse:
    """Tests for _parse_ranking_response()."""

    def test_parse_valid_json(self):
        """Parse clean JSON response."""
        response = '{"ranking": ["push_pred", "or_to_union", "correlated_to_cte"]}'
        candidates = ["push_pred", "or_to_union", "correlated_to_cte"]

        ranking = _parse_ranking_response(response, candidates)

        assert ranking == ["push_pred", "or_to_union", "correlated_to_cte"]

    def test_parse_json_in_code_block(self):
        """Parse JSON inside markdown code block."""
        response = '''Here's the ranking:
```json
{"ranking": ["push_pred", "or_to_union"]}
```
'''
        candidates = ["push_pred", "or_to_union"]

        ranking = _parse_ranking_response(response, candidates)

        assert ranking == ["push_pred", "or_to_union"]

    def test_parse_json_with_surrounding_text(self):
        """Parse JSON with text before/after."""
        response = '''Based on the query analysis:
{"ranking": ["or_to_union", "push_pred"]}
This ranking prioritizes OR decomposition.'''
        candidates = ["push_pred", "or_to_union"]

        ranking = _parse_ranking_response(response, candidates)

        assert ranking == ["or_to_union", "push_pred"]

    def test_fallback_on_invalid_json(self):
        """Return None on invalid JSON."""
        response = "This is not JSON at all"
        candidates = ["push_pred", "or_to_union"]

        ranking = _parse_ranking_response(response, candidates)

        assert ranking is None

    def test_fallback_on_missing_ranking_key(self):
        """Return None if 'ranking' key missing."""
        response = '{"transforms": ["push_pred", "or_to_union"]}'
        candidates = ["push_pred", "or_to_union"]

        ranking = _parse_ranking_response(response, candidates)

        assert ranking is None

    def test_filters_invalid_transform_ids(self):
        """Filter out transform IDs not in candidates."""
        response = '{"ranking": ["push_pred", "invalid_transform", "or_to_union"]}'
        candidates = ["push_pred", "or_to_union"]

        ranking = _parse_ranking_response(response, candidates)

        assert "invalid_transform" not in ranking
        assert ranking == ["push_pred", "or_to_union"]

    def test_adds_missing_candidates(self):
        """Add missing candidates at end of ranking."""
        response = '{"ranking": ["push_pred"]}'
        candidates = ["push_pred", "or_to_union", "remove_redundant"]

        ranking = _parse_ranking_response(response, candidates)

        assert "push_pred" in ranking
        assert "or_to_union" in ranking
        assert "remove_redundant" in ranking
        assert ranking.index("push_pred") < ranking.index("or_to_union")


class TestShouldUseLLMRanking:
    """Tests for should_use_llm_ranking()."""

    def test_many_candidates_triggers_llm(self):
        """Many candidates (>4) should trigger LLM ranking."""
        result = should_use_llm_ranking(
            node_visit_count=1,
            node_avg_reward=0.5,
            num_candidates=5,
        )
        assert result is True

    def test_few_candidates_no_trigger(self):
        """Few candidates should not trigger LLM."""
        result = should_use_llm_ranking(
            node_visit_count=1,
            node_avg_reward=0.5,
            num_candidates=3,
        )
        assert result is False

    def test_stuck_node_triggers_llm(self):
        """Stuck node (high visits, low reward, high failure) triggers LLM."""
        children_stats = {
            "push_pred": (2, 0.05),      # Low reward
            "or_to_union": (2, 0.02),    # Low reward
            "remove_redundant": (2, 0.01),  # Low reward
        }

        result = should_use_llm_ranking(
            node_visit_count=6,
            node_avg_reward=0.1,
            num_candidates=3,
            children_stats=children_stats,
        )
        assert result is True

    def test_healthy_node_no_trigger(self):
        """Node with good reward should not trigger LLM."""
        result = should_use_llm_ranking(
            node_visit_count=10,
            node_avg_reward=0.8,
            num_candidates=3,
        )
        assert result is False


# =============================================================================
# PUCT SCORE TESTS
# =============================================================================

class TestPUCTScore:
    """Tests for MCTSNode.puct_score()."""

    def test_unvisited_node_exploration_only(self):
        """Unvisited node should have pure exploration score."""
        node = MCTSNode(
            query_sql="SELECT * FROM users",
            visit_count=0,
            total_reward=0.0,
        )

        prior = 0.3
        parent_visits = 10
        c_puct = 2.0

        score = node.puct_score(parent_visits, prior, c_puct)

        # For unvisited: c_puct * prior * sqrt(parent_visits + 1)
        expected = c_puct * prior * math.sqrt(parent_visits + 1)
        assert abs(score - expected) < 1e-6

    def test_visited_node_full_formula(self):
        """Visited node should use full PUCT formula."""
        node = MCTSNode(
            query_sql="SELECT * FROM users",
            visit_count=5,
            total_reward=2.0,  # avg_reward = 0.4
        )

        prior = 0.3
        parent_visits = 20
        c_puct = 2.0

        score = node.puct_score(parent_visits, prior, c_puct)

        # PUCT = Q + c * P * sqrt(N) / (1 + n)
        expected = 0.4 + c_puct * prior * math.sqrt(parent_visits) / (1 + 5)
        assert abs(score - expected) < 1e-6

    def test_higher_prior_higher_exploration(self):
        """Higher prior should give higher exploration bonus."""
        node = MCTSNode(
            query_sql="SELECT * FROM users",
            visit_count=3,
            total_reward=0.6,
        )

        parent_visits = 15
        c_puct = 2.0

        score_low_prior = node.puct_score(parent_visits, 0.1, c_puct)
        score_high_prior = node.puct_score(parent_visits, 0.4, c_puct)

        assert score_high_prior > score_low_prior

    def test_more_visits_less_exploration(self):
        """More visits should reduce exploration bonus."""
        node_few_visits = MCTSNode(
            query_sql="SELECT * FROM users",
            visit_count=2,
            total_reward=0.4,  # avg = 0.2
        )
        node_many_visits = MCTSNode(
            query_sql="SELECT * FROM users",
            visit_count=20,
            total_reward=4.0,  # same avg = 0.2
        )

        parent_visits = 50
        prior = 0.3
        c_puct = 2.0

        score_few = node_few_visits.puct_score(parent_visits, prior, c_puct)
        score_many = node_many_visits.puct_score(parent_visits, prior, c_puct)

        # Same exploitation, but fewer visits = more exploration
        assert score_few > score_many


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestPriorIntegration:
    """Integration tests for the prior system."""

    def test_get_priors_for_node_without_llm(self):
        """get_priors_for_node should work without LLM client."""
        sql = "SELECT * FROM users WHERE id = 1 OR name = 'test'"
        transform_ids = ["push_pred", "or_to_union", "remove_redundant"]
        config = PriorConfig(use_llm_ranking=False)

        priors = get_priors_for_node(
            sql=sql,
            transform_ids=transform_ids,
            applied_transforms=[],
            config=config,
            llm_client=None,
        )

        assert len(priors) == 3
        total = sum(p.prior for p in priors.values())
        assert abs(total - 1.0) < 1e-6

    def test_get_priors_for_node_llm_fallback(self):
        """get_priors_for_node should fallback on LLM error."""
        sql = "SELECT * FROM users"
        transform_ids = ["push_pred", "or_to_union"]
        config = PriorConfig(use_llm_ranking=True)

        # Mock LLM client that raises error
        mock_llm = Mock()
        mock_llm.analyze.side_effect = Exception("LLM error")

        priors = get_priors_for_node(
            sql=sql,
            transform_ids=transform_ids,
            applied_transforms=[],
            config=config,
            llm_client=mock_llm,
        )

        # Should fallback to contextual priors
        assert len(priors) == 2
        assert all(p.source == "contextual" for p in priors.values())
