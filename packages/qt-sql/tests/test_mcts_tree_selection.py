"""Tests for MCTSTree selection/expansion behavior in PUCT (AlphaZero-style) mode."""

from unittest.mock import Mock, patch

from qt_sql.optimization.mcts.priors import PriorConfig, TransformPrior
from qt_sql.optimization.mcts.tree import MCTSTree


def _make_tree(transform_ids=None, prior_config=None, use_dag_mode=False):
    if transform_ids is None:
        transform_ids = ["a", "b"]
    if prior_config is None:
        prior_config = PriorConfig()
    return MCTSTree(
        original_sql="SELECT 1",
        llm_client=Mock(),
        validator=Mock(),
        prior_config=prior_config,
        transform_ids=transform_ids,
        use_dag_mode=use_dag_mode,
    )


def test_select_returns_best_untried_action_in_puct():
    tree = _make_tree(prior_config=PriorConfig(use_puct=True))

    priors = {
        "a": TransformPrior("a", 0.1, "test"),
        "b": TransformPrior("b", 0.9, "test"),
    }

    with patch.object(tree, "_get_priors", return_value=priors):
        node, action = tree.select()

    assert node is tree.root
    assert action == "b"


def test_select_descends_when_tried_action_best_in_puct():
    tree = _make_tree(prior_config=PriorConfig(use_puct=True))

    # Create a strong tried child.
    child = tree.root.add_child(transform_id="a", new_sql="SELECT 1")
    tree.root.visit_count = 10
    child.visit_count = 5
    child.total_reward = 5.0  # avg_reward = 1.0

    priors = {
        "a": TransformPrior("a", 0.6, "test"),
        "b": TransformPrior("b", 0.1, "test"),
    }

    with patch.object(tree, "_get_priors", return_value=priors):
        node, action = tree.select()

    # Should descend into the tried child, then pick the remaining untried action.
    assert node is child
    assert action == "b"


def test_expand_uses_forced_transform_when_provided():
    tree = _make_tree(
        prior_config=PriorConfig(use_puct=False),
        transform_ids=["a", "b"],
        use_dag_mode=False,
    )

    with patch(
        "qt_sql.optimization.mcts.tree.apply_transformation",
        return_value=("SELECT 2", None),
    ):
        child = tree.expand(tree.root, transform_id="b")

    assert child is not None
    assert "b" in tree.root.children
    assert child.applied_transforms[-1] == "b"


def test_prior_cache_allows_llm_activation_later():
    tree = _make_tree(prior_config=PriorConfig(use_puct=True, use_llm_ranking=True))

    calls = []

    def fake_get_priors_for_node(*, llm_client=None, **kwargs):
        calls.append(llm_client)
        return {"a": TransformPrior("a", 1.0, "test")}

    with patch(
        "qt_sql.optimization.mcts.llm_ranker.should_use_llm_ranking",
        side_effect=[False, True],
    ), patch(
        "qt_sql.optimization.mcts.tree.get_priors_for_node",
        side_effect=fake_get_priors_for_node,
    ):
        tree._get_priors(tree.root)
        tree._get_priors(tree.root)

    assert calls == [None, tree.llm_client]
