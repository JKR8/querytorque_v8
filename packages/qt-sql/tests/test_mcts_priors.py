"""Tests for policy priors in Hybrid MCTS."""

from qt_sql.optimization.mcts.policy import PolicyNetwork, PolicyConfig


def test_policy_uniform_priors():
    policy = PolicyNetwork(PolicyConfig())
    priors = policy.get_priors(
        sql="SELECT 1",
        available_rules=["a", "b", "c"],
    )

    assert abs(sum(priors.values()) - 1.0) < 1e-6
    assert priors["a"] == priors["b"] == priors["c"]
