"""Tests for MCTSTree selection behavior in Hybrid MCTS mode."""

from unittest.mock import Mock

from qt_sql.optimization.mcts.node import MCTSNode
from qt_sql.optimization.mcts.tree import MCTSTree, MCTSConfig
from qt_sql.optimization.mcts.policy import PolicyNetwork, PolicyConfig
from qt_sql.optimization.mcts.benchmark import BenchmarkRunner, BenchmarkResult


class _FakeBenchmark(BenchmarkRunner):
    def __init__(self):
        pass

    def run_query_robust(self, sql: str, timeout_s=None):
        return BenchmarkResult(latency_s=1.0, timed_out=False, raw_timings_s=[1.0])


def test_select_chooses_best_puct_child():
    tree = MCTSTree(
        original_sql="SELECT 1",
        policy=PolicyNetwork(PolicyConfig()),
        benchmark=_FakeBenchmark(),
        config=MCTSConfig(c_puct=1.0, fpu=1.5),
        transform_ids=[],
    )

    root = tree.root
    root.expanded = True
    root.visit_count = 10

    child_a = root.add_child(transform="a", sql="SELECT 1", prior=0.1)
    child_b = root.add_child(transform="b", sql="SELECT 1", prior=0.9)

    child_a.visit_count = 1
    child_a.value_sum = 0.5

    child_b.visit_count = 1
    child_b.value_sum = 0.1

    selected = tree.select()
    assert selected is child_b
