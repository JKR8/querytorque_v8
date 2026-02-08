from __future__ import annotations

import json
from pathlib import Path

from ado.analyst_session import AnalystIteration, AnalystSession
from ado.pipeline import Pipeline
from ado.prompts.swarm_parsers import parse_fan_out_response
from ado.schemas import PipelineResult


class _DummyPipeline:
    def __init__(self, benchmark_dir: Path, original_sql: str):
        self.benchmark_dir = benchmark_dir
        self._original_sql = original_sql

    def load_query(self, query_id: str):
        return self._original_sql


def test_swarm_parser_normalizes_missing_and_duplicate_worker_ids():
    response = """WORKER_1:
STRATEGY: first_strategy
EXAMPLES: ex1, ex2, ex3
HINT: first hint

WORKER_1:
STRATEGY: duplicate_strategy
EXAMPLES: ex4, ex5, ex6
HINT: duplicate hint

WORKER_3:
STRATEGY: third_strategy
EXAMPLES: ex7, ex8, ex9
HINT: third hint

WORKER_4:
STRATEGY: fourth_strategy
EXAMPLES: ex10, ex11, ex12
HINT: fourth hint
"""

    assignments = parse_fan_out_response(response)

    assert [a.worker_id for a in assignments] == [1, 2, 3, 4]
    assert assignments[0].strategy == "first_strategy"
    assert assignments[1].strategy == "fallback_2"


def test_analyst_session_persists_analysis_prompt_and_rewrite_response(tmp_path: Path):
    benchmark_dir = tmp_path / "bench"
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    query_id = "query_1"
    original_sql = "select 1"
    pipeline = _DummyPipeline(benchmark_dir=benchmark_dir, original_sql=original_sql)

    session = AnalystSession(
        pipeline=pipeline,
        query_id=query_id,
        original_sql=original_sql,
        max_iterations=1,
        target_speedup=2.0,
        n_workers=1,
    )

    session.iterations.append(
        AnalystIteration(
            iteration=0,
            original_sql=original_sql,
            optimized_sql="select 1 as one",
            status="IMPROVED",
            speedup=1.1,
            transforms=["pushdown"],
            prompt="rewrite prompt",
            analysis="analysis response",
            analysis_prompt="analysis prompt",
            rewrite_response="rewrite response",
            examples_used=["ex1"],
            failure_analysis="failure analysis",
        )
    )
    session.best_speedup = 1.1
    session.best_sql = "select 1 as one"

    save_dir = session.save_session()
    loaded = AnalystSession.load_session(pipeline=pipeline, session_dir=save_dir)

    assert loaded.iterations[0].analysis_prompt == "analysis prompt"
    assert loaded.iterations[0].rewrite_response == "rewrite response"
    assert (save_dir / "iteration_00" / "analysis_prompt.txt").read_text() == "analysis prompt"
    assert (save_dir / "iteration_00" / "rewrite_response.txt").read_text() == "rewrite response"


def test_run_state_writes_state_prompt_and_response_files(tmp_path: Path):
    benchmark_dir = tmp_path / "benchmark"
    queries_dir = benchmark_dir / "queries"
    queries_dir.mkdir(parents=True, exist_ok=True)
    (queries_dir / "query_1.sql").write_text("select 1")

    config = {
        "engine": "duckdb",
        "benchmark": "tpcds",
        "db_path": ":memory:",
        "scale_factor": 1,
        "timeout_seconds": 1,
        "validation_method": "3-run",
        "n_queries": 1,
        "workers_state_0": 1,
        "workers_state_n": 1,
        "promote_threshold": 1.05,
    }
    (benchmark_dir / "config.json").write_text(json.dumps(config))

    pipeline = Pipeline(benchmark_dir=benchmark_dir)
    pipeline.run_query = lambda **kwargs: PipelineResult(
        query_id=kwargs["query_id"],
        status="NEUTRAL",
        speedup=1.0,
        original_sql=kwargs["sql"],
        optimized_sql=kwargs["sql"],
        transforms_applied=[],
        prompt="state prompt artifact",
        response="state response artifact",
    )
    pipeline.learner.save_learning_summary = lambda: None
    pipeline.learner.generate_benchmark_history = lambda _benchmark_dir: None

    pipeline.run_state(state_num=0, n_workers=1, query_ids=["query_1"])

    assert (benchmark_dir / "state_0" / "prompts" / "query_1.txt").read_text() == "state prompt artifact"
    assert (benchmark_dir / "state_0" / "responses" / "query_1.txt").read_text() == "state response artifact"
