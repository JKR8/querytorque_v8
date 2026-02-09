"""Validate the analyst pipeline produces correct prompts with all required sections.

Tests use the saved artifacts from analyst_query_51/ to verify that every step
of the pipeline populates its output as intended. This is a regression test
to prevent the history-loss bug from recurring (where history=None was passed
to prompt builders, causing the LLM to repeat failed transforms).

Tested artifacts (saved by run_analyst_prompt_only.py):
    00_input.sql           — original query
    01_faiss_examples.json — FAISS retrieval results
    02_analyst_prompt.txt   — sent to analyst LLM
    03_analyst_response.txt — analyst LLM output
    04_analysis_formatted.txt — formatted for embedding in rewrite prompt
    05_rewrite_prompt.txt   — sent to rewrite LLM
"""

import ast
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure packages are importable
QT_SQL = Path(__file__).resolve().parents[1]
REPO = QT_SQL.parents[1]
sys.path.insert(0, str(QT_SQL))
sys.path.insert(0, str(REPO / "packages" / "qt-shared"))

ARTIFACTS_DIR = (
    QT_SQL / "ado" / "benchmarks" / "duckdb_tpcds" / "analyst_query_51"
)
ANALYST_PROMPT_SCRIPT = (
    QT_SQL / "ado" / "benchmarks" / "duckdb_tpcds" / "run_analyst_prompt_only.py"
)


def _extract_load_query_history_fn(benchmark_dir: Path):
    """Load the real load_query_history() function body from the script.

    run_analyst_prompt_only.py is a script with heavy top-level side effects, so
    we extract and execute only the function definition for unit testing.
    """
    source = ANALYST_PROMPT_SCRIPT.read_text()
    parsed = ast.parse(source, filename=str(ANALYST_PROMPT_SCRIPT))
    fn_node = next(
        (
            node
            for node in parsed.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "load_query_history"
        ),
        None,
    )
    assert fn_node is not None, "load_query_history() not found in script"

    module = ast.Module(body=[fn_node], type_ignores=[])
    ast.fix_missing_locations(module)

    namespace = {
        "json": json,
        "BENCHMARK_DIR": benchmark_dir,
        "logger": MagicMock(),
    }
    exec(compile(module, str(ANALYST_PROMPT_SCRIPT), "exec"), namespace)
    return namespace["load_query_history"]


# ─── Fixture: load all saved artifacts ────────────────────────────
@pytest.fixture(scope="module")
def artifacts():
    """Load all analyst_query_51 artifacts."""
    result = {}
    for name in [
        "00_input.sql",
        "01_faiss_examples.json",
        "02_analyst_prompt.txt",
        "03_analyst_response.txt",
        "04_analysis_formatted.txt",
        "05_rewrite_prompt.txt",
    ]:
        path = ARTIFACTS_DIR / name
        if path.exists():
            result[name] = path.read_text()
        else:
            result[name] = None
    return result


# ══════════════════════════════════════════════════════════════════
# Step 0: Input SQL
# ══════════════════════════════════════════════════════════════════
class TestStep0InputSQL:
    def test_input_exists(self, artifacts):
        assert artifacts["00_input.sql"] is not None, "00_input.sql missing"

    def test_input_is_valid_sql(self, artifacts):
        sql = artifacts["00_input.sql"]
        assert "SELECT" in sql.upper()
        assert "web_v1" in sql, "Expected web_v1 CTE in Q51"
        assert "store_v1" in sql, "Expected store_v1 CTE in Q51"

    def test_input_has_no_comments(self, artifacts):
        """Comments should be stripped before saving."""
        sql = artifacts["00_input.sql"]
        for line in sql.splitlines():
            assert not line.strip().startswith("--"), (
                f"Comment not stripped: {line}"
            )

    def test_input_has_no_trailing_semicolon(self, artifacts):
        sql = artifacts["00_input.sql"]
        assert not sql.rstrip().endswith(";"), "Trailing semicolon not stripped"


# ══════════════════════════════════════════════════════════════════
# Step 1: FAISS Examples
# ══════════════════════════════════════════════════════════════════
class TestStep1FAISSExamples:
    def test_faiss_json_valid(self, artifacts):
        raw = artifacts["01_faiss_examples.json"]
        assert raw is not None, "01_faiss_examples.json missing"
        examples = json.loads(raw)
        assert isinstance(examples, list)

    def test_faiss_returns_3_examples(self, artifacts):
        examples = json.loads(artifacts["01_faiss_examples.json"])
        assert len(examples) == 3, f"Expected 3 FAISS examples, got {len(examples)}"

    def test_faiss_examples_have_required_fields(self, artifacts):
        examples = json.loads(artifacts["01_faiss_examples.json"])
        required = {"id"}
        for ex in examples:
            assert required.issubset(ex.keys()), (
                f"Example missing fields: {required - ex.keys()}"
            )

    def test_faiss_picks_deferred_window(self, artifacts):
        """Q51 is the deferred_window_aggregation gold example — FAISS should pick it."""
        examples = json.loads(artifacts["01_faiss_examples.json"])
        ids = [ex["id"] for ex in examples]
        assert "deferred_window_aggregation" in ids, (
            f"Expected deferred_window_aggregation in FAISS picks, got: {ids}"
        )


# ══════════════════════════════════════════════════════════════════
# Step 2: Analyst Prompt
# ══════════════════════════════════════════════════════════════════
class TestStep2AnalystPrompt:
    def test_analyst_prompt_exists(self, artifacts):
        assert artifacts["02_analyst_prompt.txt"] is not None

    def test_has_query_id(self, artifacts):
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "query_51" in prompt

    def test_has_dialect(self, artifacts):
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "duckdb" in prompt.lower()

    def test_has_original_sql(self, artifacts):
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "web_v1" in prompt
        assert "store_v1" in prompt
        assert "ws_sales_price" in prompt

    def test_has_dag_structure(self, artifacts):
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "## Query Structure (DAG)" in prompt

    def test_dag_has_nodes(self, artifacts):
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "### 1. web_v1" in prompt
        assert "### 2. store_v1" in prompt
        assert "### 3. main_query" in prompt

    def test_dag_has_cost_stats(self, artifacts):
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "Cost" in prompt
        assert "rows" in prompt

    def test_has_previous_optimization_attempts(self, artifacts):
        """Critical: history must be present to prevent repeating failed transforms."""
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "## Previous Optimization Attempts" in prompt, (
            "CRITICAL: Analyst prompt missing history section. "
            "Without this, the LLM will repeat failed transforms."
        )

    def test_history_shows_date_cte_isolate_regression(self, artifacts):
        """Q51's state_0 attempt was date_cte_isolate → 0.87x regression."""
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "date_cte_isolate" in prompt
        assert "REGRESSION" in prompt
        assert "0.87" in prompt

    def test_has_failure_analysis_task(self, artifacts):
        """When history has attempts, the analyst must be asked to analyze failures."""
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "### 4. FAILURE ANALYSIS" in prompt, (
            "CRITICAL: Analyst prompt missing FAILURE ANALYSIS section. "
            "Without this, the LLM doesn't learn from past failures."
        )

    def test_has_faiss_picks_listed(self, artifacts):
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "deferred_window_aggregation" in prompt
        assert "early_filter" in prompt

    def test_has_all_gold_examples_catalogue(self, artifacts):
        """Analyst should see ALL available examples, not just FAISS picks."""
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "All available gold examples" in prompt
        assert "decorrelate" in prompt
        assert "single_pass_aggregation" in prompt
        assert "or_to_union" in prompt

    def test_has_structured_task(self, artifacts):
        """Prompt must guide the analyst through the 6-step methodology."""
        prompt = artifacts["02_analyst_prompt.txt"]
        assert "### 1. STRUCTURAL BREAKDOWN" in prompt
        assert "### 2. BOTTLENECK IDENTIFICATION" in prompt
        assert "### 3. PROPOSED OPTIMIZATION" in prompt
        assert "### 5. RECOMMENDED STRATEGY" in prompt
        assert "### 6. EXAMPLE SELECTION" in prompt


# ══════════════════════════════════════════════════════════════════
# Step 3: Analyst Response
# ══════════════════════════════════════════════════════════════════
class TestStep3AnalystResponse:
    def test_response_exists(self, artifacts):
        assert artifacts["03_analyst_response.txt"] is not None

    def test_response_has_structural_breakdown(self, artifacts):
        response = artifacts["03_analyst_response.txt"]
        assert "STRUCTURAL BREAKDOWN" in response.upper() or "web_v1" in response

    def test_response_has_bottleneck(self, artifacts):
        response = artifacts["03_analyst_response.txt"]
        assert "BOTTLENECK" in response.upper() or "window" in response.lower()

    def test_response_addresses_failure(self, artifacts):
        """Analyst should explain WHY date_cte_isolate failed."""
        response = artifacts["03_analyst_response.txt"]
        assert "date_cte_isolate" in response, (
            "Analyst response doesn't address the prior failed transform"
        )

    def test_response_has_recommended_strategy(self, artifacts):
        response = artifacts["03_analyst_response.txt"]
        assert "RECOMMEND" in response.upper() or "strategy" in response.lower()

    def test_response_has_example_selection(self, artifacts):
        response = artifacts["03_analyst_response.txt"]
        assert "EXAMPLES:" in response, (
            "Analyst response missing EXAMPLES: line for override parsing"
        )


# ══════════════════════════════════════════════════════════════════
# Step 4: Analysis Formatted
# ══════════════════════════════════════════════════════════════════
class TestStep4FormattedAnalysis:
    def test_formatted_exists(self, artifacts):
        assert artifacts["04_analysis_formatted.txt"] is not None

    def test_formatted_has_expert_header(self, artifacts):
        formatted = artifacts["04_analysis_formatted.txt"]
        assert "## Expert Analysis" in formatted

    def test_formatted_has_structure(self, artifacts):
        formatted = artifacts["04_analysis_formatted.txt"]
        assert "### Query Structure" in formatted or "### Performance Bottleneck" in formatted

    def test_formatted_has_failure_lessons(self, artifacts):
        formatted = artifacts["04_analysis_formatted.txt"]
        assert "Lessons from Previous Failures" in formatted or "date_cte_isolate" in formatted, (
            "Formatted analysis should carry failure lessons through to rewrite prompt"
        )


# ══════════════════════════════════════════════════════════════════
# Step 5: Rewrite Prompt
# ══════════════════════════════════════════════════════════════════
class TestStep5RewritePrompt:
    def test_rewrite_prompt_exists(self, artifacts):
        assert artifacts["05_rewrite_prompt.txt"] is not None

    def test_has_role_task(self, artifacts):
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "SQL query rewrite engine" in prompt

    def test_has_original_sql(self, artifacts):
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "web_v1" in prompt
        assert "store_v1" in prompt

    def test_has_dag_structure(self, artifacts):
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "## Query Structure (DAG)" in prompt

    def test_has_optimization_history(self, artifacts):
        """Critical: rewrite prompt must include history."""
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "## Optimization History" in prompt, (
            "CRITICAL: Rewrite prompt missing Optimization History section"
        )

    def test_history_shows_regression(self, artifacts):
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "date_cte_isolate" in prompt
        assert "REGRESSION" in prompt or "0.87" in prompt

    def test_has_expert_analysis(self, artifacts):
        """Rewrite prompt should embed the analyst's structural guidance."""
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "## Expert Analysis" in prompt or "Expert Analysis" in prompt, (
            "CRITICAL: Rewrite prompt missing Expert Analysis section"
        )

    def test_has_reference_examples(self, artifacts):
        """Gold examples with before/after SQL must be included."""
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "## Reference Examples" in prompt
        assert "BEFORE (slow)" in prompt or "BEFORE" in prompt
        assert "AFTER (fast)" in prompt or "AFTER" in prompt

    def test_has_deferred_window_example(self, artifacts):
        """The exact gold example for this query pattern should be included."""
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "deferred_window_aggregation" in prompt

    def test_has_constraints(self, artifacts):
        """Safety constraints must sandwich the examples."""
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "## Constraints" in prompt
        assert "SEMANTIC_EQUIVALENCE" in prompt
        assert "LITERAL_PRESERVATION" in prompt
        assert "CTE_COLUMN_COMPLETENESS" in prompt

    def test_has_output_format(self, artifacts):
        prompt = artifacts["05_rewrite_prompt.txt"]
        assert "## Output" in prompt
        assert "```sql" in prompt


# ══════════════════════════════════════════════════════════════════
# Unit tests: history loading functions
# ══════════════════════════════════════════════════════════════════
class TestHistoryLoading:
    """Unit tests for history loading from state_N and leaderboard."""

    def test_manual_script_loads_state_history(self):
        """Verify real script load_query_history() finds state_N validation results."""
        benchmark_dir = QT_SQL / "ado" / "benchmarks" / "duckdb_tpcds"
        state_0_val = benchmark_dir / "state_0" / "validation"
        if not state_0_val.exists():
            pytest.skip("No state_0/validation/ directory")

        # Find any query that has validation data
        val_files = list(state_0_val.glob("*.json"))
        if not val_files:
            pytest.skip("No validation files in state_0")

        query_id = val_files[0].stem
        load_query_history = _extract_load_query_history_fn(benchmark_dir)
        history = load_query_history(query_id)
        assert history is not None, f"Should find history for {query_id}"
        assert len(history["attempts"]) > 0
        assert "status" in history["attempts"][0]
        assert "speedup" in history["attempts"][0]

    def test_manual_script_loads_leaderboard_only_history(self):
        """Verify script loader can return history from leaderboard-only source."""
        with tempfile.TemporaryDirectory() as tmpdir:
            benchmark_dir = Path(tmpdir)
            leaderboard = {
                "queries": [
                    {
                        "query_id": "query_99",
                        "source": "analyst_mode",
                        "status": "IMPROVED",
                        "speedup": 1.23,
                        "transforms": ["deferred_window_aggregation"],
                    }
                ]
            }
            (benchmark_dir / "leaderboard.json").write_text(json.dumps(leaderboard))

            load_query_history = _extract_load_query_history_fn(benchmark_dir)
            history = load_query_history("query_99")
            assert history is not None
            assert len(history["attempts"]) == 1
            assert history["attempts"][0]["source"] == "analyst_mode"
            assert history["attempts"][0]["status"] == "IMPROVED"

    def test_analyst_session_loads_batch_history(self):
        """Verify AnalystSession._build_iteration_history() loads state_N results."""
        from qt_sql.analyst_session import AnalystSession

        benchmark_dir = QT_SQL / "ado" / "benchmarks" / "duckdb_tpcds"
        state_0_val = benchmark_dir / "state_0" / "validation"
        if not state_0_val.exists():
            pytest.skip("No state_0/validation/ directory")

        # Find a query with known validation data
        val_files = list(state_0_val.glob("*.json"))
        if not val_files:
            pytest.skip("No validation files")

        query_id = val_files[0].stem

        # Mock the pipeline with just benchmark_dir
        mock_pipeline = MagicMock()
        mock_pipeline.benchmark_dir = benchmark_dir

        session = AnalystSession.__new__(AnalystSession)
        session.pipeline = mock_pipeline
        session.query_id = query_id
        session.iterations = []  # Empty — first iteration
        session.best_speedup = 1.0
        session.original_sql = "SELECT 1"
        session.best_sql = "SELECT 1"

        history = session._build_iteration_history()
        assert history is not None, (
            f"CRITICAL: _build_iteration_history() returned None for {query_id} "
            f"despite state_0/validation/{query_id}.json existing. "
            f"This means the first analyst iteration has no history."
        )
        assert len(history["attempts"]) > 0
        assert history["attempts"][0]["source"] == "state_0"

    def test_analyst_session_no_history_returns_none(self):
        """When no batch results and no iterations, should return None."""
        from qt_sql.analyst_session import AnalystSession

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_pipeline = MagicMock()
            mock_pipeline.benchmark_dir = Path(tmpdir)

            session = AnalystSession.__new__(AnalystSession)
            session.pipeline = mock_pipeline
            session.query_id = "nonexistent_query"
            session.iterations = []
            session.best_speedup = 1.0
            session.original_sql = "SELECT 1"
            session.best_sql = "SELECT 1"

            history = session._build_iteration_history()
            assert history is None

    def test_analyst_session_combines_batch_and_session_history(self):
        """When both batch results and session iterations exist, both should appear."""
        from qt_sql.analyst_session import AnalystSession, AnalystIteration

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            # Create fake state_0 validation
            val_dir = tmpdir / "state_0" / "validation"
            val_dir.mkdir(parents=True)
            (val_dir / "q99.json").write_text(json.dumps({
                "status": "REGRESSION",
                "speedup": 0.75,
                "transforms_applied": ["bad_transform"],
            }))

            mock_pipeline = MagicMock()
            mock_pipeline.benchmark_dir = tmpdir

            session = AnalystSession.__new__(AnalystSession)
            session.pipeline = mock_pipeline
            session.query_id = "q99"
            session.best_speedup = 1.2
            session.original_sql = "SELECT 1"
            session.best_sql = "SELECT 1"

            # Add a session iteration
            iteration = AnalystIteration(
                iteration=0,
                original_sql="SELECT 1",
                optimized_sql="SELECT 2",
                status="IMPROVED",
                speedup=1.2,
                transforms=["good_transform"],
            )
            session.iterations = [iteration]

            history = session._build_iteration_history()
            assert history is not None
            assert len(history["attempts"]) == 2, (
                f"Expected 2 attempts (1 batch + 1 session), got {len(history['attempts'])}"
            )
            # Batch result should come first
            assert history["attempts"][0]["source"] == "state_0"
            assert history["attempts"][0]["status"] == "REGRESSION"
            # Session iteration should come second
            assert history["attempts"][1]["source"] == "analyst_iter_0"
            assert history["attempts"][1]["status"] == "IMPROVED"


# ══════════════════════════════════════════════════════════════════
# Unit tests: prompt builders accept and use history
# ══════════════════════════════════════════════════════════════════
class TestPromptBuildersUseHistory:
    """Verify build_analysis_prompt and Prompter.build_prompt include history."""

    @pytest.fixture
    def sample_history(self):
        return {
            "attempts": [
                {
                    "state": 0,
                    "source": "state_0",
                    "status": "REGRESSION",
                    "speedup": 0.87,
                    "transforms": ["date_cte_isolate"],
                }
            ],
            "promotion": None,
        }

    @pytest.fixture
    def simple_dag(self):
        """Build a minimal DAG from Q51."""
        from qt_sql.dag import DagBuilder, CostAnalyzer
        sql = "SELECT 1 AS x"
        dag = DagBuilder(sql, dialect="duckdb").build()
        costs = CostAnalyzer(dag).analyze()
        return dag, costs

    def test_analyst_prompt_includes_history(self, sample_history, simple_dag):
        from qt_sql.analyst import build_analysis_prompt

        dag, costs = simple_dag
        prompt = build_analysis_prompt(
            query_id="test_q",
            sql="SELECT 1",
            dag=dag,
            costs=costs,
            history=sample_history,
            dialect="duckdb",
        )
        assert "## Previous Optimization Attempts" in prompt
        assert "date_cte_isolate" in prompt
        assert "REGRESSION" in prompt
        assert "0.87" in prompt

    def test_analyst_prompt_has_failure_analysis_task(self, sample_history, simple_dag):
        from qt_sql.analyst import build_analysis_prompt

        dag, costs = simple_dag
        prompt = build_analysis_prompt(
            query_id="test_q",
            sql="SELECT 1",
            dag=dag,
            costs=costs,
            history=sample_history,
            dialect="duckdb",
        )
        assert "FAILURE ANALYSIS" in prompt

    def test_analyst_prompt_no_history_no_section(self, simple_dag):
        from qt_sql.analyst import build_analysis_prompt

        dag, costs = simple_dag
        prompt = build_analysis_prompt(
            query_id="test_q",
            sql="SELECT 1",
            dag=dag,
            costs=costs,
            history=None,
            dialect="duckdb",
        )
        assert "## Previous Optimization Attempts" not in prompt
        assert "FAILURE ANALYSIS" not in prompt

    def test_rewrite_prompt_includes_history(self, sample_history, simple_dag):
        from qt_sql.node_prompter import Prompter

        dag, costs = simple_dag
        prompter = Prompter()
        prompt = prompter.build_prompt(
            query_id="test_q",
            full_sql="SELECT 1",
            dag=dag,
            costs=costs,
            history=sample_history,
            dialect="duckdb",
        )
        assert "## Optimization History" in prompt
        assert "date_cte_isolate" in prompt
        assert "REGRESSION" in prompt

    def test_rewrite_prompt_no_history_no_section(self, simple_dag):
        from qt_sql.node_prompter import Prompter

        dag, costs = simple_dag
        prompter = Prompter()
        prompt = prompter.build_prompt(
            query_id="test_q",
            full_sql="SELECT 1",
            dag=dag,
            costs=costs,
            history=None,
            dialect="duckdb",
        )
        assert "## Optimization History" not in prompt


class TestPipelineHistoryWiring:
    """Regression tests for history propagation through run_query()."""

    @staticmethod
    def _make_pipeline(use_analyst: bool = True):
        from qt_sql.pipeline import Pipeline

        pipeline = Pipeline.__new__(Pipeline)
        pipeline.use_analyst = use_analyst
        pipeline.provider = "test-provider"
        pipeline.model = "test-model"
        pipeline.analyze_fn = None
        pipeline.config = type("Cfg", (), {"engine": "duckdb"})()
        pipeline.prompter = MagicMock()
        pipeline.prompter.build_prompt.return_value = "PROMPT"
        pipeline.learner = MagicMock()
        pipeline.learner.build_learning_summary.return_value = None
        pipeline.benchmark_dir = Path(".")
        pipeline._semantic_intents = {}
        pipeline._engine_version = None
        return pipeline

    def test_run_query_passes_history_to_analyst_and_rewrite_prompt(self):
        from qt_sql.pipeline import Pipeline

        history = {
            "attempts": [
                {
                    "source": "state_0",
                    "status": "REGRESSION",
                    "speedup": 0.87,
                    "transforms": ["date_cte_isolate"],
                }
            ],
            "promotion": None,
        }
        pipeline = self._make_pipeline(use_analyst=True)
        pipeline._parse_dag = MagicMock(return_value=(MagicMock(), {}, None))
        pipeline._find_examples = MagicMock(return_value=[{"id": "deferred_window_aggregation"}])
        pipeline._find_regression_warnings = MagicMock(return_value=[])
        pipeline._run_analyst = MagicMock(
            return_value=(
                "## Expert Analysis\n...",
                "raw analyst response",
                "analyst prompt text",
                [{"id": "deferred_window_aggregation"}],
            )
        )
        pipeline._validate = MagicMock(return_value=("NEUTRAL", 1.0, [], None))

        with patch("qt_sql.generate.CandidateGenerator") as mock_generator:
            mock_generator.return_value.generate.return_value = []
            result = Pipeline.run_query(
                pipeline,
                query_id="query_1",
                sql="SELECT 1",
                n_workers=1,
                history=history,
                use_analyst=True,
            )

        assert pipeline._run_analyst.call_args.kwargs["history"] is history
        assert pipeline.prompter.build_prompt.call_args.kwargs["history"] is history
        assert result.analysis_prompt == "analyst prompt text"
        assert result.analysis_formatted == "## Expert Analysis\n..."

    def test_run_query_passes_history_without_analyst(self):
        from qt_sql.pipeline import Pipeline

        history = {
            "attempts": [
                {"source": "state_0", "status": "REGRESSION", "speedup": 0.87}
            ],
            "promotion": None,
        }
        pipeline = self._make_pipeline(use_analyst=False)
        pipeline._parse_dag = MagicMock(return_value=(MagicMock(), {}, None))
        pipeline._find_examples = MagicMock(return_value=[{"id": "early_filter"}])
        pipeline._find_regression_warnings = MagicMock(return_value=[])
        pipeline._run_analyst = MagicMock()
        pipeline._validate = MagicMock(return_value=("NEUTRAL", 1.0, [], None))

        with patch("qt_sql.generate.CandidateGenerator") as mock_generator:
            mock_generator.return_value.generate.return_value = []
            Pipeline.run_query(
                pipeline,
                query_id="query_2",
                sql="SELECT 1",
                n_workers=1,
                history=history,
                use_analyst=False,
            )

        pipeline._run_analyst.assert_not_called()
        assert pipeline.prompter.build_prompt.call_args.kwargs["history"] is history

    def test_error_messages_propagate_through_pipeline(self):
        """Verify error_messages from _validate reach the learning record."""
        from qt_sql.pipeline import Pipeline

        pipeline = self._make_pipeline(use_analyst=False)
        pipeline._parse_dag = MagicMock(return_value=(MagicMock(), {}, None))
        pipeline._find_examples = MagicMock(return_value=[])
        pipeline._find_regression_warnings = MagicMock(return_value=[])

        # Simulate ERROR with actual error messages
        pipeline._validate = MagicMock(return_value=(
            "ERROR", 0.0,
            ["Catalog Error: Table 'foo' does not exist", "Binder Error: column 'x' not found"],
            "execution",
        ))

        with patch("qt_sql.generate.CandidateGenerator") as mock_gen:
            mock_gen.return_value.generate.return_value = []
            result = Pipeline.run_query(
                pipeline, query_id="query_fail", sql="SELECT 1", n_workers=1,
            )

        # Verify learner got the error messages
        call_kwargs = pipeline.learner.create_learning_record.call_args.kwargs
        assert call_kwargs["error_messages"] == [
            "Catalog Error: Table 'foo' does not exist",
            "Binder Error: column 'x' not found",
        ]
        assert call_kwargs["error_category"] == "execution"
        assert call_kwargs["status"] == "error"

    def test_fail_status_distinct_from_error(self):
        """Verify FAIL (semantic mismatch) is captured differently from ERROR."""
        from qt_sql.pipeline import Pipeline

        pipeline = self._make_pipeline(use_analyst=False)
        pipeline._parse_dag = MagicMock(return_value=(MagicMock(), {}, None))
        pipeline._find_examples = MagicMock(return_value=[])
        pipeline._find_regression_warnings = MagicMock(return_value=[])

        # Simulate FAIL with semantic error
        pipeline._validate = MagicMock(return_value=(
            "FAIL", 0.0,
            ["Row count mismatch: expected 100, got 87"],
            "semantic",
        ))

        with patch("qt_sql.generate.CandidateGenerator") as mock_gen:
            mock_gen.return_value.generate.return_value = []
            result = Pipeline.run_query(
                pipeline, query_id="query_sem", sql="SELECT 1", n_workers=1,
            )

        call_kwargs = pipeline.learner.create_learning_record.call_args.kwargs
        assert call_kwargs["error_messages"] == ["Row count mismatch: expected 100, got 87"]
        assert call_kwargs["error_category"] == "semantic"

    def test_analyst_session_error_messages_in_iteration(self):
        """Verify AnalystSession captures error_messages in AnalystIteration."""
        from qt_sql.analyst_session import AnalystSession
        from unittest.mock import PropertyMock

        pipeline = self._make_pipeline(use_analyst=True)
        pipeline._parse_dag = MagicMock(return_value=(MagicMock(), {}, None))
        pipeline._find_examples = MagicMock(return_value=[])
        pipeline._find_regression_warnings = MagicMock(return_value=[])
        pipeline._run_analyst = MagicMock(return_value=(None, None, None, []))

        # Simulate ERROR with messages
        pipeline._validate = MagicMock(return_value=(
            "ERROR", 0.0,
            ["Parser Error: syntax error at position 42"],
            "syntax",
        ))

        session = AnalystSession(
            pipeline=pipeline,
            query_id="query_test",
            original_sql="SELECT 1",
            max_iterations=1,
            target_speedup=2.0,
            n_workers=1,
        )

        with patch("qt_sql.generate.CandidateGenerator") as mock_gen:
            mock_gen.return_value.generate.return_value = []
            session.run()

        # Check iteration has error_messages
        assert len(session.iterations) == 1
        it = session.iterations[0]
        assert it.error_messages == ["Parser Error: syntax error at position 42"]
        assert it.error_category == "syntax"
        assert it.status == "ERROR"

    def test_analyst_session_errors_in_history(self):
        """Verify error_messages flow into the history dict for retry prompts."""
        from qt_sql.analyst_session import AnalystSession

        pipeline = self._make_pipeline(use_analyst=True)
        pipeline._parse_dag = MagicMock(return_value=(MagicMock(), {}, None))
        pipeline._find_examples = MagicMock(return_value=[])
        pipeline._find_regression_warnings = MagicMock(return_value=[])
        pipeline._run_analyst = MagicMock(return_value=(None, None, None, []))

        # Simulate ERROR
        pipeline._validate = MagicMock(return_value=(
            "ERROR", 0.0,
            ["column 'rk' must appear in GROUP BY clause"],
            "semantic",
        ))

        session = AnalystSession(
            pipeline=pipeline,
            query_id="query_hist",
            original_sql="SELECT 1",
            max_iterations=2,
            target_speedup=2.0,
            n_workers=1,
        )

        with patch("qt_sql.generate.CandidateGenerator") as mock_gen:
            mock_gen.return_value.generate.return_value = []
            session.run()

        # After 2 iterations, iteration 2's history should include iteration 1's errors
        assert len(session.iterations) == 2
        history = session._build_iteration_history()
        analyst_attempts = [
            a for a in history["attempts"]
            if a.get("source", "").startswith("analyst_iter")
        ]
        assert len(analyst_attempts) == 2
        assert analyst_attempts[0]["error_messages"] == ["column 'rk' must appear in GROUP BY clause"]
        assert analyst_attempts[0]["error_category"] == "semantic"

    def test_retry_preamble_shows_error_messages(self):
        """Verify the retry preamble renders error messages clearly."""
        from qt_sql.node_prompter import Prompter

        history = {
            "attempts": [
                {
                    "source": "analyst_iter_0",
                    "status": "ERROR",
                    "transforms": ["decorrelate"],
                    "failure_analysis": "The ROLLUP ordering was broken",
                    "error_messages": [
                        "Binder Error: column 'rk' not found",
                        "Result mismatch: 87 rows vs 100 expected",
                    ],
                    "error_category": "execution",
                },
            ],
            "promotion": None,
        }

        preamble = Prompter._section_retry_preamble(history)
        assert "RETRY" in preamble
        assert "attempt 2" in preamble.lower()
        assert "Binder Error: column 'rk' not found" in preamble
        assert "Result mismatch" in preamble
        assert "execution" in preamble
        assert "Expert analysis:" in preamble
        assert "ROLLUP ordering was broken" in preamble
