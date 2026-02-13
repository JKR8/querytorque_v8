"""Cache verification test for shared-prefix prompt caching.

Verifies that:
1. All 4 worker prompts share an identical prefix
2. The worker assignment suffix is unique per worker
3. LLM clients report cache metrics (when available)

Run with a live LLM to verify actual cache hits:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m pytest packages/qt-sql/tests/test_cache_hits.py -v

Or as a standalone script for live cache verification:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 packages/qt-sql/tests/test_cache_hits.py
"""

import pytest

from qt_sql.prompts.worker_shared_prefix import (
    build_shared_worker_prefix,
    build_worker_assignment,
    _deduplicate_examples,
)
from qt_sql.prompts.coach import (
    build_coach_prompt,
    build_coach_refinement_prefix,
)
from qt_sql.explain_signals import extract_vital_signs
from qt_sql.schemas import WorkerResult


# ── Fixtures ────────────────────────────────────────────────────────────

class FakeSharedBriefing:
    semantic_contract = "Preserve exact row counts and aggregation semantics."
    bottleneck_diagnosis = "scan-bound on store_sales with nested loop join."
    active_constraints = "- LITERAL_PRESERVATION: keep all literals\n- SEMANTIC_EQUIVALENCE: same rows"
    regression_warnings = "None applicable."


class FakeWorkerBriefing:
    def __init__(self, wid, strategy):
        self.worker_id = wid
        self.strategy = strategy
        self.target_logical_tree = f"[=] main_query\n  [~] cte_{wid}"
        self.examples = [f"ex_{wid}a", f"ex_{wid}b"]
        self.example_adaptation = f"Apply {strategy} pattern."
        self.hazard_flags = f"Risk: over-splitting for W{wid}."


ORIGINAL_SQL = "SELECT a, SUM(b) FROM t GROUP BY a ORDER BY a;"

FAKE_WORKERS = [
    FakeWorkerBriefing(1, "decorrelate"),
    FakeWorkerBriefing(2, "date_cte_isolate"),
    FakeWorkerBriefing(3, "prefetch_fact_join"),
    FakeWorkerBriefing(4, "or_to_union"),
]

FAKE_EXAMPLES = {
    1: [{"id": "ex_1a", "principle": "Decorrelate"}, {"id": "ex_1b", "principle": "Pushdown"}],
    2: [{"id": "ex_2a", "principle": "Date CTE"}, {"id": "ex_2b", "principle": "Isolate"}],
    3: [{"id": "ex_3a", "principle": "Prefetch"}, {"id": "ex_3b", "principle": "Join"}],
    4: [{"id": "ex_4a", "principle": "OR to UNION"}, {"id": "ex_4b", "principle": "Split"}],
}


# ── Unit Tests ──────────────────────────────────────────────────────────

class TestSharedPrefix:
    def test_prefix_identical_for_all_workers(self):
        """All 4 workers must receive the exact same prefix."""
        prefix = build_shared_worker_prefix(
            analyst_response="Analyst says: 4 strategies identified.",
            shared_briefing=FakeSharedBriefing(),
            all_worker_briefings=FAKE_WORKERS,
            all_examples=FAKE_EXAMPLES,
            original_sql=ORIGINAL_SQL,
            output_columns=["a", "sum_b"],
            dialect="duckdb",
        )

        # Build full prompts for each worker
        prompts = []
        for wid in [1, 2, 3, 4]:
            full = prefix + "\n\n" + build_worker_assignment(wid)
            prompts.append(full)

        # Verify all share the same prefix
        for i in range(1, 4):
            assert prompts[i].startswith(prefix), f"Worker {i+1} doesn't start with shared prefix"

        # Verify suffixes are unique
        suffixes = [p[len(prefix):] for p in prompts]
        assert len(set(suffixes)) == 4, "Worker suffixes should be unique"

    def test_prefix_contains_key_sections(self):
        """Shared prefix must contain all expected sections."""
        prefix = build_shared_worker_prefix(
            analyst_response="Analysis output.",
            shared_briefing=FakeSharedBriefing(),
            all_worker_briefings=FAKE_WORKERS,
            all_examples=FAKE_EXAMPLES,
            original_sql=ORIGINAL_SQL,
            output_columns=["a", "sum_b"],
            dialect="duckdb",
        )

        assert "SQL rewrite engine" in prefix
        assert "Semantic Contract" in prefix
        assert "Constraints" in prefix
        assert "Original SQL" in prefix
        assert "Worker Task Assignments" in prefix
        assert "TASK 1" in prefix
        assert "TASK 4" in prefix

    def test_worker_assignment_format(self):
        """Worker assignment suffix must reference the correct task."""
        for wid in [1, 2, 3, 4]:
            assignment = build_worker_assignment(wid)
            assert f"TASK {wid}" in assignment
            assert f"Worker {wid}" in assignment


class TestDeduplicateExamples:
    def test_no_duplicates(self):
        """Examples should be deduplicated across workers."""
        examples = {
            1: [{"id": "ex_a"}, {"id": "ex_b"}],
            2: [{"id": "ex_b"}, {"id": "ex_c"}],  # ex_b is duplicate
        }
        merged = _deduplicate_examples(examples)
        ids = [e["id"] for e in merged]
        assert ids == ["ex_a", "ex_b", "ex_c"]

    def test_preserves_order(self):
        """Worker 1 examples should come first."""
        merged = _deduplicate_examples(FAKE_EXAMPLES)
        ids = [e["id"] for e in merged]
        assert ids[0] == "ex_1a"
        assert ids[1] == "ex_1b"


class TestCoachPrompt:
    def _make_worker_results(self):
        return [
            WorkerResult(worker_id=1, strategy="decorrelate", examples_used=[],
                         optimized_sql="SELECT 1", speedup=1.5, status="IMPROVED",
                         transforms=["decorrelate"]),
            WorkerResult(worker_id=2, strategy="date_cte", examples_used=[],
                         optimized_sql="SELECT 2", speedup=0.8, status="REGRESSION",
                         transforms=["date_cte"]),
            WorkerResult(worker_id=3, strategy="prefetch", examples_used=[],
                         optimized_sql="SELECT 3", speedup=1.1, status="IMPROVED",
                         transforms=["prefetch"]),
            WorkerResult(worker_id=4, strategy="or_union", examples_used=[],
                         optimized_sql="SELECT 4", speedup=0.0, status="ERROR",
                         transforms=[], error_message="syntax error"),
        ]

    def test_coach_prompt_structure(self):
        """Coach prompt must contain race results and output format."""
        prompt = build_coach_prompt(
            original_sql=ORIGINAL_SQL,
            worker_results=self._make_worker_results(),
            vital_signs={1: "Time: 450ms | Bottleneck: NL Join"},
            race_timings=None,
            engine_profile=None,
            dialect="duckdb",
        )

        assert "Post-Mortem Specialist" in prompt
        assert "REFINEMENT DIRECTIVE" in prompt
        assert "W1" in prompt
        assert "W4" in prompt

    def test_coach_refinement_prefix(self):
        """Refinement prefix must extend round 1 prefix."""
        r1_prefix = "ROUND 1 SHARED PREFIX CONTENT"
        extended = build_coach_refinement_prefix(
            round1_prefix=r1_prefix,
            coach_directives="=== REFINEMENT DIRECTIVE FOR WORKER 1 ===\nDo better.",
            round_results_summary="W1: 1.5x IMPROVED\nW2: 0.8x REGRESSION",
        )

        assert extended.startswith(r1_prefix)
        assert "Round 1 Results" in extended
        assert "Coach Refinement Directives" in extended
        assert "REFINEMENT DIRECTIVE" in extended


class TestVitalSigns:
    def test_empty_explain(self):
        assert extract_vital_signs("") == "No EXPLAIN data available."

    def test_error_explain(self):
        text = "[EXPLAIN failed — planner rejected this SQL]\nError: column x not found"
        result = extract_vital_signs(text, dialect="postgres")
        assert "EXPLAIN failed" in result

    def test_pg_json_extraction(self):
        """PG JSON plan should extract time and buffer info."""
        plan = [{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "store_sales",
                "Actual Rows": 50000,
                "Actual Loops": 1,
                "Actual Total Time": 450.0,
                "Plan Rows": 5,
                "Shared Hit Blocks": 12000,
                "Shared Read Blocks": 500,
                "Plans": [],
            },
            "Planning Time": 2.5,
            "Execution Time": 450.0,
        }]
        result = extract_vital_signs("", plan_json=plan, dialect="postgres")
        assert "450ms" in result
        assert "Buffers" in result or "cache" in result
        # Liar: planned 5, actual 50000 = 10000x off
        assert "Liar" in result

    def test_duckdb_text_fallback(self):
        """DuckDB text explain should extract at least some info."""
        text = "Total execution time: 250ms\n  HASH_GROUP_BY  rows=5000  time=120.5ms (48%)"
        result = extract_vital_signs(text, dialect="duckdb")
        assert "250ms" in result

    def test_duckdb_json_extraction(self):
        """DuckDB JSON plan with real field names must extract bottleneck + time."""
        plan = {
            "latency": 0.450,  # 450ms
            "rows_returned": 100,
            "system_peak_temp_dir_size": 0,
            "children": [{
                "operator_name": "EXPLAIN_ANALYZE",
                "operator_timing": 3.18e-07,
                "operator_cardinality": 0,
                "children": [{
                    "operator_name": "HASH_GROUP_BY",
                    "operator_timing": 0.200,
                    "operator_cardinality": 5000,
                    "extra_info": {
                        "Groups": ["#0"],
                        "Aggregates": ["sum(#1)"],
                        "Estimated Cardinality": "100",
                    },
                    "children": [{
                        "operator_name": "HASH_JOIN",
                        "operator_timing": 0.150,
                        "operator_cardinality": 50000,
                        "extra_info": {
                            "Join Type": "INNER",
                            "Estimated Cardinality": "~1000",
                        },
                        "children": [
                            {
                                "operator_name": "SEQ_SCAN",
                                "operator_timing": 0.080,
                                "operator_cardinality": 2800000,
                                "extra_info": {
                                    "Table": "store_sales",
                                    "Estimated Cardinality": "2877532",
                                },
                                "children": [],
                            },
                            {
                                "operator_name": "SEQ_SCAN",
                                "operator_timing": 0.005,
                                "operator_cardinality": 585,
                                "extra_info": {
                                    "Table": "date_dim",
                                    "Estimated Cardinality": "585",
                                },
                                "children": [],
                            },
                        ],
                    }],
                }],
            }],
        }
        result = extract_vital_signs("", plan_json=plan, dialect="duckdb")
        assert "450ms" in result
        assert "Bottleneck" in result
        assert "HASH_GROUP_BY" in result
        # HASH_JOIN: est 1000, actual 50000 = 50x off → should be a liar
        assert "Liar" in result
        assert "HASH_JOIN" in result

    def test_duckdb_json_no_envelope_crash(self):
        """Top-level envelope node (no operator_name) must not appear as ???."""
        plan = {
            "latency": 0.1,
            "children": [{
                "operator_name": "EXPLAIN_ANALYZE",
                "operator_timing": 0.0,
                "children": [{
                    "operator_name": "SEQ_SCAN",
                    "operator_timing": 0.1,
                    "operator_cardinality": 500,
                    "extra_info": {"Estimated Cardinality": "500"},
                    "children": [],
                }],
            }],
        }
        result = extract_vital_signs("", plan_json=plan, dialect="duckdb")
        assert "???" not in result
        assert "SEQ_SCAN" in result

    def test_duckdb_json_spill_reported(self):
        """Spill indicator from system_peak_temp_dir_size."""
        plan = {
            "latency": 2.0,
            "system_peak_temp_dir_size": 134217728,  # 128MB
            "children": [{
                "operator_name": "EXPLAIN_ANALYZE",
                "operator_timing": 0.0,
                "children": [{
                    "operator_name": "HASH_GROUP_BY",
                    "operator_timing": 1.5,
                    "operator_cardinality": 1000000,
                    "extra_info": {"Estimated Cardinality": "1000000"},
                    "children": [],
                }],
            }],
        }
        result = extract_vital_signs("", plan_json=plan, dialect="duckdb")
        assert "Spill" in result
        assert "128MB" in result

    def test_duckdb_real_explain_file(self):
        """Load real saved explain JSON and verify extraction works."""
        import json
        from pathlib import Path
        explain_path = Path(__file__).parent.parent / "qt_sql" / "benchmarks" / "duckdb_tpcds" / "explains" / "query_1.json"
        if not explain_path.exists():
            pytest.skip("No real explain data available")
        data = json.loads(explain_path.read_text())
        plan_json = data.get("plan_json")
        if not plan_json:
            pytest.skip("No plan_json in explain file")
        result = extract_vital_signs("", plan_json=plan_json, dialect="duckdb")
        # Must extract SOMETHING — not the fallback "no actionable signals"
        assert "no actionable signals" not in result.lower()
        assert "???" not in result


class TestLLMClientLastUsage:
    def test_openai_client_has_last_usage(self):
        from qt_shared.llm.openai import OpenAIClient
        client = OpenAIClient(api_key="test", model="gpt-4o")
        assert hasattr(client, 'last_usage')
        assert isinstance(client.last_usage, dict)

    def test_deepseek_client_has_last_usage(self):
        from qt_shared.llm.deepseek import DeepSeekClient
        client = DeepSeekClient(api_key="test")
        assert hasattr(client, 'last_usage')

    def test_gemini_api_client_has_last_usage(self):
        from qt_shared.llm.gemini import GeminiAPIClient
        client = GeminiAPIClient(api_key="test", model="gemini-3-flash-preview")
        assert hasattr(client, 'last_usage')
        assert isinstance(client.last_usage, dict)

    def test_gemini_cli_client_has_last_usage(self):
        from qt_shared.llm.gemini import GeminiCLIClient
        client = GeminiCLIClient()
        assert hasattr(client, 'last_usage')
        assert isinstance(client.last_usage, dict)

    def test_groq_client_has_last_usage(self):
        from qt_shared.llm.groq import GroqClient
        client = GroqClient(api_key="test")
        assert hasattr(client, 'last_usage')
        assert isinstance(client.last_usage, dict)


class TestPGTextLiarNode:
    def test_liar_node_appended_to_output(self):
        """PG text-path liar-node extraction must appear in output."""
        text = (
            "Total execution time: 500ms\n"
            "-> Seq Scan est_rows=10 rows=50000\n"
        )
        result = extract_vital_signs(text, dialect="postgres")
        assert "500ms" in result
        assert "Liar" in result
        assert "50" in result  # actual rows reference


# ── Live Cache Verification (standalone script) ────────────────────────

def live_cache_test():
    """Send 4 calls with identical prefix, verify cache hits on calls 2-4.

    Run as standalone script with real LLM credentials.
    """
    import os
    import sys

    try:
        from qt_shared.llm.factory import create_llm_client
    except ImportError:
        print("Cannot import create_llm_client — run from project root with PYTHONPATH")
        sys.exit(1)

    client = create_llm_client()
    if not client:
        print("No LLM client configured. Set QT_LLM_PROVIDER in .env")
        sys.exit(1)

    # Build a substantial shared prefix (>1024 tokens for cache eligibility)
    shared_prefix = (
        "You are a SQL optimizer. Analyze the following query and provide suggestions.\n\n"
        + "Context: " + "x " * 2000  # ~2K tokens
    )

    print(f"Shared prefix: {len(shared_prefix)} chars")
    print(f"Client type: {type(client).__name__}")
    print()

    for i in range(4):
        prompt = shared_prefix + f"\n\nYOU ARE WORKER {i+1}. Say 'Worker {i+1} ready.' and nothing else."
        response = client.analyze(prompt)
        usage = getattr(client, 'last_usage', {})

        cache_hit = (
            usage.get("prompt_cache_hit_tokens", 0)
            or usage.get("cached_tokens", 0)
            or usage.get("cache_read_input_tokens", 0)
        )
        cache_miss = (
            usage.get("prompt_cache_miss_tokens", 0)
            or usage.get("cache_creation_input_tokens", 0)
        )

        print(f"Worker {i+1}: cache_hit={cache_hit}, cache_miss={cache_miss}, "
              f"response={response[:50]!r}")

    print()
    print("Workers 2-4 should show cache_hit > 0 if provider supports prefix caching.")


if __name__ == "__main__":
    live_cache_test()
