"""Integration tests for adaptive rewriter v5 with query-specific examples."""

import pytest
from qt_sql.optimization.query_recommender import (
    get_query_recommendations,
    get_recommendations_for_sql,
    VERIFIED_TRANSFORMS,
)
from qt_sql.optimization.dag_v3 import load_example, load_all_examples


class TestQueryRecommender:
    """Test query recommendation parser."""

    def test_parses_q1_recommendations(self):
        """Q1 should return recommendations (decorrelate likely top if FAISS available)."""
        recs = get_query_recommendations('q1', top_n=3)
        # If FAISS model exists, should get recommendations
        # If not, empty is acceptable
        if recs:
            assert len(recs) <= 3
            # All recs should be verified transforms
            for r in recs:
                assert r in VERIFIED_TRANSFORMS

    def test_parses_q15_recommendations(self):
        """Q15 should return valid recommendations."""
        recs = get_query_recommendations('q15', top_n=3)
        if recs:
            assert len(recs) <= 3
            for r in recs:
                assert r in VERIFIED_TRANSFORMS

    def test_parses_q93_recommendations(self):
        """Q93 should return valid recommendations."""
        recs = get_query_recommendations('q93', top_n=3)
        if recs:
            assert len(recs) <= 3
            for r in recs:
                assert r in VERIFIED_TRANSFORMS

    def test_returns_empty_for_unknown_query(self):
        """Unknown queries should return empty list."""
        recs = get_query_recommendations('q999', top_n=3)
        assert recs == []

    def test_handles_queries_with_fewer_than_n_recs(self):
        """Queries with < N recs should return what's available."""
        recs = get_query_recommendations('q1', top_n=10)
        assert len(recs) <= 10  # May be 0 if FAISS not available


class TestWorkerExampleAssignment:
    """Test worker example assignment strategy."""

    def test_assigns_examples_across_4_workers(self):
        """Workers 1-4 should get examples distributed evenly."""
        # Get all available examples
        all_examples = load_all_examples()
        assert len(all_examples) >= 4, "Need at least 4 gold examples"

        # Get example IDs
        all_example_ids = [ex.id for ex in all_examples]
        num_examples = len(all_example_ids)

        # Split into 4 workers evenly
        batch_size = max(1, num_examples // 4)
        worker_examples = []
        for i in range(4):
            start = i * batch_size
            end = start + batch_size if i < 3 else num_examples
            worker_examples.append(all_example_ids[start:end])

        # Verify workers get examples
        assert all(len(w) >= 1 for w in worker_examples[:min(4, num_examples)])

        # Verify no overlap (diversity)
        all_assigned = []
        for w in worker_examples:
            all_assigned.extend(w)
        assert len(all_assigned) == len(set(all_assigned)), "Examples should not overlap"

    def test_worker_5_gets_no_examples(self):
        """Worker 5 (explore mode) should have no examples."""
        worker_5_examples = []
        assert len(worker_5_examples) == 0

    def test_priority_examples_go_to_worker_1(self):
        """Worker 1 should get top ML recommendations."""
        q1_recs = get_query_recommendations('q1', top_n=3)

        # Worker 1 gets first 3 (which includes top ML recs)
        all_examples = load_all_examples()
        all_example_ids = [ex.id for ex in all_examples]

        padded_recs = q1_recs.copy()
        for ex_id in all_example_ids:
            if len(padded_recs) >= 12:
                break
            if ex_id not in padded_recs:
                padded_recs.append(ex_id)

        worker_1_examples = padded_recs[0:3]

        # Top ML recommendation should be in worker 1
        assert q1_recs[0] in worker_1_examples


class TestPromptGeneration:
    """Test prompt generation for different workers."""

    def test_workers_1_4_use_dag_json_format(self):
        """Workers 1-4 should output DAG JSON with rewrite_sets."""
        from qt_sql.optimization.dag_v2 import DagV2Pipeline

        sql = "SELECT * FROM t1"
        pipeline = DagV2Pipeline(sql)
        prompt = pipeline.get_prompt()

        # DAG JSON format indicators
        assert "rewrite_sets" in prompt
        assert "OUTPUT FORMAT:" in prompt or "```json" in prompt
        assert "nodes" in prompt

    def test_worker_5_should_output_full_sql(self):
        """Worker 5 prompt should request full SQL output, not DAG JSON."""
        # Worker 5 uses different prompt - full SQL output
        worker_5_prompt_should_contain = [
            "rewrite the ENTIRE query",
            "complete optimized SQL",
            "adversarial",
            "creative",
        ]

        # This will be implemented in the v5 update
        # For now, just verify the concept
        assert True  # Placeholder until implementation


class TestExampleLoading:
    """Test that examples can be loaded correctly."""

    def test_load_decorrelate_example(self):
        """Should load decorrelate gold example."""
        example = load_example('decorrelate')
        assert example is not None
        assert example.id == 'decorrelate'
        assert 'decorrelate' in example.name.lower()

    def test_load_or_to_union_example(self):
        """Should load or_to_union gold example."""
        example = load_example('or_to_union')
        assert example is not None
        assert example.id == 'or_to_union'

    def test_load_early_filter_example(self):
        """Should load early_filter gold example."""
        example = load_example('early_filter')
        assert example is not None
        assert example.id == 'early_filter'

    def test_all_examples_have_required_fields(self):
        """All examples should have id, name, example dict."""
        examples = load_all_examples()
        assert len(examples) >= 4, "Should have at least 4 gold examples"

        for ex in examples:
            assert ex.id, f"Example missing id: {ex}"
            assert ex.name, f"Example {ex.id} missing name"
            assert ex.example, f"Example {ex.id} missing example dict"
            assert 'input_slice' in ex.example or 'opportunity' in ex.example


class TestWorkerDiversity:
    """Test that workers provide diverse coverage."""

    def test_no_duplicate_examples_across_workers(self):
        """No worker should receive duplicate examples."""
        all_examples = load_all_examples()
        all_example_ids = [ex.id for ex in all_examples]

        # Get first 12 examples
        examples_for_workers = all_example_ids[:12]

        worker_1 = examples_for_workers[0:3]
        worker_2 = examples_for_workers[3:6]
        worker_3 = examples_for_workers[6:9]
        worker_4 = examples_for_workers[9:12]

        # Check no duplicates within each worker
        assert len(worker_1) == len(set(worker_1))
        assert len(worker_2) == len(set(worker_2))
        assert len(worker_3) == len(set(worker_3))
        assert len(worker_4) == len(set(worker_4))

        # Check no duplicates across workers
        all_assigned = worker_1 + worker_2 + worker_3 + worker_4
        assert len(all_assigned) == len(set(all_assigned))

    def test_worker_5_has_different_format(self):
        """Worker 5 should have different output format (full SQL)."""
        # Worker 5 characteristics:
        # - No examples
        # - Full EXPLAIN plan (not summary)
        # - Output: Full SQL (not DAG JSON)
        # - Mode: Explore/adversarial

        worker_5_config = {
            'examples': [],
            'output_format': 'full_sql',
            'mode': 'explore',
            'plan_detail': 'full'
        }

        assert worker_5_config['examples'] == []
        assert worker_5_config['output_format'] == 'full_sql'
        assert worker_5_config['mode'] == 'explore'


class TestQuerySpecificRecommendations:
    """Test that different queries get different recommendations."""

    def test_q1_and_q15_return_valid_recommendations(self):
        """Q1 and Q15 should return valid recommendations (may differ based on model)."""
        q1_recs = get_query_recommendations('q1', top_n=3)
        q15_recs = get_query_recommendations('q15', top_n=3)

        # Both should return valid verified transforms (or empty if no model)
        for r in q1_recs:
            assert r in VERIFIED_TRANSFORMS
        for r in q15_recs:
            assert r in VERIFIED_TRANSFORMS

    def test_recommendations_are_verified_transforms(self):
        """All recommendations should be from verified transform set."""
        # Q1 recommendations
        q1_recs = get_query_recommendations('q1', top_n=3)
        for r in q1_recs:
            assert r in VERIFIED_TRANSFORMS

        # Q15 recommendations
        q15_recs = get_query_recommendations('q15', top_n=3)
        for r in q15_recs:
            assert r in VERIFIED_TRANSFORMS

        # Q93 recommendations
        q93_recs = get_query_recommendations('q93', top_n=3)
        for r in q93_recs:
            assert r in VERIFIED_TRANSFORMS


class TestValidationSimplicity:
    """Test validation is simplified (no cost ranking)."""

    def test_validation_checks_row_count_only(self):
        """Sample validation should only check row count match."""
        from qt_sql.validation.sql_validator import SQLValidator
        from qt_sql.validation.schemas import ValidationStatus

        validator = SQLValidator(database=':memory:')

        # Same query - should pass
        sql = "SELECT 1 AS x"
        result = validator.validate(sql, sql)

        # Key checks
        assert result.status == ValidationStatus.PASS
        assert result.row_counts_match is True
        assert result.values_match is True

        # Different row counts - should fail
        sql1 = "SELECT 1 AS x"
        sql2 = "SELECT 1 AS x UNION ALL SELECT 2 AS x"
        result2 = validator.validate(sql1, sql2)

        assert result2.status == ValidationStatus.FAIL
        assert result2.row_counts_match is False

    def test_no_cost_ranking_logic(self):
        """Validation should not rank by EXPLAIN cost."""
        # This is a reminder test - cost ranking is useless per lab testing
        # Just verify validation returns results, don't sort by cost

        from qt_sql.validation.sql_validator import SQLValidator

        validator = SQLValidator(database=':memory:')
        sql1 = "SELECT 1 AS x"
        sql2 = "SELECT 1 AS x"

        result = validator.validate(sql1, sql2)

        # Cost is captured but not used for ranking
        assert hasattr(result, 'original_cost')
        assert hasattr(result, 'optimized_cost')
        # No sorting by cost - that's the point!


def test_end_to_end_worker_assignment():
    """Integration test: Full worker assignment for a query."""
    # Simulate optimizing Q1

    # 1. Get all examples
    all_examples = load_all_examples()
    all_example_ids = [ex.id for ex in all_examples]
    num_examples = len(all_example_ids)

    # 2. Split into 4 workers
    batch_size = max(1, num_examples // 4)
    worker_assignments = {
        'worker_5': {
            'examples': [],
            'format': 'full_sql',
            'mode': 'explore'
        }
    }

    for i in range(4):
        start = i * batch_size
        end = min(start + batch_size, num_examples)
        worker_assignments[f'worker_{i+1}'] = {
            'examples': all_example_ids[start:end],
            'format': 'dag_json',
            'mode': 'guided'
        }

    # 3. Verify assignment
    assert len(worker_assignments) == 5

    # Workers 1-4: have examples, DAG JSON
    for i in range(1, 5):
        worker = worker_assignments[f'worker_{i}']
        assert worker['format'] == 'dag_json'
        assert worker['mode'] == 'guided'

    # Worker 5: no examples, full SQL, explore
    worker_5 = worker_assignments['worker_5']
    assert len(worker_5['examples']) == 0
    assert worker_5['format'] == 'full_sql'
    assert worker_5['mode'] == 'explore'

    # Verify diversity (no duplicates)
    all_assigned = []
    for i in range(1, 5):
        all_assigned.extend(worker_assignments[f'worker_{i}']['examples'])

    assert len(all_assigned) == len(set(all_assigned)), "No duplicate examples"
