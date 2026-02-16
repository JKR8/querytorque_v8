"""Tests for the transform detection system.

Verifies:
1. extract_precondition_features() maps tags → uppercase features correctly
2. detect_transforms() scores and ranks transforms by feature overlap
3. All 25 gold trials from trials.jsonl are detectable (correct transform in top-3)
"""

import json
from pathlib import Path

import pytest

from qt_sql.tag_index import extract_precondition_features
from qt_sql.detection import TransformMatch, detect_transforms, load_transforms
from qt_sql.knowledge.normalization import normalize_transform_id

TRIALS_PATH = Path(__file__).resolve().parent.parent / "qt_sql" / "knowledge" / "trials.jsonl"


# =============================================================================
# TestFeatureExtraction — unit tests for extract_precondition_features()
# =============================================================================


class TestFeatureExtraction:
    """Unit tests for extract_precondition_features()."""

    def test_agg_avg(self):
        sql = "SELECT AVG(price) FROM products GROUP BY category"
        feats = extract_precondition_features(sql)
        assert "AGG_AVG" in feats
        assert "GROUP_BY" in feats

    def test_agg_sum(self):
        sql = "SELECT SUM(amount) FROM orders GROUP BY customer_id"
        feats = extract_precondition_features(sql)
        assert "AGG_SUM" in feats

    def test_agg_count(self):
        sql = "SELECT COUNT(*) FROM orders GROUP BY status"
        feats = extract_precondition_features(sql)
        assert "AGG_COUNT" in feats

    def test_multiple_aggregate_types(self):
        sql = "SELECT AVG(price), SUM(qty), COUNT(*) FROM sales GROUP BY store_id"
        feats = extract_precondition_features(sql)
        assert {"AGG_AVG", "AGG_SUM", "AGG_COUNT"} <= feats

    def test_subquery_threshold_2(self):
        sql = """
        SELECT * FROM t
        WHERE a > (SELECT AVG(a) FROM t2)
          AND b > (SELECT AVG(b) FROM t3)
        """
        feats = extract_precondition_features(sql)
        assert "SCALAR_SUB_2+" in feats

    def test_subquery_threshold_5(self):
        sql = """
        SELECT
          (SELECT COUNT(*) FROM t1),
          (SELECT COUNT(*) FROM t2),
          (SELECT COUNT(*) FROM t3),
          (SELECT COUNT(*) FROM t4),
          (SELECT COUNT(*) FROM t5)
        FROM dual
        """
        feats = extract_precondition_features(sql)
        assert "SCALAR_SUB_5+" in feats
        assert "SCALAR_SUB_2+" in feats

    def test_table_repeat_3(self):
        sql = """
        SELECT * FROM store_sales s1
        JOIN store_sales s2 ON s1.id = s2.id
        JOIN store_sales s3 ON s1.id = s3.id
        """
        feats = extract_precondition_features(sql)
        assert "TABLE_REPEAT_3+" in feats

    def test_table_repeat_8(self):
        subs = ", ".join(
            f"(SELECT COUNT(*) FROM store_sales WHERE qty > {i})" for i in range(8)
        )
        sql = f"SELECT {subs} FROM store_sales"
        feats = extract_precondition_features(sql)
        # store_sales appears 9 times total (8 subqueries + 1 outer FROM)
        assert "TABLE_REPEAT_8+" in feats
        assert "TABLE_REPEAT_3+" in feats

    def test_multi_table_5(self):
        sql = """
        SELECT * FROM t1
        JOIN t2 ON t1.id = t2.id
        JOIN t3 ON t1.id = t3.id
        JOIN t4 ON t1.id = t4.id
        JOIN t5 ON t1.id = t5.id
        """
        feats = extract_precondition_features(sql)
        assert "MULTI_TABLE_5+" in feats

    def test_multi_table_below_threshold(self):
        sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
        feats = extract_precondition_features(sql)
        assert "MULTI_TABLE_5+" not in feats

    def test_exists_counting(self):
        sql = """
        SELECT * FROM t
        WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)
          AND EXISTS (SELECT 1 FROM t3 WHERE t3.id = t.id)
          AND EXISTS (SELECT 1 FROM t4 WHERE t4.id = t.id)
        """
        feats = extract_precondition_features(sql)
        assert "EXISTS_3+" in feats
        assert "EXISTS" in feats

    def test_basic_mappings(self):
        sql = """
        WITH cte AS (SELECT * FROM date_dim WHERE d_year = 2002)
        SELECT d_year, COUNT(*)
        FROM cte
        LEFT JOIN orders ON cte.d_date_sk = orders.date_sk
        WHERE d_moy BETWEEN 1 AND 6
        GROUP BY d_year
        HAVING COUNT(*) > 10
        """
        feats = extract_precondition_features(sql)
        assert "CTE" in feats
        assert "DATE_DIM" in feats
        assert "GROUP_BY" in feats
        assert "HAVING" in feats
        assert "LEFT_JOIN" in feats
        assert "BETWEEN" in feats

    def test_case_expr(self):
        sql = "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t"
        feats = extract_precondition_features(sql)
        assert "CASE_EXPR" in feats

    def test_window_func(self):
        sql = "SELECT SUM(x) OVER (PARTITION BY grp ORDER BY id) FROM t"
        feats = extract_precondition_features(sql)
        assert "WINDOW_FUNC" in feats

    def test_rollup(self):
        sql = "SELECT a, b, SUM(c) FROM t GROUP BY ROLLUP(a, b)"
        feats = extract_precondition_features(sql)
        assert "ROLLUP" in feats

    def test_union_intersect(self):
        sql = """
        SELECT a FROM t1
        UNION
        SELECT a FROM t2
        INTERSECT
        SELECT a FROM t3
        """
        feats = extract_precondition_features(sql)
        assert "UNION" in feats
        assert "INTERSECT" in feats

    def test_or_branch(self):
        sql = """
        SELECT * FROM store_sales
        WHERE ss_zip IN ('10', '20')
           OR ss_state IN ('CA', 'NY')
           OR ss_price > 100
        """
        feats = extract_precondition_features(sql)
        assert "OR_BRANCH" in feats

    def test_empty_sql(self):
        feats = extract_precondition_features("")
        assert feats == set()


# =============================================================================
# TestTransformDetection — unit tests for detect_transforms()
# =============================================================================


class TestTransformDetection:
    """Unit tests for detect_transforms()."""

    @pytest.fixture
    def transforms(self):
        return load_transforms()

    def test_load_transforms(self, transforms):
        assert len(transforms) >= 30
        ids = {t["id"] for t in transforms}
        assert "decorrelate" in ids
        assert "or_to_union" in ids
        # New transforms from Feb 12 benchmark distillation
        assert "aggregate_pushdown" in ids
        assert "inner_join_conversion" in ids
        assert "self_join_decomposition" in ids
        assert "dimension_prefetch_star" in ids
        assert "intersect_to_exists" in ids

    def test_engine_filter_duckdb(self, transforms):
        sql = "SELECT SUM(x) FROM store_sales GROUP BY y"
        matches = detect_transforms(sql, transforms, engine="duckdb")
        for m in matches:
            assert "duckdb" in m.engines

    def test_engine_filter_postgresql(self, transforms):
        sql = "SELECT SUM(x) FROM store_sales GROUP BY y"
        matches = detect_transforms(sql, transforms, engine="postgresql")
        for m in matches:
            assert "postgresql" in m.engines

    def test_no_engine_filter_returns_all(self, transforms):
        sql = """
        SELECT AVG(price), SUM(qty), COUNT(*)
        FROM store_sales
        JOIN date_dim ON ss_date_sk = d_date_sk
        WHERE d_year BETWEEN 2000 AND 2002
        GROUP BY d_year
        HAVING COUNT(*) > 10
        """
        all_matches = detect_transforms(sql, transforms, engine=None)
        duck_matches = detect_transforms(sql, transforms, engine="duckdb")
        assert len(all_matches) >= len(duck_matches)

    def test_overlap_ratio_range(self, transforms):
        sql = "SELECT COUNT(*) FROM t GROUP BY x"
        matches = detect_transforms(sql, transforms)
        for m in matches:
            assert 0.0 <= m.overlap_ratio <= 1.0

    def test_match_dataclass_fields(self, transforms):
        sql = "SELECT COUNT(*) FROM t GROUP BY x"
        matches = detect_transforms(sql, transforms)
        assert len(matches) > 0
        m = matches[0]
        assert isinstance(m.id, str)
        assert isinstance(m.overlap_ratio, float)
        assert isinstance(m.matched_features, list)
        assert isinstance(m.missing_features, list)
        assert isinstance(m.total_required, int)
        assert isinstance(m.engines, list)

    def test_sorted_by_overlap(self, transforms):
        sql = """
        SELECT AVG(price), SUM(qty), COUNT(*)
        FROM store_sales s1
        JOIN store_sales s2 ON s1.id = s2.id
        JOIN date_dim ON s1.date_sk = d_date_sk
        WHERE d_year BETWEEN 2000 AND 2002
        GROUP BY d_year
        """
        matches = detect_transforms(sql, transforms)
        for i in range(len(matches) - 1):
            assert matches[i].overlap_ratio >= matches[i + 1].overlap_ratio


# =============================================================================
# TestGoldTrialDetection — parametrized over 25 gold trials
# =============================================================================

# Known legitimate ties: these transforms share identical precondition features
# on the same query SQL, so both appearing in top-3 is correct behavior.
_KNOWN_TIES = {
    # Q9: pushdown and single_pass_aggregation have identical features
    "pushdown": {"single_pass_aggregation"},
    "single_pass_aggregation": {"pushdown"},
    # Q14: intersect_to_exists and multi_intersect_exists_cte are near-identical
    "intersect_to_exists": {"multi_intersect_exists_cte"},
    "multi_intersect_exists_cte": {"intersect_to_exists"},
}


def _acceptable_ids(raw_expected: str) -> set[str]:
    expected = normalize_transform_id(raw_expected)
    tie_raw = set(_KNOWN_TIES.get(raw_expected, set())) | set(
        _KNOWN_TIES.get(expected, set())
    )
    ties = {normalize_transform_id(t) for t in tie_raw}
    return {expected} | ties


def _load_gold_trials():
    """Load first 25 trials from trials.jsonl (the gold set)."""
    trials = []
    with open(TRIALS_PATH) as f:
        for i, line in enumerate(f):
            if i >= 25:
                break
            trial = json.loads(line)
            trials.append(trial)
    return trials


def _trial_id(trial):
    """Generate readable test ID for parametrize."""
    return f"trial{trial['id']}_{trial['transform']}"


_GOLD_TRIALS = _load_gold_trials()


@pytest.mark.parametrize("trial", _GOLD_TRIALS, ids=[_trial_id(t) for t in _GOLD_TRIALS])
class TestGoldTrialDetection:
    """Verify all 25 gold trials are detectable: correct transform in top-3."""

    def test_transform_in_top3(self, trial):
        transforms = load_transforms()
        sql = trial["query_sql"]
        expected = normalize_transform_id(trial["transform"])
        engine = trial["engine"]

        # Use appropriate dialect
        dialect = "postgres" if engine == "postgresql" else "duckdb"

        matches = detect_transforms(sql, transforms, engine=engine, dialect=dialect)
        top3_ids = [m.id for m in matches[:3]]

        # Check: expected transform in top-3, OR a known tie partner is in top-3
        acceptable = _acceptable_ids(trial["transform"])

        assert any(
            tid in acceptable for tid in top3_ids
        ), (
            f"Trial {trial['id']}: expected '{expected}' (or ties {acceptable - {expected}}) "
            f"in top-3, got {top3_ids}. "
            f"All overlaps: {[(m.id, f'{m.overlap_ratio:.2f}') for m in matches[:5]]}"
        )

    def test_overlap_above_threshold(self, trial):
        transforms = load_transforms()
        sql = trial["query_sql"]
        expected = normalize_transform_id(trial["transform"])
        engine = trial["engine"]
        dialect = "postgres" if engine == "postgresql" else "duckdb"

        matches = detect_transforms(sql, transforms, engine=engine, dialect=dialect)

        # Find the expected transform's match
        acceptable = _acceptable_ids(trial["transform"])

        match = None
        for m in matches:
            if m.id in acceptable:
                match = m
                break

        assert match is not None, (
            f"Trial {trial['id']}: '{expected}' not found in matches at all"
        )
        assert match.overlap_ratio >= 0.5, (
            f"Trial {trial['id']}: '{match.id}' overlap {match.overlap_ratio:.2f} < 0.5. "
            f"Matched: {match.matched_features}, Missing: {match.missing_features}"
        )
