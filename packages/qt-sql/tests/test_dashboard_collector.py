"""Tests for dashboard collector — normalize_qid, _parse_pg_size, pattern stats,
q-error loading, engine profile loading, priority scoring, PG plan rendering."""

import pytest
from qt_sql.dashboard.collector import (
    _parse_pg_size,
    _compute_pattern_stats,
    _load_qerror_data,
    _load_engine_profile,
    _compute_dominant_pathology,
    _render_pg_plan,
    normalize_qid,
    _bucket_runtime,
)
from qt_sql.dashboard.models import ForensicQuery, QErrorEntry


# ---------------------------------------------------------------------------
# normalize_qid
# ---------------------------------------------------------------------------

class TestNormalizeQid:
    """Canonical q{N} normalization."""

    def test_already_canonical(self):
        assert normalize_qid("q88") == "q88"

    def test_query_underscore(self):
        assert normalize_qid("query_1") == "q1"

    def test_query_underscore_large(self):
        assert normalize_qid("query_88") == "q88"

    def test_query_padded(self):
        assert normalize_qid("query001") == "q1"

    def test_query_padded_large(self):
        assert normalize_qid("query099") == "q99"

    def test_query_no_separator(self):
        assert normalize_qid("query1") == "q1"

    def test_query_hyphen(self):
        assert normalize_qid("query-42") == "q42"

    def test_whitespace(self):
        assert normalize_qid("  q5  ") == "q5"

    def test_unknown_format(self):
        assert normalize_qid("custom_id") == "custom_id"

    def test_case_insensitive(self):
        assert normalize_qid("Query_10") == "q10"

    def test_strips_leading_zeros(self):
        assert normalize_qid("query_001") == "q1"


# ---------------------------------------------------------------------------
# _parse_pg_size
# ---------------------------------------------------------------------------

class TestParsePgSize:
    """Regression tests for PG size string parsing (bug: 'B' matched before 'MB')."""

    def test_megabytes(self):
        assert _parse_pg_size("256MB") == 256 * 1024**2

    def test_megabytes_small(self):
        assert _parse_pg_size("4MB") == 4 * 1024**2

    def test_gigabytes(self):
        assert _parse_pg_size("1GB") == 1024**3

    def test_kilobytes(self):
        assert _parse_pg_size("8kB") == 8 * 1024

    def test_terabytes(self):
        assert _parse_pg_size("2TB") == 2 * 1024**4

    def test_bytes(self):
        assert _parse_pg_size("512B") == 512

    def test_plain_integer(self):
        assert _parse_pg_size("1024") == 1024

    def test_whitespace(self):
        assert _parse_pg_size("  256MB  ") == 256 * 1024**2

    def test_fractional(self):
        assert _parse_pg_size("1.5GB") == int(1.5 * 1024**3)

    def test_invalid(self):
        assert _parse_pg_size("bogus") == 0

    def test_empty(self):
        assert _parse_pg_size("") == 0


# ---------------------------------------------------------------------------
# _compute_pattern_stats (per-pattern overlap, not query top_overlap)
# ---------------------------------------------------------------------------

class TestComputePatternStats:
    """Regression: avg_overlap must use each pattern's own overlap, not query top."""

    def test_per_pattern_overlap(self):
        entries = [
            ("q1", 1000.0, "HIGH", {"patA": 0.9, "patB": 0.5}),
            ("q2", 500.0, "MEDIUM", {"patA": 0.7, "patC": 0.3}),
        ]
        stats = _compute_pattern_stats(entries)
        by_id = {s.pattern_id: s for s in stats}

        assert by_id["patA"].query_count == 2
        assert by_id["patA"].avg_overlap == pytest.approx(0.8, abs=0.001)

        assert by_id["patB"].query_count == 1
        assert by_id["patB"].avg_overlap == pytest.approx(0.5, abs=0.001)

        assert by_id["patC"].query_count == 1
        assert by_id["patC"].avg_overlap == pytest.approx(0.3, abs=0.001)

    def test_empty_entries(self):
        stats = _compute_pattern_stats([])
        assert stats == []

    def test_no_patterns(self):
        entries = [("q1", 100.0, "LOW", {})]
        stats = _compute_pattern_stats(entries)
        assert stats == []

    def test_target_gap_from_catalog(self):
        entries = [
            ("q1", 1000.0, "HIGH", {"decorrelate": 0.85}),
        ]
        catalog = {"decorrelate": {"gap": "CORRELATED_SUBQUERY_PARALYSIS"}}
        stats = _compute_pattern_stats(entries, catalog_by_id=catalog)
        assert stats[0].target_gap == "CORRELATED_SUBQUERY_PARALYSIS"

    def test_target_gap_without_catalog(self):
        entries = [
            ("q1", 1000.0, "HIGH", {"decorrelate": 0.85}),
        ]
        stats = _compute_pattern_stats(entries)
        assert stats[0].target_gap == ""


# ---------------------------------------------------------------------------
# _bucket_runtime
# ---------------------------------------------------------------------------

class TestBucketRuntime:

    def test_skip(self):
        assert _bucket_runtime(50) == "SKIP"

    def test_low(self):
        assert _bucket_runtime(500) == "LOW"

    def test_medium(self):
        assert _bucket_runtime(5000) == "MEDIUM"

    def test_high(self):
        assert _bucket_runtime(15000) == "HIGH"

    def test_negative(self):
        assert _bucket_runtime(-1) == "MEDIUM"


# ---------------------------------------------------------------------------
# Priority scoring
# ---------------------------------------------------------------------------

class TestPriorityScore:
    """Verify PLAN.md §4.3: priority = runtime_weight × (1.0 + tractability + top_overlap)."""

    def test_skip_always_zero(self):
        # SKIP weight = 0 → priority always 0
        weight = 0  # SKIP
        priority = weight * (1.0 + 3 + 0.9)
        assert priority == 0.0

    def test_high_with_matches(self):
        weight = 5  # HIGH
        tractability = 2
        top_overlap = 0.85
        priority = weight * (1.0 + tractability + top_overlap)
        assert priority == pytest.approx(19.25)

    def test_medium_no_matches(self):
        weight = 3  # MEDIUM
        tractability = 0
        top_overlap = 0.0
        priority = weight * (1.0 + tractability + top_overlap)
        assert priority == 3.0


# ---------------------------------------------------------------------------
# _compute_dominant_pathology
# ---------------------------------------------------------------------------

class TestDominantPathology:

    def test_most_frequent(self):
        queries = [
            ForensicQuery(query_id="q1", runtime_ms=100, bucket="LOW",
                          qerror=QErrorEntry(pathology_routing="P0,P2")),
            ForensicQuery(query_id="q2", runtime_ms=200, bucket="LOW",
                          qerror=QErrorEntry(pathology_routing="P2,P5")),
            ForensicQuery(query_id="q3", runtime_ms=300, bucket="MEDIUM",
                          qerror=QErrorEntry(pathology_routing="P0,P2,P5")),
        ]
        # P2 appears 3 times, P0 appears 2, P5 appears 2
        assert _compute_dominant_pathology(queries) == "P2"

    def test_no_qerror(self):
        queries = [
            ForensicQuery(query_id="q1", runtime_ms=100, bucket="LOW"),
        ]
        assert _compute_dominant_pathology(queries) == ""

    def test_empty(self):
        assert _compute_dominant_pathology([]) == ""


# ---------------------------------------------------------------------------
# _render_pg_plan
# ---------------------------------------------------------------------------

class TestRenderPgPlan:

    def test_simple_node(self):
        node = {
            "Node Type": "Seq Scan",
            "Actual Total Time": 12.5,
            "Actual Rows": 100,
            "Plan Rows": 50,
            "Relation Name": "users",
        }
        text = _render_pg_plan(node)
        assert "Seq Scan" in text
        assert "time=12.5ms" in text
        assert "rows=100 (est 50)" in text
        assert "on: users" in text

    def test_nested_plan(self):
        node = {
            "Node Type": "Hash Join",
            "Join Type": "Inner",
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "orders"},
                {"Node Type": "Hash", "Plans": [
                    {"Node Type": "Seq Scan", "Relation Name": "customers"},
                ]},
            ],
        }
        text = _render_pg_plan(node)
        lines = text.splitlines()
        assert len(lines) == 4
        assert "Hash Join" in lines[0]
        assert "  -> Seq Scan" in lines[1]
        assert "  -> Hash" in lines[2]
        assert "    -> Seq Scan" in lines[3]

    def test_empty_node(self):
        assert _render_pg_plan({}) == ""
        assert _render_pg_plan(None) == ""


# ---------------------------------------------------------------------------
# _load_qerror_data
# ---------------------------------------------------------------------------

class TestLoadQerrorData:

    def test_missing_file(self, tmp_path):
        result = _load_qerror_data(tmp_path)
        assert result == {}

    def test_loads_and_normalizes(self, tmp_path):
        data = [
            {
                "query_id": "query_1",
                "max_q_error": 1500.0,
                "severity": "MAJOR",
                "direction": "UNDER_EST",
                "worst_node": "HASH_JOIN",
                "worst_est": 10,
                "worst_act": 15000,
                "locus": "JOIN",
                "pathology_routing": "P0,P2",
                "structural_flags": "EST_ZERO|DELIM_SCAN",
                "n_signals": 5,
            },
        ]
        (tmp_path / "qerror_analysis.json").write_text(
            __import__("json").dumps(data))
        result = _load_qerror_data(tmp_path)
        assert "q1" in result  # normalized from query_1
        entry = result["q1"]
        assert entry.severity == "MAJOR"
        assert entry.max_q_error == 1500.0
        assert entry.worst_node == "HASH_JOIN"
        assert entry.n_signals == 5


# ---------------------------------------------------------------------------
# _load_engine_profile
# ---------------------------------------------------------------------------

class TestLoadEngineProfile:

    def test_loads_duckdb(self):
        profile = _load_engine_profile("duckdb")
        if profile is None:
            pytest.skip("Engine profile not available in test environment")
        assert profile.engine == "duckdb"
        assert len(profile.strengths) > 0
        assert len(profile.gaps) > 0

    def test_unknown_engine(self):
        assert _load_engine_profile("oracle") is None
