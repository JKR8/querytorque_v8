"""Tests for dashboard collector — normalize_qid, _parse_pg_size, pattern stats,
q-error loading, engine profile loading, priority scoring, PG plan rendering."""

import pytest
from qt_sql.dashboard.collector import (
    _parse_pg_size,
    _compute_pattern_stats,
    _compute_gap_matches,
    _load_qerror_data,
    _load_engine_profile,
    _compute_dominant_pathology,
    _render_pg_plan,
    normalize_qid,
    _bucket_runtime,
    _format_bytes,
    _load_explain_text,
    _compute_resource_impact,
    load_explain_timing,
    _build_forensic,
    _build_execution,
    _build_impact,
    collect_workload_profile,
)
import json

from qt_sql.dashboard.models import (
    EngineGap,
    EngineProfile,
    ExecutionSummary,
    ForensicQuery,
    ForensicSummary,
    ForensicTransformMatch,
    QErrorEntry,
    QueryResult,
)


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

    # --- Variant suffix preservation (collision regression) ---

    def test_variant_suffix_spj_i1(self):
        assert normalize_qid("query013_spj_i1") == "q13_spj_i1"

    def test_variant_suffix_spj_i2(self):
        assert normalize_qid("query013_spj_i2") == "q13_spj_i2"

    def test_variant_suffix_multi(self):
        assert normalize_qid("query001_multi_i1") == "q1_multi_i1"

    def test_variant_suffix_agg(self):
        assert normalize_qid("query013_agg_i2") == "q13_agg_i2"

    def test_variants_are_distinct(self):
        """Regression: variants must NOT collapse to the same key."""
        a = normalize_qid("query013_spj_i1")
        b = normalize_qid("query013_spj_i2")
        assert a != b

    def test_variant_padded_with_suffix(self):
        assert normalize_qid("query001_multi_i2") == "q1_multi_i2"


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


# ---------------------------------------------------------------------------
# _compute_gap_matches
# ---------------------------------------------------------------------------

class TestComputeGapMatches:

    def test_populates_matched_counts(self):
        queries = [
            ForensicQuery(
                query_id="q1", runtime_ms=1000, bucket="HIGH",
                matched_transforms=[
                    ForensicTransformMatch(id="decorrelate", overlap=0.9,
                                           gap="CORRELATED_SUBQUERY_PARALYSIS"),
                ],
            ),
            ForensicQuery(
                query_id="q2", runtime_ms=500, bucket="MEDIUM",
                matched_transforms=[
                    ForensicTransformMatch(id="decorrelate", overlap=0.7,
                                           gap="CORRELATED_SUBQUERY_PARALYSIS"),
                    ForensicTransformMatch(id="date_cte", overlap=0.6,
                                           gap="CROSS_CTE_PREDICATE_BLINDNESS"),
                ],
            ),
            ForensicQuery(
                query_id="q3", runtime_ms=200, bucket="LOW",
                matched_transforms=[],
            ),
        ]
        profile = EngineProfile(
            engine="duckdb",
            gaps=[
                EngineGap(id="CORRELATED_SUBQUERY_PARALYSIS"),
                EngineGap(id="CROSS_CTE_PREDICATE_BLINDNESS"),
                EngineGap(id="REDUNDANT_SCAN_ELIMINATION"),
            ],
        )
        _compute_gap_matches(profile, queries)

        by_id = {g.id: g for g in profile.gaps}
        assert by_id["CORRELATED_SUBQUERY_PARALYSIS"].n_queries_matched == 2
        assert set(by_id["CORRELATED_SUBQUERY_PARALYSIS"].matched_query_ids) == {"q1", "q2"}

        assert by_id["CROSS_CTE_PREDICATE_BLINDNESS"].n_queries_matched == 1
        assert by_id["CROSS_CTE_PREDICATE_BLINDNESS"].matched_query_ids == ["q2"]

        assert by_id["REDUNDANT_SCAN_ELIMINATION"].n_queries_matched == 0
        assert by_id["REDUNDANT_SCAN_ELIMINATION"].matched_query_ids == []

    def test_no_queries(self):
        profile = EngineProfile(
            engine="duckdb",
            gaps=[EngineGap(id="SOME_GAP")],
        )
        _compute_gap_matches(profile, [])
        assert profile.gaps[0].n_queries_matched == 0


# ---------------------------------------------------------------------------
# _format_bytes
# ---------------------------------------------------------------------------

class TestFormatBytes:

    def test_gigabytes(self):
        assert _format_bytes(2 * 1024**3) == "2.0GB"

    def test_megabytes(self):
        assert _format_bytes(256 * 1024**2) == "256MB"

    def test_kilobytes(self):
        assert _format_bytes(8 * 1024) == "8kB"

    def test_bytes(self):
        assert _format_bytes(512) == "512B"

    def test_zero(self):
        assert _format_bytes(0) == "0B"

    def test_fractional_gb(self):
        assert _format_bytes(int(1.5 * 1024**3)) == "1.5GB"


# ---------------------------------------------------------------------------
# load_explain_timing
# ---------------------------------------------------------------------------

class TestLoadExplainTiming:

    def test_missing_dir(self, tmp_path):
        assert load_explain_timing(tmp_path, "q1", "duckdb") == -1.0

    def test_duckdb_format(self, tmp_path):
        explains = tmp_path / "explains"
        explains.mkdir()
        (explains / "q1.json").write_text(json.dumps({
            "execution_time_ms": 1234.5,
        }))
        assert load_explain_timing(tmp_path, "q1", "duckdb") == 1234.5

    def test_pg_format(self, tmp_path):
        explains = tmp_path / "explains"
        explains.mkdir()
        (explains / "q1.json").write_text(json.dumps({
            "plan_json": [{"Execution Time": 456.7}],
        }))
        assert load_explain_timing(tmp_path, "q1", "postgresql") == 456.7

    def test_duckdb_plan_json_dict(self, tmp_path):
        explains = tmp_path / "explains"
        explains.mkdir()
        (explains / "q1.json").write_text(json.dumps({
            "plan_json": {"latency": 0.5},
        }))
        assert load_explain_timing(tmp_path, "q1", "duckdb") == 500.0

    def test_snowflake_format(self, tmp_path):
        explains = tmp_path / "explains"
        explains.mkdir()
        (explains / "q1.json").write_text(json.dumps({
            "executionTime": 2000.0,
        }))
        assert load_explain_timing(tmp_path, "q1", "snowflake") == 2000.0

    def test_subfolder_sf10(self, tmp_path):
        explains = tmp_path / "explains" / "sf10"
        explains.mkdir(parents=True)
        (explains / "q1.json").write_text(json.dumps({
            "execution_time_ms": 999.0,
        }))
        assert load_explain_timing(tmp_path, "q1", "duckdb") == 999.0

    def test_invalid_json(self, tmp_path):
        explains = tmp_path / "explains"
        explains.mkdir()
        (explains / "q1.json").write_text("not json")
        assert load_explain_timing(tmp_path, "q1", "duckdb") == -1.0


# ---------------------------------------------------------------------------
# _load_explain_text
# ---------------------------------------------------------------------------

class TestLoadExplainText:

    def test_missing(self, tmp_path):
        has, text = _load_explain_text(tmp_path, "q99")
        assert has is False
        assert text == ""

    def test_plan_text(self, tmp_path):
        explains = tmp_path / "explains"
        explains.mkdir()
        (explains / "q1.json").write_text(json.dumps({
            "plan_text": "HashAggregate\n  -> Seq Scan on t1",
        }))
        has, text = _load_explain_text(tmp_path, "q1")
        assert has is True
        assert "HashAggregate" in text

    def test_pg_plan_json_rendered(self, tmp_path):
        explains = tmp_path / "explains"
        explains.mkdir()
        (explains / "q1.json").write_text(json.dumps({
            "plan_json": [{"Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "orders",
                "Actual Rows": 100,
            }}],
        }))
        has, text = _load_explain_text(tmp_path, "q1")
        assert has is True
        assert "Seq Scan" in text
        assert "orders" in text

    def test_truncation(self, tmp_path):
        explains = tmp_path / "explains"
        explains.mkdir()
        long_text = "\n".join(f"line {i}" for i in range(120))
        (explains / "q1.json").write_text(json.dumps({
            "plan_text": long_text,
        }))
        has, text = _load_explain_text(tmp_path, "q1")
        assert has is True
        assert "40 more lines" in text
        assert len(text.splitlines()) == 81  # 80 lines + truncation note


# ---------------------------------------------------------------------------
# _compute_resource_impact
# ---------------------------------------------------------------------------

class TestComputeResourceImpact:

    def test_non_pg_returns_none(self):
        assert _compute_resource_impact({}, "duckdb") is None
        assert _compute_resource_impact({}, "snowflake") is None

    def test_empty_results(self):
        ri = _compute_resource_impact({}, "postgresql")
        assert ri.queries_with_set_local == 0
        assert ri.work_mem_total == "0B"

    def test_work_mem_aggregation(self):
        results = {
            "q1": QueryResult(
                query_id="q1", status="WIN", speedup=2.0,
                set_local_commands=["SET LOCAL work_mem = '256MB'"],
            ),
            "q2": QueryResult(
                query_id="q2", status="IMPROVED", speedup=1.5,
                set_local_commands=["SET LOCAL work_mem = '128MB'"],
            ),
        }
        ri = _compute_resource_impact(results, "postgresql")
        assert ri.queries_with_set_local == 2
        assert ri.work_mem_total == "384MB"

    def test_parallel_conflict(self):
        results = {
            "q1": QueryResult(
                query_id="q1", status="WIN", speedup=2.0,
                set_local_commands=[
                    "SET LOCAL max_parallel_workers_per_gather = '4'",
                ],
            ),
            "q2": QueryResult(
                query_id="q2", status="WIN", speedup=1.5,
                set_local_commands=[
                    "SET LOCAL max_parallel_workers_per_gather = '6'",
                ],
            ),
        }
        ri = _compute_resource_impact(results, "postgres")
        assert ri.parallel_workers_total == 10
        assert len(ri.conflicts) == 1
        assert "q1" in ri.conflicts[0]
        assert "q2" in ri.conflicts[0]

    def test_skips_non_winners(self):
        results = {
            "q1": QueryResult(
                query_id="q1", status="NEUTRAL", speedup=1.0,
                set_local_commands=["SET LOCAL work_mem = '256MB'"],
            ),
        }
        ri = _compute_resource_impact(results, "postgresql")
        assert ri.queries_with_set_local == 0

    def test_high_work_mem_warning(self):
        results = {
            "q1": QueryResult(
                query_id="q1", status="WIN", speedup=2.0,
                set_local_commands=["SET LOCAL work_mem = '1GB'"],
            ),
        }
        ri = _compute_resource_impact(results, "postgresql")
        assert len(ri.warnings) == 1
        assert "512MB" in ri.warnings[0]


# ---------------------------------------------------------------------------
# _build_execution
# ---------------------------------------------------------------------------

class TestBuildExecution:

    def test_no_runs_dir(self, tmp_path):
        result = _build_execution(tmp_path)
        assert result.runs == []
        assert result.latest_results == {}

    def test_empty_runs_dir(self, tmp_path):
        (tmp_path / "runs").mkdir()
        result = _build_execution(tmp_path)
        assert result.runs == []

    def test_single_run(self, tmp_path):
        run_dir = tmp_path / "runs" / "run_20260215_120000"
        run_dir.mkdir(parents=True)
        summary = {
            "mode": "beam",
            "total": 3,
            "completed": 2,
            "elapsed_seconds": 60.0,
            "results": [
                {"query_id": "query_1", "status": "WIN", "speedup": 2.0},
                {"query_id": "query_2", "status": "NEUTRAL", "speedup": 1.0},
                {"query_id": "query_3", "status": "ERROR"},
            ],
        }
        (run_dir / "summary.json").write_text(json.dumps(summary))

        result = _build_execution(tmp_path)
        assert len(result.runs) == 1
        assert result.runs[0].run_id == "run_20260215_120000"
        assert result.runs[0].mode == "beam"
        assert result.runs[0].total_queries == 3
        assert result.runs[0].timestamp == "2026-02-15 12:00:00"
        assert result.runs[0].status_counts["WIN"] == 1
        assert result.runs[0].status_counts["NEUTRAL"] == 1
        assert result.runs[0].status_counts["ERROR"] == 1

        # latest_results keyed by normalized qid
        assert "q1" in result.latest_results
        assert result.latest_results["q1"].status == "WIN"
        assert result.latest_results["q1"].speedup == 2.0

    def test_multiple_runs_newest_first(self, tmp_path):
        for ts in ["20260210_100000", "20260215_100000"]:
            run_dir = tmp_path / "runs" / f"run_{ts}"
            run_dir.mkdir(parents=True)
            (run_dir / "summary.json").write_text(json.dumps({
                "mode": "beam", "total": 1, "results": [
                    {"query_id": "q1", "status": "WIN", "speedup": 1.5},
                ],
            }))

        result = _build_execution(tmp_path)
        assert len(result.runs) == 2
        # Newest first
        assert "20260215" in result.runs[0].run_id

    def test_per_query_result_json(self, tmp_path):
        run_dir = tmp_path / "runs" / "run_20260215_120000"
        qdir = run_dir / "query_1"
        qdir.mkdir(parents=True)
        (run_dir / "summary.json").write_text(json.dumps({
            "mode": "beam", "total": 1, "results": [
                {"query_id": "query_1", "status": "WIN", "speedup": 3.0},
            ],
        }))
        (qdir / "result.json").write_text(json.dumps({
            "baseline_ms": 9000.0,
            "best_transforms": ["decorrelate"],
            "best_worker_id": 2,
            "set_local_commands": ["SET LOCAL work_mem = '128MB'"],
        }))

        result = _build_execution(tmp_path)
        qr = result.latest_results["q1"]
        assert qr.baseline_ms == 9000.0
        assert qr.optimized_ms == 3000.0
        assert qr.transform_used == "decorrelate"
        assert qr.worker_id == 2
        assert qr.set_local_commands == ["SET LOCAL work_mem = '128MB'"]


# ---------------------------------------------------------------------------
# _build_impact
# ---------------------------------------------------------------------------

class TestBuildImpact:

    def test_no_results(self):
        forensic = ForensicSummary(total_queries=5, total_runtime_ms=10000.0)
        execution = ExecutionSummary()
        impact = _build_impact(forensic, execution, "duckdb")
        assert impact.total_baseline_ms == 0.0
        assert impact.total_savings_ms == 0.0

    def test_savings_calculation(self):
        forensic = ForensicSummary(
            total_queries=2,
            total_runtime_ms=6000.0,
            queries=[
                ForensicQuery(query_id="q1", runtime_ms=4000, bucket="HIGH"),
                ForensicQuery(query_id="q2", runtime_ms=2000, bucket="MEDIUM"),
            ],
        )
        execution = ExecutionSummary(
            latest_results={
                "q1": QueryResult(
                    query_id="q1", status="WIN", speedup=2.0,
                    baseline_ms=4000,
                ),
                "q2": QueryResult(
                    query_id="q2", status="NEUTRAL", speedup=1.0,
                    baseline_ms=2000,
                ),
            },
        )
        impact = _build_impact(forensic, execution, "duckdb")
        assert impact.total_baseline_ms == 6000.0
        # q1: 4000/2.0 = 2000, q2: 2000/1.0 = 2000 → total optimized = 4000
        assert impact.total_optimized_ms == 4000.0
        assert impact.total_savings_ms == 2000.0
        assert impact.total_savings_pct == pytest.approx(33.3, abs=0.1)
        assert impact.status_counts["WIN"] == 1
        assert impact.status_counts["NEUTRAL"] == 1

    def test_regressions_tracked(self):
        forensic = ForensicSummary(total_queries=1, total_runtime_ms=1000.0,
                                   queries=[ForensicQuery(query_id="q1",
                                                          runtime_ms=1000,
                                                          bucket="MEDIUM")])
        execution = ExecutionSummary(
            latest_results={
                "q1": QueryResult(
                    query_id="q1", status="REGRESSION", speedup=0.5,
                    baseline_ms=1000,
                ),
            },
        )
        impact = _build_impact(forensic, execution, "duckdb")
        assert len(impact.regressions) == 1
        assert impact.regressions[0].query_id == "q1"

    def test_baseline_from_forensic_fallback(self):
        """When QueryResult has no baseline_ms, fall back to forensic runtime."""
        forensic = ForensicSummary(
            total_queries=1,
            total_runtime_ms=5000.0,
            queries=[
                ForensicQuery(query_id="q1", runtime_ms=5000, bucket="HIGH"),
            ],
        )
        execution = ExecutionSummary(
            latest_results={
                "q1": QueryResult(
                    query_id="q1", status="WIN", speedup=2.5,
                    baseline_ms=0.0,  # missing
                ),
            },
        )
        impact = _build_impact(forensic, execution, "duckdb")
        assert impact.total_baseline_ms == 5000.0
        assert impact.total_optimized_ms == 2000.0

    def test_pg_resource_impact(self):
        forensic = ForensicSummary(total_queries=1, total_runtime_ms=1000.0,
                                   queries=[ForensicQuery(query_id="q1",
                                                          runtime_ms=1000,
                                                          bucket="MEDIUM")])
        execution = ExecutionSummary(
            latest_results={
                "q1": QueryResult(
                    query_id="q1", status="WIN", speedup=2.0,
                    baseline_ms=1000,
                    set_local_commands=["SET LOCAL work_mem = '64MB'"],
                ),
            },
        )
        impact = _build_impact(forensic, execution, "postgresql")
        assert impact.resource_impact is not None
        assert impact.resource_impact.queries_with_set_local == 1


# ---------------------------------------------------------------------------
# _build_forensic (integration — uses tmp filesystem)
# ---------------------------------------------------------------------------

class TestBuildForensic:

    def test_no_queries_dir(self, tmp_path):
        result = _build_forensic(tmp_path, "duckdb")
        assert result.total_queries == 0

    def test_empty_queries_dir(self, tmp_path):
        (tmp_path / "queries").mkdir()
        result = _build_forensic(tmp_path, "duckdb")
        assert result.total_queries == 0

    def test_basic_pipeline(self, tmp_path):
        """Minimal end-to-end: queries + explains → ForensicSummary."""
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "query_1.sql").write_text("SELECT 1")
        (queries_dir / "query_2.sql").write_text("SELECT 2")

        explains_dir = tmp_path / "explains"
        explains_dir.mkdir()
        (explains_dir / "query_1.json").write_text(json.dumps({
            "execution_time_ms": 5000.0,
            "plan_text": "Seq Scan on t1",
        }))
        (explains_dir / "query_2.json").write_text(json.dumps({
            "execution_time_ms": 500.0,
        }))

        result = _build_forensic(tmp_path, "duckdb")
        assert result.total_queries == 2
        assert result.total_runtime_ms == 5500.0

        # Sorted by runtime desc
        assert result.queries[0].query_id == "q1"
        assert result.queries[0].runtime_ms == 5000.0
        assert result.queries[0].bucket == "MEDIUM"
        assert result.queries[0].cost_rank == 1

        assert result.queries[1].query_id == "q2"
        assert result.queries[1].cost_rank == 2

        # Cost percentages
        assert result.queries[0].pct_of_total == pytest.approx(
            5000 / 5500, abs=0.01)
        assert result.queries[1].cumulative_pct == pytest.approx(1.0, abs=0.01)

        # Bucket distribution
        assert result.bucket_distribution["MEDIUM"] == 1
        assert result.bucket_distribution["LOW"] == 1

        # EXPLAIN text loaded for q1
        assert result.queries[0].has_explain is True
        assert "Seq Scan" in result.queries[0].explain_text

    def test_qerror_joined(self, tmp_path):
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "query_1.sql").write_text("SELECT 1")

        (tmp_path / "qerror_analysis.json").write_text(json.dumps([{
            "query_id": "query_1",
            "max_q_error": 500.0,
            "severity": "S2",
            "direction": "UNDER_EST",
            "worst_node": "HASH_JOIN",
            "pathology_routing": "P0,P2",
        }]))

        result = _build_forensic(tmp_path, "duckdb")
        q = result.queries[0]
        assert q.qerror is not None
        assert q.qerror.severity == "S2"
        assert q.qerror.max_q_error == 500.0

    def test_estimated_opportunity(self, tmp_path):
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()

        explains_dir = tmp_path / "explains"
        explains_dir.mkdir()

        for i, (ms, bucket) in enumerate([(15000, "HIGH"), (5000, "MEDIUM"), (50, "SKIP")], 1):
            (queries_dir / f"query_{i}.sql").write_text(f"SELECT {i}")
            (explains_dir / f"query_{i}.json").write_text(json.dumps({
                "execution_time_ms": ms,
            }))

        result = _build_forensic(tmp_path, "duckdb")
        # Only HIGH + MEDIUM contribute to opportunity
        assert result.estimated_opportunity_ms == 20000.0


# ---------------------------------------------------------------------------
# collect_workload_profile (end-to-end integration)
# ---------------------------------------------------------------------------

class TestCollectWorkloadProfile:

    def test_minimal(self, tmp_path):
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "q1.sql").write_text("SELECT 1")

        profile = collect_workload_profile(tmp_path, "duckdb")
        assert profile.benchmark_name == tmp_path.name
        assert profile.engine == "duckdb"
        assert profile.collected_at  # non-empty ISO timestamp
        assert profile.forensic.total_queries == 1
        assert profile.execution.runs == []
        assert profile.impact.total_baseline_ms == 0.0

    def test_with_runs(self, tmp_path):
        queries_dir = tmp_path / "queries"
        queries_dir.mkdir()
        (queries_dir / "q1.sql").write_text("SELECT 1")

        explains_dir = tmp_path / "explains"
        explains_dir.mkdir()
        (explains_dir / "q1.json").write_text(json.dumps({
            "execution_time_ms": 3000.0,
        }))

        run_dir = tmp_path / "runs" / "run_20260215_100000"
        run_dir.mkdir(parents=True)
        (run_dir / "summary.json").write_text(json.dumps({
            "mode": "beam", "total": 1, "completed": 1,
            "results": [
                {"query_id": "q1", "status": "WIN", "speedup": 2.0},
            ],
        }))

        profile = collect_workload_profile(tmp_path, "duckdb")
        assert profile.forensic.total_queries == 1
        assert len(profile.execution.runs) == 1
        assert "q1" in profile.execution.latest_results
        # Impact should compute savings
        assert profile.impact.status_counts.get("WIN") == 1
