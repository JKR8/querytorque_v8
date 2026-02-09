"""Smoke tests for plan_scanner — no DB or API calls."""
import json
import tempfile
from pathlib import Path

import pytest

from qt_sql.plan_scanner import (
    PLAN_SPACE_COMBOS,
    ComboResult,
    ScanResult,
    _build_set_local_cmds,
    _collect_scan_counts,
    _collect_predicate_placement,
    _find_bottleneck_joins,
    _assess_confidence,
    _format_bottleneck_joins,
    _format_scan_counts,
    _format_predicate_placement,
    _compose_strategy,
    format_scan_for_prompt,
    format_explore_for_prompt,
    load_scan_result,
    load_known_sql_ceiling,
)

# ── Real fixture from existing scan run ─────────────────────────────────
FIXTURE_DIR = Path(__file__).resolve().parent.parent / "qt_sql" / "benchmarks" / "postgres_dsb_76"
FIXTURE_JSON = FIXTURE_DIR / "plan_scanner" / "query001_multi_i1.json"
FIXTURE_Q065 = FIXTURE_DIR / "plan_scanner" / "query065_multi_i1.json"
FIXTURE_Q080 = FIXTURE_DIR / "plan_scanner" / "query080_multi_i1.json"
FIXTURE_EXPLORE = FIXTURE_DIR / "plan_explore" / "query001_multi_i1.json"


class TestBuildSetLocalCmds:
    def test_single_flag(self):
        cmds = _build_set_local_cmds({"enable_nestloop": "off"})
        assert cmds == ["SET LOCAL enable_nestloop = 'off'"]

    def test_multiple_flags(self):
        cmds = _build_set_local_cmds({"enable_nestloop": "off", "enable_mergejoin": "off"})
        assert len(cmds) == 2
        assert "SET LOCAL enable_nestloop = 'off'" in cmds
        assert "SET LOCAL enable_mergejoin = 'off'" in cmds

    def test_empty_config(self):
        assert _build_set_local_cmds({}) == []

    def test_memory_value(self):
        cmds = _build_set_local_cmds({"work_mem": "256MB"})
        assert cmds == ["SET LOCAL work_mem = '256MB'"]


class TestPlanSpaceCombos:
    def test_combo_count(self):
        assert len(PLAN_SPACE_COMBOS) == 22  # 17 base + 5 compound

    def test_all_combos_have_valid_config(self):
        for name, config in PLAN_SPACE_COMBOS.items():
            assert isinstance(config, dict), f"{name} config is not a dict"
            assert len(config) > 0, f"{name} has empty config"
            for k, v in config.items():
                assert isinstance(k, str) and isinstance(v, str), f"{name}: {k}={v}"

    def test_compound_combos_exist(self):
        """Issue 3: compound combos for interaction effects."""
        assert "jit_off_mem_256mb" in PLAN_SPACE_COMBOS
        assert "jit_off_no_parallel" in PLAN_SPACE_COMBOS
        assert "mem_256mb_max_par" in PLAN_SPACE_COMBOS
        assert "no_reorder_mem_256mb" in PLAN_SPACE_COMBOS
        assert "ssd_no_jit" in PLAN_SPACE_COMBOS


class TestScanResultRoundTrip:
    @pytest.fixture
    def sample_result(self) -> ScanResult:
        return ScanResult(
            query_id="test_q1",
            baseline_ms=500.0,
            baseline_plan_node="Aggregate",
            baseline_rows=42,
            combos=[
                ComboResult(
                    combo_name="no_nestloop",
                    config={"enable_nestloop": "off"},
                    set_local_commands=["SET LOCAL enable_nestloop = 'off'"],
                    time_ms=250.0,
                    speedup=2.0,
                    top_plan_node="Hash Join",
                    row_count=42,
                    rows_match=True,
                ),
                ComboResult(
                    combo_name="no_hashjoin",
                    config={"enable_hashjoin": "off"},
                    set_local_commands=["SET LOCAL enable_hashjoin = 'off'"],
                    time_ms=600.0,
                    speedup=0.833,
                    top_plan_node="Merge Join",
                    row_count=42,
                    rows_match=True,
                ),
                ComboResult(
                    combo_name="bad_combo",
                    config={"jit": "off"},
                    set_local_commands=["SET LOCAL jit = 'off'"],
                    time_ms=0.0,
                    speedup=0.0,
                    top_plan_node="Error",
                    row_count=0,
                    rows_match=False,
                    error="timeout exceeded",
                ),
            ],
            ceiling_speedup=2.0,
            ceiling_combo="no_nestloop",
            scanned_at="2026-02-09T20:00:00",
        )

    def test_to_dict_and_back(self, sample_result):
        d = sample_result.to_dict()
        restored = ScanResult.from_dict(d)
        assert restored.query_id == sample_result.query_id
        assert restored.baseline_ms == 500.0
        assert restored.ceiling_speedup == 2.0
        assert restored.ceiling_combo == "no_nestloop"
        assert len(restored.combos) == 3
        assert restored.combos[2].error == "timeout exceeded"

    def test_json_serialization(self, sample_result):
        d = sample_result.to_dict()
        json_str = json.dumps(d)
        parsed = json.loads(json_str)
        restored = ScanResult.from_dict(parsed)
        assert restored.ceiling_speedup == 2.0

    def test_rounding(self, sample_result):
        d = sample_result.to_dict()
        assert d["baseline_ms"] == 500.0
        assert d["combos"][0]["speedup"] == 2.0


class TestLoadFromFixture:
    @pytest.mark.skipif(not FIXTURE_JSON.exists(), reason="No fixture file")
    def test_load_real_fixture(self):
        data = json.loads(FIXTURE_JSON.read_text())
        result = ScanResult.from_dict(data)
        assert result.query_id == "query001_multi_i1"
        assert result.baseline_ms == 8477.6
        assert result.ceiling_combo == "work_mem_256mb"
        assert result.ceiling_speedup == 1.354
        assert len(result.combos) == 17  # pre-compound combos fixture

    @pytest.mark.skipif(not FIXTURE_JSON.exists(), reason="No fixture file")
    def test_load_scan_result_function(self):
        result = load_scan_result(FIXTURE_DIR, "query001_multi_i1")
        assert result is not None
        assert result.query_id == "query001_multi_i1"

    def test_load_scan_result_missing(self):
        result = load_scan_result(Path("/tmp/nonexistent"), "nope")
        assert result is None


# ── New: Test EXPLAIN plan extraction helpers ──────────────────────────


class TestCollectScanCounts:
    def test_single_table(self):
        plan = {"Node Type": "Seq Scan", "Relation Name": "orders"}
        assert _collect_scan_counts(plan) == {"orders": 1}

    def test_nested_joins(self):
        plan = {
            "Node Type": "Hash Join",
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "orders"},
                {"Node Type": "Index Scan", "Relation Name": "customer"},
            ],
        }
        assert _collect_scan_counts(plan) == {"orders": 1, "customer": 1}

    def test_redundant_scans(self):
        """Same table scanned multiple times (self-join or UNION branches)."""
        plan = {
            "Node Type": "Append",
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "catalog_sales"},
                {"Node Type": "Seq Scan", "Relation Name": "catalog_sales"},
                {"Node Type": "Index Scan", "Relation Name": "date_dim"},
            ],
        }
        assert _collect_scan_counts(plan) == {"catalog_sales": 2, "date_dim": 1}

    def test_no_scans(self):
        plan = {"Node Type": "Result"}
        assert _collect_scan_counts(plan) == {}

    def test_deeply_nested(self):
        plan = {
            "Node Type": "Aggregate",
            "Plans": [{
                "Node Type": "Hash Join",
                "Plans": [
                    {
                        "Node Type": "Hash Join",
                        "Plans": [
                            {"Node Type": "Seq Scan", "Relation Name": "date_dim"},
                            {"Node Type": "Seq Scan", "Relation Name": "catalog_sales"},
                        ],
                    },
                    {
                        "Node Type": "Append",
                        "Plans": [
                            {"Node Type": "Seq Scan", "Relation Name": "date_dim"},
                            {"Node Type": "Seq Scan", "Relation Name": "date_dim"},
                        ],
                    },
                ],
            }],
        }
        counts = _collect_scan_counts(plan)
        assert counts["date_dim"] == 3
        assert counts["catalog_sales"] == 1


class TestCollectPredicatePlacement:
    def test_index_cond_early(self):
        plan = {
            "Node Type": "Index Scan",
            "Relation Name": "date_dim",
            "Index Cond": "(d_year = 2000)",
        }
        placements = _collect_predicate_placement(plan)
        assert len(placements) == 1
        assert placements[0]["placement"] == "EARLY"
        assert placements[0]["predicate"] == "(d_year = 2000)"

    def test_filter_late(self):
        plan = {
            "Node Type": "Seq Scan",
            "Relation Name": "catalog_sales",
            "Filter": "(cs_wholesale_cost > 50)",
        }
        placements = _collect_predicate_placement(plan)
        assert len(placements) == 1
        assert placements[0]["placement"] == "LATE"

    def test_join_filter_late(self):
        plan = {
            "Node Type": "Nested Loop",
            "Join Filter": "(cs_quantity > inv_quantity)",
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "inventory"},
                {"Node Type": "Seq Scan", "Relation Name": "catalog_sales"},
            ],
        }
        placements = _collect_predicate_placement(plan)
        join_late = [p for p in placements if p["placement"] == "JOIN_LATE"]
        assert len(join_late) == 1
        assert "(cs_quantity > inv_quantity)" in join_late[0]["predicate"]

    def test_hash_cond_equi(self):
        plan = {
            "Node Type": "Hash Join",
            "Hash Cond": "(o.customer_id = c.id)",
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "orders"},
                {"Node Type": "Seq Scan", "Relation Name": "customer"},
            ],
        }
        placements = _collect_predicate_placement(plan)
        equi = [p for p in placements if p["placement"] == "JOIN_EQUI"]
        assert len(equi) == 1

    def test_mixed_placements(self):
        plan = {
            "Node Type": "Hash Join",
            "Hash Cond": "(s.item_sk = i.item_sk)",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "store_sales",
                    "Alias": "s",
                    "Filter": "(ss_quantity > 10)",
                },
                {
                    "Node Type": "Index Scan",
                    "Relation Name": "item",
                    "Alias": "i",
                    "Index Cond": "(i_category = 'Electronics')",
                },
            ],
        }
        placements = _collect_predicate_placement(plan)
        early = [p for p in placements if p["placement"] == "EARLY"]
        late = [p for p in placements if p["placement"] == "LATE"]
        equi = [p for p in placements if p["placement"] == "JOIN_EQUI"]
        assert len(early) == 1
        assert len(late) == 1
        assert len(equi) == 1


class TestFindBottleneckJoins:
    def test_single_join(self):
        plan = {
            "Node Type": "Hash Join",
            "Total Cost": 5000,
            "Plan Rows": 100,
            "Join Type": "Inner",
            "Hash Cond": "(a.id = b.id)",
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "orders", "Plan Rows": 10000},
                {"Node Type": "Seq Scan", "Relation Name": "customer", "Plan Rows": 500},
            ],
        }
        joins = _find_bottleneck_joins(plan)
        assert len(joins) == 1
        assert joins[0]["join_type"] == "Hash Join"
        assert joins[0]["left_rows"] == 10000
        assert joins[0]["right_rows"] == 500
        assert not joins[0]["is_non_equi"]

    def test_non_equi_join(self):
        plan = {
            "Node Type": "Nested Loop",
            "Total Cost": 8000,
            "Plan Rows": 50,
            "Join Type": "Inner",
            "Join Filter": "(cs_quantity > inv_quantity)",
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "inventory", "Plan Rows": 170000},
                {"Node Type": "Seq Scan", "Relation Name": "catalog_sales", "Plan Rows": 11000000},
            ],
        }
        joins = _find_bottleneck_joins(plan)
        assert len(joins) == 1
        assert joins[0]["is_non_equi"]
        assert joins[0]["join_type"] == "Nested Loop"

    def test_sorted_by_cost(self):
        plan = {
            "Node Type": "Hash Join",
            "Total Cost": 9000,
            "Plan Rows": 50,
            "Hash Cond": "(a = b)",
            "Plans": [
                {
                    "Node Type": "Nested Loop",
                    "Total Cost": 5000,
                    "Plan Rows": 100,
                    "Join Filter": "(x > y)",
                    "Plans": [
                        {"Node Type": "Seq Scan", "Relation Name": "t1", "Plan Rows": 1000},
                        {"Node Type": "Seq Scan", "Relation Name": "t2", "Plan Rows": 2000},
                    ],
                },
                {"Node Type": "Seq Scan", "Relation Name": "t3", "Plan Rows": 300},
            ],
        }
        joins = _find_bottleneck_joins(plan)
        assert len(joins) == 2
        # Highest cost first
        assert joins[0]["total_cost"] == 9000
        assert joins[1]["total_cost"] == 5000


class TestAssessConfidence:
    def test_high_confidence(self):
        level, detail = _assess_confidence(5000.0, 1.5)
        assert level == "HIGH"

    def test_medium_confidence(self):
        level, detail = _assess_confidence(300.0, 1.2)
        assert level == "MEDIUM"

    def test_low_confidence_fast_baseline(self):
        level, detail = _assess_confidence(30.0, 1.3)
        assert level == "LOW"

    def test_low_confidence_noise_floor(self):
        level, detail = _assess_confidence(100.0, 1.05)
        assert level == "LOW"

    def test_zero_baseline(self):
        level, detail = _assess_confidence(0.0, 1.0)
        assert level == "UNKNOWN"


class TestFormatBottleneckJoins:
    def test_nested_loop_bottleneck(self):
        joins = [{
            "join_type": "Nested Loop",
            "join_subtype": "Inner",
            "condition": "(cs_quantity > inv_quantity)",
            "is_non_equi": True,
            "total_cost": 8000,
            "plan_rows": 50,
            "left_rows": 170000,
            "right_rows": 11000000,
            "left_tables": ["inventory"],
            "right_tables": ["catalog_sales"],
            "depth": 0,
        }]
        text = _format_bottleneck_joins(joins)
        assert "BOTTLENECK_JOIN:" in text
        assert "inventory" in text
        assert "catalog_sales" in text
        assert "non-equi" in text
        assert "INPUT_SIZES:" in text
        assert "170K" in text
        assert "11.0M" in text
        assert "REDUCTION_OPPORTUNITY:" in text

    def test_hash_join_bottleneck(self):
        joins = [{
            "join_type": "Hash Join",
            "join_subtype": "Inner",
            "condition": "(a.id = b.id)",
            "is_non_equi": False,
            "total_cost": 5000,
            "plan_rows": 100,
            "left_rows": 10000,
            "right_rows": 500,
            "left_tables": ["orders"],
            "right_tables": ["customer"],
            "depth": 0,
        }]
        text = _format_bottleneck_joins(joins)
        assert "Hash Join" in text
        assert "build side" in text

    def test_empty_list(self):
        assert _format_bottleneck_joins([]) == ""


class TestFormatScanCounts:
    def test_no_redundancy(self):
        text = _format_scan_counts({"orders": 1, "customer": 1})
        assert "SCAN_COUNTS:" in text
        assert "REDUNDANT_SCAN_OPPORTUNITY: none" in text

    def test_redundant_scans(self):
        text = _format_scan_counts({"catalog_sales": 3, "date_dim": 1})
        assert "catalog_sales: 3 scans" in text
        assert "date_dim: 1 scan" in text
        assert "consolidate" in text

    def test_empty(self):
        assert _format_scan_counts({}) == ""


class TestFormatPredicatePlacement:
    def test_late_predicates_shown(self):
        placements = [
            {"predicate": "(cs_cost > 50)", "table": "catalog_sales", "placement": "LATE", "node_type": "Seq Scan"},
            {"predicate": "(d_year = 2000)", "table": "date_dim", "placement": "EARLY", "node_type": "Index Scan"},
        ]
        text = _format_predicate_placement(placements)
        assert "PREDICATE_PLACEMENT:" in text
        assert "LATE" in text
        assert "EARLY" in text
        assert "PUSHDOWN_OPPORTUNITIES:" in text

    def test_no_late_predicates(self):
        placements = [
            {"predicate": "(d_year = 2000)", "table": "date_dim", "placement": "EARLY", "node_type": "Index Scan"},
        ]
        text = _format_predicate_placement(placements)
        assert text == ""  # Nothing actionable

    def test_empty(self):
        assert _format_predicate_placement([]) == ""


class TestComposeStrategy:
    def test_locked_nestloop_high_memory(self):
        text = _compose_strategy(
            join_class="JOINS: LOCKED on nested loops",
            memory_class="HIGH",
            join_detail="LOCKED on nested loops",
            bottleneck_joins=[{
                "join_type": "Nested Loop",
                "total_cost": 8000,
            }],
        )
        assert "STRATEGY:" in text
        assert "LOCKED" in text
        assert "nested loop" in text.lower()
        assert "work_mem" in text

    def test_locked_hash_minor_memory(self):
        text = _compose_strategy(
            join_class="JOINS: LOCKED on hash joins",
            memory_class="MINOR",
            join_detail="LOCKED on hash joins",
            bottleneck_joins=[{
                "join_type": "Hash Join",
                "total_cost": 5000,
            }],
        )
        assert "hash" in text.lower()
        assert "cardinality" in text.lower()

    def test_sensitive_joins(self):
        text = _compose_strategy(
            join_class="JOINS: SENSITIVE — force_hash gives 2.50x",
            memory_class="MINOR",
            join_detail="SENSITIVE",
            bottleneck_joins=[],
        )
        assert "SENSITIVE" in text
        assert "guide optimizer" in text

    def test_stable_joins(self):
        text = _compose_strategy(
            join_class="JOINS: Stable",
            memory_class="MINOR",
            join_detail="Stable",
            bottleneck_joins=[],
        )
        assert "STABLE" in text


# ── Format scan for prompt tests (updated for enriched output) ─────────


class TestFormatScanForPrompt:
    def test_format_with_improvements(self):
        result = ScanResult(
            query_id="q1",
            baseline_ms=1000.0,
            baseline_plan_node="Aggregate",
            baseline_rows=10,
            combos=[
                ComboResult(
                    combo_name="work_mem_256mb",
                    config={"work_mem": "256MB"},
                    set_local_commands=["SET LOCAL work_mem = '256MB'"],
                    time_ms=500.0,
                    speedup=2.0,
                    top_plan_node="Hash Aggregate",
                    row_count=10,
                    rows_match=True,
                ),
                ComboResult(
                    combo_name="no_nestloop",
                    config={"enable_nestloop": "off"},
                    set_local_commands=["SET LOCAL enable_nestloop = 'off'"],
                    time_ms=1200.0,
                    speedup=0.833,
                    top_plan_node="Merge Join",
                    row_count=10,
                    rows_match=True,
                ),
            ],
            ceiling_speedup=2.0,
            ceiling_combo="work_mem_256mb",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "Baseline: 1000ms" in text
        assert "CONFIG_CEILING:" in text
        assert "2.00x" in text
        assert "work_mem" in text
        assert "MEMORY:" in text
        assert "CONFIDENCE:" in text
        assert "STRATEGY:" in text

    def test_format_all_neutral(self):
        result = ScanResult(
            query_id="q2",
            baseline_ms=100.0,
            baseline_plan_node="Seq Scan",
            baseline_rows=5,
            combos=[
                ComboResult(
                    combo_name="no_jit",
                    config={"jit": "off"},
                    set_local_commands=["SET LOCAL jit = 'off'"],
                    time_ms=102.0,
                    speedup=0.98,
                    top_plan_node="Seq Scan",
                    row_count=5,
                    rows_match=True,
                ),
            ],
            ceiling_speedup=0.98,
            ceiling_combo="baseline",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "Baseline: 100ms" in text
        assert "NONE" in text
        assert "All improvement must come from SQL rewrite" in text

    @pytest.mark.skipif(not FIXTURE_JSON.exists(), reason="No fixture file")
    def test_format_real_fixture(self):
        data = json.loads(FIXTURE_JSON.read_text())
        result = ScanResult.from_dict(data)
        text = format_scan_for_prompt(result)
        assert "Baseline:" in text
        assert "CONFIG_CEILING:" in text
        assert "work_mem_256mb" in text

    def test_format_with_errors_excluded(self):
        """Combos with errors should not appear in the formatted output table."""
        result = ScanResult(
            query_id="q3",
            baseline_ms=200.0,
            baseline_plan_node="Sort",
            baseline_rows=3,
            combos=[
                ComboResult(
                    combo_name="force_hash",
                    config={"enable_nestloop": "off", "enable_mergejoin": "off"},
                    set_local_commands=[],
                    time_ms=100.0,
                    speedup=2.0,
                    top_plan_node="Hash Join",
                    row_count=3,
                    rows_match=True,
                ),
                ComboResult(
                    combo_name="broken",
                    config={"jit": "off"},
                    set_local_commands=[],
                    time_ms=0.0,
                    speedup=999.0,
                    top_plan_node="Error",
                    row_count=0,
                    rows_match=False,
                    error="connection lost",
                ),
            ],
            ceiling_speedup=2.0,
            ceiling_combo="force_hash",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "broken" not in text
        assert "force_hash" in text

    def test_format_locked_nestloop(self):
        """Q080-like: all join alternatives catastrophic → LOCKED on nested loops."""
        result = ScanResult(
            query_id="q80",
            baseline_ms=2500.0,
            baseline_plan_node="Aggregate",
            baseline_rows=100,
            combos=[
                ComboResult(
                    combo_name="no_nestloop",
                    config={"enable_nestloop": "off"},
                    set_local_commands=[], time_ms=25000.0, speedup=0.07,
                    top_plan_node="Hash Join", row_count=100, rows_match=True,
                ),
                ComboResult(
                    combo_name="force_hash",
                    config={"enable_nestloop": "off", "enable_mergejoin": "off"},
                    set_local_commands=[], time_ms=30000.0, speedup=0.06,
                    top_plan_node="Hash Join", row_count=100, rows_match=True,
                ),
                ComboResult(
                    combo_name="force_merge",
                    config={"enable_nestloop": "off", "enable_hashjoin": "off"},
                    set_local_commands=[], time_ms=60000.0, speedup=0.03,
                    top_plan_node="Merge Join", row_count=100, rows_match=True,
                ),
                ComboResult(
                    combo_name="work_mem_256mb",
                    config={"work_mem": "256MB"},
                    set_local_commands=[], time_ms=2400.0, speedup=1.04,
                    top_plan_node="Aggregate", row_count=100, rows_match=True,
                ),
            ],
            ceiling_speedup=1.04,
            ceiling_combo="work_mem_256mb",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "LOCKED" in text
        assert "nested loops" in text
        assert "NONE" in text  # ceiling 1.04 < 1.10 → NONE

    def test_format_locked_with_explore_bottleneck(self):
        """Issue 2: LOCKED joins + explore data → bottleneck sub-signals."""
        result = ScanResult(
            query_id="q72",
            baseline_ms=2500.0,
            baseline_plan_node="Aggregate",
            baseline_rows=100,
            combos=[
                ComboResult(
                    combo_name="no_nestloop",
                    config={"enable_nestloop": "off"},
                    set_local_commands=[], time_ms=25000.0, speedup=0.07,
                    top_plan_node="Hash Join", row_count=100, rows_match=True,
                ),
                ComboResult(
                    combo_name="force_hash",
                    config={"enable_nestloop": "off", "enable_mergejoin": "off"},
                    set_local_commands=[], time_ms=30000.0, speedup=0.06,
                    top_plan_node="Hash Join", row_count=100, rows_match=True,
                ),
                ComboResult(
                    combo_name="work_mem_256mb",
                    config={"work_mem": "256MB"},
                    set_local_commands=[], time_ms=2400.0, speedup=1.04,
                    top_plan_node="Aggregate", row_count=100, rows_match=True,
                ),
            ],
            ceiling_speedup=1.04,
            ceiling_combo="work_mem_256mb",
            scanned_at="2026-02-09T20:00:00",
        )
        explore = {
            "bottleneck_joins": [{
                "join_type": "Nested Loop",
                "join_subtype": "Inner",
                "condition": "(cs_quantity > inv_quantity)",
                "is_non_equi": True,
                "total_cost": 8000,
                "plan_rows": 50,
                "left_rows": 170000,
                "right_rows": 11000000,
                "left_tables": ["inventory"],
                "right_tables": ["catalog_sales"],
                "depth": 0,
            }],
            "scan_counts": {"catalog_sales": 1, "inventory": 1, "date_dim": 3},
            "predicate_placement": [
                {"predicate": "(d_year = 1998)", "table": "d1", "placement": "EARLY", "node_type": "Index Scan"},
                {"predicate": "(cs_wholesale_cost > 50)", "table": "catalog_sales", "placement": "LATE", "node_type": "Seq Scan"},
            ],
        }
        text = format_scan_for_prompt(result, explore=explore)
        assert "LOCKED" in text
        assert "BOTTLENECK_JOIN:" in text
        assert "inventory" in text
        assert "catalog_sales" in text
        assert "non-equi" in text
        assert "SCAN_COUNTS:" in text
        assert "date_dim: 3 scans" in text
        assert "PREDICATE_PLACEMENT:" in text
        assert "PUSHDOWN_OPPORTUNITIES:" in text

    def test_format_join_sensitive(self):
        """High ceiling from join type change → SENSITIVE joins."""
        result = ScanResult(
            query_id="q_join",
            baseline_ms=5000.0,
            baseline_plan_node="Nested Loop",
            baseline_rows=50,
            combos=[
                ComboResult(
                    combo_name="no_nestloop",
                    config={"enable_nestloop": "off"},
                    set_local_commands=[], time_ms=2000.0, speedup=2.50,
                    top_plan_node="Hash Join", row_count=50, rows_match=True,
                ),
                ComboResult(
                    combo_name="force_hash",
                    config={"enable_nestloop": "off", "enable_mergejoin": "off"},
                    set_local_commands=[], time_ms=2000.0, speedup=2.50,
                    top_plan_node="Hash Join", row_count=50, rows_match=True,
                ),
                ComboResult(
                    combo_name="work_mem_256mb",
                    config={"work_mem": "256MB"},
                    set_local_commands=[], time_ms=4900.0, speedup=1.02,
                    top_plan_node="Nested Loop", row_count=50, rows_match=True,
                ),
            ],
            ceiling_speedup=2.50,
            ceiling_combo="no_nestloop",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "SENSITIVE" in text
        assert "2.50x" in text
        assert "HIGH" in text  # ceiling 2.50 >= 1.50

    def test_format_jit_recommendation(self):
        """JIT overhead → recommend turning off."""
        result = ScanResult(
            query_id="q_jit",
            baseline_ms=800.0,
            baseline_plan_node="Aggregate",
            baseline_rows=20,
            combos=[
                ComboResult(
                    combo_name="no_jit",
                    config={"jit": "off"},
                    set_local_commands=[], time_ms=600.0, speedup=1.33,
                    top_plan_node="Aggregate", row_count=20, rows_match=True,
                ),
                ComboResult(
                    combo_name="work_mem_256mb",
                    config={"work_mem": "256MB"},
                    set_local_commands=[], time_ms=780.0, speedup=1.03,
                    top_plan_node="Aggregate", row_count=20, rows_match=True,
                ),
            ],
            ceiling_speedup=1.33,
            ceiling_combo="no_jit",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "JIT:" in text
        assert "1.33x" in text
        assert "CONFIG:" in text
        assert "jit" in text

    def test_format_config_recommendations(self):
        """Multiple config recommendations → CONFIG line with all recs."""
        result = ScanResult(
            query_id="q_cfg",
            baseline_ms=3000.0,
            baseline_plan_node="Hash Join",
            baseline_rows=200,
            combos=[
                ComboResult(
                    combo_name="no_jit",
                    config={"jit": "off"},
                    set_local_commands=[], time_ms=2400.0, speedup=1.25,
                    top_plan_node="Hash Join", row_count=200, rows_match=True,
                ),
                ComboResult(
                    combo_name="work_mem_256mb",
                    config={"work_mem": "256MB"},
                    set_local_commands=[], time_ms=1500.0, speedup=2.00,
                    top_plan_node="Hash Join", row_count=200, rows_match=True,
                ),
                ComboResult(
                    combo_name="max_parallel",
                    config={"max_parallel_workers_per_gather": "8"},
                    set_local_commands=[], time_ms=2100.0, speedup=1.43,
                    top_plan_node="Gather", row_count=200, rows_match=True,
                ),
            ],
            ceiling_speedup=2.0,
            ceiling_combo="work_mem_256mb",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "CONFIG:" in text
        assert "work_mem" in text
        assert "jit" in text
        assert "max_parallel" in text

    def test_format_parallelism_overhead(self):
        """Parallelism hurts → recommend disabling."""
        result = ScanResult(
            query_id="q_par",
            baseline_ms=200.0,
            baseline_plan_node="Index Scan",
            baseline_rows=5,
            combos=[
                ComboResult(
                    combo_name="no_parallel",
                    config={"max_parallel_workers_per_gather": "0"},
                    set_local_commands=[], time_ms=170.0, speedup=1.18,
                    top_plan_node="Index Scan", row_count=5, rows_match=True,
                ),
                ComboResult(
                    combo_name="work_mem_256mb",
                    config={"work_mem": "256MB"},
                    set_local_commands=[], time_ms=198.0, speedup=1.01,
                    top_plan_node="Index Scan", row_count=5, rows_match=True,
                ),
            ],
            ceiling_speedup=1.18,
            ceiling_combo="no_parallel",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "PARALLELISM:" in text
        assert "Overhead" in text

    def test_format_dual_ceiling(self):
        """Issue 1: known SQL ceiling alongside config ceiling."""
        result = ScanResult(
            query_id="q72",
            baseline_ms=2500.0,
            baseline_plan_node="Aggregate",
            baseline_rows=100,
            combos=[
                ComboResult(
                    combo_name="no_jit",
                    config={"jit": "off"},
                    set_local_commands=[], time_ms=1930.0, speedup=1.29,
                    top_plan_node="Aggregate", row_count=100, rows_match=True,
                ),
                ComboResult(
                    combo_name="work_mem_256mb",
                    config={"work_mem": "256MB"},
                    set_local_commands=[], time_ms=2450.0, speedup=1.02,
                    top_plan_node="Aggregate", row_count=100, rows_match=True,
                ),
            ],
            ceiling_speedup=1.29,
            ceiling_combo="no_jit",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(
            result,
            known_sql_ceiling=2.68,
            known_sql_technique="bilateral_input_reduction",
        )
        assert "CONFIG_CEILING: 1.29x" in text
        assert "KNOWN_SQL_CEILING: 2.68x" in text
        assert "bilateral_input_reduction" in text
        assert "TOTAL_HEADROOM: HIGH" in text

    def test_format_low_confidence(self):
        """Issue 5: fast baseline → low confidence flag."""
        result = ScanResult(
            query_id="q_fast",
            baseline_ms=30.0,
            baseline_plan_node="Seq Scan",
            baseline_rows=5,
            combos=[
                ComboResult(
                    combo_name="no_jit",
                    config={"jit": "off"},
                    set_local_commands=[], time_ms=25.0, speedup=1.20,
                    top_plan_node="Seq Scan", row_count=5, rows_match=True,
                ),
            ],
            ceiling_speedup=1.20,
            ceiling_combo="no_jit",
            scanned_at="2026-02-09T20:00:00",
        )
        text = format_scan_for_prompt(result)
        assert "CONFIDENCE: LOW" in text

    @pytest.mark.skipif(not FIXTURE_Q065.exists(), reason="No Q065 fixture")
    def test_format_real_q065(self):
        """Q065 fixture: verify enriched output for real scan data."""
        data = json.loads(FIXTURE_Q065.read_text())
        result = ScanResult.from_dict(data)
        text = format_scan_for_prompt(result)
        assert "Baseline:" in text
        assert "JOINS:" in text
        assert "MEMORY:" in text

    @pytest.mark.skipif(not FIXTURE_Q080.exists(), reason="No Q080 fixture")
    def test_format_real_q080(self):
        """Q080 fixture: should detect LOCKED nested loops."""
        data = json.loads(FIXTURE_Q080.read_text())
        result = ScanResult.from_dict(data)
        text = format_scan_for_prompt(result)
        assert "Baseline:" in text
        assert "JOINS:" in text
        assert "MEMORY:" in text


# ── Format explore for prompt tests ────────────────────────────────────


class TestFormatExploreForPrompt:
    def test_basic_format(self):
        data = {
            "n_distinct_plans": 5,
            "n_plan_changers": 3,
            "baseline_joins": ["Hash Join(Inner)", "Nested Loop(Inner)"],
            "vulnerabilities": [
                {"type": "JOIN_TYPE_TRAP", "combos": ["a", "b"], "detail": ["a: X → Y"]},
            ],
            "plan_changers": ["no_nestloop", "force_hash", "max_parallel"],
        }
        text = format_explore_for_prompt(data)
        assert "MODERATE" in text
        assert "Plan changers:" in text

    def test_enriched_with_scan_counts(self):
        data = {
            "n_distinct_plans": 3,
            "n_plan_changers": 2,
            "baseline_joins": [],
            "vulnerabilities": [],
            "plan_changers": [],
            "scan_counts": {"catalog_sales": 3, "date_dim": 1},
            "predicate_placement": [
                {"predicate": "(cs_cost > 50)", "table": "cs", "placement": "LATE", "node_type": "Seq Scan"},
            ],
            "bottleneck_joins": [{
                "join_type": "Hash Join",
                "join_subtype": "Inner",
                "condition": "(a = b)",
                "is_non_equi": False,
                "total_cost": 5000,
                "plan_rows": 100,
                "left_rows": 10000,
                "right_rows": 500,
                "left_tables": ["orders"],
                "right_tables": ["customer"],
                "depth": 0,
            }],
        }
        text = format_explore_for_prompt(data)
        assert "SCAN_COUNTS:" in text
        assert "catalog_sales: 3 scans" in text
        assert "BOTTLENECK_JOIN:" in text
        assert "PREDICATE_PLACEMENT:" in text

    @pytest.mark.skipif(not FIXTURE_EXPLORE.exists(), reason="No explore fixture")
    def test_format_real_explore(self):
        data = json.loads(FIXTURE_EXPLORE.read_text())
        text = format_explore_for_prompt(data)
        assert "Plan diversity:" in text


class TestLoadKnownSqlCeiling:
    def test_missing_file(self):
        assert load_known_sql_ceiling(Path("/tmp/nonexistent"), "q1") is None

    def test_load_from_file(self, tmp_path):
        data = {
            "q072": {"speedup": 2.68, "technique": "bilateral_input_reduction"},
            "q080": {"speedup": 3.32, "technique": "comma_join_conversion"},
        }
        (tmp_path / "known_ceilings.json").write_text(json.dumps(data))
        result = load_known_sql_ceiling(tmp_path, "q072")
        assert result is not None
        assert result[0] == 2.68
        assert result[1] == "bilateral_input_reduction"

    def test_missing_query(self, tmp_path):
        data = {"q080": {"speedup": 3.32, "technique": "comma_join_conversion"}}
        (tmp_path / "known_ceilings.json").write_text(json.dumps(data))
        assert load_known_sql_ceiling(tmp_path, "q999") is None
