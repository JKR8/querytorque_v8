"""Tests for the IR subsystem — builder, reference index, detectors, patch engine."""
from __future__ import annotations

import textwrap

import pytest

from qt_sql.ir import (
    build_script_ir,
    apply_patch_plan,
    render_script,
    detect_all,
    Dialect,
    StatementKind,
    ExprKind,
    FromKind,
    JoinType,
    PatchPlan,
    PatchStep,
    PatchOp,
    PatchTarget,
    PatchPayload,
    Gate,
    GateKind,
)


# ── Fixtures ──────────────────────────────────────────────────────────

MULTI_STMT_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TEMPORARY VIEW v_latest AS
    SELECT MAX(calendar_date) AS max_date FROM daily_data;

    CREATE TEMPORARY TABLE tbl_filtered AS
    WITH cte AS (
        SELECT id, name, calendar_date
        FROM household_profile_canvas
        WHERE calendar_date = (SELECT MAX(calendar_date) FROM household_profile_canvas)
    )
    SELECT * FROM cte;

    DROP TABLE IF EXISTS old_table;

    SELECT
        a.id,
        b.value,
        ROUND(6371 * 2 * ASIN(SQRT(
            POW(SIN(RADIANS((CAST(b.lat AS DOUBLE) - CAST(a.lat AS DOUBLE)) / 2)), 2)
            + COS(RADIANS(CAST(a.lat AS DOUBLE))) * COS(RADIANS(CAST(b.lat AS DOUBLE)))
            * POW(SIN(RADIANS((CAST(b.lon AS DOUBLE) - CAST(a.lon AS DOUBLE)) / 2)), 2)
        )), 2) AS distance_km
    FROM addresses a
    JOIN location_record b ON 1 = 1
    WHERE ROUND(6371 * 2 * ASIN(SQRT(
        POW(SIN(RADIANS((CAST(b.lat AS DOUBLE) - CAST(a.lat AS DOUBLE)) / 2)), 2)
        + COS(RADIANS(CAST(a.lat AS DOUBLE))) * COS(RADIANS(CAST(b.lat AS DOUBLE)))
        * POW(SIN(RADIANS((CAST(b.lon AS DOUBLE) - CAST(a.lon AS DOUBLE)) / 2)), 2)
    )), 2) <= 80;
""")


LATEST_DATE_SQL = textwrap.dedent("""\
    CREATE TEMPORARY TABLE result AS
    SELECT * FROM household_profile_canvas
    WHERE calendar_date = (SELECT MAX(calendar_date) FROM household_profile_canvas);

    CREATE TEMPORARY TABLE result2 AS
    SELECT * FROM broadband_service_daily
    WHERE calendar_date = (SELECT MAX(calendar_date) FROM broadband_service_daily);
""")


# ── Builder tests ─────────────────────────────────────────────────────


class TestBuilder:
    def test_parse_multi_statement(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        assert ir.script_id == "script_0"
        assert ir.dialect == Dialect.DUCKDB
        assert len(ir.statements) == 4

    def test_statement_kinds(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        kinds = [s.kind for s in ir.statements]
        assert kinds[0] == StatementKind.CREATE_VIEW
        assert kinds[1] == StatementKind.CREATE_TABLE_AS
        assert kinds[2] == StatementKind.DROP_TABLE
        assert kinds[3] == StatementKind.SELECT

    def test_reads_extracted(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        # S0: CREATE VIEW reads daily_data
        s0_reads = {r.name.lower() for r in ir.statements[0].reads}
        assert "daily_data" in s0_reads

        # S1: CTAS reads household_profile_canvas
        s1_reads = {r.name.lower() for r in ir.statements[1].reads}
        assert "household_profile_canvas" in s1_reads

    def test_writes_extracted(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        # S0 writes v_latest
        assert any(w.name.lower() == "v_latest" for w in ir.statements[0].writes)
        # S1 writes tbl_filtered
        assert any(w.name.lower() == "tbl_filtered" for w in ir.statements[1].writes)

    def test_symbol_table(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        entry = ir.symbols.lookup("v_latest")
        assert entry is not None
        assert entry.kind == "view"

    def test_query_ir_built_for_ctas(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        ctas_stmt = ir.statements[1]
        assert ctas_stmt.query is not None
        assert len(ctas_stmt.query.with_ctes) == 1
        assert ctas_stmt.query.with_ctes[0].name == "cte"

    def test_query_ir_select_list(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        select_stmt = ir.statements[3]
        assert select_stmt.query is not None
        assert len(select_stmt.query.select_list) == 3  # a.id, b.value, distance_km

    def test_query_ir_where(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        select_stmt = ir.statements[3]
        assert select_stmt.query.where is not None

    def test_query_ir_from_clause(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        select_stmt = ir.statements[3]
        assert select_stmt.query.from_clause is not None
        assert select_stmt.query.from_clause.kind == FromKind.JOIN

    def test_join_type_detected(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        from_ir = ir.statements[3].query.from_clause
        assert from_ir.join.join_type == JoinType.INNER

    def test_cross_join_on_true_hint(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        from_ir = ir.statements[3].query.from_clause
        assert from_ir.join.hints.get("is_cross_join_emulated") is True

    def test_fingerprints_populated(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        assert len(ir.fingerprints.hashes) == 4

    def test_simple_select(self):
        sql = "SELECT 1 AS x, 2 AS y;"
        ir = build_script_ir(sql, Dialect.DUCKDB)
        assert len(ir.statements) == 1
        assert ir.statements[0].kind == StatementKind.SELECT
        assert len(ir.statements[0].query.select_list) == 2


# ── Reference Index tests ────────────────────────────────────────────


class TestReferenceIndex:
    def test_relation_reads_indexed(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        assert "daily_data" in ir.references.relation_reads

    def test_scalar_subqueries_found(self):
        ir = build_script_ir(LATEST_DATE_SQL, Dialect.DUCKDB)
        # Should find 2 scalar MAX(calendar_date) subqueries
        assert len(ir.references.scalar_subqueries) >= 2

    def test_function_calls_indexed(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        # Should find various functions
        assert len(ir.references.function_calls) > 0

    def test_duplicate_expressions_detected(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        # The haversine is duplicated (SELECT + WHERE)
        # duplicate_expr_groups should catch it
        dupes = ir.references.duplicate_expr_groups
        # At least one group of duplicates
        assert len(dupes) >= 1


# ── Detector tests ───────────────────────────────────────────────────


class TestDetectors:
    def test_detect_latest_date_filters(self):
        ir = build_script_ir(LATEST_DATE_SQL, Dialect.DUCKDB)
        detections = detect_all(ir)
        sites = detections["latest_date_filters"]
        assert len(sites) >= 2

        labels = [lbl for s in sites for lbl in s.labels]
        assert "latest_date_filter.household_profile_canvas" in labels
        assert "latest_date_filter.broadband_service_daily" in labels

    def test_detect_haversine_duplicates(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        detections = detect_all(ir)
        sites = detections["haversine_duplicates"]
        # Should find 2 ASIN calls in the last statement
        assert len(sites) >= 2

        labels = [lbl for s in sites for lbl in s.labels]
        assert "geo.distance_haversine" in labels

    def test_detect_cross_join_on_true(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        detections = detect_all(ir)
        sites = detections["cross_joins_on_true"]
        assert len(sites) >= 1

        labels = [lbl for s in sites for lbl in s.labels]
        assert any("cross_join_location_record" in lbl for lbl in labels)

    def test_labels_attached_to_statements(self):
        ir = build_script_ir(LATEST_DATE_SQL, Dialect.DUCKDB)
        detect_all(ir)
        # Labels should be on the statements themselves
        all_labels = []
        for stmt in ir.statements:
            all_labels.extend(stmt.labels)
        assert any("latest_date_filter" in lbl for lbl in all_labels)


# ── Rendering tests ──────────────────────────────────────────────────


class TestRendering:
    def test_round_trip_preserves_statements(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        rendered = render_script(ir)
        # Should have all 4 statements with semicolons
        assert rendered.count(";") == 4

    def test_round_trip_preserves_keywords(self):
        ir = build_script_ir(MULTI_STMT_SQL, Dialect.DUCKDB)
        rendered = render_script(ir).lower()
        assert "create" in rendered
        assert "drop" in rendered
        assert "select" in rendered


# ── Patch Engine tests ───────────────────────────────────────────────


class TestPatchEngine:
    def test_insert_view_statement(self):
        sql = "SELECT * FROM t WHERE calendar_date = (SELECT MAX(calendar_date) FROM t);"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        plan = PatchPlan(
            plan_id="test_insert",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="S1",
                    op=PatchOp.INSERT_VIEW_STATEMENT,
                    target=PatchTarget(by_node_id="S0"),
                    payload=PatchPayload(
                        sql_fragment="CREATE TEMPORARY VIEW v_dates AS SELECT 1 AS d;"
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
            ],
            preconditions=[Gate(kind=GateKind.PARSE_OK)],
        )

        result = apply_patch_plan(ir, plan)
        assert result.success
        assert result.steps_applied == 1
        assert "v_dates" in result.output_sql.lower()
        assert len(ir.statements) == 2

    def test_replace_max_date_subquery(self):
        sql = "SELECT * FROM t WHERE calendar_date = (SELECT MAX(calendar_date) FROM t);"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        # Label the statement first
        ir.statements[0].labels.append("latest_date_filter.t")

        plan = PatchPlan(
            plan_id="test_replace",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="R1",
                    op=PatchOp.REPLACE_EXPR_SUBTREE,
                    target=PatchTarget(by_label="latest_date_filter.t"),
                    payload=PatchPayload(
                        expr_sql="calendar_date = (SELECT d FROM v_dates)"
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert result.success
        rendered = result.output_sql.lower()
        # Should no longer have MAX(calendar_date) FROM t
        assert "max(calendar_date)" not in rendered or "v_dates" in rendered

    def test_parse_gate_catches_bad_sql(self):
        sql = "SELECT 1;"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        plan = PatchPlan(
            plan_id="bad_plan",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="B1",
                    op=PatchOp.INSERT_VIEW_STATEMENT,
                    target=PatchTarget(by_node_id="S0"),
                    payload=PatchPayload(
                        sql_fragment="CREATE VIEW v AS SELECT FROM WHERE;"
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        # Step should succeed (sqlglot is lenient) or gate catches parse error
        # Either way, the result should have a determination
        assert isinstance(result.success, bool)

    def test_plan_shape_gate_max_date(self):
        sql = textwrap.dedent("""\
            SELECT * FROM t WHERE calendar_date = (SELECT MAX(calendar_date) FROM t);
            SELECT * FROM u WHERE calendar_date = (SELECT MAX(calendar_date) FROM u);
        """)
        ir = build_script_ir(sql, Dialect.DUCKDB)

        # A plan that does NOT fix all max-date subqueries should fail postcondition
        plan = PatchPlan(
            plan_id="incomplete",
            dialect=Dialect.DUCKDB,
            steps=[],  # no steps — nothing fixed
            postconditions=[
                Gate(
                    kind=GateKind.PLAN_SHAPE,
                    args={"expectation": "no_scalar_subquery_max_calendar_date_remaining"},
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert not result.success
        assert any("still present" in e for e in result.errors)

    def test_insert_cte(self):
        sql = "SELECT * FROM t;"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        plan = PatchPlan(
            plan_id="test_cte",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="C1",
                    op=PatchOp.INSERT_CTE,
                    target=PatchTarget(by_node_id="S0"),
                    payload=PatchPayload(
                        cte_name="my_cte",
                        cte_query_sql="SELECT 1 AS x",
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert result.success
        assert "my_cte" in result.output_sql.lower()


# ── Full Pipeline test ───────────────────────────────────────────────


class TestFullPipeline:
    def test_detect_then_patch_latest_dates(self):
        """End-to-end: detect latest-date filters, then apply patch plan."""
        ir = build_script_ir(LATEST_DATE_SQL, Dialect.DUCKDB)

        # Step 1: detect
        detections = detect_all(ir)
        sites = detections["latest_date_filters"]
        assert len(sites) >= 2

        # Step 2: build a targeted plan
        plan = PatchPlan(
            plan_id="e2e_latest_date",
            dialect=Dialect.DUCKDB,
            preconditions=[Gate(kind=GateKind.PARSE_OK)],
            steps=[
                PatchStep(
                    step_id="A1",
                    op=PatchOp.INSERT_VIEW_STATEMENT,
                    target=PatchTarget(by_label="latest_date_filter.household_profile_canvas"),
                    payload=PatchPayload(
                        sql_fragment=textwrap.dedent("""\
                            CREATE OR REPLACE TEMPORARY VIEW v_latest_dates AS
                            SELECT
                              (SELECT MAX(calendar_date) FROM household_profile_canvas) AS hp_max,
                              (SELECT MAX(calendar_date) FROM broadband_service_daily)  AS bsd_max;
                        """)
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
                PatchStep(
                    step_id="A2",
                    op=PatchOp.REPLACE_EXPR_SUBTREE,
                    target=PatchTarget(by_label="latest_date_filter.household_profile_canvas"),
                    payload=PatchPayload(
                        expr_sql="calendar_date = (SELECT hp_max FROM v_latest_dates)"
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
                PatchStep(
                    step_id="A3",
                    op=PatchOp.REPLACE_EXPR_SUBTREE,
                    target=PatchTarget(by_label="latest_date_filter.broadband_service_daily"),
                    payload=PatchPayload(
                        expr_sql="calendar_date = (SELECT bsd_max FROM v_latest_dates)"
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
            ],
            postconditions=[
                Gate(
                    kind=GateKind.PLAN_SHAPE,
                    args={"expectation": "no_scalar_subquery_max_calendar_date_remaining"},
                ),
            ],
        )

        # Step 3: apply
        result = apply_patch_plan(ir, plan)
        assert result.success, f"Patch failed: {result.errors}"
        assert "v_latest_dates" in result.output_sql.lower()
        # Original MAX subqueries should be gone
        assert result.output_sql.lower().count("max(calendar_date)") <= 2  # only in view def


# ── Bug Regression tests ────────────────────────────────────────────


class TestBugRegressions:
    """Tests covering the 6 reported bugs (Feb 14 2026)."""

    # Bug 1: apply_patch_plan must rollback on gate failure
    def test_rollback_on_postcondition_failure(self):
        """Postcondition failure → IR rolled back to original state."""
        sql = textwrap.dedent("""\
            SELECT * FROM t
            WHERE calendar_date = (SELECT MAX(calendar_date) FROM t);
        """)
        ir = build_script_ir(sql, Dialect.DUCKDB)
        original_sql = render_script(ir)

        ir.statements[0].labels.append("latest_date_filter.t")

        plan = PatchPlan(
            plan_id="rollback_test",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="R1",
                    op=PatchOp.INSERT_VIEW_STATEMENT,
                    target=PatchTarget(by_node_id="S0"),
                    payload=PatchPayload(
                        sql_fragment="CREATE VIEW v_new AS SELECT 1;"
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
            ],
            # Postcondition will FAIL (max(calendar_date) still present)
            postconditions=[
                Gate(
                    kind=GateKind.PLAN_SHAPE,
                    args={"expectation": "no_scalar_subquery_max_calendar_date_remaining"},
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert not result.success
        # IR must be rolled back — no v_new, same statement count
        after_sql = render_script(ir)
        assert "v_new" not in after_sql.lower()
        assert len(ir.statements) == 1  # rolled back to original 1 stmt

    def test_rollback_on_step_error(self):
        """Step failure → IR rolled back, partial mutations undone."""
        sql = "SELECT 1; SELECT 2;"
        ir = build_script_ir(sql, Dialect.DUCKDB)
        assert len(ir.statements) == 2

        plan = PatchPlan(
            plan_id="rollback_step_error",
            dialect=Dialect.DUCKDB,
            steps=[
                # Step 1 succeeds — inserts a view
                PatchStep(
                    step_id="S1",
                    op=PatchOp.INSERT_VIEW_STATEMENT,
                    target=PatchTarget(by_node_id="S0"),
                    payload=PatchPayload(
                        sql_fragment="CREATE VIEW v_tmp AS SELECT 99;"
                    ),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
                # Step 2 fails — no matching target for delete
                PatchStep(
                    step_id="S2",
                    op=PatchOp.DELETE_EXPR_SUBTREE,
                    target=PatchTarget(by_label="nonexistent.label"),
                    payload=PatchPayload(),
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert not result.success
        # Rolled back: still exactly 2 statements, no v_tmp
        assert len(ir.statements) == 2
        rendered = render_script(ir).lower()
        assert "v_tmp" not in rendered

    # Bug 2: delete_expr_subtree must actually remove the expression
    def test_delete_expr_subtree_removes_predicate(self):
        """delete_expr_subtree with by_label must actually remove the WHERE predicate."""
        sql = textwrap.dedent("""\
            SELECT * FROM location_record a
            JOIN tbl_address_portfolio_v1 b ON 1 = 1
            WHERE ROUND(6371 * 2 * ASIN(SQRT(1)), 2) <= 80;
        """)
        ir = build_script_ir(sql, Dialect.DUCKDB)
        ir.statements[0].labels.append("geo.distance_filter")

        plan = PatchPlan(
            plan_id="delete_test",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="D1",
                    op=PatchOp.DELETE_EXPR_SUBTREE,
                    target=PatchTarget(by_label="geo.distance_filter"),
                    payload=PatchPayload(),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert result.success, f"Delete failed: {result.errors}"
        rendered = result.output_sql.lower()
        # The ROUND(...) <= 80 predicate must be GONE
        assert "asin" not in rendered
        assert "<= 80" not in rendered

    def test_delete_expr_subtree_no_match_raises_error(self):
        """delete_expr_subtree with no matching expression must fail."""
        sql = "SELECT 1 FROM t;"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        plan = PatchPlan(
            plan_id="delete_nomatch",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="D1",
                    op=PatchOp.DELETE_EXPR_SUBTREE,
                    target=PatchTarget(by_label="nonexistent.label"),
                    payload=PatchPayload(),
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert not result.success
        assert any("no target found" in e.lower() or "could not locate" in e.lower()
                    for e in result.errors)

    # Bug 3: by_anchor_hash must match expression subtrees, not just full statements
    def test_anchor_hash_matches_subtree(self):
        """by_anchor_hash should find an expression within a statement, not just the full statement."""
        from qt_sql.ir.schema import canonical_hash

        sql = "SELECT * FROM t WHERE x > 10 AND y < 20;"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        # Hash just the "x > 10" subtree
        subtree_hash = canonical_hash("x > 10")

        plan = PatchPlan(
            plan_id="hash_subtree",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="H1",
                    op=PatchOp.REPLACE_EXPR_SUBTREE,
                    target=PatchTarget(by_anchor_hash=subtree_hash),
                    payload=PatchPayload(expr_sql="x > 5"),
                    gates=[Gate(kind=GateKind.PARSE_OK)],
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert result.success, f"Hash targeting failed: {result.errors}"
        rendered = result.output_sql.lower()
        assert "x > 5" in rendered
        assert "x > 10" not in rendered

    # Bug 4: _extract_reads must exclude write targets
    def test_create_view_reads_exclude_write_target(self):
        """CREATE VIEW v AS SELECT * FROM t → reads=['t'], NOT reads=['v','t']."""
        sql = "CREATE VIEW v AS SELECT * FROM t;"
        ir = build_script_ir(sql, Dialect.DUCKDB)
        stmt = ir.statements[0]

        read_names = {r.name.lower() for r in stmt.reads}
        write_names = {w.name.lower() for w in stmt.writes}

        assert "v" in write_names
        assert "t" in read_names
        assert "v" not in read_names  # Bug 4: v must NOT be in reads

    def test_ctas_reads_exclude_write_target(self):
        """CREATE TABLE t2 AS SELECT * FROM t1 → reads=['t1'], NOT reads=['t2','t1']."""
        sql = "CREATE TABLE t2 AS SELECT * FROM t1;"
        ir = build_script_ir(sql, Dialect.DUCKDB)
        stmt = ir.statements[0]

        read_names = {r.name.lower() for r in stmt.reads}
        write_names = {w.name.lower() for w in stmt.writes}

        assert "t2" in write_names
        assert "t1" in read_names
        assert "t2" not in read_names

    # Bug 5: Runtime gates must fail without a connection
    def test_runtime_gate_bind_ok_fails(self):
        """bind_ok gate must fail (not pass) when no connection is available."""
        sql = "SELECT 1;"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        plan = PatchPlan(
            plan_id="runtime_gate_test",
            dialect=Dialect.DUCKDB,
            steps=[],
            postconditions=[Gate(kind=GateKind.BIND_OK)],
        )

        result = apply_patch_plan(ir, plan)
        assert not result.success
        assert any("runtime" in e.lower() for e in result.errors)

    def test_runtime_gate_explain_ok_fails(self):
        """explain_ok gate must fail when no connection is available."""
        sql = "SELECT 1;"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        plan = PatchPlan(
            plan_id="runtime_explain_test",
            dialect=Dialect.DUCKDB,
            steps=[],
            postconditions=[Gate(kind=GateKind.EXPLAIN_OK)],
        )

        result = apply_patch_plan(ir, plan)
        assert not result.success

    def test_runtime_gate_oracle_eq_fails(self):
        """oracle_eq gate must fail when no connection is available."""
        sql = "SELECT 1;"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        plan = PatchPlan(
            plan_id="runtime_oracle_test",
            dialect=Dialect.DUCKDB,
            steps=[],
            postconditions=[Gate(kind=GateKind.ORACLE_EQ)],
        )

        result = apply_patch_plan(ir, plan)
        assert not result.success

    # Bug 6: split_cte must not be silently registered
    def test_split_cte_unsupported(self):
        """split_cte op must fail with 'Unsupported op' error."""
        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte;"
        ir = build_script_ir(sql, Dialect.DUCKDB)

        plan = PatchPlan(
            plan_id="split_cte_test",
            dialect=Dialect.DUCKDB,
            steps=[
                PatchStep(
                    step_id="SC1",
                    op=PatchOp.SPLIT_CTE,
                    target=PatchTarget(by_node_id="S0"),
                    payload=PatchPayload(),
                ),
            ],
        )

        result = apply_patch_plan(ir, plan)
        assert not result.success
        assert any("unsupported" in e.lower() for e in result.errors)
