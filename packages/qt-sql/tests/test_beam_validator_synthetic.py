"""Tests for synthetic validator integration in beam patch pipeline.

Validates that SyntheticValidator correctly:
- Passes equivalent queries (Gate 3 pass)
- Rejects row count mismatches (Gate 3 fail)
- Rejects execution errors (Gate 3 fail)
- Returns actionable error messages for LLM retry
"""

import sys
from pathlib import Path

import pytest
import duckdb

# Ensure sibling package imports work
QT_SQL_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = QT_SQL_ROOT.parents[1]
QT_SHARED_PATH = REPO_ROOT / "packages" / "qt-shared"
if QT_SHARED_PATH.exists():
    sys.path.insert(0, str(QT_SHARED_PATH))


# ── SyntheticValidator unit tests ───────────────────────────────────────────

class TestSyntheticValidatorPair:
    """Test validate_sql_pair() directly."""

    def _make_validator(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        return SyntheticValidator(reference_db=None, dialect='duckdb')

    def test_identical_queries_match(self):
        """Identical queries should produce matching results."""
        v = self._make_validator()
        result = v.validate_sql_pair(
            original_sql="SELECT 1 AS x, 2 AS y",
            optimized_sql="SELECT 1 AS x, 2 AS y",
        )
        assert result['match'] is True
        assert result['orig_success'] is True
        assert result['opt_success'] is True
        assert result['row_count_match'] is True

    def test_equivalent_rewrite_matches(self):
        """Semantically equivalent rewrite should match."""
        v = self._make_validator()
        original = "SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a"
        optimized = "SELECT 3 AS a UNION ALL SELECT 1 AS a UNION ALL SELECT 2 AS a"
        result = v.validate_sql_pair(
            original_sql=original,
            optimized_sql=optimized,
        )
        assert result['match'] is True
        assert result['orig_rows'] == 3
        assert result['opt_rows'] == 3

    def test_row_count_mismatch_fails(self):
        """Different row counts should fail validation."""
        v = self._make_validator()
        result = v.validate_sql_pair(
            original_sql="SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3",
            optimized_sql="SELECT 1 AS x UNION ALL SELECT 2",
        )
        assert result['match'] is False
        assert result['row_count_match'] is False
        assert "row count mismatch" in result['reason'].lower()

    def test_optimized_execution_error_fails(self):
        """Optimized query that errors should fail with error message."""
        v = self._make_validator()
        result = v.validate_sql_pair(
            original_sql="SELECT 1 AS x",
            optimized_sql="SELECT * FROM nonexistent_table_xyz",
        )
        assert result['match'] is False
        assert result['opt_success'] is False
        assert result['opt_error'] is not None
        assert "optimized" in result['reason'].lower() or "failed" in result['reason'].lower()

    def test_value_mismatch_fails(self):
        """Same row count but different values should fail."""
        v = self._make_validator()
        result = v.validate_sql_pair(
            original_sql="SELECT 1 AS x, 'hello' AS y",
            optimized_sql="SELECT 1 AS x, 'world' AS y",
        )
        assert result['match'] is False
        assert result['row_count_match'] is True
        assert "value mismatch" in result['reason'].lower()

    def test_non_tpcds_schema_query_matches(self):
        """Generic OLTP-style schemas should validate, not just benchmark naming."""
        v = self._make_validator()
        original = """
            SELECT c.customer_id, SUM(o.total_amount) AS total_spend
            FROM customers c
            JOIN orders o ON o.customer_id = c.customer_id
            WHERE c.state IN ('CA', 'TX')
              AND o.order_date BETWEEN '2021-01-01' AND '2021-12-31'
            GROUP BY c.customer_id
        """
        optimized = """
            SELECT c.customer_id, SUM(o.total_amount) AS total_spend
            FROM orders o
            INNER JOIN customers c ON c.customer_id = o.customer_id
            WHERE o.order_date BETWEEN '2021-01-01' AND '2021-12-31'
              AND c.state IN ('CA', 'TX')
            GROUP BY c.customer_id
        """
        result = v.validate_sql_pair(
            original_sql=original,
            optimized_sql=optimized,
        )
        assert result['match'] is True
        assert result['orig_success'] is True
        assert result['opt_success'] is True


class TestTableInferenceHelpers:
    """Table-resolution helpers should be generic and ambiguity-safe."""

    def test_get_table_from_column_generic_prefix(self):
        from qt_sql.validation.synthetic_validator import get_table_from_column

        table = get_table_from_column(
            "order_date",
            {"orders", "line_items", "customers"},
        )
        assert table == "orders"

    def test_get_table_from_column_abbreviation(self):
        from qt_sql.validation.synthetic_validator import get_table_from_column

        table = get_table_from_column(
            "ca_city",
            {"customer_address", "customer"},
        )
        assert table == "customer_address"

    def test_get_table_from_column_ambiguous_returns_none(self):
        from qt_sql.validation.synthetic_validator import get_table_from_column

        table = get_table_from_column(
            "c_id",
            {"customer", "catalog"},
        )
        assert table is None

    def test_find_primary_key_prefers_table_key_and_avoids_fact_fk_guess(self):
        from qt_sql.validation.synthetic_validator import find_primary_key_column

        assert find_primary_key_column(
            "customer",
            ["c_customer_sk", "c_customer_id", "c_current_cdemo_sk"],
        ) == "c_customer_sk"
        # Fact-like table with multiple equally plausible *_sk columns should
        # not force an arbitrary synthetic PK.
        assert find_primary_key_column(
            "store_returns",
            ["sr_item_sk", "sr_customer_sk", "sr_store_sk", "sr_reason_sk"],
        ) is None


class TestTypeSimilarityMapping:
    """Vector-style lexical mapping for type inference fallback."""

    def test_similarity_integer_offset(self):
        from qt_sql.validation.synthetic_validator import infer_type_by_similarity

        inferred, score = infer_type_by_similarity("ca_gmt_offset")
        assert inferred == "INTEGER"
        assert score > 0

    def test_similarity_decimal_rate(self):
        from qt_sql.validation.synthetic_validator import infer_type_by_similarity

        inferred, score = infer_type_by_similarity("gross_margin_rate")
        assert inferred == "DECIMAL(18,2)"
        assert score > 0

    def test_schema_infer_uses_similarity_fallback(self):
        from qt_sql.validation.synthetic_validator import SchemaExtractor

        ex = SchemaExtractor("SELECT 1 AS x")
        # Not covered by strong literal suffix rules; should use similarity fallback.
        assert ex._infer_column_type("w_gmt_offset") == "INTEGER"


class TestSyntheticGenerationRobustness:
    """Low-level generation tests to ensure generic (non-TPCDS) behavior."""

    def test_pk_tracking_for_generic_id_schema(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE customers (customer_id INTEGER, state VARCHAR)")
        schema = {
            "columns": {
                "customer_id": {"type": "INTEGER", "nullable": False},
                "state": {"type": "VARCHAR(2)", "nullable": True},
            },
            "key": "customer_id",
        }

        gen = SyntheticDataGenerator(conn)
        gen.generate_table_data("customers", schema, row_count=20)

        assert "customers" in gen.foreign_key_values
        assert len(gen.foreign_key_values["customers"]) == 20
        assert set(gen.foreign_key_values["customers"]) == set(range(1, 21))

    def test_fk_generation_uses_parent_values_for_id_columns(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE customers (customer_id INTEGER, state VARCHAR)")
        conn.execute("CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, total_amount DECIMAL(10,2))")

        customer_schema = {
            "columns": {
                "customer_id": {"type": "INTEGER", "nullable": False},
                "state": {"type": "VARCHAR(2)", "nullable": True},
            },
            "key": "customer_id",
        }
        orders_schema = {
            "columns": {
                "order_id": {"type": "INTEGER", "nullable": False},
                "customer_id": {"type": "INTEGER", "nullable": False},
                "total_amount": {"type": "DECIMAL(10,2)", "nullable": True},
            },
            "key": "order_id",
        }

        gen = SyntheticDataGenerator(conn)
        gen.generate_table_data("customers", customer_schema, row_count=50)

        # Force child FK selection to prefer this filtered subset.
        gen.filter_matched_values["customers"] = [1, 2, 3]
        gen.generate_table_data(
            "orders",
            orders_schema,
            row_count=200,
            foreign_keys={"customer_id": ("customers", "customer_id")},
        )

        used_fks = {r[0] for r in conn.execute("SELECT DISTINCT customer_id FROM orders").fetchall()}
        assert used_fks.issubset({1, 2, 3})

    def test_sibling_fact_tables_share_fk_anchor_overlap(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE item (i_item_sk INTEGER, i_category VARCHAR)")
        conn.execute("CREATE TABLE store_returns (sr_item_sk INTEGER, sr_qty INTEGER)")
        conn.execute("CREATE TABLE web_returns (wr_item_sk INTEGER, wr_qty INTEGER)")

        item_schema = {
            "columns": {
                "i_item_sk": {"type": "INTEGER", "nullable": False},
                "i_category": {"type": "VARCHAR(20)", "nullable": True},
            },
            "key": "i_item_sk",
        }
        sr_schema = {
            "columns": {
                "sr_item_sk": {"type": "INTEGER", "nullable": False},
                "sr_qty": {"type": "INTEGER", "nullable": True},
            },
            "key": "sr_item_sk",
        }
        wr_schema = {
            "columns": {
                "wr_item_sk": {"type": "INTEGER", "nullable": False},
                "wr_qty": {"type": "INTEGER", "nullable": True},
            },
            "key": "wr_item_sk",
        }

        gen = SyntheticDataGenerator(conn)
        gen.generate_table_data("item", item_schema, row_count=100)
        gen.filter_matched_values["item"] = list(range(1, 41))

        gen.generate_table_data(
            "store_returns",
            sr_schema,
            row_count=80,
            foreign_keys={"sr_item_sk": ("item", "i_item_sk")},
        )
        gen.generate_table_data(
            "web_returns",
            wr_schema,
            row_count=80,
            foreign_keys={"wr_item_sk": ("item", "i_item_sk")},
        )

        sr_items = {r[0] for r in conn.execute("SELECT DISTINCT sr_item_sk FROM store_returns").fetchall()}
        wr_items = {r[0] for r in conn.execute("SELECT DISTINCT wr_item_sk FROM web_returns").fetchall()}
        overlap = sr_items & wr_items

        assert overlap
        assert len(overlap) >= 8
        assert sr_items.issubset(set(range(1, 41)))
        assert wr_items.issubset(set(range(1, 41)))

    def test_temporal_dim_fk_avoids_anchor_lockstep(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        gen = SyntheticDataGenerator(conn)
        gen.filter_matched_values["date_dim"] = [1, 2, 3]

        vals = [
            gen._generate_value(
                "sold_date_sk",
                "INTEGER",
                row_idx=i,
                total_rows=50,
                foreign_keys={"sold_date_sk": ("date_dim", "d_date_sk")},
                table_name="catalog_sales",
            )
            for i in range(3)
        ]

        # Random choices from seeded RNG are stable and should not mirror
        # deterministic row_idx-based anchor cycling [1, 2, 3].
        assert vals != [1, 2, 3]

    def test_non_pk_surrogate_like_columns_are_not_forced_sequential(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        conn.execute(
            "CREATE TABLE store_returns (sr_return_sk INTEGER, sr_reason_sk INTEGER, sr_return_amt DECIMAL(10,2))"
        )
        schema = {
            "columns": {
                "sr_return_sk": {"type": "INTEGER", "nullable": False},
                "sr_reason_sk": {"type": "INTEGER", "nullable": False},
                "sr_return_amt": {"type": "DECIMAL(10,2)", "nullable": True},
            },
            "key": "sr_return_sk",
        }

        gen = SyntheticDataGenerator(conn)
        gen.generate_table_data("store_returns", schema, row_count=120)

        # PK remains dense/sequential.
        pk_vals = [r[0] for r in conn.execute("SELECT sr_return_sk FROM store_returns ORDER BY sr_return_sk").fetchall()]
        assert pk_vals == list(range(1, 121))

        # Non-PK key-like column should have duplicates / varied domain,
        # not a forced 1..N sequence tied to row index.
        distinct_reason = conn.execute("SELECT COUNT(DISTINCT sr_reason_sk) FROM store_returns").fetchone()[0]
        assert distinct_reason < 120

    def test_numeric_date_sk_not_generated_as_date_string(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        gen = SyntheticDataGenerator(conn)
        v = gen._generate_value(
            "c_first_sales_date_sk",
            "INTEGER",
            row_idx=0,
            total_rows=120,
            table_name="customer",
            primary_key_col="c_customer_sk",
        )
        assert isinstance(v, int)

    def test_date_dim_week_seq_is_calendar_consistent(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        gen = SyntheticDataGenerator(conn)
        # First 7 days should map to week 1, day 8 -> week 2.
        vals = [
            gen._generate_value(
                "d_week_seq",
                "INTEGER",
                row_idx=i,
                total_rows=100,
                table_name="date_dim",
            )
            for i in range(8)
        ]
        assert vals[:7] == [1] * 7
        assert vals[7] == 2

    def test_filter_literal_injection_between_and_in(self, monkeypatch):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        gen = SyntheticDataGenerator(conn)
        gen.filter_literal_values = {
            "orders": {
                "order_date": ["BETWEEN:2021-01-01:2021-01-31"],
                "state": ["CA", "TX"],
            }
        }

        # Make injection deterministic (always inject).
        monkeypatch.setattr(gen.random, "random", lambda: 0.0)

        d = gen._generate_value(
            "order_date",
            "DATE",
            row_idx=0,
            total_rows=100,
            table_name="orders",
        )
        s = gen._generate_value(
            "state",
            "VARCHAR(2)",
            row_idx=0,
            total_rows=100,
            table_name="orders",
        )

        assert "2021-01-01" <= d <= "2021-01-31"
        assert s in {"CA", "TX"}

    def test_filter_literal_injection_numeric_between(self, monkeypatch):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        gen = SyntheticDataGenerator(conn)
        gen.filter_literal_values = {
            "orders": {
                "quantity": ["BETWEEN:10:20"],
            }
        }

        monkeypatch.setattr(gen.random, "random", lambda: 0.0)
        q = gen._generate_value(
            "quantity",
            "INTEGER",
            row_idx=0,
            total_rows=100,
            table_name="orders",
        )
        assert 10 <= q <= 20


class TestConstraintGraphPropagation:
    """Query-graph propagation should move predicates across join equalities."""

    def test_filter_values_propagate_across_joined_columns(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
            SELECT *
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.customer_id IN (42, 43)
        """
        tables = {
            "orders": {
                "columns": {
                    "order_id": {"type": "INTEGER", "nullable": False},
                    "customer_id": {"type": "INTEGER", "nullable": False},
                },
                "alias": "o",
                "key": "order_id",
            },
            "customers": {
                "columns": {
                    "customer_id": {"type": "INTEGER", "nullable": False},
                },
                "alias": "c",
                "key": "customer_id",
            },
        }

        extracted = v._extract_filter_values(sql, tables)
        graph = v._build_join_column_graph(sql, tables)
        propagated = v._propagate_filter_values_across_joins(extracted, graph, tables)
        assert "customers" in propagated
        assert "customer_id" in propagated["customers"]
        assert set(propagated["customers"]["customer_id"]) >= {"42", "43"}

    def test_generation_order_respects_fk_dependencies(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        tables = {"a": {"columns": {}}, "b": {"columns": {}}, "c": {"columns": {}}}
        fk_relationships = {
            "b": {"a_id": ("a", "id")},
            "c": {"b_id": ("b", "id")},
        }
        order = v._get_table_generation_order(tables, fk_relationships)
        assert order.index("a") < order.index("b") < order.index("c")

    def test_generation_order_handles_cycles(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        tables = {"a": {"columns": {}}, "b": {"columns": {}}}
        fk_relationships = {
            "a": {"b_id": ("b", "id")},
            "b": {"a_id": ("a", "id")},
        }
        order = v._get_table_generation_order(tables, fk_relationships)
        assert set(order) == {"a", "b"}
        assert len(order) == 2

    def test_reverse_propagates_filtered_child_fk_to_parent(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator, SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE parent (parent_id INTEGER, label VARCHAR)")
        conn.execute("CREATE TABLE child (child_id INTEGER, parent_id INTEGER, state VARCHAR)")
        conn.execute("INSERT INTO parent VALUES (1, 'p1'), (2, 'p2'), (3, 'p3'), (4, 'p4'), (5, 'p5')")
        conn.execute(
            "INSERT INTO child VALUES "
            "(1, 1, 'TX'), (2, 2, 'CA'), (3, 3, 'CA'), (4, 4, 'NY'), (5, 5, 'WA')"
        )

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        v.conn = conn
        gen = SyntheticDataGenerator(conn)
        gen.filter_matched_values["parent"] = [1, 2, 3, 4, 5]

        tables = {
            "parent": {
                "columns": {
                    "parent_id": {"type": "INTEGER", "nullable": False},
                    "label": {"type": "VARCHAR", "nullable": True},
                }
            },
            "child": {
                "columns": {
                    "child_id": {"type": "INTEGER", "nullable": False},
                    "parent_id": {"type": "INTEGER", "nullable": False},
                    "state": {"type": "VARCHAR", "nullable": True},
                }
            },
        }
        fk_relationships = {"child": {"parent_id": ("parent", "parent_id")}}
        filter_values = {"child": {"state": ["CA"]}}

        v._reverse_propagate_parent_key_matches(gen, "child", tables, fk_relationships, filter_values)
        assert gen.filter_matched_values["parent"] == [2, 3]

    def test_extract_filters_maps_simple_cte_alias_back_to_base_column(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
            WITH x AS (
                SELECT sr_reason_sk AS ctr_reason_sk
                FROM store_returns
            )
            SELECT *
            FROM x
            WHERE x.ctr_reason_sk BETWEEN 43 AND 46
        """
        tables = {
            "store_returns": {
                "columns": {
                    "sr_reason_sk": {"type": "INTEGER", "nullable": True},
                }
            }
        }

        filters = v._extract_filter_values(sql, tables)
        assert "store_returns" in filters
        assert filters["store_returns"]["sr_reason_sk"] == ["BETWEEN:43:46"]

    def test_varchar_id_generation_uses_generic_table_prefix(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        gen = SyntheticDataGenerator(conn)
        value = gen._generate_value(
            "external_id",
            "VARCHAR(20)",
            row_idx=7,
            total_rows=100,
            table_name="purchase_orders",
        )
        assert value.startswith("PO")
        assert value.endswith("0000007")

    def test_detect_fk_from_joins_supports_generic_id_columns(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
            SELECT *
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
        """
        tables = {
            "orders": {
                "columns": {
                    "order_id": {"type": "INTEGER", "nullable": False},
                    "customer_id": {"type": "INTEGER", "nullable": False},
                    "total_amount": {"type": "DECIMAL(10,2)", "nullable": True},
                },
                "alias": "o",
                "key": "order_id",
            },
            "customers": {
                "columns": {
                    "customer_id": {"type": "INTEGER", "nullable": False},
                    "state": {"type": "VARCHAR(2)", "nullable": True},
                },
                "alias": "c",
                "key": "customer_id",
            },
        }

        fk = v._detect_fk_from_joins(sql, tables)
        assert "orders" in fk
        assert fk["orders"]["customer_id"] == ("customers", "customer_id")

    def test_detect_foreign_keys_supports_generic_names(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
            SELECT *
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
        """
        tables = {
            "orders": {
                "columns": {
                    "order_id": {"type": "INTEGER", "nullable": False},
                    "customer_id": {"type": "INTEGER", "nullable": False},
                },
                "alias": "o",
                "key": "order_id",
            },
            "customers": {
                "columns": {
                    "customer_id": {"type": "INTEGER", "nullable": False},
                    "state": {"type": "VARCHAR(2)", "nullable": True},
                },
                "alias": "c",
                "key": "customer_id",
            },
        }

        fk = v._detect_foreign_keys(sql, tables)
        assert "orders" in fk
        assert fk["orders"]["customer_id"] == ("customers", "customer_id")


# ── PatchGateValidator integration tests ────────────────────────────────────

class TestPatchGateValidatorSemantics:
    """Test Gate 3 (SEMANTIC_MATCH) via PatchGateValidator."""

    def _make_gate_validator(self):
        from qt_sql.patches.beam_patch_validator import PatchGateValidator
        from unittest.mock import MagicMock
        executor = MagicMock()
        # DSN=None means no reference DB (pure AST-inferred schema)
        return PatchGateValidator(dialect='duckdb', dsn=None, executor=executor)

    def test_gate3_pass_identical(self):
        """Gate 3 should pass for identical queries."""
        gv = self._make_gate_validator()
        gate = gv.validate_semantics(
            original_sql="SELECT 1 AS x, 2 AS y",
            output_sql="SELECT 1 AS x, 2 AS y",
        )
        assert gate.gate_name == "SEMANTIC_MATCH"
        assert gate.passed is True
        assert gate.details.get("validation_type") == "synthetic"

    def test_gate3_fail_row_mismatch(self):
        """Gate 3 should fail for row count mismatch with clear error."""
        gv = self._make_gate_validator()
        gate = gv.validate_semantics(
            original_sql="SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
            output_sql="SELECT 1 UNION ALL SELECT 2",
        )
        assert gate.gate_name == "SEMANTIC_MATCH"
        assert gate.passed is False
        assert gate.error is not None
        assert "row count mismatch" in gate.error.lower()

    def test_gate3_fail_execution_error(self):
        """Gate 3 should fail for execution errors with actionable message."""
        gv = self._make_gate_validator()
        gate = gv.validate_semantics(
            original_sql="SELECT 1 AS x",
            output_sql="SELECT * FROM this_table_does_not_exist",
        )
        assert gate.gate_name == "SEMANTIC_MATCH"
        assert gate.passed is False
        assert gate.error is not None
        # Error should mention the optimized query failed
        assert "optimized failed" in gate.error.lower() or "failed" in gate.error.lower()

    def test_gate3_error_message_actionable(self):
        """Error messages should be descriptive enough for LLM retry."""
        gv = self._make_gate_validator()
        gate = gv.validate_semantics(
            original_sql="SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3",
            output_sql="SELECT 1 AS x UNION ALL SELECT 2",
        )
        # Error should contain specifics about the mismatch
        assert gate.error is not None
        assert len(gate.error) > 10  # Not a generic "failed" message


# ── DSN handling tests ──────────────────────────────────────────────────────

class TestDSNHandling:
    """Test that various DSN schemes don't crash init."""

    def test_duckdb_uri_memory(self):
        """duckdb:///:memory: should not crash PatchGateValidator."""
        from qt_sql.patches.beam_patch_validator import PatchGateValidator
        from unittest.mock import MagicMock
        gv = PatchGateValidator(dialect='duckdb', dsn='duckdb:///:memory:', executor=MagicMock())
        gate = gv.validate_semantics("SELECT 1 AS x", "SELECT 1 AS x")
        assert gate.gate_name == "SEMANTIC_MATCH"
        assert gate.passed is True

    def test_duckdb_uri_with_path(self):
        """duckdb:///some/path should init without crashing (no file needed for synthetic)."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator, SchemaFromDB
        # SchemaFromDB.supports_dsn should accept duckdb:// URIs
        assert SchemaFromDB.supports_dsn('duckdb:///path/to/db.duckdb') is True
        # But SyntheticValidator won't crash even if file doesn't exist — it'll
        # just fail gracefully during SchemaFromDB init. For this test,
        # use None reference_db (pure AST mode).
        v = SyntheticValidator(reference_db=None, dialect='duckdb')
        result = v.validate_sql_pair("SELECT 1 AS x", "SELECT 1 AS x")
        assert result['match'] is True

    def test_unsupported_dsn_skips_schema_extraction(self):
        """Unsupported DSN (e.g. snowflake://) should skip SchemaFromDB, not crash."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator, SchemaFromDB
        assert SchemaFromDB.supports_dsn('snowflake://account/db') is False
        # Should not raise — falls back to pure AST schema inference
        v = SyntheticValidator(reference_db='snowflake://account/db', dialect='snowflake')
        assert v.schema_extractor is None

    def test_none_dsn(self):
        """dsn=None should work (pure synthetic, no reference DB)."""
        from qt_sql.patches.beam_patch_validator import PatchGateValidator
        from unittest.mock import MagicMock
        gv = PatchGateValidator(dialect='duckdb', dsn=None, executor=MagicMock())
        gate = gv.validate_semantics("SELECT 42 AS val", "SELECT 42 AS val")
        assert gate.passed is True

    def test_schema_from_db_supports_dsn(self):
        """Verify supports_dsn for all expected DSN formats."""
        from qt_sql.validation.synthetic_validator import SchemaFromDB
        # Supported: real databases with persistent tables
        assert SchemaFromDB.supports_dsn('postgres://user:pass@host/db') is True
        assert SchemaFromDB.supports_dsn('postgresql://user:pass@host/db') is True
        assert SchemaFromDB.supports_dsn('duckdb:///path.duckdb') is True
        assert SchemaFromDB.supports_dsn('/path/to/file.duckdb') is True
        assert SchemaFromDB.supports_dsn('/path/to/file.db') is True
        # Not supported: in-memory (no tables to introspect)
        assert SchemaFromDB.supports_dsn(':memory:') is False
        assert SchemaFromDB.supports_dsn('duckdb:///:memory:') is False
        # Not supported: unknown schemes
        assert SchemaFromDB.supports_dsn('snowflake://account/db') is False
        assert SchemaFromDB.supports_dsn(None) is False
