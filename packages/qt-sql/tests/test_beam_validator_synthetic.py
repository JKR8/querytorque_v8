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
