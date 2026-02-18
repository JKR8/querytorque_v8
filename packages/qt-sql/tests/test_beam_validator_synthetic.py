"""Tests for synthetic validator integration in beam patch pipeline.

Validates that SyntheticValidator correctly:
- Passes equivalent queries (Gate 3 pass)
- Rejects row count mismatches (Gate 3 fail)
- Rejects execution errors (Gate 3 fail)
- Returns actionable error messages for LLM retry
"""

import json
import os
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
            optimized_sql="SELECT CAST('abc' AS INTEGER) AS x",
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

    def test_validate_sql_pair_merges_schema_from_optimized_query(self):
        """Optimized query may reference extra columns/tables not in original."""
        v = self._make_validator()
        original = "SELECT c_customer_id FROM customer LIMIT 5"
        optimized = """
            SELECT c.c_customer_id
            FROM customer c
            LEFT JOIN customer_demographics cd
              ON c.c_current_cdemo_sk = cd.cd_demo_sk
            LIMIT 5
        """
        result = v.validate_sql_pair(
            original_sql=original,
            optimized_sql=optimized,
            target_rows=200,
        )
        assert result["orig_success"] is True
        assert result["opt_success"] is True
        assert "column" not in (result.get("reason", "").lower())

    def test_select_star_without_reference_schema_is_low_confidence(self):
        """Pure AST mode should fail closed for SELECT * without concrete schema."""
        v = self._make_validator()
        result = v.validate_sql_pair(
            original_sql="SELECT * FROM orders",
            optimized_sql="SELECT orders_sk FROM orders",
        )
        assert result['match'] is False
        assert "low-confidence schema" in result['reason'].lower()


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
        assert find_primary_key_column(
            "store_sales",
            ["ss_item_sk", "ss_sold_date_sk", "ss_ticket_number"],
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

    def test_composite_fk_generation_keeps_parent_key_pairs(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE store_sales (ss_ticket_number INTEGER, ss_item_sk INTEGER)")
        conn.execute("CREATE TABLE store_returns (sr_ticket_number INTEGER, sr_item_sk INTEGER)")

        sales_schema = {
            "columns": {
                "ss_ticket_number": {"type": "INTEGER", "nullable": False},
                "ss_item_sk": {"type": "INTEGER", "nullable": False},
            },
            "key": "ss_item_sk",
        }
        returns_schema = {
            "columns": {
                "sr_ticket_number": {"type": "INTEGER", "nullable": False},
                "sr_item_sk": {"type": "INTEGER", "nullable": False},
            },
            "key": "sr_item_sk",
        }

        gen = SyntheticDataGenerator(conn)
        gen.generate_table_data("store_sales", sales_schema, row_count=500)
        gen.generate_table_data(
            "store_returns",
            returns_schema,
            row_count=300,
            foreign_keys={
                "sr_ticket_number": ("store_sales", "ss_ticket_number"),
                "sr_item_sk": ("store_sales", "ss_item_sk"),
            },
        )

        parent_pairs = {
            (r[0], r[1])
            for r in conn.execute("SELECT DISTINCT ss_ticket_number, ss_item_sk FROM store_sales").fetchall()
        }
        child_pairs = {
            (r[0], r[1])
            for r in conn.execute("SELECT DISTINCT sr_ticket_number, sr_item_sk FROM store_returns").fetchall()
        }

        assert child_pairs
        assert child_pairs.issubset(parent_pairs)

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

    def test_parse_decimal_type_supports_single_and_bare_decimal(self):
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        conn = duckdb.connect(":memory:")
        gen = SyntheticDataGenerator(conn)

        assert gen._parse_decimal_type("DECIMAL(10,2)") == {"precision": 10, "scale": 2}
        assert gen._parse_decimal_type("DECIMAL(10)") == {"precision": 10, "scale": 0}
        assert gen._parse_decimal_type("DECIMAL") == {"precision": 18, "scale": 2}
        assert gen._parse_decimal_type("INTEGER") is None


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

    def test_resolve_ambiguous_columns_does_not_guess_multi_match(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        import sqlglot
        from sqlglot import exp

        sql = """
            SELECT c.customer_id, o.customer_id
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            ORDER BY customer_id
        """
        fixed = SyntheticValidator._resolve_ambiguous_columns(sql)
        parsed = sqlglot.parse_one(fixed)
        order = parsed.find(exp.Order)
        cols = list(order.find_all(exp.Column)) if order else []
        assert cols
        assert cols[0].table in (None, "")

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

    def test_extract_filters_evaluates_constant_arithmetic_expressions(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
            SELECT *
            FROM date_dim d
            WHERE d.d_year = 2002 - 1
              AND d.d_moy BETWEEN 33 * 0.01 AND 53 * 0.01
        """
        tables = {
            "date_dim": {
                "columns": {
                    "d_year": {"type": "INTEGER", "nullable": True},
                    "d_moy": {"type": "INTEGER", "nullable": True},
                },
                "alias": "d",
            }
        }

        filters = v._extract_filter_values(sql, tables)
        assert filters["date_dim"]["d_year"] == ["2001"]
        assert filters["date_dim"]["d_moy"] == ["BETWEEN:0.33:0.53"]

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

    def test_detect_fk_from_joins_supports_ticket_number_pairs(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
            SELECT *
            FROM store_sales ss
            JOIN store_returns sr
              ON ss.ss_ticket_number = sr.sr_ticket_number
             AND ss.ss_item_sk = sr.sr_item_sk
        """
        tables = {
            "store_sales": {
                "columns": {
                    "ss_ticket_number": {"type": "INTEGER", "nullable": False},
                    "ss_item_sk": {"type": "INTEGER", "nullable": False},
                },
                "alias": "ss",
                "key": "ss_item_sk",
            },
            "store_returns": {
                "columns": {
                    "sr_ticket_number": {"type": "INTEGER", "nullable": False},
                    "sr_item_sk": {"type": "INTEGER", "nullable": False},
                },
                "alias": "sr",
                "key": "sr_item_sk",
            },
        }

        fk = v._detect_fk_from_joins(sql, tables)
        assert "store_returns" in fk
        assert fk["store_returns"]["sr_ticket_number"] == ("store_sales", "ss_ticket_number")
        assert fk["store_returns"]["sr_item_sk"] == ("store_sales", "ss_item_sk")

    def test_detect_fk_from_joins_prefers_parent_pk_direction(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
            SELECT *
            FROM store_sales ss
            JOIN item i ON i.i_item_sk = ss.ss_item_sk
        """
        tables = {
            "store_sales": {
                "columns": {
                    "ss_item_sk": {"type": "INTEGER", "nullable": False},
                    "ss_ticket_number": {"type": "INTEGER", "nullable": False},
                    "ss_sold_date_sk": {"type": "INTEGER", "nullable": False},
                },
                "alias": "ss",
            },
            "item": {
                "columns": {
                    "i_item_sk": {"type": "INTEGER", "nullable": False},
                    "i_category": {"type": "VARCHAR", "nullable": True},
                    "i_brand_id": {"type": "INTEGER", "nullable": True},
                },
                "alias": "i",
            },
        }

        fk = v._detect_fk_from_joins(sql, tables)
        assert "store_sales" in fk
        assert fk["store_sales"]["ss_item_sk"] == ("item", "i_item_sk")

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

    def test_llm_repair_loop_can_fix_zero_row_validation(self, tmp_path):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        def repair_stub(_prompt: str) -> str:
            return (
                '{"actions":[{"type":"set_filter_values","table":"customers",'
                '"column":"state","values":["ZZZ_FIX"]}],"note":"Inject values for LIKE predicate"}'
            )

        sql = """
            SELECT c.customer_id
            FROM customers c
            WHERE c.state LIKE 'ZZZ%'
        """
        sql_file = tmp_path / "repair_case.sql"
        sql_file.write_text(sql)

        v = SyntheticValidator(
            reference_db=None,
            dialect="duckdb",
            llm_max_retries=1,
            repair_analyze_fn=repair_stub,
            repair_mode="hybrid",
        )
        result = v.validate(
            str(sql_file),
            target_rows=100,
            min_rows=1,
            max_rows=1000,
        )

        assert result["success"] is True
        assert result["actual_rows"] > 0
        assert result.get("llm_repair_attempts") == 1
        assert result.get("llm_repair_history")

    def test_llm_swarm_prompt_contains_repair_log_context(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        prompts = []

        def repair_stub(prompt: str) -> str:
            prompts.append(prompt)
            return (
                '{"actions":[{"type":"set_row_count","table":"orders","row_count":4000}],'
                '"note":"top up orders"}'
            )

        v = SyntheticValidator(
            reference_db=None,
            dialect="duckdb",
            llm_max_retries=1,
            repair_analyze_fn=repair_stub,
            repair_mode="add_only",
            llm_swarm_size=2,
        )
        tables = {
            "orders": {"columns": {"order_id": {}, "customer_id": {}}},
            "customers": {"columns": {"customer_id": {}}},
        }
        plan = v._request_llm_repair_plan(
            sql="SELECT * FROM orders WHERE customer_id = 1",
            last_error=None,
            actual_rows=0,
            min_rows=10,
            max_rows=1000,
            tables=tables,
            fk_relationships={"orders": {"customer_id": ("customers", "customer_id")}},
            filter_values={"orders": {"customer_id": ["1"]}},
            table_row_counts={"orders": 2000, "customers": 2000},
            generation_order=["customers", "orders"],
            attempt=1,
            repair_log=[
                {
                    "attempt": 1,
                    "source": "llm",
                    "rows_before": 0,
                    "rows_after": 1,
                    "row_delta": 1,
                    "regressed": False,
                    "plan_actions": [{"type": "set_row_count", "table": "orders", "row_count": 2500}],
                }
            ],
        )
        assert plan is not None
        assert len(prompts) == 2
        assert all("REPAIR_LOG=" in p for p in prompts)
        assert all("SWARM_STRATEGY=" in p for p in prompts)

    def test_llm_swarm_variants_are_phase_scoped(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        cov = v._swarm_variants(1, phase="coverage")
        adv = v._swarm_variants(1, phase="adversarial")

        assert cov
        assert adv
        assert all(str(x.get("strategy_id", "")).startswith("coverage_") for x in cov)
        assert all(str(x.get("strategy_id", "")).startswith("adversarial_") for x in adv)

    def test_llm_prompt_includes_role_examples(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        prompts = []

        def repair_stub(prompt: str) -> str:
            prompts.append(prompt)
            return (
                '{"actions":[{"type":"set_row_count","table":"orders","row_count":4000}],'
                '"note":"top up orders"}'
            )

        v = SyntheticValidator(
            reference_db=None,
            dialect="duckdb",
            llm_max_retries=1,
            repair_analyze_fn=repair_stub,
            repair_mode="add_only",
            llm_swarm_size=1,
        )
        tables = {"orders": {"columns": {"order_id": {}, "status": {}}}}

        _ = v._request_llm_repair_plan(
            sql="SELECT order_id FROM orders WHERE status='A'",
            last_error=None,
            actual_rows=2,
            min_rows=10,
            max_rows=1000,
            tables=tables,
            fk_relationships={},
            filter_values={"orders": {"status": ["A"]}},
            table_row_counts={"orders": 2000},
            generation_order=["orders"],
            attempt=1,
            repair_log=[],
            phase="coverage",
        )
        assert prompts
        assert "ROLE=coverage" in prompts[-1]
        assert "Coverage: top up selective fact + filter dimension" in prompts[-1]

        _ = v._request_llm_repair_plan(
            sql="SELECT order_id FROM orders WHERE status='A'",
            last_error=None,
            actual_rows=2,
            min_rows=10,
            max_rows=1000,
            tables=tables,
            fk_relationships={},
            filter_values={"orders": {"status": ["A"]}},
            table_row_counts={"orders": 2000},
            generation_order=["orders"],
            attempt=2,
            repair_log=[],
            phase="adversarial",
        )
        assert "ROLE=adversarial" in prompts[-1]
        assert "Adversarial: add edge-case overlap rows" in prompts[-1]

    def test_llm_swarm_prefers_targeted_plan_over_global_scale(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        def repair_stub(prompt: str) -> str:
            if "coverage_filter_roots" in prompt:
                return (
                    '{"actions":[{"type":"set_row_count","table":"orders","row_count":4500}],'
                    '"note":"target filter root"}'
                )
            if "coverage_filter_plus_bridge" in prompt:
                return (
                    '{"actions":[{"type":"scale_row_counts","multiplier":2.2}],'
                    '"note":"global scale"}'
                )
            return (
                '{"actions":[{"type":"set_row_count","table":"customers","row_count":7000}],'
                '"note":"target previously regressed table"}'
            )

        v = SyntheticValidator(
            reference_db=None,
            dialect="duckdb",
            llm_max_retries=1,
            repair_analyze_fn=repair_stub,
            repair_mode="add_only",
            llm_swarm_size=3,
        )
        tables = {
            "orders": {"columns": {"order_id": {}, "customer_id": {}, "status": {}}},
            "customers": {"columns": {"customer_id": {}, "state": {}}},
        }
        plan = v._request_llm_repair_plan(
            sql="SELECT o.order_id FROM orders o JOIN customers c ON c.customer_id=o.customer_id WHERE o.status='A'",
            last_error=None,
            actual_rows=2,
            min_rows=100,
            max_rows=1000,
            tables=tables,
            fk_relationships={"orders": {"customer_id": ("customers", "customer_id")}},
            filter_values={"orders": {"status": ["A"]}},
            table_row_counts={"orders": 2500, "customers": 2500},
            generation_order=["customers", "orders"],
            attempt=1,
            repair_log=[
                {
                    "attempt": 1,
                    "source": "llm",
                    "rows_before": 20,
                    "rows_after": 10,
                    "row_delta": -10,
                    "regressed": True,
                    "plan_actions": [{"type": "set_row_count", "table": "customers", "row_count": 5000}],
                }
            ],
        )
        assert plan is not None
        assert plan["actions"]
        assert plan["actions"][0]["type"] == "set_row_count"
        assert plan["actions"][0]["table"] == "orders"
        assert plan.get("_swarm", {}).get("chosen_strategy") == "coverage_filter_roots"

    def test_llm_swarm_normalizes_set_row_count_count_alias(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        def repair_stub(_prompt: str) -> str:
            return (
                '{"actions":[{"type":"set_row_count","table":"orders","count":4200}],'
                '"note":"use count alias"}'
            )

        v = SyntheticValidator(
            reference_db=None,
            dialect="duckdb",
            llm_max_retries=1,
            repair_analyze_fn=repair_stub,
            repair_mode="add_only",
            llm_swarm_size=1,
        )
        tables = {"orders": {"columns": {"order_id": {}, "status": {}}}}
        plan = v._request_llm_repair_plan(
            sql="SELECT order_id FROM orders WHERE status='A'",
            last_error=None,
            actual_rows=0,
            min_rows=10,
            max_rows=1000,
            tables=tables,
            fk_relationships={},
            filter_values={"orders": {"status": ["A"]}},
            table_row_counts={"orders": 2000},
            generation_order=["orders"],
            attempt=1,
            repair_log=[],
        )
        assert plan is not None
        assert plan["actions"][0]["type"] == "set_row_count"
        assert plan["actions"][0]["table"] == "orders"
        assert plan["actions"][0]["row_count"] == 4200

    def test_dag_targeted_fallback_topup_limits_scope_and_includes_bridge(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        tables = {
            "store_returns": {"columns": {}},
            "date_dim": {"columns": {}},
            "store": {"columns": {}},
            "customer": {"columns": {}},
            "customer_demographics": {"columns": {}},
        }
        fk_relationships = {
            "store_returns": {
                "sr_returned_date_sk": ("date_dim", "d_date_sk"),
                "sr_store_sk": ("store", "s_store_sk"),
                "sr_customer_sk": ("customer", "c_customer_sk"),
            },
            "customer": {
                "c_current_cdemo_sk": ("customer_demographics", "cd_demo_sk"),
            },
        }
        filter_values = {
            "date_dim": {"d_year": ["2002"]},
            "store": {"s_state": ["TX"]},
        }
        table_row_counts = {
            "store_returns": 6000,
            "date_dim": 3000,
            "store": 2000,
            "customer": 5000,
            "customer_demographics": 3000,
        }

        plan = v._build_fallback_repair_plan(
            actual_rows=1,
            min_rows=100,
            max_rows=1000,
            tables=tables,
            table_row_counts=table_row_counts,
            filter_values=filter_values,
            fk_relationships=fk_relationships,
        )

        assert plan is not None
        actions = [a for a in plan["actions"] if a.get("type") == "set_row_count"]
        assert 1 <= len(actions) <= 2
        action_tables = {a["table"] for a in actions}
        # Must include filter roots and at least one non-root join bridge/fact table.
        assert action_tables & {"date_dim", "store"}
        assert any(t not in {"date_dim", "store"} for t in action_tables)
        assert "scale_row_counts" not in [a.get("type") for a in plan["actions"]]

    def test_dag_targeting_avoids_regressing_table_pair(self):
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        tables = {
            "store_returns": {"columns": {}},
            "date_dim": {"columns": {}},
            "store": {"columns": {}},
            "customer": {"columns": {}},
            "customer_demographics": {"columns": {}},
        }
        fk_relationships = {
            "store_returns": {
                "sr_returned_date_sk": ("date_dim", "d_date_sk"),
                "sr_store_sk": ("store", "s_store_sk"),
                "sr_customer_sk": ("customer", "c_customer_sk"),
            },
            "customer": {
                "c_current_cdemo_sk": ("customer_demographics", "cd_demo_sk"),
            },
        }
        filter_values = {
            "customer": {"c_birth_month": ["2"]},
            "customer_demographics": {"cd_gender": ["F"]},
            "store": {"s_state": ["TX"]},
            "store_returns": {"sr_reason_sk": ["BETWEEN:43:46"]},
            "date_dim": {"d_year": ["2002"]},
        }
        table_row_counts = {
            "store_returns": 12000,
            "date_dim": 1000,
            "store": 1000,
            "customer": 12000,
            "customer_demographics": 1000,
        }

        selected = v._select_targeted_topup_tables(
            tables=tables,
            table_row_counts=table_row_counts,
            filter_values=filter_values,
            fk_relationships=fk_relationships,
            max_tables=2,
            avoid_table_pairs={("customer", "store_returns")},
            table_penalties={"customer": 2.0, "store_returns": 2.0},
        )
        assert len(selected) >= 1
        assert tuple(sorted(selected[:2])) != ("customer", "store_returns")


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


class TestDSB76SyntheticDatasetReadiness:
    """Dataset-level checks for prebuilt DSB76 synthetic DB artifacts."""

    BENCH_DIR = QT_SQL_ROOT / "qt_sql" / "benchmarks" / "postgres_dsb_76"
    MANIFEST_PATH = BENCH_DIR / "manifest.json"
    QUERIES_DIR = BENCH_DIR / "queries"
    DEFAULT_SYNTH_DB = Path("/mnt/d/qt_synth/postgres_dsb_76_synthetic.duckdb")
    DEFAULT_SYNTH_REPORT = Path("/mnt/d/qt_synth/postgres_dsb_76_synthetic.report.json")

    @classmethod
    def _resolve_synth_db(cls) -> Path | None:
        env_path = os.getenv("QT_DSB76_SYNTH_DB", "").strip()
        if env_path:
            p = Path(env_path)
            if p.exists():
                return p

        if cls.DEFAULT_SYNTH_DB.exists():
            return cls.DEFAULT_SYNTH_DB

        report_env = os.getenv("QT_DSB76_SYNTH_REPORT", "").strip()
        report_path = Path(report_env) if report_env else cls.DEFAULT_SYNTH_REPORT
        if report_path.exists():
            try:
                payload = json.loads(report_path.read_text(encoding="utf-8"))
                out_db = payload.get("summary", {}).get("out_db", "")
                if out_db:
                    p = Path(out_db)
                    if p.exists():
                        return p
            except Exception:
                pass
        return None

    @classmethod
    def _query_ids(cls) -> list[str]:
        payload = json.loads(cls.MANIFEST_PATH.read_text(encoding="utf-8"))
        return list(payload["queries"])

    def test_all_queries_return_rows_on_prebuilt_synthetic_db(self):
        """All 76 benchmark queries must return >0 rows on prebuilt synthetic DB."""
        from qt_sql.validation.build_dsb76_synthetic_db import (
            _count_query_rows,
            _read_first_statement,
            _to_duckdb_sql,
        )

        db_path = self._resolve_synth_db()
        if not db_path:
            pytest.skip(
                "No prebuilt synthetic DB found. Set QT_DSB76_SYNTH_DB to a built "
                "postgres_dsb_76 synthetic DuckDB file."
            )

        timeout_s = int(os.getenv("QT_DSB76_SYNTH_QUERY_TIMEOUT_S", "20"))
        conn = duckdb.connect(str(db_path), read_only=True)
        failures: list[str] = []
        try:
            for query_id in self._query_ids():
                sql_path = self.QUERIES_DIR / f"{query_id}.sql"
                if not sql_path.exists():
                    failures.append(f"{query_id}: missing_sql_file")
                    continue

                sql = _read_first_statement(sql_path)
                sql_duckdb = _to_duckdb_sql(sql, "postgres")

                try:
                    row_count = _count_query_rows(conn, sql_duckdb, timeout_s=timeout_s)
                except Exception as e:
                    failures.append(f"{query_id}: execution_error={e}")
                    continue

                if row_count <= 0:
                    failures.append(f"{query_id}: row_count={row_count}")
        finally:
            conn.close()

        assert not failures, (
            "Synthetic dataset readiness failed; expected every query to return rows.\n"
            + "\n".join(failures[:30])
        )

    def test_validator_accepts_original_sql_for_a_real_dsb_query(self):
        """Guardrail: original SQL should pass validate_sql_pair against itself."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator

        query_id = "query001_multi_i1"
        sql = (self.QUERIES_DIR / f"{query_id}.sql").read_text(encoding="utf-8")
        sql = sql.strip().rstrip(";")

        validator = SyntheticValidator(reference_db=None, dialect="postgres")
        result = validator.validate_sql_pair(original_sql=sql, optimized_sql=sql)

        assert result["orig_success"] is True, result
        assert result["opt_success"] is True, result
        assert result["match"] is True, result
        assert result["orig_rows"] > 0, result


# ---------------------------------------------------------------------------
# Phase 2: Predicate-context type inference
# ---------------------------------------------------------------------------

class TestPredicateContextTypeInference:
    """Verify that predicate context (BETWEEN with CAST-DATE, col+INTERVAL, col=DATE)
    overrides name-based heuristic type inference."""

    def test_between_cast_date_overrides_decimal(self):
        """Column used in BETWEEN with CAST(... AS DATE) bounds must be typed DATE."""
        from qt_sql.validation.synthetic_validator import SchemaExtractor, SyntheticValidator
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
        SELECT t.amount
        FROM transactions t
        JOIN calendar c ON t.txn_date = c.cal_date
        WHERE c.cal_date BETWEEN CAST('2020-01-01' AS DATE) AND CAST('2020-12-31' AS DATE)
        """
        extractor = SchemaExtractor(sql)
        tables = extractor.extract_tables()
        v._predicate_type_hints.clear()
        v._extract_filter_values(sql, tables)
        assert ("calendar", "cal_date") in v._predicate_type_hints
        assert v._predicate_type_hints[("calendar", "cal_date")] == "DATE"

    def test_col_plus_interval_gets_date_hint(self):
        """Column in arithmetic with INTERVAL must be inferred as DATE."""
        from qt_sql.validation.synthetic_validator import SchemaExtractor, SyntheticValidator
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
        SELECT *
        FROM orders o
        WHERE o.order_date + INTERVAL '30' DAY > CAST('2021-06-01' AS DATE)
        """
        extractor = SchemaExtractor(sql)
        tables = extractor.extract_tables()
        v._predicate_type_hints.clear()
        v._extract_filter_values(sql, tables)
        assert ("orders", "order_date") in v._predicate_type_hints
        assert v._predicate_type_hints[("orders", "order_date")] == "DATE"

    def test_eq_cast_date_gets_hint(self):
        """col = CAST('...' AS DATE) should produce a DATE type hint."""
        from qt_sql.validation.synthetic_validator import SchemaExtractor, SyntheticValidator
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = """
        SELECT * FROM events e
        WHERE e.event_dt = CAST('2022-03-15' AS DATE)
        """
        extractor = SchemaExtractor(sql)
        tables = extractor.extract_tables()
        v._predicate_type_hints.clear()
        v._extract_filter_values(sql, tables)
        assert ("events", "event_dt") in v._predicate_type_hints
        assert v._predicate_type_hints[("events", "event_dt")] == "DATE"

    def test_type_override_applied_in_validate(self, tmp_path):
        """Full validate() flow should apply predicate type override to tables dict."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = (
            "SELECT dd.d_date, COUNT(*)\n"
            "FROM date_dim dd\n"
            "WHERE dd.d_date BETWEEN CAST('2000-01-01' AS DATE) AND CAST('2000-12-31' AS DATE)\n"
            "GROUP BY dd.d_date"
        )
        sql_file = tmp_path / "date_test.sql"
        sql_file.write_text(sql, encoding="utf-8")
        result = v.validate(str(sql_file), target_rows=10)
        assert result["success"] is True, result


# ---------------------------------------------------------------------------
# Phase 3: Generic (non-TPC-DS) schema tests
# ---------------------------------------------------------------------------

class TestGenericSchemas:
    """Verify the engine works on non-TPC-DS schemas — e-commerce, SaaS, etc."""

    def test_ecommerce_schema_validates(self, tmp_path):
        """Simple e-commerce query with customers, orders, products tables."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        sql = (
            "SELECT c.customer_name, COUNT(o.order_id) AS order_count\n"
            "FROM customers c\n"
            "JOIN orders o ON c.customer_id = o.customer_id\n"
            "WHERE o.order_date >= CAST('2023-01-01' AS DATE)\n"
            "GROUP BY c.customer_name\n"
            "HAVING COUNT(o.order_id) > 1"
        )
        sql_file = tmp_path / "ecommerce.sql"
        sql_file.write_text(sql, encoding="utf-8")
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        result = v.validate(str(sql_file), target_rows=10)
        assert result["success"] is True, result

    def test_saas_analytics_schema(self, tmp_path):
        """SaaS analytics query with subscriptions and usage tables."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        sql = (
            "SELECT s.plan_type, SUM(u.call_count) AS total_calls\n"
            "FROM subscriptions s\n"
            "JOIN usage_logs u ON s.subscription_id = u.subscription_id\n"
            "WHERE u.log_date BETWEEN CAST('2024-01-01' AS DATE) AND CAST('2024-06-30' AS DATE)\n"
            "GROUP BY s.plan_type"
        )
        sql_file = tmp_path / "saas.sql"
        sql_file.write_text(sql, encoding="utf-8")
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        result = v.validate(str(sql_file), target_rows=10)
        assert result["success"] is True, result

    def test_temporal_dimension_detection(self):
        """_is_temporal_dimension correctly identifies temporal dims by column inspection."""
        from qt_sql.validation.synthetic_validator import _is_temporal_dimension
        # Classic date_dim
        assert _is_temporal_dimension("date_dim", {
            "d_date_sk": {"type": "INTEGER"},
            "d_date": {"type": "DATE"},
            "d_year": {"type": "INTEGER"},
            "d_month": {"type": "INTEGER"},
        })
        # Generic calendar table
        assert _is_temporal_dimension("calendar", {
            "cal_id": {"type": "INTEGER"},
            "cal_date": {"type": "DATE"},
            "cal_year": {"type": "INTEGER"},
            "cal_month": {"type": "INTEGER"},
            "cal_quarter": {"type": "INTEGER"},
        })
        # Non-temporal table
        assert not _is_temporal_dimension("products", {
            "product_id": {"type": "INTEGER"},
            "product_name": {"type": "VARCHAR(100)"},
            "price": {"type": "DECIMAL(18,2)"},
        })

    def test_validate_sql_pair_generic_schema(self):
        """validate_sql_pair works on non-TPC-DS schemas."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        original = (
            "SELECT c.city, COUNT(*) AS cnt\n"
            "FROM stores s JOIN cities c ON s.city_id = c.city_id\n"
            "GROUP BY c.city"
        )
        optimized = (
            "SELECT c.city, COUNT(*) AS cnt\n"
            "FROM stores s JOIN cities c ON s.city_id = c.city_id\n"
            "GROUP BY c.city"
        )
        result = v.validate_sql_pair(original_sql=original, optimized_sql=optimized)
        assert result["match"] is True, result


# ---------------------------------------------------------------------------
# Phase 4: Multi-Row Witness Generation
# ---------------------------------------------------------------------------

class TestMultiRowWitness:
    """Verify multi-row witness generation detects equivalence issues."""

    def test_multi_witness_passes_identical_queries(self):
        """Identical queries should pass all witness variants."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = (
            "SELECT p.product_name, COUNT(*) AS cnt\n"
            "FROM products p JOIN sales s ON p.product_id = s.product_id\n"
            "GROUP BY p.product_name"
        )
        result = v.validate_sql_pair(
            original_sql=sql,
            optimized_sql=sql,
            witness_mode="multi",
        )
        assert result["match"] is True, result
        assert "witness_results" in result
        for wr in result["witness_results"]:
            assert wr["match"] is True, wr

    def test_multi_witness_single_mode_no_witnesses(self):
        """In single mode, no witness_results key should be present."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        v = SyntheticValidator(reference_db=None, dialect="duckdb")
        sql = (
            "SELECT p.product_name, COUNT(*) AS cnt\n"
            "FROM products p JOIN sales s ON p.product_id = s.product_id\n"
            "GROUP BY p.product_name"
        )
        result = v.validate_sql_pair(
            original_sql=sql,
            optimized_sql=sql,
            witness_mode="single",
        )
        assert result["match"] is True, result
        assert "witness_results" not in result

    def test_clone_witness_shifts_keys(self):
        """Clone witness should shift surrogate keys while preserving data."""
        import duckdb
        from qt_sql.validation.witness_generator import MultiRowWitnessGenerator

        conn = duckdb.connect(":memory:")
        tables = {
            "items": {
                "columns": {
                    "item_id": {"type": "INTEGER"},
                    "item_name": {"type": "VARCHAR(50)"},
                }
            }
        }
        conn.execute('CREATE TABLE items (item_id INTEGER, item_name VARCHAR(50))')
        conn.execute("INSERT INTO items VALUES (1, 'foo'), (2, 'bar')")

        gen = MultiRowWitnessGenerator(
            conn=conn,
            tables=tables,
            filter_values={},
            fk_relationships={},
            generation_order=["items"],
            table_row_counts={"items": 10},
        )
        # Run clone witness populate
        gen._populate_clone_witness()
        rows = conn.execute("SELECT item_id FROM items ORDER BY item_id").fetchall()
        # All item_ids should be >= 10001 (shifted by +10000)
        assert all(r[0] >= 10001 for r in rows), f"Expected shifted keys, got {rows}"
        conn.close()

    def test_witness_generator_import(self):
        """MultiRowWitnessGenerator is importable from validation package."""
        from qt_sql.validation import MultiRowWitnessGenerator
        assert MultiRowWitnessGenerator is not None


# ---------------------------------------------------------------------------
# Phase 5: Determinism & Regression Tests
# ---------------------------------------------------------------------------

class TestDeterminismInvariants:
    """Verify that same SQL + same config produces identical synthetic data."""

    def test_validate_pair_deterministic(self):
        """Two calls to validate_sql_pair with same input produce same result."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        sql = (
            "SELECT c.city_name, SUM(o.total_amount) AS revenue\n"
            "FROM customers c JOIN orders o ON c.customer_id = o.customer_id\n"
            "GROUP BY c.city_name"
        )
        v1 = SyntheticValidator(reference_db=None, dialect="duckdb")
        r1 = v1.validate_sql_pair(original_sql=sql, optimized_sql=sql, target_rows=50)
        v2 = SyntheticValidator(reference_db=None, dialect="duckdb")
        r2 = v2.validate_sql_pair(original_sql=sql, optimized_sql=sql, target_rows=50)
        assert r1["match"] == r2["match"]
        assert r1["orig_rows"] == r2["orig_rows"]
        assert r1["opt_rows"] == r2["opt_rows"]

    def test_validate_file_deterministic(self, tmp_path):
        """Two calls to validate() with same SQL file produce same row count."""
        from qt_sql.validation.synthetic_validator import SyntheticValidator
        sql = (
            "SELECT p.product_name, COUNT(*) AS cnt\n"
            "FROM products p JOIN sales s ON p.product_id = s.product_id\n"
            "GROUP BY p.product_name"
        )
        sql_file = tmp_path / "determ.sql"
        sql_file.write_text(sql, encoding="utf-8")
        v1 = SyntheticValidator(reference_db=None, dialect="duckdb")
        r1 = v1.validate(str(sql_file), target_rows=50)
        v2 = SyntheticValidator(reference_db=None, dialect="duckdb")
        r2 = v2.validate(str(sql_file), target_rows=50)
        assert r1["success"] == r2["success"]
        assert r1["actual_rows"] == r2["actual_rows"]


class TestKnownBugRegressions:
    """Regression tests for previously fixed bugs."""

    def test_d_date_not_decimal(self):
        """Regression: d_date must be DATE, not DECIMAL (name heuristic bug)."""
        from qt_sql.validation.synthetic_validator import SchemaExtractor
        sql = "SELECT d.d_date FROM date_dim d WHERE d.d_date = CAST('2000-01-01' AS DATE)"
        extractor = SchemaExtractor(sql)
        tables = extractor.extract_tables()
        col_type = tables["date_dim"]["columns"]["d_date"]
        if isinstance(col_type, dict):
            col_type = col_type.get("type", "")
        assert "DATE" in col_type.upper(), f"d_date should be DATE, got {col_type}"

    def test_scope_aware_column_resolution(self):
        """Regression: columns in subqueries resolve to correct scope."""
        from qt_sql.validation.synthetic_validator import SchemaExtractor
        sql = (
            "SELECT a.order_id\n"
            "FROM orders a\n"
            "WHERE a.order_id IN (\n"
            "  SELECT b.order_id FROM returns b WHERE b.return_qty > 0\n"
            ")"
        )
        extractor = SchemaExtractor(sql)
        tables = extractor.extract_tables()
        # Both tables should be present
        assert "orders" in tables
        assert "returns" in tables
        # return_qty belongs to returns, not orders
        assert "return_qty" in tables["returns"]["columns"]

    def test_multi_alias_tracking(self):
        """Regression: same table with different aliases should be one schema entry."""
        from qt_sql.validation.synthetic_validator import SchemaExtractor
        sql = (
            "SELECT a.item_id, b.item_id\n"
            "FROM items a\n"
            "JOIN items b ON a.item_id = b.item_id"
        )
        extractor = SchemaExtractor(sql)
        tables = extractor.extract_tables()
        # items should appear once, not duplicated as 'a' and 'b'
        assert "items" in tables
        assert "a" not in tables
        assert "b" not in tables
