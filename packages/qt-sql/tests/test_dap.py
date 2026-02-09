"""Tests for DAP (Decomposed Attention Protocol) implementation.

Tests:
1. Format detection (dap vs rewrite_sets vs unknown)
2. DAP payload parsing
3. DAP assembly
4. AST equivalence validation (good and bad rewrites)
5. End-to-end DAP through SQLRewriter.apply_response()
6. Legacy rewrite_sets backward compatibility
7. Logic Tree generation
"""

import json
import pytest

from qt_sql.sql_rewriter import (
    DAPAssembler,
    DAPComponent,
    DAPPayload,
    DAPStatement,
    ASTValidationResult,
    ResponseParser,
    SQLRewriter,
    validate_ast_equivalence,
)


# ── Fixtures ──

ORIGINAL_SQL = (
    "SELECT ss_sold_date_sk, SUM(ss_sales_price) as total "
    "FROM store_sales "
    "WHERE ss_sold_date_sk IN "
    "(SELECT d_date_sk FROM date_dim WHERE d_year = 2001) "
    "GROUP BY ss_sold_date_sk"
)

DAP_PAYLOAD_DICT = {
    "spec_version": "1.0",
    "dialect": "duckdb",
    "rewrite_rules": [
        {
            "id": "R1",
            "type": "pushdown",
            "description": "Push date filter into CTE",
            "applied_to": ["date_filtered"],
        }
    ],
    "statements": [
        {
            "target_table": None,
            "change": "modified",
            "components": {
                "date_filtered": {
                    "type": "cte",
                    "change": "added",
                    "sql": "SELECT d_date_sk FROM date_dim WHERE d_year = 2001",
                    "interfaces": {
                        "outputs": ["d_date_sk"],
                        "consumes": [],
                    },
                },
                "main_query": {
                    "type": "main_query",
                    "change": "modified",
                    "sql": (
                        "SELECT ss_sold_date_sk, SUM(ss_sales_price) as total "
                        "FROM store_sales "
                        "INNER JOIN date_filtered ON ss_sold_date_sk = d_date_sk "
                        "GROUP BY ss_sold_date_sk"
                    ),
                    "interfaces": {
                        "outputs": ["ss_sold_date_sk", "total"],
                        "consumes": ["date_filtered"],
                    },
                },
            },
            "reconstruction_order": ["date_filtered", "main_query"],
            "assembly_template": "WITH date_filtered AS ({date_filtered}) {main_query}",
        }
    ],
    "macros": {},
    "frozen_blocks": [],
    "runtime_config": [],
    "validation_checks": [],
}


class TestFormatDetection:
    def test_detect_dap(self):
        json_str = json.dumps({"spec_version": "1.0", "statements": []})
        assert ResponseParser.detect_format(json_str) == "dap"

    def test_detect_rewrite_sets(self):
        json_str = json.dumps({"rewrite_sets": []})
        assert ResponseParser.detect_format(json_str) == "rewrite_sets"

    def test_detect_unknown(self):
        assert ResponseParser.detect_format("not json") == "unknown"
        assert ResponseParser.detect_format('{"other": 1}') == "unknown"


class TestDAPParsing:
    def test_parse_full_payload(self):
        payload = ResponseParser.parse_dap_payload(json.dumps(DAP_PAYLOAD_DICT))
        assert payload is not None
        assert payload.spec_version == "1.0"
        assert payload.dialect == "duckdb"
        assert len(payload.statements) == 1
        assert len(payload.rewrite_rules) == 1

        stmt = payload.statements[0]
        assert "date_filtered" in stmt.components
        assert "main_query" in stmt.components
        assert stmt.reconstruction_order == ["date_filtered", "main_query"]

    def test_parse_component_details(self):
        payload = ResponseParser.parse_dap_payload(json.dumps(DAP_PAYLOAD_DICT))
        comp = payload.statements[0].components["date_filtered"]
        assert comp.type == "cte"
        assert comp.change == "added"
        assert "d_date_sk" in comp.sql
        assert comp.interfaces["outputs"] == ["d_date_sk"]

    def test_parse_invalid_json(self):
        assert ResponseParser.parse_dap_payload("not json") is None

    def test_parse_non_dap_json(self):
        assert ResponseParser.parse_dap_payload('{"rewrite_sets": []}') is None

    def test_parse_runtime_config(self):
        d = dict(DAP_PAYLOAD_DICT)
        d["runtime_config"] = ["SET LOCAL work_mem = '512MB'"]
        payload = ResponseParser.parse_dap_payload(json.dumps(d))
        assert payload.runtime_config == ["SET LOCAL work_mem = '512MB'"]


class TestDAPAssembler:
    def test_assemble_with_template(self):
        payload = ResponseParser.parse_dap_payload(json.dumps(DAP_PAYLOAD_DICT))
        assembler = DAPAssembler(dialect="duckdb")
        sql = assembler.assemble(ORIGINAL_SQL, payload)
        assert "WITH date_filtered AS" in sql
        assert "INNER JOIN date_filtered" in sql
        assert "main_query" not in sql.lower()

    def test_assemble_fallback_topo_sort(self):
        """Test assembly when no template is provided."""
        d = dict(DAP_PAYLOAD_DICT)
        d["statements"] = [dict(d["statements"][0])]
        d["statements"][0]["assembly_template"] = ""
        payload = ResponseParser.parse_dap_payload(json.dumps(d))
        assembler = DAPAssembler(dialect="duckdb")
        sql = assembler.assemble(ORIGINAL_SQL, payload)
        assert "date_filtered" in sql.lower()
        assert "SELECT" in sql.upper()

    def test_assemble_empty_payload(self):
        payload = DAPPayload(spec_version="1.0", dialect="duckdb")
        assembler = DAPAssembler(dialect="duckdb")
        sql = assembler.assemble(ORIGINAL_SQL, payload)
        assert sql == ORIGINAL_SQL

    def test_macro_expansion(self):
        assembler = DAPAssembler(dialect="duckdb")
        sql = "SELECT * FROM t1 -- [MACRO: addr_map]"
        macros = {
            "addr_map": {
                "sql": "JOIN address_mapping am ON t1.id = am.id",
                "used_in": [],
            }
        }
        expanded = assembler._expand_macros(sql, macros)
        assert "JOIN address_mapping" in expanded
        assert "MACRO" not in expanded


class TestASTValidation:
    def test_good_rewrite_passes(self):
        orig = "SELECT a, b, SUM(c) as total FROM t1 WHERE x = 1 GROUP BY a, b"
        rewr = (
            "WITH filtered AS (SELECT a, b, c FROM t1 WHERE x = 1) "
            "SELECT a, b, SUM(c) as total FROM filtered GROUP BY a, b"
        )
        result = validate_ast_equivalence(orig, rewr, "duckdb")
        assert result.valid
        assert not result.errors

    def test_column_count_mismatch(self):
        orig = "SELECT a, b, SUM(c) as total FROM t1 GROUP BY a, b"
        rewr = "SELECT a, SUM(c) as total FROM t1 GROUP BY a"
        result = validate_ast_equivalence(orig, rewr, "duckdb")
        assert not result.valid
        assert any("Column count" in e for e in result.errors)

    def test_column_name_mismatch(self):
        orig = "SELECT a, b FROM t1"
        rewr = "SELECT a, c FROM t1"
        result = validate_ast_equivalence(orig, rewr, "duckdb")
        assert not result.valid
        assert any("mismatch" in e.lower() for e in result.errors)

    def test_missing_base_table(self):
        orig = "SELECT a FROM t1 JOIN t2 ON t1.id = t2.id"
        rewr = "SELECT a FROM t1"
        result = validate_ast_equivalence(orig, rewr, "duckdb")
        assert not result.valid
        assert any("Missing" in e for e in result.errors)

    def test_parse_error_in_rewritten(self):
        orig = "SELECT a FROM t1"
        rewr = "SELCT a FROM t1"  # typo
        result = validate_ast_equivalence(orig, rewr, "duckdb")
        assert not result.valid


class TestEndToEndDAP:
    def test_apply_response_dap(self):
        llm_response = (
            "## Modified Logic Tree\n```\nQUERY...\n```\n\n"
            "```json\n" + json.dumps(DAP_PAYLOAD_DICT) + "\n```"
        )
        rewriter = SQLRewriter(ORIGINAL_SQL, dialect="duckdb")
        result = rewriter.apply_response(llm_response)
        assert result.success, f"DAP e2e failed: {result.error}"
        assert result.transform == "pushdown"
        assert "date_filtered" in result.optimized_sql.lower()

    def test_apply_response_legacy(self):
        legacy = {
            "rewrite_sets": [
                {
                    "id": "rs_01",
                    "transform": "pushdown",
                    "nodes": {
                        "main_query": (
                            "SELECT ss_sold_date_sk, SUM(ss_sales_price) as total "
                            "FROM store_sales GROUP BY ss_sold_date_sk"
                        )
                    },
                    "node_contracts": {
                        "main_query": ["ss_sold_date_sk", "total"],
                    },
                }
            ]
        }
        llm_response = "```json\n" + json.dumps(legacy) + "\n```"
        rewriter = SQLRewriter(ORIGINAL_SQL, dialect="duckdb")
        result = rewriter.apply_response(llm_response)
        assert result.success, f"Legacy e2e failed: {result.error}"
        assert result.transform == "pushdown"

    def test_apply_response_raw_sql_fallback(self):
        llm_response = (
            "Here is the rewrite:\n\n"
            "```sql\n"
            "SELECT ss_sold_date_sk, SUM(ss_sales_price) as total "
            "FROM store_sales GROUP BY ss_sold_date_sk\n"
            "```"
        )
        rewriter = SQLRewriter(ORIGINAL_SQL, dialect="duckdb")
        result = rewriter.apply_response(llm_response)
        assert result.success


class TestLogicTree:
    def test_build_logic_tree(self):
        from qt_sql.logic_tree import build_logic_tree
        from qt_sql.dag import DagBuilder, CostAnalyzer

        sql = (
            "WITH filtered AS ("
            "  SELECT d_date_sk FROM date_dim WHERE d_year = 2001"
            ") "
            "SELECT ss_sold_date_sk, SUM(ss_sales_price) as total "
            "FROM store_sales "
            "JOIN filtered ON ss_sold_date_sk = d_date_sk "
            "GROUP BY ss_sold_date_sk"
        )
        dag = DagBuilder(sql, dialect="duckdb").build()
        # Build empty costs dict (no EXPLAIN available)
        costs = {}
        tree = build_logic_tree(sql, dag, costs, "duckdb")
        assert "QUERY:" in tree
        assert "[CTE]" in tree
        assert "[MAIN]" in tree
        assert "[=]" in tree
        # Verify box-drawing characters
        assert "├──" in tree or "└──" in tree
