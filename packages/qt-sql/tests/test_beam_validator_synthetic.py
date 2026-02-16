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
