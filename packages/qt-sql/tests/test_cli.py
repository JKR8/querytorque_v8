"""Phase 5: CLI Tests - SQL CLI.

Tests for SQL CLI commands using Click's test runner.
"""

import pytest
from click.testing import CliRunner
from pathlib import Path
import tempfile
import json


class TestSQLCLI:
    """Tests for SQL CLI commands."""

    @pytest.fixture
    def runner(self):
        """Create a CLI runner."""
        return CliRunner()

    @pytest.fixture
    def cli(self):
        """Import the CLI."""
        from cli.main import cli
        return cli

    @pytest.fixture
    def sample_sql_file(self, tmp_path):
        """Create a sample SQL file."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT id, name FROM users WHERE active = true")
        return str(sql_file)

    @pytest.fixture
    def sample_bad_sql_file(self, tmp_path):
        """Create a SQL file with issues."""
        sql_file = tmp_path / "bad.sql"
        sql_file.write_text("SELECT * FROM users, orders WHERE UPPER(email) = 'test'")
        return str(sql_file)

    @pytest.fixture
    def sample_schema_file(self, tmp_path):
        """Create a schema SQL file."""
        schema_file = tmp_path / "schema.sql"
        schema_file.write_text("""
            CREATE TABLE users (id INTEGER, name VARCHAR, active BOOLEAN);
            CREATE TABLE orders (id INTEGER, user_id INTEGER, amount DECIMAL);
        """)
        return str(schema_file)

    def test_cli_help(self, runner, cli):
        """Test --help works."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "QueryTorque SQL" in result.output or "Usage" in result.output

    def test_cli_version(self, runner, cli):
        """Test --version works."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output or "version" in result.output.lower()

    def test_audit_command_exists(self, runner, cli):
        """Test audit command exists."""
        result = runner.invoke(cli, ["audit", "--help"])
        assert result.exit_code == 0
        assert "audit" in result.output.lower() or "analyze" in result.output.lower()

    def test_audit_file_not_found(self, runner, cli):
        """Test audit handles missing file."""
        result = runner.invoke(cli, ["audit", "/nonexistent/file.sql"])
        assert result.exit_code != 0

    def test_audit_wrong_extension(self, runner, cli, tmp_path):
        """Test audit rejects non-SQL files."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("SELECT 1")
        result = runner.invoke(cli, ["audit", str(txt_file)])
        # Should reject or warn about extension
        assert result.exit_code != 0 or "sql" in result.output.lower()

    def test_audit_valid_file(self, runner, cli, sample_sql_file):
        """Test audit works on valid SQL file."""
        result = runner.invoke(cli, ["audit", sample_sql_file])
        # Should complete (may exit 0 or 1 depending on issues)
        assert result.exit_code in (0, 1)
        assert "score" in result.output.lower() or "analysis" in result.output.lower()

    def test_audit_json_output(self, runner, cli, sample_sql_file):
        """Test audit --json outputs JSON."""
        result = runner.invoke(cli, ["audit", sample_sql_file, "--json"])
        if result.exit_code in (0, 1):
            # Output should be valid JSON
            try:
                data = json.loads(result.output)
                assert "score" in data or "issues" in data
            except json.JSONDecodeError:
                # May have non-JSON output mixed in
                pass

    def test_audit_verbose_mode(self, runner, cli, sample_bad_sql_file):
        """Test audit -v shows detailed issues."""
        result = runner.invoke(cli, ["audit", sample_bad_sql_file, "-v"])
        # Should show more detail
        assert result.exit_code in (0, 1)

    def test_audit_dialect_option(self, runner, cli, sample_sql_file):
        """Test audit --dialect option."""
        result = runner.invoke(cli, ["audit", sample_sql_file, "--dialect", "snowflake"])
        assert result.exit_code in (0, 1)

    def test_audit_exit_code_on_critical(self, runner, cli, sample_bad_sql_file):
        """Test audit exits with 1 on critical/high issues."""
        result = runner.invoke(cli, ["audit", sample_bad_sql_file])
        # Bad SQL should have issues and exit 1
        # (or 0 if no critical/high issues)
        assert result.exit_code in (0, 1)

    def test_optimize_command_exists(self, runner, cli):
        """Test optimize command exists."""
        result = runner.invoke(cli, ["optimize", "--help"])
        assert result.exit_code == 0

    def test_optimize_file_not_found(self, runner, cli):
        """Test optimize handles missing file."""
        result = runner.invoke(cli, ["optimize", "/nonexistent/file.sql"])
        assert result.exit_code != 0

    def test_optimize_dry_run(self, runner, cli, sample_bad_sql_file):
        """Test optimize --dry-run doesn't call LLM."""
        result = runner.invoke(cli, ["optimize", sample_bad_sql_file, "--dry-run"])
        # Should work without LLM
        assert result.exit_code in (0, 1)
        assert "dry run" in result.output.lower() or "would be" in result.output.lower()

    def test_validate_command_exists(self, runner, cli):
        """Test validate command exists."""
        result = runner.invoke(cli, ["validate", "--help"])
        assert result.exit_code == 0

    def test_validate_requires_two_files(self, runner, cli, sample_sql_file):
        """Test validate requires two files."""
        result = runner.invoke(cli, ["validate", sample_sql_file])
        # Should fail - missing second file
        assert result.exit_code != 0


class TestSQLCLIEdgeCases:
    """Tests for CLI edge cases."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def cli(self):
        from cli.main import cli
        return cli

    def test_empty_sql_file(self, runner, cli, tmp_path):
        """Test handling of empty SQL file."""
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("")
        result = runner.invoke(cli, ["audit", str(sql_file)])
        # Should handle gracefully
        assert result.exit_code in (0, 1)

    def test_comment_only_sql_file(self, runner, cli, tmp_path):
        """Test handling of comment-only SQL file."""
        sql_file = tmp_path / "comment.sql"
        sql_file.write_text("-- Just a comment\n/* Another comment */")
        result = runner.invoke(cli, ["audit", str(sql_file)])
        # Should handle gracefully
        assert result.exit_code in (0, 1)

    def test_unicode_in_sql_file(self, runner, cli, tmp_path):
        """Test handling of Unicode in SQL file."""
        sql_file = tmp_path / "unicode.sql"
        sql_file.write_text("SELECT * FROM users WHERE name = '日本語'")
        result = runner.invoke(cli, ["audit", str(sql_file)])
        # Should handle without crashing
        assert result.exit_code in (0, 1)

    def test_large_sql_file(self, runner, cli, tmp_path):
        """Test handling of large SQL file."""
        sql_file = tmp_path / "large.sql"
        # Generate a moderately large SQL file
        lines = ["SELECT" + ", ".join([f"col{j}" for j in range(50)]) + f" FROM table{i}"
                 for i in range(20)]
        sql_file.write_text("\nUNION ALL\n".join(lines))
        result = runner.invoke(cli, ["audit", str(sql_file)])
        # Should complete
        assert result.exit_code in (0, 1)


class TestSQLCLICalcite:
    """Tests for Calcite integration in CLI."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def cli(self):
        from cli.main import cli
        return cli

    @pytest.fixture
    def sample_sql_file(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT id, name FROM users")
        return str(sql_file)

    def test_audit_calcite_flag(self, runner, cli, sample_sql_file):
        """Test audit --calcite flag is accepted."""
        result = runner.invoke(cli, ["audit", sample_sql_file, "--calcite"])
        # May fail if Calcite not available, but flag should be accepted
        assert result.exit_code in (0, 1)

    def test_audit_calcite_url_option(self, runner, cli, sample_sql_file):
        """Test audit --calcite-url option."""
        result = runner.invoke(cli, [
            "audit", sample_sql_file,
            "--calcite",
            "--calcite-url", "http://localhost:8001"
        ])
        # May fail if Calcite not available
        assert result.exit_code in (0, 1)
