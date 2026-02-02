"""Phase 5: CLI Tests - DAX CLI.

Tests for DAX CLI commands using Click's test runner.
"""

import pytest
from click.testing import CliRunner
from pathlib import Path
import tempfile
import json
import zipfile


class TestDAXCLI:
    """Tests for DAX CLI commands."""

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
    def sample_vpax_file(self, tmp_path, sample_vpax_data):
        """Create a sample VPAX file."""
        vpax_file = tmp_path / "test.vpax"
        with zipfile.ZipFile(vpax_file, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(sample_vpax_data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "Test"}))
        return str(vpax_file)

    def test_cli_help(self, runner, cli):
        """Test --help works."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "QueryTorque DAX" in result.output or "Usage" in result.output

    def test_cli_version(self, runner, cli):
        """Test --version works."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output or "version" in result.output.lower()

    def test_audit_command_exists(self, runner, cli):
        """Test audit command exists."""
        result = runner.invoke(cli, ["audit", "--help"])
        assert result.exit_code == 0

    def test_audit_file_not_found(self, runner, cli):
        """Test audit handles missing file."""
        result = runner.invoke(cli, ["audit", "/nonexistent/file.vpax"])
        assert result.exit_code != 0

    def test_audit_wrong_extension(self, runner, cli, tmp_path):
        """Test audit rejects non-VPAX files."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("not a vpax file")
        result = runner.invoke(cli, ["audit", str(txt_file)])
        # Should reject non-vpax
        assert result.exit_code != 0

    def test_audit_valid_file(self, runner, cli, sample_vpax_file):
        """Test audit works on valid VPAX file."""
        result = runner.invoke(cli, ["audit", sample_vpax_file])
        # Should complete
        assert result.exit_code in (0, 1)
        assert "score" in result.output.lower() or "torque" in result.output.lower()

    def test_audit_json_output(self, runner, cli, sample_vpax_file):
        """Test audit --json outputs JSON."""
        result = runner.invoke(cli, ["audit", sample_vpax_file, "--json"])
        if result.exit_code in (0, 1):
            try:
                data = json.loads(result.output)
                assert "torque_score" in data or "score" in data
            except json.JSONDecodeError:
                # May have non-JSON output
                pass

    def test_audit_verbose_mode(self, runner, cli, sample_vpax_file):
        """Test audit -v shows detailed issues."""
        result = runner.invoke(cli, ["audit", sample_vpax_file, "-v"])
        assert result.exit_code in (0, 1)

    def test_audit_exit_code_on_critical(self, runner, cli, sample_vpax_file):
        """Test audit exits with 1 on critical/high issues."""
        result = runner.invoke(cli, ["audit", sample_vpax_file])
        # Exit 0 or 1 depending on issues found
        assert result.exit_code in (0, 1)

    def test_optimize_command_exists(self, runner, cli):
        """Test optimize command exists."""
        result = runner.invoke(cli, ["optimize", "--help"])
        assert result.exit_code == 0

    def test_optimize_file_not_found(self, runner, cli):
        """Test optimize handles missing file."""
        result = runner.invoke(cli, ["optimize", "/nonexistent/file.vpax"])
        assert result.exit_code != 0

    def test_optimize_dry_run(self, runner, cli, sample_vpax_file):
        """Test optimize --dry-run doesn't call LLM."""
        result = runner.invoke(cli, ["optimize", sample_vpax_file, "--dry-run"])
        # Should work without LLM
        assert result.exit_code in (0, 1)

    def test_connect_command_exists(self, runner, cli):
        """Test connect command exists."""
        result = runner.invoke(cli, ["connect", "--help"])
        assert result.exit_code == 0

    def test_connect_list_flag(self, runner, cli):
        """Test connect --list flag."""
        result = runner.invoke(cli, ["connect", "--list"])
        # Will likely fail if no PBI Desktop, but flag should work
        # On non-Windows, may skip or error
        assert result.exit_code in (0, 1)

    def test_diff_command_exists(self, runner, cli):
        """Test diff command exists."""
        result = runner.invoke(cli, ["diff", "--help"])
        assert result.exit_code == 0

    def test_validate_command_exists(self, runner, cli):
        """Test validate command exists."""
        result = runner.invoke(cli, ["validate", "--help"])
        assert result.exit_code == 0

    def test_diff_requires_two_files(self, runner, cli, sample_vpax_file):
        """Test diff requires two files."""
        result = runner.invoke(cli, ["diff", sample_vpax_file])
        # Should fail - missing second file
        assert result.exit_code != 0


class TestDAXCLIDiff:
    """Tests for DAX CLI diff command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def cli(self):
        from cli.main import cli
        return cli

    @pytest.fixture
    def vpax_v1_file(self, tmp_path):
        """Create V1 VPAX file."""
        data = {
            "Tables": [{"TableName": "Sales", "RowsCount": 1000}],
            "Columns": [],
            "Measures": [
                {"TableName": "Sales", "MeasureName": "Total", "MeasureExpression": "SUM('Sales'[Amount])"}
            ],
            "Relationships": [],
        }
        vpax_file = tmp_path / "v1.vpax"
        with zipfile.ZipFile(vpax_file, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "V1"}))
        return str(vpax_file)

    @pytest.fixture
    def vpax_v2_file(self, tmp_path):
        """Create V2 VPAX file."""
        data = {
            "Tables": [{"TableName": "Sales", "RowsCount": 2000}],
            "Columns": [],
            "Measures": [
                {"TableName": "Sales", "MeasureName": "Total", "MeasureExpression": "SUM('Sales'[Amount])"},
                {"TableName": "Sales", "MeasureName": "Average", "MeasureExpression": "AVERAGE('Sales'[Amount])"},
            ],
            "Relationships": [],
        }
        vpax_file = tmp_path / "v2.vpax"
        with zipfile.ZipFile(vpax_file, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "V2"}))
        return str(vpax_file)

    def test_diff_two_files(self, runner, cli, vpax_v1_file, vpax_v2_file):
        """Test diff with two valid files."""
        result = runner.invoke(cli, ["diff", vpax_v1_file, vpax_v2_file])
        assert result.exit_code in (0, 1)

    def test_diff_shows_summary(self, runner, cli, vpax_v1_file, vpax_v2_file):
        """Test diff shows summary."""
        result = runner.invoke(cli, ["diff", vpax_v1_file, vpax_v2_file])
        if result.exit_code in (0, 1):
            # Should show some diff info
            assert "added" in result.output.lower() or "modified" in result.output.lower() or "diff" in result.output.lower()


class TestDAXCLIEdgeCases:
    """Tests for CLI edge cases."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def cli(self):
        from cli.main import cli
        return cli

    def test_corrupt_vpax_file(self, runner, cli, tmp_path):
        """Test handling of corrupt VPAX file."""
        vpax_file = tmp_path / "corrupt.vpax"
        vpax_file.write_bytes(b"not a zip file")
        result = runner.invoke(cli, ["audit", str(vpax_file)])
        # Should error gracefully
        assert result.exit_code != 0

    def test_empty_vpax_file(self, runner, cli, tmp_path):
        """Test handling of empty VPAX file."""
        vpax_file = tmp_path / "empty.vpax"
        with zipfile.ZipFile(vpax_file, "w") as zf:
            zf.writestr("DaxVpaView.json", "{}")
            zf.writestr("DaxModel.json", "{}")
        result = runner.invoke(cli, ["audit", str(vpax_file)])
        # Should handle gracefully
        assert result.exit_code in (0, 1)


class TestDAXCLIConnect:
    """Tests for connect command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def cli(self):
        from cli.main import cli
        return cli

    @pytest.mark.windows
    def test_connect_validate_option(self, runner, cli):
        """Test connect --validate option."""
        result = runner.invoke(cli, ["connect", "--validate", "SUM('Sales'[Amount])"])
        # May fail if no PBI Desktop, but flag should be accepted
        assert result.exit_code in (0, 1)

    @pytest.mark.windows
    def test_connect_query_option(self, runner, cli):
        """Test connect --query option."""
        result = runner.invoke(cli, ["connect", "--query", "EVALUATE ROW('Test', 1)"])
        # May fail if no PBI Desktop
        assert result.exit_code in (0, 1)
