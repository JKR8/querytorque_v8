"""Validation tests for the V0 Prompt Pack.

Verifies that all 11 rendered prompt samples exist, are non-empty,
and contain expected content markers per the PROMPT_SPEC.
"""

from __future__ import annotations

from pathlib import Path

import pytest

V0_DIR = Path(__file__).resolve().parent.parent / "qt_sql" / "prompts" / "samples" / "V0"

# Expected files with minimum size (bytes) and required content markers
EXPECTED_FILES = {
    "01_oneshot_script_everyhousehold.md": {
        "min_bytes": 5000,
        "markers": ["pipeline optimization", "spec_version"],
    },
    "02_oneshot_query_88.md": {
        "min_bytes": 5000,
        "markers": ["query_88", "store_sales", "spec_version"],
    },
    "03_expert_analyst_query_88.md": {
        "min_bytes": 5000,
        "markers": ["query_88", "store_sales", "WORKER"],
    },
    "04_expert_worker_query_88.md": {
        "min_bytes": 5000,
        "markers": ["spec_version", "store_sales"],
    },
    "05_swarm_analyst_query_88.md": {
        "min_bytes": 5000,
        "markers": ["WORKER 1 BRIEFING", "STRATEGY"],
    },
    "06_fan_out_query_88.md": {
        "min_bytes": 3000,
        "markers": ["query_88", "worker"],
    },
    "07_worker_query_88.md": {
        "min_bytes": 5000,
        "markers": ["spec_version", "store_sales"],
    },
    "08_snipe_analyst_query_88.md": {
        "min_bytes": 5000,
        "markers": ["Worker", "store_sales"],
    },
    "09_sniper_iter1_query_88.md": {
        "min_bytes": 5000,
        "markers": ["spec_version", "store_sales"],
    },
    "10_sniper_iter2_query_88.md": {
        "min_bytes": 5000,
        "markers": ["PREVIOUS SNIPER ATTEMPT", "spec_version"],
    },
    "11_pg_tuner_query_88.md": {
        "min_bytes": 3000,
        "markers": ["SET LOCAL", "work_mem", "params"],
    },
}


class TestV0PromptPack:
    """Validate the V0 prompt pack exists and has correct content."""

    def test_v0_directory_exists(self):
        assert V0_DIR.exists(), f"V0 directory not found at {V0_DIR}"

    @pytest.mark.parametrize("filename,spec", list(EXPECTED_FILES.items()))
    def test_file_exists_and_nonempty(self, filename, spec):
        path = V0_DIR / filename
        assert path.exists(), f"Missing: {filename}"
        size = path.stat().st_size
        assert size >= spec["min_bytes"], (
            f"{filename}: {size} bytes < minimum {spec['min_bytes']}"
        )

    @pytest.mark.parametrize("filename,spec", list(EXPECTED_FILES.items()))
    def test_file_contains_markers(self, filename, spec):
        path = V0_DIR / filename
        if not path.exists():
            pytest.skip(f"{filename} not found")
        content = path.read_text()
        for marker in spec["markers"]:
            assert marker.lower() in content.lower(), (
                f"{filename}: missing expected marker '{marker}'"
            )

    def test_all_11_files_present(self):
        md_files = sorted(f.name for f in V0_DIR.glob("*.md") if f.name != "README.md")
        assert len(md_files) == 11, (
            f"Expected 11 prompt files, found {len(md_files)}: {md_files}"
        )

    def test_examples_loaded(self):
        """Verify tag-matched examples appear (not 0) — regression guard for path fix."""
        path = V0_DIR / "02_oneshot_query_88.md"
        if not path.exists():
            pytest.skip("02_oneshot not found")
        content = path.read_text()
        assert "Top 0 " not in content, (
            "Examples section shows 'Top 0' — example path likely broken"
        )
        # Should have real examples
        assert "Tag-Matched Examples" in content, "Missing tag-matched examples section"

    def test_readme_exists(self):
        readme = V0_DIR / "README.md"
        assert readme.exists(), "V0/README.md not found"
        content = readme.read_text()
        assert "Q88" in content
        assert "everyhousehold" in content

    def test_prompt_spec_exists(self):
        spec = V0_DIR.parent / "PROMPT_SPEC.md"
        assert spec.exists(), "PROMPT_SPEC.md not found"
        content = spec.read_text()
        assert "build_script_oneshot_prompt" in content
        assert "build_analyst_briefing_prompt" in content
        assert "build_worker_prompt" in content
        assert "build_sniper_prompt" in content
        assert "build_pg_tuner_prompt" in content
