"""In-flight artifact validation for the analyst pipeline.

These checks run after each step of the pipeline to catch problems
immediately rather than discovering them after the full run completes.

Each check function takes the artifact content as a string and the
query context dict, and raises AssertionError if validation fails.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def check_input_sql(sql: str, query_id: str) -> None:
    """Validate step 00: input SQL."""
    assert sql and len(sql) > 10, f"Input SQL too short ({len(sql)} chars)"
    assert "SELECT" in sql.upper(), "Input SQL missing SELECT"
    for line in sql.splitlines():
        assert not line.strip().startswith("--"), f"Comment not stripped: {line}"
    assert not sql.rstrip().endswith(";"), "Trailing semicolon not stripped"
    logger.info(f"[{query_id}] check: 00_input.sql OK")


def check_faiss_examples(examples_json: str, query_id: str) -> None:
    """Validate step 01: FAISS examples."""
    examples = json.loads(examples_json)
    assert isinstance(examples, list), "FAISS examples not a list"
    assert len(examples) >= 1, "No FAISS examples returned"
    for ex in examples:
        assert "id" in ex, f"FAISS example missing 'id': {ex}"
    logger.info(f"[{query_id}] check: 01_faiss_examples.json OK ({len(examples)} examples)")


def check_analyst_prompt(
    prompt: str,
    query_id: str,
    history: Optional[Dict[str, Any]] = None,
) -> None:
    """Validate step 02: analyst prompt."""
    assert f"## Query" in prompt, "Missing query header"
    assert "## Query Structure (Logical Tree)" in prompt, "Missing logical tree section"
    assert "### 1." in prompt, "Missing logical tree node 1"

    # History checks — the critical regression gate
    if history and history.get("attempts"):
        assert "## Previous Optimization Attempts" in prompt, (
            "CRITICAL: Analyst prompt missing history despite history being available. "
            "This will cause the LLM to repeat failed transforms."
        )
        assert "### 4. FAILURE ANALYSIS" in prompt, (
            "CRITICAL: Analyst prompt missing FAILURE ANALYSIS task."
        )
        # Verify at least one attempt's transform name appears
        for attempt in history["attempts"]:
            transforms = attempt.get("transforms", [])
            if transforms:
                assert any(t in prompt for t in transforms), (
                    f"History transform {transforms} not found in analyst prompt"
                )
                break

    assert "### 1. STRUCTURAL BREAKDOWN" in prompt, "Missing methodology step 1"
    assert "### 5. RECOMMENDED STRATEGY" in prompt, "Missing methodology step 5"
    logger.info(f"[{query_id}] check: 02_analyst_prompt.txt OK")


def check_analyst_response(
    response: str,
    query_id: str,
    history: Optional[Dict[str, Any]] = None,
) -> None:
    """Validate step 03: analyst LLM response."""
    assert len(response) > 100, f"Analyst response too short ({len(response)} chars)"
    assert "EXAMPLES:" in response, (
        "Analyst response missing 'EXAMPLES:' line for override parsing"
    )

    # If history has failures, the response should address them
    if history and history.get("attempts"):
        failed = [a for a in history["attempts"]
                  if a.get("speedup", 1.0) < 0.95 or a.get("status") in ("ERROR", "REGRESSION")]
        if failed:
            transforms = []
            for a in failed:
                transforms.extend(a.get("transforms", []))
            if transforms:
                found = any(t in response for t in transforms)
                if not found:
                    logger.warning(
                        f"[{query_id}] Analyst response may not address prior failures: "
                        f"{transforms}"
                    )

    logger.info(f"[{query_id}] check: 03_analyst_response.txt OK")


def check_formatted_analysis(formatted: str, query_id: str) -> None:
    """Validate step 04: formatted analysis."""
    assert "## Expert Analysis" in formatted, "Missing '## Expert Analysis' header"
    assert len(formatted) > 100, f"Formatted analysis too short ({len(formatted)} chars)"
    logger.info(f"[{query_id}] check: 04_analysis_formatted.txt OK")


def check_rewrite_prompt(
    prompt: str,
    query_id: str,
    history: Optional[Dict[str, Any]] = None,
) -> None:
    """Validate step 05: rewrite prompt."""
    assert "SQL query rewrite engine" in prompt, "Missing role/task header"
    assert "## Query Structure (Logical Tree)" in prompt, "Missing logical tree section"

    # History must flow through
    if history and history.get("attempts"):
        assert "## Optimization History" in prompt, (
            "CRITICAL: Rewrite prompt missing Optimization History despite history "
            "being available. The rewriter will not know about prior failures."
        )

    assert "## Constraints" in prompt, "Missing constraints section"
    assert "SEMANTIC_EQUIVALENCE" in prompt, "Missing SEMANTIC_EQUIVALENCE constraint"
    assert "## Output" in prompt, "Missing output format section"
    logger.info(f"[{query_id}] check: 05_rewrite_prompt.txt OK")


def run_all_prompt_checks(
    artifacts: Dict[str, str],
    query_id: str,
    history: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Run all available artifact checks. Returns list of failures (empty = all OK)."""
    failures = []
    checks = [
        ("00_input.sql", lambda c: check_input_sql(c, query_id)),
        ("01_faiss_examples.json", lambda c: check_faiss_examples(c, query_id)),
        ("02_analyst_prompt.txt", lambda c: check_analyst_prompt(c, query_id, history)),
        ("03_analyst_response.txt", lambda c: check_analyst_response(c, query_id, history)),
        ("04_analysis_formatted.txt", lambda c: check_formatted_analysis(c, query_id)),
        ("05_rewrite_prompt.txt", lambda c: check_rewrite_prompt(c, query_id, history)),
    ]
    for name, check_fn in checks:
        content = artifacts.get(name)
        if content is None:
            continue
        try:
            check_fn(content)
        except AssertionError as e:
            failures.append(f"{name}: {e}")
            logger.error(f"[{query_id}] CHECK FAILED {name}: {e}")
    return failures
